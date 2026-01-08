package target

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
)

// PoolStats contains connection pool statistics
type PoolStats struct {
	MaxConns          int32 // Maximum number of connections
	TotalConns        int32 // Total number of connections
	AcquiredConns     int32 // Connections currently in use
	IdleConns         int32 // Connections currently idle
	AcquireCount      int64 // Total number of successful acquires
	EmptyAcquireCount int64 // Acquires that waited for a connection
}

// Pool manages a pool of PostgreSQL connections
type Pool struct {
	pool       *pgxpool.Pool
	config     *config.TargetConfig
	maxConns   int
	sourceType string // "mssql" or "postgres" - used for DDL generation
}

// NewPool creates a new PostgreSQL connection pool
// sourceType indicates the source database type ("mssql" or "postgres") for DDL generation
func NewPool(cfg *config.TargetConfig, maxConns int, sourceType string) (*Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing dsn: %w", err)
	}

	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(maxConns / 4)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &Pool{pool: pool, config: cfg, maxConns: maxConns, sourceType: sourceType}, nil
}

// Close closes all connections in the pool
func (p *Pool) Close() {
	p.pool.Close()
}

// Ping tests the connection to the database
func (p *Pool) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Pool returns the underlying pgxpool
func (p *Pool) Pool() *pgxpool.Pool {
	return p.pool
}

// Stats returns current connection pool statistics
func (p *Pool) Stats() PoolStats {
	stats := p.pool.Stat()
	return PoolStats{
		MaxConns:          stats.MaxConns(),
		TotalConns:        stats.TotalConns(),
		AcquiredConns:     stats.AcquiredConns(),
		IdleConns:         stats.IdleConns(),
		AcquireCount:      stats.AcquireCount(),
		EmptyAcquireCount: stats.EmptyAcquireCount(),
	}
}

// MaxConns returns the configured maximum connections
func (p *Pool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *Pool) DBType() string {
	return "postgres"
}

// CreateSchema creates the target schema if it doesn't exist
func (p *Pool) CreateSchema(ctx context.Context, schema string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quotePGIdent(schema)))
	return err
}

// CreateTable creates a table from source metadata
func (p *Pool) CreateTable(ctx context.Context, t *source.Table, targetSchema string) error {
	return p.CreateTableWithOptions(ctx, t, targetSchema, false)
}

// CreateTableWithOptions creates a table with optional UNLOGGED
func (p *Pool) CreateTableWithOptions(ctx context.Context, t *source.Table, targetSchema string, unlogged bool) error {
	ddl := GenerateDDLWithOptions(t, targetSchema, unlogged, p.sourceType)

	_, err := p.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}

	return nil
}

// SetTableLogged converts an UNLOGGED table to LOGGED
func (p *Pool) SetTableLogged(ctx context.Context, schema, table string) error {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	sql := fmt.Sprintf("ALTER TABLE %s.%q SET LOGGED", schema, sanitizedTable)
	_, err := p.pool.Exec(ctx, sql)
	return err
}

// TruncateTable truncates a table
func (p *Pool) TruncateTable(ctx context.Context, schema, table string) error {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	_, err := p.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualifyPGTable(schema, sanitizedTable)))
	return err
}

// DropTable drops a table if it exists
func (p *Pool) DropTable(ctx context.Context, schema, table string) error {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	_, err := p.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifyPGTable(schema, sanitizedTable)))
	return err
}

// TableExists checks if a table exists in the schema
func (p *Pool) TableExists(ctx context.Context, schema, table string) (bool, error) {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// CreatePrimaryKey creates a primary key on the table
func (p *Pool) CreatePrimaryKey(ctx context.Context, t *source.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	pkCols := ""
	for i, col := range t.PrimaryKey {
		if i > 0 {
			pkCols += ", "
		}
		// Sanitize PK column name for PostgreSQL
		pkCols += quotePGIdent(SanitizePGIdentifier(col))
	}

	// Sanitize table name for PostgreSQL
	sanitizedTableName := SanitizePGIdentifier(t.Name)

	sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)",
		qualifyPGTable(targetSchema, sanitizedTableName), pkCols)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// GetRowCount returns the row count for a table
func (p *Pool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	var count int64
	err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifyPGTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// HasPrimaryKey checks if a table has a primary key constraint
func (p *Pool) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	// Sanitize table name for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(table)
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE constraint_type = 'PRIMARY KEY'
			AND table_schema = $1
			AND table_name = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// PrepareUpsertStaging is a no-op for PostgreSQL (batched upsert doesn't need staging)
func (p *Pool) PrepareUpsertStaging(ctx context.Context, schema, table string) error {
	return nil
}

// ExecuteUpsertMerge is a no-op for PostgreSQL (upsert happens in UpsertChunk)
func (p *Pool) ExecuteUpsertMerge(ctx context.Context, schema, table string, cols []string, pkCols []string, mergeChunkSize int) error {
	return nil
}

// CheckUpsertStagingReady is a no-op for PostgreSQL (doesn't use staging tables)
func (p *Pool) CheckUpsertStagingReady(ctx context.Context, schema, table string) (bool, int64, error) {
	return false, 0, nil
}

// ResetSequence resets identity sequence to max value
func (p *Pool) ResetSequence(ctx context.Context, schema string, t *source.Table) error {
	// Find identity column
	var identityCol string
	for _, c := range t.Columns {
		if c.IsIdentity {
			identityCol = c.Name
			break
		}
	}

	if identityCol == "" {
		return nil
	}

	// Sanitize identifiers for PostgreSQL
	sanitizedTable := SanitizePGIdentifier(t.Name)
	sanitizedCol := SanitizePGIdentifier(identityCol)

	// Get max value from the table
	var maxVal int64
	err := p.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", quotePGIdent(sanitizedCol), qualifyPGTable(schema, sanitizedTable))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", sanitizedTable, sanitizedCol, err)
	}

	if maxVal == 0 {
		return nil // Empty table, no need to reset
	}

	// For GENERATED AS IDENTITY columns, use ALTER TABLE ... RESTART WITH
	// This works for both SERIAL and IDENTITY columns
	sql := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d`,
		qualifyPGTable(schema, sanitizedTable), quotePGIdent(sanitizedCol), maxVal+1)

	_, err = p.pool.Exec(ctx, sql)
	if err != nil {
		// Fall back to pg_get_serial_sequence for SERIAL columns
		fallbackSQL := fmt.Sprintf(`
			SELECT setval(
				pg_get_serial_sequence('%s', '%s'),
				%d
			)
		`, qualifyPGTable(schema, sanitizedTable), sanitizedCol, maxVal)
		_, err = p.pool.Exec(ctx, fallbackSQL)
	}
	return err
}

// CreateIndex creates an index on the target table
func (p *Pool) CreateIndex(ctx context.Context, t *source.Table, idx *source.Index, targetSchema string) error {
	// Sanitize table name
	sanitizedTable := SanitizePGIdentifier(t.Name)

	// Build column list with sanitized names
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = quotePGIdent(SanitizePGIdentifier(col))
	}

	unique := ""
	if idx.IsUnique {
		unique = "UNIQUE "
	}

	// Generate index name (PostgreSQL has length limits)
	idxName := fmt.Sprintf("idx_%s_%s", sanitizedTable, SanitizePGIdentifier(idx.Name))
	if len(idxName) > 63 {
		idxName = idxName[:63]
	}

	sql := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, quotePGIdent(idxName), qualifyPGTable(targetSchema, sanitizedTable), strings.Join(cols, ", "))

	// Add included columns if any (PostgreSQL INCLUDE syntax)
	if len(idx.IncludeCols) > 0 {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			includeCols[i] = quotePGIdent(SanitizePGIdentifier(col))
		}
		sql += fmt.Sprintf(" INCLUDE (%s)", strings.Join(includeCols, ", "))
	}

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// CreateForeignKey creates a foreign key constraint on the target table
func (p *Pool) CreateForeignKey(ctx context.Context, t *source.Table, fk *source.ForeignKey, targetSchema string) error {
	// Sanitize table names
	sanitizedTable := SanitizePGIdentifier(t.Name)
	sanitizedRefTable := SanitizePGIdentifier(fk.RefTable)

	// Build column lists with sanitized names
	cols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		cols[i] = quotePGIdent(SanitizePGIdentifier(col))
	}

	refCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		refCols[i] = quotePGIdent(SanitizePGIdentifier(col))
	}

	// Map SQL Server referential actions to PostgreSQL
	onDelete := mapReferentialAction(fk.OnDelete)
	onUpdate := mapReferentialAction(fk.OnUpdate)

	// Generate FK name with sanitized names
	fkName := fmt.Sprintf("fk_%s_%s", sanitizedTable, SanitizePGIdentifier(fk.Name))
	if len(fkName) > 63 {
		fkName = fkName[:63]
	}

	sql := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s (%s)
		ON DELETE %s
		ON UPDATE %s
	`, qualifyPGTable(targetSchema, sanitizedTable), quotePGIdent(fkName),
		strings.Join(cols, ", "),
		qualifyPGTable(targetSchema, sanitizedRefTable), strings.Join(refCols, ", "),
		onDelete, onUpdate)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// CreateCheckConstraint creates a check constraint on the target table
func (p *Pool) CreateCheckConstraint(ctx context.Context, t *source.Table, chk *source.CheckConstraint, targetSchema string) error {
	// Sanitize table name
	sanitizedTable := SanitizePGIdentifier(t.Name)

	// Convert SQL Server CHECK syntax to PostgreSQL
	// Most basic checks are compatible
	definition := convertCheckDefinition(chk.Definition)

	// Generate constraint name with sanitized names
	chkName := fmt.Sprintf("chk_%s_%s", sanitizedTable, SanitizePGIdentifier(chk.Name))
	if len(chkName) > 63 {
		chkName = chkName[:63]
	}

	sql := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		CHECK %s
	`, qualifyPGTable(targetSchema, sanitizedTable), quotePGIdent(chkName), definition)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// mapReferentialAction converts SQL Server referential action to PostgreSQL
func mapReferentialAction(action string) string {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL":
		return "SET NULL"
	case "SET_DEFAULT":
		return "SET DEFAULT"
	case "NO_ACTION":
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

// convertCheckDefinition converts SQL Server CHECK definition to PostgreSQL
func convertCheckDefinition(def string) string {
	// Replace SQL Server specific functions/syntax
	// Most basic checks are compatible
	result := def

	// Replace [column] with "column"
	// This is a simplified conversion - complex expressions may need manual review
	for {
		start := strings.Index(result, "[")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "]")
		if end == -1 {
			break
		}
		colName := result[start+1 : start+end]
		result = result[:start] + `"` + colName + `"` + result[start+end+1:]
	}

	// Replace getdate() with CURRENT_TIMESTAMP
	result = strings.ReplaceAll(result, "getdate()", "CURRENT_TIMESTAMP")
	result = strings.ReplaceAll(result, "GETDATE()", "CURRENT_TIMESTAMP")

	return result
}

// UpsertChunk performs INSERT ... ON CONFLICT ... DO UPDATE for upsert mode
// Uses batched multi-row VALUES for performance (instead of per-row inserts)
func (p *Pool) UpsertChunk(ctx context.Context, schema, table string, cols []string, pkCols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	if len(pkCols) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Disable statement timeout for bulk operations
	_, err = conn.Exec(ctx, "SET statement_timeout = 0")
	if err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}

	// PostgreSQL has a limit of ~65535 parameters per query
	// Calculate batch size: maxParams / numCols
	const maxParams = 65000 // Leave some headroom
	batchSize := maxParams / len(cols)
	if batchSize > len(rows) {
		batchSize = len(rows)
	}
	if batchSize < 1 {
		batchSize = 1
	}

	// Execute in batches using transactions for efficiency
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Process rows in batches
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		// Build batched upsert SQL with multi-row VALUES
		upsertSQL, args := buildPGBatchedUpsertSQL(schema, table, cols, pkCols, batch)

		_, err := tx.Exec(ctx, upsertSQL, args...)
		if err != nil {
			return fmt.Errorf("executing batched upsert: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// buildPGBatchedUpsertSQL generates PostgreSQL batched upsert statement with multi-row VALUES
// INSERT INTO schema.table (cols) VALUES ($1, $2, ...), ($N+1, $N+2, ...), ...
// ON CONFLICT (pk_cols) DO UPDATE SET col1 = EXCLUDED.col1, ...
// WHERE (table.col1, ...) IS DISTINCT FROM (EXCLUDED.col1, ...)
func buildPGBatchedUpsertSQL(schema, table string, cols []string, pkCols []string, rows [][]any) (string, []any) {
	var sb strings.Builder
	numCols := len(cols)

	// Build column list
	quotedCols := make([]string, numCols)
	for i, col := range cols {
		quotedCols[i] = quotePGIdent(col)
	}

	// Build PK column list for ON CONFLICT
	quotedPKs := make([]string, len(pkCols))
	for i, pk := range pkCols {
		quotedPKs[i] = quotePGIdent(pk)
	}

	// Build SET clause (exclude PK columns)
	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var targetCols []string
	var excludedCols []string
	for _, col := range cols {
		if !pkSet[col] {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", quotePGIdent(col), quotePGIdent(col)))
			targetCols = append(targetCols, quotePGIdent(table)+"."+quotePGIdent(col))
			excludedCols = append(excludedCols, "EXCLUDED."+quotePGIdent(col))
		}
	}

	// Build multi-row VALUES clause and flatten args
	args := make([]any, 0, len(rows)*numCols)
	valueTuples := make([]string, len(rows))

	for rowIdx, row := range rows {
		params := make([]string, numCols)
		for colIdx := range cols {
			paramNum := rowIdx*numCols + colIdx + 1
			params[colIdx] = fmt.Sprintf("$%d", paramNum)
			args = append(args, row[colIdx])
		}
		valueTuples[rowIdx] = "(" + strings.Join(params, ", ") + ")"
	}

	// INSERT INTO schema.table (cols) VALUES (...), (...), ...
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		qualifyPGTable(schema, table),
		strings.Join(quotedCols, ", "),
		strings.Join(valueTuples, ", ")))

	// ON CONFLICT (pk_cols)
	sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s)", strings.Join(quotedPKs, ", ")))

	if len(setClauses) > 0 {
		// DO UPDATE SET col1 = EXCLUDED.col1, ...
		sb.WriteString(fmt.Sprintf(" DO UPDATE SET %s", strings.Join(setClauses, ", ")))
		// WHERE clause for change detection (skip update if no changes)
		sb.WriteString(fmt.Sprintf(" WHERE (%s) IS DISTINCT FROM (%s)",
			strings.Join(targetCols, ", "),
			strings.Join(excludedCols, ", ")))
	} else {
		// All columns are PK - DO NOTHING
		sb.WriteString(" DO NOTHING")
	}

	return sb.String(), args
}

// UpsertChunkWithWriter performs high-performance upsert using staging tables with writer isolation.
// This approach uses:
// 1. Per-writer TEMP staging table (avoids contention between parallel writers)
// 2. Binary COPY protocol via pgx.CopyFrom (5-10x faster than INSERT VALUES)
// 3. Single INSERT...SELECT...ON CONFLICT to merge (one statement per chunk)
// 4. IS DISTINCT FROM to prevent unnecessary row updates (critical for WAL/bloat prevention)
func (p *Pool) UpsertChunkWithWriter(ctx context.Context, schema, table string,
	cols []string, pkCols []string, rows [][]any, writerID int, partitionID *int) error {

	if len(rows) == 0 {
		return nil
	}

	if len(pkCols) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Disable statement timeout for bulk operations
	_, err = conn.Exec(ctx, "SET statement_timeout = 0")
	if err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}

	// 1. Generate safe staging table name (handles long names with hash fallback)
	stagingTable := safePGStagingName(schema, table, writerID, partitionID)

	// 2. Create TEMP staging table if not exists
	// TEMP tables are session-scoped and auto-dropped on disconnect (no cleanup needed)
	// They are also inherently UNLOGGED, reducing WAL I/O
	createSQL := fmt.Sprintf(
		`CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS)`,
		quotePGIdent(stagingTable), qualifyPGTable(schema, table))
	if _, err := conn.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// 3. Truncate staging (reuse table across chunks, faster than DROP/CREATE)
	if _, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", quotePGIdent(stagingTable))); err != nil {
		return fmt.Errorf("truncating staging table: %w", err)
	}

	// 4. Binary COPY into staging using pgx.CopyFrom
	// This is 5-10x faster than INSERT VALUES due to:
	// - Binary protocol (no parsing overhead)
	// - Single round-trip for all rows
	// - No parameter binding overhead
	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{stagingTable},
		cols,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copying to staging: %w", err)
	}

	// 5. Merge staging into target with IS DISTINCT FROM to prevent empty updates
	// CRITICAL: Without IS DISTINCT FROM, PostgreSQL writes new row versions even for
	// identical data, causing table bloat and excessive WAL writes
	mergeSQL := buildPGStagingMergeSQL(schema, table, stagingTable, cols, pkCols)
	_, err = conn.Exec(ctx, mergeSQL)
	if err != nil {
		return fmt.Errorf("merging staging to target: %w", err)
	}

	return nil
}

// safePGStagingName generates a safe staging table name that handles long identifiers.
// PostgreSQL has a 63-byte limit for identifiers.
// If the name would be too long, falls back to a hash-based name.
func safePGStagingName(schema, table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	base := fmt.Sprintf("_stg_%s_%s", schema, table)
	maxLen := 63 // PostgreSQL identifier limit

	if len(base)+len(suffix) > maxLen {
		// Use hash for long names
		hash := sha256.Sum256([]byte(schema + table))
		base = fmt.Sprintf("_stg_%x", hash[:8]) // 16 hex chars
	}
	return base + suffix
}

// buildPGStagingMergeSQL generates the INSERT...SELECT...ON CONFLICT statement
// that merges data from staging table to target table.
// Includes IS DISTINCT FROM to prevent unnecessary row updates.
func buildPGStagingMergeSQL(schema, table, stagingTable string, cols, pkCols []string) string {
	// Build column lists
	quotedCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCols[i] = quotePGIdent(col)
	}
	colStr := strings.Join(quotedCols, ", ")

	// Build PK list for ON CONFLICT
	quotedPKs := make([]string, len(pkCols))
	for i, pk := range pkCols {
		quotedPKs[i] = quotePGIdent(pk)
	}
	pkStr := strings.Join(quotedPKs, ", ")

	// Build SET clause and IS DISTINCT FROM clause (exclude PK columns)
	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var targetCols []string
	var excludedCols []string
	for _, col := range cols {
		if !pkSet[col] {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", quotePGIdent(col), quotePGIdent(col)))
			targetCols = append(targetCols, fmt.Sprintf("%s.%s", quotePGIdent(table), quotePGIdent(col)))
			excludedCols = append(excludedCols, fmt.Sprintf("EXCLUDED.%s", quotePGIdent(col)))
		}
	}

	var sb strings.Builder

	// INSERT INTO schema.table (cols) SELECT cols FROM staging
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		qualifyPGTable(schema, table), colStr, colStr, quotePGIdent(stagingTable)))

	// ON CONFLICT (pk_cols)
	sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s)", pkStr))

	if len(setClauses) > 0 {
		// DO UPDATE SET col1 = EXCLUDED.col1, ...
		sb.WriteString(fmt.Sprintf(" DO UPDATE SET %s", strings.Join(setClauses, ", ")))

		// WHERE clause using IS DISTINCT FROM to prevent unnecessary updates
		// This is CRITICAL to avoid table bloat and excessive WAL writes
		// PostgreSQL MVCC creates new row versions even for identical data without this
		sb.WriteString(fmt.Sprintf(" WHERE (%s) IS DISTINCT FROM (%s)",
			strings.Join(targetCols, ", "),
			strings.Join(excludedCols, ", ")))
	} else {
		// All columns are PK - DO NOTHING
		sb.WriteString(" DO NOTHING")
	}

	return sb.String()
}
