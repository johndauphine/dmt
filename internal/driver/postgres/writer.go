package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// sanitizePGIdentifier converts an identifier to PostgreSQL-friendly format.
// Converts to lowercase, replaces special chars with underscores.
func sanitizePGIdentifier(ident string) string {
	if ident == "" {
		return "col_"
	}
	s := strings.ToLower(ident)
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}
	if s == "" {
		return "col_"
	}
	return s
}

// Writer implements driver.Writer for PostgreSQL.
type Writer struct {
	pool       *pgxpool.Pool
	config     *dbconfig.TargetConfig
	maxConns   int
	sourceType string
	dialect    *Dialect
	typeMapper driver.TypeMapper
	cachedDB   *sql.DB // Cached database/sql wrapper for tuning analysis
}

// NewWriter creates a new PostgreSQL writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

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

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	logging.Info("Connected to PostgreSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		pool.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Info("AI Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	return &Writer{
		pool:       pool,
		config:     cfg,
		maxConns:   maxConns,
		sourceType: opts.SourceType,
		dialect:    dialect,
		typeMapper: opts.TypeMapper,
	}, nil
}

// Close closes all connections.
func (w *Writer) Close() {
	if w.cachedDB != nil {
		w.cachedDB.Close()
	}
	w.pool.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.pool.Ping(ctx)
}

// DB returns a database/sql connection for tuning analysis.
// The connection is cached and reused across calls to avoid resource leaks.
func (w *Writer) DB() *sql.DB {
	if w.cachedDB == nil {
		// Create stdlib connector from pool config (only once)
		w.cachedDB = stdlib.OpenDBFromPool(w.pool)
	}
	return w.cachedDB
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "postgres"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	poolStats := w.pool.Stat()
	return stats.PoolStats{
		DBType:      "postgres",
		MaxConns:    int(poolStats.MaxConns()),
		ActiveConns: int(poolStats.AcquiredConns()),
		IdleConns:   int(poolStats.IdleConns()),
		WaitCount:   poolStats.EmptyAcquireCount(),
		WaitTimeMs:  0,
	}
}

// CreateSchema creates the target schema if it doesn't exist.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	_, err := w.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", w.dialect.QuoteIdentifier(schema)))
	return err
}

// CreateTable creates a table from source metadata.
func (w *Writer) CreateTable(ctx context.Context, t *driver.Table, targetSchema string) error {
	return w.CreateTableWithOptions(ctx, t, targetSchema, driver.TableOptions{})
}

// CreateTableWithOptions creates a table with options.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	ddl := w.generateDDL(t, targetSchema, opts.Unlogged)

	_, err := w.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}
	return nil
}

func (w *Writer) generateDDL(t *driver.Table, targetSchema string, unlogged bool) string {
	var sb strings.Builder

	if unlogged {
		sb.WriteString("CREATE UNLOGGED TABLE ")
	} else {
		sb.WriteString("CREATE TABLE ")
	}
	// Sanitize table name for PostgreSQL (lowercase, safe chars)
	sanitizedTable := sanitizePGIdentifier(t.Name)
	sb.WriteString(w.dialect.QualifyTable(targetSchema, sanitizedTable))
	sb.WriteString(" (\n")

	// Check if using AI mapper for logging
	_, isAIMapper := w.typeMapper.(*driver.AITypeMapper)
	if isAIMapper {
		logging.Debug("AI Type Mapping: generating DDL for table %s (%d columns)", t.Name, len(t.Columns))
	}

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString("  ")
		// Sanitize column name
		sanitizedCol := sanitizePGIdentifier(col.Name)
		sb.WriteString(w.dialect.QuoteIdentifier(sanitizedCol))
		sb.WriteString(" ")

		// Map type using configured mapper (static or AI)
		typeInfo := driver.TypeInfo{
			SourceDBType: w.sourceType,
			TargetDBType: "postgres",
			DataType:     col.DataType,
			MaxLength:    col.MaxLength,
			Precision:    col.Precision,
			Scale:        col.Scale,
			SampleValues: col.SampleValues,
		}
		pgType := w.typeMapper.MapType(typeInfo)

		// Log AI type mappings
		if isAIMapper {
			logging.Debug("AI Type Mapping: %s.%s: %s(%d,%d,%d) -> %s",
				t.Name, col.Name, col.DataType, col.MaxLength, col.Precision, col.Scale, pgType)
		}

		sb.WriteString(pgType)

		// Identity
		if col.IsIdentity {
			sb.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
		}

		// Nullable
		if !col.IsNullable && !col.IsIdentity {
			sb.WriteString(" NOT NULL")
		}
	}

	sb.WriteString("\n)")
	return sb.String()
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGIdentifier(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGIdentifier(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGIdentifier(table)
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// SetTableLogged converts an UNLOGGED table to LOGGED.
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGIdentifier(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// CreatePrimaryKey creates the primary key constraint.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	sanitizedTable := sanitizePGIdentifier(t.Name)
	cols := make([]string, len(t.PrimaryKey))
	for i, c := range t.PrimaryKey {
		cols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	pkName := fmt.Sprintf("pk_%s", sanitizedTable)
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(pkName),
		strings.Join(cols, ", "))

	_, err := w.pool.Exec(ctx, sql)
	return err
}

// CreateIndex creates an index.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	sanitizedTable := sanitizePGIdentifier(t.Name)
	sanitizedIdxName := sanitizePGIdentifier(idx.Name)

	var sb strings.Builder
	sb.WriteString("CREATE ")
	if idx.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(w.dialect.QuoteIdentifier(sanitizedIdxName))
	sb.WriteString(" ON ")
	sb.WriteString(w.dialect.QualifyTable(targetSchema, sanitizedTable))
	sb.WriteString(" (")

	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	if len(idx.IncludeCols) > 0 {
		sb.WriteString(" INCLUDE (")
		inc := make([]string, len(idx.IncludeCols))
		for i, c := range idx.IncludeCols {
			inc[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
		}
		sb.WriteString(strings.Join(inc, ", "))
		sb.WriteString(")")
	}

	_, err := w.pool.Exec(ctx, sb.String())
	return err
}

// CreateForeignKey creates a foreign key constraint.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	sanitizedTable := sanitizePGIdentifier(t.Name)
	sanitizedFKName := sanitizePGIdentifier(fk.Name)
	sanitizedRefTable := sanitizePGIdentifier(fk.RefTable)

	cols := make([]string, len(fk.Columns))
	refCols := make([]string, len(fk.RefColumns))
	for i, c := range fk.Columns {
		cols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}
	for i, c := range fk.RefColumns {
		refCols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(sanitizedFKName),
		strings.Join(cols, ", "),
		w.dialect.QualifyTable(fk.RefSchema, sanitizedRefTable),
		strings.Join(refCols, ", "))

	if fk.OnDelete != "" && fk.OnDelete != "NO_ACTION" {
		sql += " ON DELETE " + strings.ReplaceAll(fk.OnDelete, "_", " ")
	}
	if fk.OnUpdate != "" && fk.OnUpdate != "NO_ACTION" {
		sql += " ON UPDATE " + strings.ReplaceAll(fk.OnUpdate, "_", " ")
	}

	_, err := w.pool.Exec(ctx, sql)
	return err
}

// CreateCheckConstraint creates a check constraint.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	sanitizedTable := sanitizePGIdentifier(t.Name)
	sanitizedChkName := sanitizePGIdentifier(chk.Name)

	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK %s",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(sanitizedChkName),
		chk.Definition)

	_, err := w.pool.Exec(ctx, sql)
	return err
}

// HasPrimaryKey checks if a table has a primary key.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGIdentifier(table)
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	sanitizedTable := sanitizePGIdentifier(table)
	err = w.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.pool.QueryRow(ctx,
		`SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// This may be slow on large tables.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	sanitizedTable := sanitizePGIdentifier(table)
	var count int64
	err := w.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// ResetSequence resets the sequence for an identity column.
func (w *Writer) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
	sanitizedTable := sanitizePGIdentifier(t.Name)
	for _, col := range t.Columns {
		if col.IsIdentity {
			// Find the sequence name (uses sanitized names)
			sanitizedCol := sanitizePGIdentifier(col.Name)
			seqName := fmt.Sprintf("%s_%s_seq", sanitizedTable, sanitizedCol)
			query := fmt.Sprintf("SELECT setval('%s.%s', COALESCE((SELECT MAX(%s) FROM %s), 1))",
				schema, seqName, w.dialect.QuoteIdentifier(sanitizedCol), w.dialect.QualifyTable(schema, sanitizedTable))
			if _, err := w.pool.Exec(ctx, query); err != nil {
				logging.Debug("Failed to reset sequence %s: %v", seqName, err)
			}
		}
	}
	return nil
}

// WriteBatch writes a batch of rows using COPY protocol.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Sanitize table and column names to match how they were created (lowercase)
	sanitizedTable := sanitizePGIdentifier(opts.Table)
	sanitizedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		sanitizedCols[i] = sanitizePGIdentifier(col)
	}

	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{opts.Schema, sanitizedTable},
		sanitizedCols,
		pgx.CopyFromRows(opts.Rows),
	)
	return err
}

// UpsertBatch performs an upsert using staging table + INSERT ON CONFLICT.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Create staging table name (unique per writer)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)))
	stagingTable := fmt.Sprintf("_stg_%x", hash[:8])

	// Create temp table
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL) ON COMMIT DELETE ROWS",
		w.dialect.QuoteIdentifier(stagingTable),
		w.dialect.QualifyTable(opts.Schema, opts.Table)))
	if err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// COPY into staging
	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{stagingTable},
		opts.Columns,
		pgx.CopyFromRows(opts.Rows),
	)
	if err != nil {
		return fmt.Errorf("copying to staging: %w", err)
	}

	// Build INSERT ... ON CONFLICT
	upsertSQL := w.buildUpsertSQL(opts, stagingTable)

	_, err = conn.Exec(ctx, upsertSQL)
	if err != nil {
		return fmt.Errorf("upserting: %w", err)
	}

	// Truncate staging (for safety, though ON COMMIT DELETE ROWS should handle it)
	_, _ = conn.Exec(ctx, fmt.Sprintf("TRUNCATE %s", w.dialect.QuoteIdentifier(stagingTable)))

	return nil
}

func (w *Writer) buildUpsertSQL(opts driver.UpsertBatchOptions, stagingTable string) string {
	var sb strings.Builder

	// Column lists
	quotedCols := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(c)
	}
	colList := strings.Join(quotedCols, ", ")

	// PK columns for conflict
	quotedPK := make([]string, len(opts.PKColumns))
	for i, c := range opts.PKColumns {
		quotedPK[i] = w.dialect.QuoteIdentifier(c)
	}
	pkList := strings.Join(quotedPK, ", ")

	// Build UPDATE SET clause with IS DISTINCT FROM change detection
	var setClauses []string
	var distinctClauses []string
	for i, col := range opts.Columns {
		isPK := false
		for _, pk := range opts.PKColumns {
			if col == pk {
				isPK = true
				break
			}
		}
		if !isPK {
			qCol := w.dialect.QuoteIdentifier(col)
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", qCol, qCol))

			// Skip spatial columns from change detection if needed
			colType := ""
			if i < len(opts.ColumnTypes) {
				colType = strings.ToLower(opts.ColumnTypes[i])
			}
			if colType != "geography" && colType != "geometry" {
				distinctClauses = append(distinctClauses, fmt.Sprintf("%s.%s", opts.Table, qCol))
			}
		}
	}

	sb.WriteString("INSERT INTO ")
	sb.WriteString(w.dialect.QualifyTable(opts.Schema, opts.Table))
	sb.WriteString(" (")
	sb.WriteString(colList)
	sb.WriteString(") SELECT ")
	sb.WriteString(colList)
	sb.WriteString(" FROM ")
	sb.WriteString(w.dialect.QuoteIdentifier(stagingTable))
	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(pkList)
	sb.WriteString(") DO UPDATE SET ")
	sb.WriteString(strings.Join(setClauses, ", "))

	// Add IS DISTINCT FROM clause for change detection
	if len(distinctClauses) > 0 {
		sb.WriteString(" WHERE (")
		sb.WriteString(strings.Join(distinctClauses, ", "))
		sb.WriteString(") IS DISTINCT FROM (")

		excludedClauses := make([]string, len(distinctClauses))
		for i, dc := range distinctClauses {
			// Replace table prefix with EXCLUDED
			excludedClauses[i] = strings.Replace(dc, opts.Table+".", "EXCLUDED.", 1)
		}
		sb.WriteString(strings.Join(excludedClauses, ", "))
		sb.WriteString(")")
	}

	return sb.String()
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.pool.QueryRow(ctx, query, args...).Scan(dest)
}
