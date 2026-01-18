package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
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

// sanitizePGIdentifier converts an identifier to PostgreSQL-friendly lowercase format.
// Simply lowercases and replaces special chars with underscores.
// Example: VoteTypes -> votetypes, UserId -> userid, User-Id -> user_id
func sanitizePGIdentifier(ident string) string {
	if ident == "" {
		return "col_"
	}
	s := strings.ToLower(ident)
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()
	// Prefix with col_ if starts with digit
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}
	if s == "" {
		return "col_"
	}
	return s
}

// sanitizePGTableName is an alias for sanitizePGIdentifier for table names.
func sanitizePGTableName(ident string) string {
	return sanitizePGIdentifier(ident)
}

// Writer implements driver.Writer for PostgreSQL.
type Writer struct {
	pool               *pgxpool.Pool
	config             *dbconfig.TargetConfig
	maxConns           int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI
	cachedDB           *sql.DB                      // Cached database/sql wrapper for tuning analysis
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

	logging.Debug("Connected to PostgreSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		pool.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		pool.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Debug("AI Table-Level Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	w := &Writer{
		pool:               pool,
		config:             cfg,
		maxConns:           maxConns,
		sourceType:         opts.SourceType,
		dialect:            dialect,
		typeMapper:         opts.TypeMapper,
		tableMapper:        tableMapper,
		finalizationMapper: finalizationMapper,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// gatherDatabaseContext collects PostgreSQL database metadata for AI context.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	ctx := context.Background()

	dbCtx := &driver.DatabaseContext{
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "lower",
		CaseSensitiveIdentifiers: true, // PostgreSQL preserves case in quotes
		CaseSensitiveData:        true, // Default is case-sensitive
		MaxIdentifierLength:      63,
		VarcharSemantics:         "char", // PostgreSQL VARCHAR is always characters
		BytesPerChar:             4,      // UTF-8 max
		MaxVarcharLength:         10485760,
	}

	// Query server version
	var version string
	if w.pool.QueryRow(ctx, "SELECT version()").Scan(&version) == nil {
		dbCtx.Version = version
		// Parse major version using regex to handle any version format
		// Matches patterns like "PostgreSQL 16.1", "PostgreSQL 17", etc.
		versionRegex := regexp.MustCompile(`PostgreSQL\s+(\d+)`)
		if matches := versionRegex.FindStringSubmatch(version); len(matches) > 1 {
			if majorVer, err := strconv.Atoi(matches[1]); err == nil {
				dbCtx.MajorVersion = majorVer
			}
		}
	}

	// Query encoding
	var encoding string
	if w.pool.QueryRow(ctx, "SHOW server_encoding").Scan(&encoding) == nil {
		dbCtx.Charset = encoding
		dbCtx.Encoding = encoding
		if encoding == "UTF8" {
			dbCtx.BytesPerChar = 4
		} else if encoding == "LATIN1" || encoding == "SQL_ASCII" {
			dbCtx.BytesPerChar = 1
		}
	}

	// Query collation
	var collation sql.NullString
	if w.pool.QueryRow(ctx, `
		SELECT datcollate FROM pg_database WHERE datname = current_database()
	`).Scan(&collation) == nil && collation.Valid {
		dbCtx.Collation = collation.String
	}

	// Query LC_CTYPE for character classification
	var lcCtype sql.NullString
	if w.pool.QueryRow(ctx, `
		SELECT datctype FROM pg_database WHERE datname = current_database()
	`).Scan(&lcCtype) == nil && lcCtype.Valid {
		if dbCtx.Notes != "" {
			dbCtx.Notes += "; "
		}
		dbCtx.Notes += "LC_CTYPE=" + lcCtype.String
	}

	// Standard PostgreSQL features
	dbCtx.Features = []string{"TEXT", "JSON", "JSONB", "ARRAY", "HSTORE", "UUID", "BYTEA", "NUMERIC"}

	// Version-specific features
	if dbCtx.MajorVersion >= 14 {
		dbCtx.Features = append(dbCtx.Features, "MULTIRANGE")
	}
	if dbCtx.MajorVersion >= 15 {
		dbCtx.Features = append(dbCtx.Features, "JSON_TABLE")
	}

	logging.Debug("PostgreSQL context: encoding=%s, collation=%s, version=%d",
		dbCtx.Encoding, dbCtx.Collation, dbCtx.MajorVersion)

	return dbCtx
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

// CreateTableWithOptions creates a table with options using AI-generated DDL.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	// Use table-level AI DDL generation with full database context
	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  "postgres",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}

	resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("AI DDL generation failed for table %s: %w", t.FullName(), err)
	}

	ddl := resp.CreateTableDDL

	// Handle unlogged option - modify the DDL if needed
	if opts.Unlogged && !strings.Contains(strings.ToUpper(ddl), "UNLOGGED") {
		ddl = strings.Replace(ddl, "CREATE TABLE", "CREATE UNLOGGED TABLE", 1)
	}

	logging.Debug("AI generated DDL for %s:\n%s", t.FullName(), ddl)

	// Log column type mappings
	for colName, colType := range resp.ColumnTypes {
		logging.Debug("  Column %s -> %s", colName, colType)
	}

	_, err = w.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, ddl)
	}
	return nil
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGTableName(table)
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
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// CreatePrimaryKey creates the primary key constraint.
// This is idempotent - it checks if a PK already exists before creating one.
// AI-generated DDL includes the PK inline, so this check is necessary.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	// Check if PK already exists (AI-generated DDL includes PK inline)
	hasPK, err := w.HasPrimaryKey(ctx, targetSchema, t.Name)
	if err != nil {
		return fmt.Errorf("checking for existing PK: %w", err)
	}
	if hasPK {
		return nil // PK already exists, nothing to do
	}

	sanitizedTable := sanitizePGTableName(t.Name)
	cols := make([]string, len(t.PrimaryKey))
	for i, c := range t.PrimaryKey {
		cols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	pkName := fmt.Sprintf("pk_%s", sanitizedTable)
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(pkName),
		strings.Join(cols, ", "))

	_, err = w.pool.Exec(ctx, sql)
	return err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// This provides context to AI for generating indexes, FKs, etc.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	// Use pg_get_tabledef extension if available, otherwise build from catalog
	var ddl string

	// First try the extension (if installed)
	err := w.pool.QueryRow(ctx,
		`SELECT pg_get_tabledef($1, $2)`,
		schema, table,
	).Scan(&ddl)
	if err == nil && ddl != "" {
		return ddl
	}

	// Fallback: build DDL from information_schema
	rows, err := w.pool.Query(ctx, `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", schema, table, err)
		return ""
	}
	defer rows.Close()

	var sb strings.Builder
	// Use dialect's QuoteIdentifier for proper escaping
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n",
		w.dialect.QuoteIdentifier(schema),
		w.dialect.QuoteIdentifier(table)))

	first := true
	for rows.Next() {
		var colName, dataType, isNullable string
		var charMaxLen, numPrecision, numScale sql.NullInt64
		var colDefault sql.NullString

		if err := rows.Scan(&colName, &dataType, &charMaxLen, &numPrecision, &numScale, &isNullable, &colDefault); err != nil {
			logging.Debug("Failed to scan column for %s.%s: %v", schema, table, err)
			continue
		}

		if !first {
			sb.WriteString(",\n")
		}
		first = false

		sb.WriteString(fmt.Sprintf("    %s ", w.dialect.QuoteIdentifier(colName)))

		// Build type with precision
		typeStr := dataType
		if charMaxLen.Valid && charMaxLen.Int64 > 0 {
			typeStr = fmt.Sprintf("%s(%d)", dataType, charMaxLen.Int64)
		} else if numPrecision.Valid && numPrecision.Int64 > 0 {
			if numScale.Valid && numScale.Int64 > 0 {
				typeStr = fmt.Sprintf("%s(%d,%d)", dataType, numPrecision.Int64, numScale.Int64)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", dataType, numPrecision.Int64)
			}
		}
		sb.WriteString(typeStr)

		if isNullable == "NO" {
			sb.WriteString(" NOT NULL")
		}
		if colDefault.Valid && colDefault.String != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", colDefault.String))
		}
	}

	// Check if any columns were found
	if first {
		logging.Debug("No columns found for table %s.%s", schema, table)
		return ""
	}

	sb.WriteString("\n);")
	return sb.String()
}

// CreateIndex creates an index using AI-generated DDL.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for index creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedIdx := &driver.Index{
		Name:     sanitizePGIdentifier(idx.Name),
		Columns:  make([]string, len(idx.Columns)),
		IsUnique: idx.IsUnique,
		Filter:   idx.Filter,
	}
	for i, col := range idx.Columns {
		sanitizedIdx.Columns[i] = sanitizePGIdentifier(col)
	}
	if len(idx.IncludeCols) > 0 {
		sanitizedIdx.IncludeCols = make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			sanitizedIdx.IncludeCols[i] = sanitizePGIdentifier(col)
		}
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:           driver.DDLTypeIndex,
		SourceDBType:   w.sourceType,
		TargetDBType:   "postgres",
		Table:          sanitizedTable,
		Index:          sanitizedIdx,
		TargetSchema:   targetSchema,
		TargetContext:  w.dbContext,
		TargetTableDDL: targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI index DDL generation failed for %s.%s: %w", t.Name, idx.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}

// CreateForeignKey creates a foreign key constraint using AI-generated DDL.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for foreign key creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedFK := &driver.ForeignKey{
		Name:       sanitizePGIdentifier(fk.Name),
		Columns:    make([]string, len(fk.Columns)),
		RefSchema:  fk.RefSchema,
		RefTable:   sanitizePGIdentifier(fk.RefTable),
		RefColumns: make([]string, len(fk.RefColumns)),
		OnDelete:   fk.OnDelete,
		OnUpdate:   fk.OnUpdate,
	}
	for i, col := range fk.Columns {
		sanitizedFK.Columns[i] = sanitizePGIdentifier(col)
	}
	for i, col := range fk.RefColumns {
		sanitizedFK.RefColumns[i] = sanitizePGIdentifier(col)
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:           driver.DDLTypeForeignKey,
		SourceDBType:   w.sourceType,
		TargetDBType:   "postgres",
		Table:          sanitizedTable,
		ForeignKey:     sanitizedFK,
		TargetSchema:   targetSchema,
		TargetContext:  w.dbContext,
		TargetTableDDL: targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI FK DDL generation failed for %s.%s: %w", t.Name, fk.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}

// CreateCheckConstraint creates a check constraint using AI-generated DDL.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for check constraint creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedChk := &driver.CheckConstraint{
		Name:       sanitizePGIdentifier(chk.Name),
		Definition: chk.Definition,
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:            driver.DDLTypeCheckConstraint,
		SourceDBType:    w.sourceType,
		TargetDBType:    "postgres",
		Table:           sanitizedTable,
		CheckConstraint: sanitizedChk,
		TargetSchema:    targetSchema,
		TargetContext:   w.dbContext,
		TargetTableDDL:  targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI check constraint DDL generation failed for %s.%s: %w", t.Name, chk.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}

// HasPrimaryKey checks if a table has a primary key.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGTableName(table)
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
	sanitizedTable := sanitizePGTableName(table)
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
	sanitizedTable := sanitizePGTableName(table)
	var count int64
	err := w.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// ResetSequence resets the sequence for an identity column.
func (w *Writer) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
	sanitizedTable := sanitizePGTableName(t.Name)
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
	sanitizedTable := sanitizePGTableName(opts.Table)
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
