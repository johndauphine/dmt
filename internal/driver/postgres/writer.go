package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL writer code is split by responsibility:
// identifiers.go, writer_context.go, writer_ddl.go, writer_rowcount.go,
// writer_copy.go, writer_upsert.go, and writer_raw.go.

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
	copyBatchBytes     int                          // Max bytes per CopyFrom call (derived from TCP buffer size)
}

// NewWriter creates a new PostgreSQL writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// pgx errors here can quote the raw DSN - see #231. ScrubError
	// strips the password before the message ever reaches a log
	// aggregator.
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("parsing dsn: %w", err))
	}

	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(maxConns / 4)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("creating pool: %w", err))
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
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

	// Log mapper initialization. Type-switch to surface which mapper
	// is in use - useful for debugging when AI fallback is/isn't
	// firing (#170).
	driver.LogTypeMapperInit(opts.TypeMapper)

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
		copyBatchBytes:     probeCopyBatchBytes(pool),
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// Close closes all connections.
// Reset() is called first to immediately close idle connections and mark acquired
// connections for destruction. This prevents Close() from blocking indefinitely
// when a connection is held by a stalled operation (e.g. a COPY waiting for data).
func (w *Writer) Close() {
	if w.cachedDB != nil {
		w.cachedDB.Close()
	}
	w.pool.Reset()
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
	return shared.RowCountWithFallback(
		func() (int64, error) { return w.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return w.GetRowCountExact(ctx, schema, table, false) },
	)
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
// Postgres has no NOLOCK equivalent (uses MVCC); strictConsistency
// is accepted for interface symmetry and ignored here.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	sanitizedTable := sanitizePGTableName(table)
	var count int64
	query, err := shared.ExactRowCountQuery(w.dialect, schema, sanitizedTable)
	if err != nil {
		return 0, err
	}
	err = w.pool.QueryRow(ctx, query).Scan(&count)
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
		switch encoding {
		case "UTF8":
			dbCtx.BytesPerChar = 4
		case "LATIN1", "SQL_ASCII":
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

// UpsertBatch performs an upsert using staging table + INSERT ON CONFLICT.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// Create staging table name (unique per writer)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)))
	stagingTable := fmt.Sprintf("_stg_%x", hash[:8])

	// Create temp table. COPY and INSERT must run in the same transaction
	// because the staging table uses ON COMMIT DELETE ROWS.
	_, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL) ON COMMIT DELETE ROWS",
		w.dialect.QuoteIdentifier(stagingTable),
		w.dialect.QualifyTable(opts.Schema, opts.Table)))
	if err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// Adaptive sub-batching for staging COPY
	batchSize := copyBatchSize(opts.Rows, w.copyBatchBytes)
	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		subBatch := opts.Rows[start:end]
		const upsertMB = 1024 * 1024
		upsertBatchBytes := estimateRowBytes(subBatch, 100) * len(subBatch)
		upsertTimeoutSecs := (upsertBatchBytes + upsertMB - 1) / upsertMB
		if upsertTimeoutSecs < 30 {
			upsertTimeoutSecs = 30
		}
		copyCtx, cancel := context.WithTimeout(ctx, time.Duration(upsertTimeoutSecs)*time.Second)
		_, err = tx.CopyFrom(
			copyCtx,
			pgx.Identifier{stagingTable},
			opts.Columns,
			pgx.CopyFromRows(subBatch),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("copying to staging [%d:%d]: %w", start, end, err)
		}
	}

	// Build INSERT ... ON CONFLICT
	upsertSQL := w.buildUpsertSQL(opts, stagingTable)

	_, err = tx.Exec(ctx, upsertSQL)
	if err != nil {
		return fmt.Errorf("upserting: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
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
