package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	mssql "github.com/microsoft/go-mssqldb"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Writer implements driver.Writer for SQL Server.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	defaultBatchSize   int
	compatLevel        int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI
}

// NewWriter creates a new SQL Server writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// go-mssqldb errors can quote the raw DSN — see #231. ScrubError
	// strips the password.
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("opening connection: %w", err))
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	// Query database compatibility level
	var compatLevel int
	err = db.QueryRow(`
		SELECT compatibility_level
		FROM sys.databases
		WHERE name = DB_NAME()
	`).Scan(&compatLevel)
	if err != nil {
		compatLevel = 0
	}

	logging.Debug("Connected to MSSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	// Log mapper initialization. Type-switch to surface which mapper
	// is in use — useful for debugging when AI fallback is/isn't
	// firing (#170).
	driver.LogTypeMapperInit(opts.TypeMapper)

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	w := &Writer{
		db:                 db,
		config:             cfg,
		maxConns:           maxConns,
		defaultBatchSize:   opts.BatchSize,
		compatLevel:        compatLevel,
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

// gatherDatabaseContext collects SQL Server database metadata for AI context.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	ctx := &driver.DatabaseContext{
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "insensitive",
		CaseSensitiveIdentifiers: false,
		MaxIdentifierLength:      128,
		VarcharSemantics:         "byte", // VARCHAR = bytes, NVARCHAR = chars
		BytesPerChar:             2,      // NVARCHAR uses 2 bytes per char
		MaxNVarcharLength:        4000,   // NVARCHAR(n) max is 4000 chars; beyond that use NVARCHAR(MAX)
	}

	// Query server version
	var version string
	if w.db.QueryRow("SELECT @@VERSION").Scan(&version) == nil {
		ctx.Version = version
		// Parse major version using regex
		// @@VERSION returns something like "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6"
		// Try to match the product year first (2016, 2017, 2019, 2022, etc.)
		yearRegex := regexp.MustCompile(`SQL Server (\d{4})`)
		if matches := yearRegex.FindStringSubmatch(version); len(matches) > 1 {
			if year, err := strconv.Atoi(matches[1]); err == nil {
				// Map year to major version number
				switch {
				case year >= 2022:
					ctx.MajorVersion = 16
				case year >= 2019:
					ctx.MajorVersion = 15
				case year >= 2017:
					ctx.MajorVersion = 14
				case year >= 2016:
					ctx.MajorVersion = 13
				case year >= 2014:
					ctx.MajorVersion = 12
				default:
					ctx.MajorVersion = 11
				}
			}
		}
		// Fallback: try to parse version number directly (e.g., "16.0.1000.6")
		if ctx.MajorVersion == 0 {
			verNumRegex := regexp.MustCompile(`- (\d+)\.`)
			if matches := verNumRegex.FindStringSubmatch(version); len(matches) > 1 {
				if majorVer, err := strconv.Atoi(matches[1]); err == nil {
					ctx.MajorVersion = majorVer
				}
			}
		}
		if ctx.MajorVersion == 0 {
			logging.Warn("Could not parse SQL Server version from '%s', version-specific features may not be detected", version)
		}
	}

	// Query database collation
	var collation sql.NullString
	if w.db.QueryRow("SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation')").Scan(&collation) == nil && collation.Valid {
		ctx.Collation = collation.String
		// Parse collation for case sensitivity
		upperCollation := strings.ToUpper(collation.String)
		if strings.Contains(upperCollation, "_CS_") {
			ctx.CaseSensitiveData = true
		} else if strings.Contains(upperCollation, "_CI_") {
			ctx.CaseSensitiveData = false
		}
		// Parse for accent sensitivity
		if strings.Contains(upperCollation, "_AS") {
			ctx.Notes = "Accent-sensitive collation"
		}
	}

	// Query code page from collation
	var codePage sql.NullInt64
	if w.db.QueryRow(`
		SELECT COLLATIONPROPERTY(DATABASEPROPERTYEX(DB_NAME(), 'Collation'), 'CodePage')
	`).Scan(&codePage) == nil && codePage.Valid {
		ctx.CodePage = int(codePage.Int64)
		switch ctx.CodePage {
		case 65001:
			ctx.Encoding = "UTF-8"
		case 1252:
			ctx.Encoding = "Latin1 (Windows-1252)"
		case 1200:
			ctx.Encoding = "UTF-16LE"
		default:
			ctx.Encoding = fmt.Sprintf("CP%d", ctx.CodePage)
		}
	}

	// Set charset based on typical SQL Server setup
	ctx.Charset = "SQL_Latin1_General_CP1"
	if ctx.CodePage == 65001 {
		ctx.Charset = "UTF-8"
	}
	ctx.NationalCharset = "UTF-16"

	// Max varchar lengths
	ctx.MaxVarcharLength = 8000 // VARCHAR max, NVARCHAR max is 4000 chars

	// Features based on compatibility level
	ctx.Features = []string{"NVARCHAR", "VARCHAR_MAX", "DATETIME2", "JSON"}
	if w.compatLevel >= 130 { // SQL Server 2016+
		ctx.Features = append(ctx.Features, "JSON_FUNCTIONS", "TEMPORAL_TABLES")
	}
	if w.compatLevel >= 150 { // SQL Server 2019+
		ctx.Features = append(ctx.Features, "UTF8_SUPPORT")
	}

	logging.Debug("MSSQL context: collation=%s, code_page=%d, compat_level=%d",
		ctx.Collation, ctx.CodePage, w.compatLevel)

	return ctx
}

// Close closes all connections.
func (w *Writer) Close() {
	w.db.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

// DB returns the underlying database connection for tuning analysis.
func (w *Writer) DB() *sql.DB {
	return w.db
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	return shared.ExecRaw(ctx, w.db, query, args...)
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return shared.QueryRowRaw(ctx, w.db, query, dest, args...)
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
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`
	err := w.db.QueryRowContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table)).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// The Writer never read uncommitted data even pre-#253 (no NOLOCK
// hint), so strictConsistency is accepted for interface symmetry
// with the Reader but is effectively a no-op here.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, w.db, w.dialect, schema, table)
}

func convertRowForBulkCopy(row []any, columnTypes []string) []any {
	result := make([]any, len(row))
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			if isTextColumn(columnTypes, i) && utf8.Valid(b) {
				result[i] = string(b)
			} else if isASCIINumeric(b) {
				result[i] = string(b)
			} else {
				result[i] = v
			}
		} else {
			result[i] = v
		}
	}
	return result
}

func isTextColumn(columnTypes []string, index int) bool {
	if index >= len(columnTypes) {
		return false
	}

	columnType := normalizeColumnType(columnTypes[index])
	switch columnType {
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"nchar", "nvarchar", "ntext", "string", "uniqueidentifier", "uuid",
		"json", "jsonb", "xml", "enum", "set":
		return true
	default:
		return false
	}
}

func normalizeColumnType(columnType string) string {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	if idx := strings.IndexAny(columnType, "( \t\r\n"); idx >= 0 {
		columnType = columnType[:idx]
	}
	return columnType
}

func isASCIINumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	hasDigit := false
	hasDot := false
	hasE := false
	i := 0

	if b[i] == '+' || b[i] == '-' {
		i++
		if i >= len(b) {
			return false
		}
	}

	for i < len(b) {
		c := b[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '.':
			if hasDot || hasE {
				return false
			}
			hasDot = true
		case c == 'E' || c == 'e':
			if hasE || !hasDigit {
				return false
			}
			hasE = true
			i++
			if i < len(b) && (b[i] == '+' || b[i] == '-') {
				i++
			}
			if i >= len(b) || b[i] < '0' || b[i] > '9' {
				return false
			}
			continue
		default:
			return false
		}
		i++
	}

	return hasDigit
}

func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}

	if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
		return mssqlErr.SQLErrorNumber() == 1205
	}

	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "1205")
}

// WriteBatch writes a batch of rows using TDS bulk copy.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	// Resume-safe path for ROW_NUMBER-paged tables: stage + insert-only MERGE
	// so replayed rows that already exist become silent no-ops (#227).
	if opts.IdempotentOnDup {
		return w.writeBatchIdempotent(ctx, opts)
	}

	fullTableName := fmt.Sprintf("[%s].[%s]", opts.Schema, opts.Table)

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}
	defer conn.Close()

	// Set lock timeout to prevent indefinite waits on row/page locks during
	// parallel bulk inserts. 5 minutes is generous for bulk operations.
	// Without this, concurrent writers or uncommitted transactions can
	// block each other indefinitely.
	if _, err := conn.ExecContext(ctx, "SET LOCK_TIMEOUT 300000"); err != nil {
		return fmt.Errorf("setting lock timeout: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	err = conn.Raw(func(driverConn any) error {
		mssqlConn, ok := driverConn.(*mssql.Conn)
		if !ok {
			return fmt.Errorf("expected *mssql.Conn, got %T", driverConn)
		}

		// Sub-batch rows to avoid accumulating too much data in the TDS
		// session buffer between CreateBulkContext and Done().
		// Use per-call BatchSize, then writer default, then fallback.
		// Per-call BatchSize overrides writer default (from target.chunk_size config).
		batchRows := opts.BatchSize
		if batchRows <= 0 {
			batchRows = w.defaultBatchSize
		}
		if batchRows <= 0 {
			return fmt.Errorf("batch size not configured: set chunk_size in config or enable AI tuning")
		}
		for start := 0; start < len(opts.Rows); start += batchRows {
			end := start + batchRows
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]

			bulk := mssqlConn.CreateBulkContext(ctx, fullTableName, opts.Columns)
			// No TABLOCK — enables parallel BCP writers per table.
			// TABLOCK serializes writes but enables minimal logging.
			// Without it, writes are fully logged but parallelizable.
			bulk.Options.Tablock = false
			bulk.Options.RowsPerBatch = len(subBatch)
			if len(opts.OrderColumns) > 0 {
				orderHints := make([]string, len(opts.OrderColumns))
				for i, col := range opts.OrderColumns {
					orderHints[i] = fmt.Sprintf("[%s] ASC", col)
				}
				bulk.Options.Order = orderHints
			}

			for _, row := range subBatch {
				if err := bulk.AddRow(convertRowForBulkCopy(row, opts.ColumnTypes)); err != nil {
					return fmt.Errorf("adding row: %w", err)
				}
			}

			rowsAffected, err := bulk.Done()
			if err != nil {
				return fmt.Errorf("finalizing bulk insert: %w", err)
			}

			if rowsAffected != int64(len(subBatch)) {
				return fmt.Errorf("bulk insert: expected %d rows, got %d", len(subBatch), rowsAffected)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("bulk copy: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing bulk copy: %w", err)
	}

	return nil
}
