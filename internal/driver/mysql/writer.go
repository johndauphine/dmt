package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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

// Writer implements driver.Writer for MySQL/MariaDB.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	defaultBatchSize   int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dropDDLMapper      driver.TableDropDDLMapper    // AI-driven DROP TABLE DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI
	isMariaDB          bool
}

// NewWriter creates a new MySQL/MariaDB writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// go-sql-driver/mysql can echo the DSN in its open/ping errors —
	// see #231. ScrubError strips the password.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("opening connection: %w", err))
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	// Detect MySQL vs MariaDB
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("querying version: %w", err)
	}
	isMariaDB := strings.Contains(strings.ToLower(version), "mariadb")

	dbType := "MySQL"
	if isMariaDB {
		dbType = "MariaDB"
	}
	logging.Debug("Connected to %s target: %s:%d/%s (%s)", dbType, cfg.Host, cfg.Port, cfg.Database, version)

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

	// Check if type mapper implements drop DDL mapper
	dropDDLMapper, _ := opts.TypeMapper.(driver.TableDropDDLMapper)

	w := &Writer{
		db:                 db,
		config:             cfg,
		maxConns:           maxConns,
		defaultBatchSize:   opts.BatchSize,
		sourceType:         opts.SourceType,
		dialect:            dialect,
		typeMapper:         opts.TypeMapper,
		tableMapper:        tableMapper,
		finalizationMapper: finalizationMapper,
		dropDDLMapper:      dropDDLMapper,
		isMariaDB:          isMariaDB,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext(version)

	return w, nil
}

// gatherDatabaseContext collects MySQL/MariaDB database metadata for AI context.
func (w *Writer) gatherDatabaseContext(version string) *driver.DatabaseContext {
	dbCtx := &driver.DatabaseContext{
		Version:                  version,
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "preserve",
		CaseSensitiveIdentifiers: false, // Depends on OS/config
		MaxIdentifierLength:      64,
		VarcharSemantics:         "char", // utf8mb4 VARCHAR is characters
		BytesPerChar:             4,      // utf8mb4 max
	}

	// Parse version for major version number using regex
	// Matches patterns like "8.0.32", "5.7.44", "10.11.6-MariaDB", etc.
	versionRegex := regexp.MustCompile(`^(\d+)\.`)
	if matches := versionRegex.FindStringSubmatch(version); len(matches) > 1 {
		if majorVer, err := strconv.Atoi(matches[1]); err == nil {
			dbCtx.MajorVersion = majorVer
		}
	}

	if w.isMariaDB {
		dbCtx.StorageEngine = "MariaDB"
	}

	// Log warning if version couldn't be parsed
	if dbCtx.MajorVersion == 0 {
		logging.Warn("Could not parse MySQL/MariaDB version from '%s', version-specific features may not be detected", version)
	}

	// Query character set and collation
	var charsetVar, collationVar string
	if w.db.QueryRow("SELECT @@character_set_database, @@collation_database").Scan(&charsetVar, &collationVar) == nil {
		dbCtx.Charset = charsetVar
		dbCtx.Collation = collationVar

		// Determine bytes per char based on charset
		switch {
		case strings.HasPrefix(charsetVar, "utf8mb4"):
			dbCtx.BytesPerChar = 4
			dbCtx.Encoding = "UTF-8"
		case strings.HasPrefix(charsetVar, "utf8"):
			dbCtx.BytesPerChar = 3
			dbCtx.Encoding = "UTF-8 (3-byte)"
		case charsetVar == "latin1":
			dbCtx.BytesPerChar = 1
			dbCtx.Encoding = "Latin1"
		default:
			dbCtx.BytesPerChar = 1
			dbCtx.Encoding = charsetVar
		}

		// Parse collation for case sensitivity
		upperCollation := strings.ToUpper(collationVar)
		if strings.Contains(upperCollation, "_CS") || strings.Contains(upperCollation, "_BIN") {
			dbCtx.CaseSensitiveData = true
		} else if strings.Contains(upperCollation, "_CI") {
			dbCtx.CaseSensitiveData = false
		}
	}

	// Query lower_case_table_names for identifier case sensitivity
	// Use -1 as sentinel to distinguish "not queried" from actual value of 0
	lowerCaseTableNames := -1
	if w.db.QueryRow("SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames) == nil {
		switch lowerCaseTableNames {
		case 0:
			dbCtx.CaseSensitiveIdentifiers = true
			dbCtx.IdentifierCase = "preserve"
		case 1:
			dbCtx.CaseSensitiveIdentifiers = false
			dbCtx.IdentifierCase = "lower"
		case 2:
			dbCtx.CaseSensitiveIdentifiers = false
			dbCtx.IdentifierCase = "preserve"
		}
	}

	// Query default storage engine
	var engine string
	if w.db.QueryRow("SELECT @@default_storage_engine").Scan(&engine) == nil {
		dbCtx.StorageEngine = engine
	}

	// Max varchar length depends on charset
	// utf8mb4: 16383 chars (65535 bytes / 4)
	// utf8: 21844 chars (65535 bytes / 3)
	// latin1: 65535 chars
	if dbCtx.BytesPerChar > 0 {
		dbCtx.MaxVarcharLength = 65535 / dbCtx.BytesPerChar
	} else {
		// Fallback to safe default if charset detection failed
		dbCtx.MaxVarcharLength = 16383 // Assume utf8mb4 (most restrictive)
	}

	// Standard MySQL features
	dbCtx.Features = []string{"JSON", "SPATIAL", "FULLTEXT"}
	if w.isMariaDB {
		dbCtx.Features = append(dbCtx.Features, "SEQUENCES", "SYSTEM_VERSIONING")
	}
	if dbCtx.MajorVersion >= 8 || (w.isMariaDB && dbCtx.MajorVersion >= 10) {
		dbCtx.Features = append(dbCtx.Features, "CTE", "WINDOW_FUNCTIONS")
	}

	// Log with appropriate handling of sentinel value
	if lowerCaseTableNames >= 0 {
		logging.Debug("MySQL context: charset=%s, collation=%s, storage_engine=%s, lower_case=%d",
			dbCtx.Charset, dbCtx.Collation, dbCtx.StorageEngine, lowerCaseTableNames)
	} else {
		logging.Debug("MySQL context: charset=%s, collation=%s, storage_engine=%s, lower_case=unknown",
			dbCtx.Charset, dbCtx.Collation, dbCtx.StorageEngine)
	}

	return dbCtx
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
	return "mysql"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "mysql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	return shared.ExecRaw(ctx, w.db, query, args...)
}

// QueryRowRaw executes a raw SQL query that returns a single row.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return shared.QueryRowRaw(ctx, w.db, query, dest, args...)
}

// GetRowCount returns the row count for a table.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return shared.RowCountWithFallback(
		func() (int64, error) { return w.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return w.GetRowCountExact(ctx, schema, table, false) },
	)
}

// GetRowCountFast returns an approximate row count using system statistics.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var count int64
	err := w.db.QueryRowContext(ctx, `
		SELECT TABLE_ROWS FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// MySQL has no NOLOCK equivalent (uses MVCC); strictConsistency is
// accepted for interface symmetry and ignored here.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, w.db, w.dialect, schema, table)
}

// safeStagingName generates a safe staging table name.
func (w *Writer) safeStagingName(table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	base := fmt.Sprintf("_stg_%s", table)
	maxLen := 60 // MySQL max identifier is 64, leave room for suffix

	if len(base)+len(suffix) > maxLen {
		hash := sha256.Sum256([]byte(table))
		base = fmt.Sprintf("_stg_%x", hash[:8])
	}
	return base + suffix
}

// convertRowValues converts row values to MySQL-compatible types.
func convertRowValues(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		switch val := v.(type) {
		case []byte:
			// Keep binary data as-is for MySQL
			result[i] = val
		case bool:
			// MySQL uses 1/0 for boolean
			if val {
				result[i] = 1
			} else {
				result[i] = 0
			}
		default:
			result[i] = v
		}
	}
	return result
}

// WriteBatch writes a batch of rows using multi-row INSERT.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	// Build column list
	quotedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	fullTableName := w.dialect.QualifyTable(opts.Schema, opts.Table)

	// Resume-safe path for ROW_NUMBER-paged tables: append
	// ON DUPLICATE KEY UPDATE <pk> = <pk> so replayed rows that already
	// exist become no-ops (#227). Deliberately NOT INSERT IGNORE — that
	// also silently masks data-conversion errors, which we don't want.
	var dupSuffix string
	if opts.IdempotentOnDup {
		if len(opts.PKColumns) == 0 {
			return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
		}
		quotedPK := w.dialect.QuoteIdentifier(opts.PKColumns[0])
		dupSuffix = " ON DUPLICATE KEY UPDATE " + quotedPK + " = " + quotedPK
	}

	// Process in batches to avoid max_allowed_packet limits and placeholder limits.
	// Per-call BatchSize (from AI tuner) takes priority over the writer's default.
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // Fallback default
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.insertBatch(ctx, fullTableName, colList, opts.Columns, batch, dupSuffix); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) insertBatch(ctx context.Context, tableName, colList string, columns []string, rows [][]any, dupSuffix string) error {
	if len(rows) == 0 {
		return nil
	}

	query := buildInsertSQL(tableName, colList, len(columns), len(rows), dupSuffix)

	// Flatten all values
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, convertRowValues(row)...)
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

// buildInsertSQL returns a multi-row INSERT statement, optionally followed by
// dupSuffix (e.g. " ON DUPLICATE KEY UPDATE id = id" for the #227
// IdempotentOnDup path). Extracted so the resume-safe shape is unit-testable
// without a real MySQL connection.
func buildInsertSQL(tableName, colList string, numCols, numRows int, dupSuffix string) string {
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	rowPlaceholders := make([]string, numRows)
	for i := range rowPlaceholders {
		rowPlaceholders[i] = rowPlaceholder
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
		tableName, colList, strings.Join(rowPlaceholders, ", "), dupSuffix)
}

// UpsertBatch performs upsert using INSERT ... ON DUPLICATE KEY UPDATE.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	// Build column list
	quotedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	fullTableName := w.dialect.QualifyTable(opts.Schema, opts.Table)

	// Build UPDATE clause for non-PK columns
	pkSet := make(map[string]bool)
	for _, pk := range opts.PKColumns {
		pkSet[strings.ToLower(pk)] = true
	}

	var updateClauses []string
	for _, col := range opts.Columns {
		if !pkSet[strings.ToLower(col)] {
			qCol := w.dialect.QuoteIdentifier(col)
			// Use new.col syntax (MySQL 8.0.19+) instead of deprecated VALUES(col)
			updateClauses = append(updateClauses, fmt.Sprintf("%s = new.%s", qCol, qCol))
		}
	}

	updateClause := ""
	if len(updateClauses) > 0 {
		updateClause = " ON DUPLICATE KEY UPDATE " + strings.Join(updateClauses, ", ")
	}

	// Process in batches.
	// Per-call BatchSize (from AI tuner) takes priority over the writer's default.
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.upsertBatch(ctx, fullTableName, colList, opts.Columns, batch, updateClause); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) upsertBatch(ctx context.Context, tableName, colList string, columns []string, rows [][]any, updateClause string) error {
	if len(rows) == 0 {
		return nil
	}

	// Build placeholder row
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	// Build all row placeholders
	rowPlaceholders := make([]string, len(rows))
	for i := range rows {
		rowPlaceholders[i] = rowPlaceholder
	}

	// Use AS new alias (MySQL 8.0.19+) for the new row reference in ON DUPLICATE KEY UPDATE
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new%s",
		tableName, colList, strings.Join(rowPlaceholders, ", "), updateClause)

	// Flatten all values
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, convertRowValues(row)...)
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}
