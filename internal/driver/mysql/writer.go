package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Writer implements driver.Writer for MySQL/MariaDB.
type Writer struct {
	db           *sql.DB
	config       *dbconfig.TargetConfig
	maxConns     int
	rowsPerBatch int
	sourceType   string
	dialect      *Dialect
	typeMapper   driver.TypeMapper
	tableMapper  driver.TableTypeMapper  // Table-level DDL generation
	dbContext    *driver.DatabaseContext // Cached database context for AI
	isMariaDB    bool
}

// NewWriter creates a new MySQL/MariaDB writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
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
	logging.Info("Connected to %s target: %s:%d/%s (%s)", dbType, cfg.Host, cfg.Port, cfg.Database, version)

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

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Info("AI Table-Level Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	w := &Writer{
		db:           db,
		config:       cfg,
		maxConns:     maxConns,
		rowsPerBatch: opts.RowsPerBatch,
		sourceType:   opts.SourceType,
		dialect:      dialect,
		typeMapper:   opts.TypeMapper,
		tableMapper:  tableMapper,
		isMariaDB:    isMariaDB,
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

// CreateSchema creates the target schema (database) if it doesn't exist.
// Note: In MySQL, schema = database.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil // Using default database
	}
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", w.dialect.QuoteIdentifier(schema)))
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
		TargetDBType:  "mysql",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}

	resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("AI DDL generation failed for table %s: %w", t.FullName(), err)
	}

	logging.Debug("AI generated DDL for %s:\n%s", t.FullName(), resp.CreateTableDDL)

	// Log column type mappings
	for colName, colType := range resp.ColumnTypes {
		logging.Debug("  Column %s -> %s", colName, colType)
	}

	_, err = w.db.ExecContext(ctx, resp.CreateTableDDL)
	if err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, resp.CreateTableDDL)
	}

	return nil
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// SetTableLogged is a no-op for MySQL (no unlogged tables).
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// CreatePrimaryKey is a no-op because PK is created with the table.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key constraint.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// GetRowCount returns the row count for a table.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	return w.GetRowCountExact(ctx, schema, table)
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
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ResetSequence resets AUTO_INCREMENT to max value.
func (w *Writer) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
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

	var maxVal int64
	err := w.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
			w.dialect.QuoteIdentifier(identityCol),
			w.dialect.QualifyTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil
	}

	_, err = w.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d",
			w.dialect.QualifyTable(schema, t.Name), maxVal+1))
	return err
}

// CreateIndex creates an index on the target table.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = w.dialect.QuoteIdentifier(col)
	}

	unique := ""
	if idx.IsUnique {
		unique = "UNIQUE "
	}

	idxName := fmt.Sprintf("idx_%s_%s", t.Name, idx.Name)
	if len(idxName) > 64 { // MySQL max identifier length
		idxName = idxName[:64]
	}

	sqlStmt := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique,
		w.dialect.QuoteIdentifier(idxName),
		w.dialect.QualifyTable(targetSchema, t.Name),
		strings.Join(cols, ", "))

	_, err := w.db.ExecContext(ctx, sqlStmt)
	return err
}

// CreateForeignKey creates a foreign key constraint.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	cols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		cols[i] = w.dialect.QuoteIdentifier(col)
	}

	refCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		refCols[i] = w.dialect.QuoteIdentifier(col)
	}

	onDelete := mapReferentialAction(fk.OnDelete)
	onUpdate := mapReferentialAction(fk.OnUpdate)

	fkName := fmt.Sprintf("fk_%s_%s", t.Name, fk.Name)
	if len(fkName) > 64 {
		fkName = fkName[:64]
	}

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s (%s)
		ON DELETE %s
		ON UPDATE %s
	`, w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(fkName),
		strings.Join(cols, ", "),
		w.dialect.QualifyTable(targetSchema, fk.RefTable),
		strings.Join(refCols, ", "),
		onDelete, onUpdate)

	_, err := w.db.ExecContext(ctx, sqlStmt)
	return err
}

// CreateCheckConstraint creates a check constraint.
// Note: MySQL 8.0.16+ supports CHECK constraints, earlier versions ignore them.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	definition := convertCheckDefinition(chk.Definition)

	chkName := fmt.Sprintf("chk_%s_%s", t.Name, chk.Name)
	if len(chkName) > 64 {
		chkName = chkName[:64]
	}

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		CHECK %s
	`, w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(chkName),
		definition)

	_, err := w.db.ExecContext(ctx, sqlStmt)
	return err
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

	// Process in batches to avoid max_allowed_packet limits
	batchSize := w.rowsPerBatch
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.insertBatch(ctx, fullTableName, colList, opts.Columns, batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) insertBatch(ctx context.Context, tableName, colList string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	// Build placeholder row: (?, ?, ?, ...)
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

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName, colList, strings.Join(rowPlaceholders, ", "))

	// Flatten all values
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, convertRowValues(row)...)
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
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

	// Process in batches
	batchSize := w.rowsPerBatch
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

func mapReferentialAction(action string) string {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL", "SET NULL":
		return "SET NULL"
	case "SET_DEFAULT", "SET DEFAULT":
		return "SET DEFAULT"
	case "RESTRICT":
		return "RESTRICT"
	case "NO_ACTION", "NO ACTION":
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

func convertCheckDefinition(def string) string {
	result := def

	// Convert double-quoted identifiers to backticks
	for {
		start := strings.Index(result, `"`)
		if start == -1 {
			break
		}
		end := strings.Index(result[start+1:], `"`)
		if end == -1 {
			break
		}
		colName := result[start+1 : start+1+end]
		result = result[:start] + "`" + colName + "`" + result[start+end+2:]
	}

	// Convert PostgreSQL/MSSQL functions to MySQL equivalents
	result = strings.ReplaceAll(result, "GETDATE()", "NOW()")
	result = strings.ReplaceAll(result, "getdate()", "NOW()")
	result = strings.ReplaceAll(result, "CURRENT_TIMESTAMP", "NOW()")
	result = strings.ReplaceAll(result, "current_timestamp", "NOW()")

	return result
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a raw SQL query that returns a single row.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}
