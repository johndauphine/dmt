package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
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

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Info("AI Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	return &Writer{
		db:           db,
		config:       cfg,
		maxConns:     maxConns,
		rowsPerBatch: opts.RowsPerBatch,
		sourceType:   opts.SourceType,
		dialect:      dialect,
		typeMapper:   opts.TypeMapper,
		isMariaDB:    isMariaDB,
	}, nil
}

// Close closes all connections.
func (w *Writer) Close() {
	w.db.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
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

// CreateTableWithOptions creates a table with options.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	ddl := w.generateDDL(t, targetSchema)

	_, err := w.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}

	return nil
}

func (w *Writer) generateDDL(t *driver.Table, targetSchema string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", w.dialect.QualifyTable(targetSchema, t.Name)))

	// Check if using AI mapper for logging
	_, isAIMapper := w.typeMapper.(*driver.AITypeMapper)
	if isAIMapper {
		logging.Debug("AI Type Mapping: generating DDL for table %s (%d columns)", t.Name, len(t.Columns))
	}

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		// Map type using configured mapper (static or AI)
		typeInfo := driver.TypeInfo{
			SourceDBType: w.sourceType,
			TargetDBType: "mysql",
			DataType:     col.DataType,
			MaxLength:    col.MaxLength,
			Precision:    col.Precision,
			Scale:        col.Scale,
			SampleValues: col.SampleValues,
		}
		mysqlType := w.typeMapper.MapType(typeInfo)

		// Log AI type mappings
		if isAIMapper {
			logging.Debug("AI Type Mapping: %s.%s: %s(%d,%d,%d) -> %s",
				t.Name, col.Name, col.DataType, col.MaxLength, col.Precision, col.Scale, mysqlType)
		}

		sb.WriteString(fmt.Sprintf("    %s %s", w.dialect.QuoteIdentifier(col.Name), mysqlType))

		if col.IsIdentity {
			sb.WriteString(" AUTO_INCREMENT")
		}

		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		} else {
			sb.WriteString(" NULL")
		}
	}

	// Add PRIMARY KEY constraint
	if len(t.PrimaryKey) > 0 {
		pkCols := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			pkCols[i] = w.dialect.QuoteIdentifier(col)
		}
		sb.WriteString(fmt.Sprintf(",\n    PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	sb.WriteString("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")

	return sb.String()
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
			updateClauses = append(updateClauses, fmt.Sprintf("%s = VALUES(%s)", qCol, qCol))
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

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
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
