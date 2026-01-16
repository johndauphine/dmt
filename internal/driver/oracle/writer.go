package oracle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/godror/godror"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Writer implements driver.Writer for Oracle Database.
type Writer struct {
	db            *sql.DB
	config        *dbconfig.TargetConfig
	maxConns      int
	rowsPerBatch  int
	sourceType    string
	dialect       *Dialect
	typeMapper    driver.TypeMapper
	oracleVersion string
}

// NewWriter creates a new Oracle writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	idleConns := maxConns / 4
	if idleConns < 1 {
		idleConns = 1
	}
	db.SetMaxIdleConns(idleConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Detect Oracle version
	var version string
	if err := db.QueryRow("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1").Scan(&version); err != nil {
		version = "Oracle (version unknown)"
	}

	logging.Info("Connected to Oracle target: %s:%d/%s (%s)", cfg.Host, cfg.Port, cfg.Database, version)

	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Info("AI Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	return &Writer{
		db:            db,
		config:        cfg,
		maxConns:      maxConns,
		rowsPerBatch:  opts.RowsPerBatch,
		sourceType:    opts.SourceType,
		dialect:       dialect,
		typeMapper:    opts.TypeMapper,
		oracleVersion: version,
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
	return "oracle"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "oracle",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// CreateSchema creates the target schema if it doesn't exist.
// Note: In Oracle, creating a schema typically requires DBA privileges.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil
	}
	// Oracle schema = user. Creating a user requires DBA privileges.
	// For most use cases, the schema should already exist.
	logging.Debug("Oracle schema creation skipped (requires DBA privileges). Using existing schema: %s", schema)
	return nil
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

	_, isAIMapper := w.typeMapper.(*driver.AITypeMapper)
	if isAIMapper {
		logging.Debug("AI Type Mapping: generating DDL for table %s (%d columns)", t.Name, len(t.Columns))
	}

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		// Map type using AI or static mapper
		typeInfo := driver.TypeInfo{
			SourceDBType: w.sourceType,
			TargetDBType: "oracle",
			DataType:     col.DataType,
			MaxLength:    col.MaxLength,
			Precision:    col.Precision,
			Scale:        col.Scale,
			SampleValues: col.SampleValues,
		}
		oracleType := w.typeMapper.MapType(typeInfo)

		if isAIMapper {
			logging.Debug("AI Type Mapping: %s.%s: %s(%d,%d,%d) -> %s",
				t.Name, col.Name, col.DataType, col.MaxLength, col.Precision, col.Scale, oracleType)
		}

		sb.WriteString(fmt.Sprintf("    %s %s", w.dialect.QuoteIdentifier(col.Name), oracleType))

		// Handle identity columns (Oracle 12c+)
		if col.IsIdentity {
			sb.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
		}

		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		}
	}

	// Add PRIMARY KEY constraint
	if len(t.PrimaryKey) > 0 {
		pkCols := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			pkCols[i] = w.dialect.QuoteIdentifier(col)
		}
		// Generate short PK name
		pkName := truncateIdentifier(fmt.Sprintf("pk_%s", t.Name), 30)
		sb.WriteString(fmt.Sprintf(",\n    CONSTRAINT %s PRIMARY KEY (%s)",
			w.dialect.QuoteIdentifier(pkName), strings.Join(pkCols, ", ")))
	}

	sb.WriteString("\n)")

	return sb.String()
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE",
		w.dialect.QualifyTable(schema, table)))
	if err != nil {
		// ORA-00942: table or view does not exist - ignore this error
		if strings.Contains(err.Error(), "ORA-00942") {
			return nil
		}
		return err
	}
	return nil
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s",
		w.dialect.QualifyTable(schema, table)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	schemaName := strings.ToUpper(schema)
	if schemaName == "" {
		schemaName = strings.ToUpper(w.config.User)
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM ALL_TABLES
		WHERE OWNER = :1 AND TABLE_NAME = :2
	`, schemaName, strings.ToUpper(table)).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// SetTableLogged is a no-op for Oracle (no unlogged tables like PostgreSQL).
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// CreatePrimaryKey is a no-op because PK is created with the table.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key constraint.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	schemaName := strings.ToUpper(schema)
	if schemaName == "" {
		schemaName = strings.ToUpper(w.config.User)
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM ALL_CONSTRAINTS
		WHERE OWNER = :1 AND TABLE_NAME = :2 AND CONSTRAINT_TYPE = 'P'
	`, schemaName, strings.ToUpper(table)).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// GetRowCount returns the row count for a table.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}
	return w.GetRowCountExact(ctx, schema, table)
}

// GetRowCountFast returns an approximate row count using statistics.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	schemaName := strings.ToUpper(schema)
	if schemaName == "" {
		schemaName = strings.ToUpper(w.config.User)
	}

	var count sql.NullInt64
	err := w.db.QueryRowContext(ctx, `
		SELECT NUM_ROWS FROM ALL_TABLES
		WHERE OWNER = :1 AND TABLE_NAME = :2
	`, schemaName, strings.ToUpper(table)).Scan(&count)
	if err != nil || !count.Valid {
		return 0, err
	}
	return count.Int64, nil
}

// GetRowCountExact returns the exact row count using COUNT(*).
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s",
		w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ResetSequence resets the identity sequence to max value + 1.
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

	schemaName := strings.ToUpper(schema)
	if schemaName == "" {
		schemaName = strings.ToUpper(w.config.User)
	}

	// For Oracle 12c+ IDENTITY columns, find associated sequence
	var seqName sql.NullString
	err := w.db.QueryRowContext(ctx, `
		SELECT SEQUENCE_NAME
		FROM ALL_TAB_IDENTITY_COLS
		WHERE OWNER = :1 AND TABLE_NAME = :2 AND COLUMN_NAME = :3
	`, schemaName, strings.ToUpper(t.Name), strings.ToUpper(identityCol)).Scan(&seqName)

	if err != nil || !seqName.Valid {
		return nil // No identity sequence found
	}

	// Get max value
	var maxVal sql.NullInt64
	err = w.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MAX(%s) FROM %s",
			w.dialect.QuoteIdentifier(identityCol),
			w.dialect.QualifyTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if !maxVal.Valid || maxVal.Int64 == 0 {
		return nil
	}

	// Reset sequence using ALTER SEQUENCE (Oracle 12.2+)
	_, err = w.db.ExecContext(ctx,
		fmt.Sprintf("ALTER SEQUENCE %s.%s RESTART START WITH %d",
			w.dialect.QuoteIdentifier(schemaName),
			w.dialect.QuoteIdentifier(seqName.String),
			maxVal.Int64+1))

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

	idxName := truncateIdentifier(fmt.Sprintf("idx_%s_%s", t.Name, idx.Name), 30)

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

	fkName := truncateIdentifier(fmt.Sprintf("fk_%s_%s", t.Name, fk.Name), 30)

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s (%s)
		ON DELETE %s
	`, w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(fkName),
		strings.Join(cols, ", "),
		w.dialect.QualifyTable(targetSchema, fk.RefTable),
		strings.Join(refCols, ", "),
		onDelete)

	_, err := w.db.ExecContext(ctx, sqlStmt)
	return err
}

func mapReferentialAction(action string) string {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL", "SET NULL":
		return "SET NULL"
	default:
		return "NO ACTION"
	}
}

// CreateCheckConstraint creates a check constraint.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	definition := convertCheckDefinition(chk.Definition)

	chkName := truncateIdentifier(fmt.Sprintf("chk_%s_%s", t.Name, chk.Name), 30)

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		CHECK (%s)
	`, w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(chkName),
		definition)

	_, err := w.db.ExecContext(ctx, sqlStmt)
	return err
}

func convertCheckDefinition(def string) string {
	result := def

	// Convert MySQL backtick identifiers to Oracle double quotes
	for {
		start := strings.Index(result, "`")
		if start == -1 {
			break
		}
		end := strings.Index(result[start+1:], "`")
		if end == -1 {
			break
		}
		colName := result[start+1 : start+1+end]
		result = result[:start] + `"` + colName + `"` + result[start+end+2:]
	}

	// Convert SQL Server brackets to Oracle double quotes
	result = strings.ReplaceAll(result, "[", `"`)
	result = strings.ReplaceAll(result, "]", `"`)

	// Convert common functions
	result = strings.ReplaceAll(result, "GETDATE()", "SYSDATE")
	result = strings.ReplaceAll(result, "NOW()", "SYSDATE")
	result = strings.ReplaceAll(result, "CURRENT_TIMESTAMP", "SYSTIMESTAMP")

	return result
}

// WriteBatch writes a batch of rows using INSERT ALL.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	quotedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	fullTableName := w.dialect.QualifyTable(opts.Schema, opts.Table)

	// Process in batches
	batchSize := w.rowsPerBatch
	if batchSize <= 0 {
		batchSize = 500 // Smaller default for INSERT ALL
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

	// Build placeholders for single-row INSERT
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, colList, strings.Join(placeholders, ", "))

	// Use prepared statement with transaction for better performance
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		args := convertRowValues(row)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	return tx.Commit()
}

// UpsertBatch performs upsert using MERGE statement.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	// Process in batches
	batchSize := w.rowsPerBatch
	if batchSize <= 0 {
		batchSize = 200 // Smaller for MERGE complexity
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.mergeBatch(ctx, opts.Schema, opts.Table, opts.Columns, opts.PKColumns, batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) mergeBatch(ctx context.Context, schema, table string, columns, pkColumns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	fullTableName := w.dialect.QualifyTable(schema, table)

	// Build MERGE statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("MERGE INTO %s tgt\n", fullTableName))
	sb.WriteString("USING (\n")

	// Build inline view with UNION ALL
	paramIdx := 1
	args := make([]any, 0, len(rows)*len(columns))

	for i, row := range rows {
		if i > 0 {
			sb.WriteString("    UNION ALL\n")
		}

		sb.WriteString("    SELECT ")
		placeholders := make([]string, len(columns))
		for j, col := range columns {
			placeholders[j] = fmt.Sprintf(":%d AS %s", paramIdx, w.dialect.QuoteIdentifier(col))
			paramIdx++
		}
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteString(" FROM DUAL\n")

		args = append(args, convertRowValues(row)...)
	}

	sb.WriteString(") src\n")

	// Build ON clause (match on PK columns)
	onClauses := make([]string, len(pkColumns))
	for i, pk := range pkColumns {
		qPK := w.dialect.QuoteIdentifier(pk)
		onClauses[i] = fmt.Sprintf("tgt.%s = src.%s", qPK, qPK)
	}
	sb.WriteString(fmt.Sprintf("ON (%s)\n", strings.Join(onClauses, " AND ")))

	// Build UPDATE clause (for non-PK columns)
	pkSet := make(map[string]bool)
	for _, pk := range pkColumns {
		pkSet[strings.ToUpper(pk)] = true
	}

	var updateClauses []string
	for _, col := range columns {
		if !pkSet[strings.ToUpper(col)] {
			qCol := w.dialect.QuoteIdentifier(col)
			updateClauses = append(updateClauses, fmt.Sprintf("tgt.%s = src.%s", qCol, qCol))
		}
	}

	if len(updateClauses) > 0 {
		sb.WriteString("WHEN MATCHED THEN\n")
		sb.WriteString(fmt.Sprintf("    UPDATE SET %s\n", strings.Join(updateClauses, ", ")))
	}

	// Build INSERT clause
	quotedCols := make([]string, len(columns))
	srcCols := make([]string, len(columns))
	for i, col := range columns {
		qCol := w.dialect.QuoteIdentifier(col)
		quotedCols[i] = qCol
		srcCols[i] = "src." + qCol
	}

	sb.WriteString("WHEN NOT MATCHED THEN\n")
	sb.WriteString(fmt.Sprintf("    INSERT (%s)\n", strings.Join(quotedCols, ", ")))
	sb.WriteString(fmt.Sprintf("    VALUES (%s)", strings.Join(srcCols, ", ")))

	_, err := w.db.ExecContext(ctx, sb.String(), args...)
	return err
}

func convertRowValues(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		switch val := v.(type) {
		case []byte:
			// Keep binary data as-is
			result[i] = val
		case bool:
			// Oracle uses 1/0 for boolean
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

// ExecRaw executes raw SQL.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a query and scans the result.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}

// truncateIdentifier truncates an identifier to maxLen chars, using hash for uniqueness if needed.
func truncateIdentifier(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}

	// Use hash for uniqueness
	hash := sha256.Sum256([]byte(name))
	hashStr := fmt.Sprintf("%x", hash[:6])

	// Keep prefix + hash
	prefix := name[:maxLen-len(hashStr)-1]
	return prefix + "_" + hashStr
}
