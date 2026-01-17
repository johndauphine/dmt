package oracle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/godror/godror"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Writer implements driver.Writer for Oracle Database.
type Writer struct {
	db              *sql.DB
	config          *dbconfig.TargetConfig
	maxConns        int
	oracleBatchSize int
	sourceType      string
	dialect         *Dialect
	typeMapper      driver.TypeMapper
	tableMapper     driver.TableTypeMapper // Table-level DDL generation
	oracleVersion   string
	dbContext       *driver.DatabaseContext // Cached database context for AI
}

// NewWriter creates a new Oracle writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool - more connections for parallel writes
	db.SetMaxOpenConns(maxConns * 2)
	db.SetMaxIdleConns(maxConns)
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

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Info("AI Table-Level Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	w := &Writer{
		db:              db,
		config:          cfg,
		maxConns:        maxConns,
		oracleBatchSize: opts.OracleBatchSize,
		sourceType:      opts.SourceType,
		dialect:         dialect,
		typeMapper:      opts.TypeMapper,
		tableMapper:     tableMapper,
		oracleVersion:   version,
	}

	// Gather database context for AI (best effort - don't fail if metadata unavailable)
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// gatherDatabaseContext collects Oracle database metadata for AI context.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	ctx := &driver.DatabaseContext{
		Version:                  w.oracleVersion,
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "upper",
		CaseSensitiveIdentifiers: false, // Oracle folds to uppercase
		CaseSensitiveData:        true,  // Default binary comparison
		MaxIdentifierLength:      128,   // Oracle 12.2+, 30 for older
		VarcharSemantics:         "byte",
		BytesPerChar:             4, // AL32UTF8 max
	}

	// Query NLS parameters for character set info
	rows, err := w.db.Query(`
		SELECT PARAMETER, VALUE FROM NLS_DATABASE_PARAMETERS
		WHERE PARAMETER IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET', 'NLS_SORT', 'NLS_COMP', 'NLS_LENGTH_SEMANTICS')
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var param, value string
			if rows.Scan(&param, &value) == nil {
				switch param {
				case "NLS_CHARACTERSET":
					ctx.Charset = value
					// Determine bytes per char based on charset
					if strings.Contains(value, "UTF8") || strings.Contains(value, "AL32UTF8") {
						ctx.BytesPerChar = 4
						ctx.Encoding = "UTF-8"
					} else if strings.Contains(value, "UTF16") {
						ctx.BytesPerChar = 2
						ctx.Encoding = "UTF-16"
					} else {
						ctx.BytesPerChar = 1
						ctx.Encoding = value
					}
				case "NLS_NCHAR_CHARACTERSET":
					ctx.NationalCharset = value
				case "NLS_SORT":
					ctx.Collation = value
					// BINARY = case-sensitive, others typically case-insensitive
					ctx.CaseSensitiveData = strings.Contains(strings.ToUpper(value), "BINARY")
				case "NLS_COMP":
					// BINARY = case-sensitive comparisons
					if strings.ToUpper(value) != "BINARY" {
						ctx.CaseSensitiveData = false
					}
				case "NLS_LENGTH_SEMANTICS":
					ctx.VarcharSemantics = strings.ToLower(value)
				}
			}
		}
	}

	// Query max string size (extended = 32767, standard = 4000)
	var maxStringSize string
	if w.db.QueryRow("SELECT VALUE FROM V$PARAMETER WHERE NAME = 'max_string_size'").Scan(&maxStringSize) == nil {
		if strings.ToUpper(maxStringSize) == "EXTENDED" {
			ctx.MaxVarcharLength = 32767
			ctx.Features = append(ctx.Features, "EXTENDED_VARCHAR")
		} else {
			ctx.MaxVarcharLength = 4000
		}
	} else {
		ctx.MaxVarcharLength = 4000 // Default
	}

	// Parse version for major version number using regex
	// Matches patterns like "Oracle 23c", "Oracle Database 19c", "Release 21.0.0", etc.
	versionRegex := regexp.MustCompile(`(?:Oracle[^0-9]*|Release\s+)(\d+)`)
	if matches := versionRegex.FindStringSubmatch(w.oracleVersion); len(matches) > 1 {
		if majorVer, err := strconv.Atoi(matches[1]); err == nil {
			ctx.MajorVersion = majorVer
		}
	}

	// Set identifier length based on major version
	// Oracle 12.2+ supports 128-character identifiers
	if ctx.MajorVersion >= 12 {
		ctx.MaxIdentifierLength = 128
		// Check for 12.1 which has 30-char limit
		if ctx.MajorVersion == 12 && !strings.Contains(w.oracleVersion, "12.2") && !strings.Contains(w.oracleVersion, "12c Release 2") {
			ctx.MaxIdentifierLength = 30
		}
	} else if ctx.MajorVersion > 0 {
		ctx.MaxIdentifierLength = 30
	} else {
		// Unknown version - default to conservative settings
		ctx.MajorVersion = 12
		ctx.MaxIdentifierLength = 30
		logging.Warn("Could not parse Oracle version from '%s', defaulting to version 12 with 30-char identifiers", w.oracleVersion)
	}

	// Add version-specific features
	if ctx.MajorVersion >= 23 {
		ctx.Features = append(ctx.Features, "BOOLEAN_TYPE", "JSON_RELATIONAL_DUALITY")
	}

	// Standard Oracle features
	ctx.Features = append(ctx.Features, "CLOB", "BLOB", "NUMBER", "TIMESTAMP_WITH_TIMEZONE")

	// Build notes
	var notes []string
	if ctx.VarcharSemantics == "byte" {
		notes = append(notes, "VARCHAR2 uses BYTE semantics - use VARCHAR2(n CHAR) for character lengths")
	}
	if ctx.BytesPerChar > 1 {
		notes = append(notes, fmt.Sprintf("Multi-byte charset: up to %d bytes per character", ctx.BytesPerChar))
	}
	ctx.Notes = strings.Join(notes, "; ")

	logging.Debug("Oracle context: charset=%s, varchar_semantics=%s, max_identifier=%d, max_varchar=%d",
		ctx.Charset, ctx.VarcharSemantics, ctx.MaxIdentifierLength, ctx.MaxVarcharLength)

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

// CreateTableWithOptions creates a table with options using AI-generated DDL.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	// Use table-level AI DDL generation with full database context
	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  "oracle",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext, // Passed from migration coordinator
		TargetContext: w.dbContext,        // Oracle-specific context
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

// GetDatabaseContext returns the database context for AI type mapping.
func (w *Writer) GetDatabaseContext() *driver.DatabaseContext {
	return w.dbContext
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

	// Reset sequence using ALTER SEQUENCE RESTART (Oracle 12.2+)
	// For Oracle 12.1, RESTART is not supported - we use the INCREMENT BY trick
	qualifiedSeq := fmt.Sprintf("%s.%s",
		w.dialect.QuoteIdentifier(schemaName),
		w.dialect.QuoteIdentifier(seqName.String))

	nextVal := maxVal.Int64 + 1

	_, err = w.db.ExecContext(ctx,
		fmt.Sprintf("ALTER SEQUENCE %s RESTART START WITH %d", qualifiedSeq, nextVal))

	if err != nil {
		// Oracle 12.1 doesn't support RESTART - use INCREMENT BY workaround
		// Note: This workaround has a small race condition window where other sessions
		// could call NEXTVAL with the temporary INCREMENT BY value. This is acceptable
		// because sequence reset runs during migration setup before concurrent access.

		// Get current sequence value
		var currVal int64
		if err2 := w.db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT %s.CURRVAL FROM DUAL", qualifiedSeq)).Scan(&currVal); err2 != nil {
			// CURRVAL not available (sequence not used yet), try NEXTVAL
			if err3 := w.db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT %s.NEXTVAL FROM DUAL", qualifiedSeq)).Scan(&currVal); err3 != nil {
				logging.Debug("Cannot reset identity sequence %s: %v", seqName.String, err)
				return nil // Non-fatal, sequence will just continue from where it was
			}
		}

		// Calculate increment needed to jump to target value
		increment := nextVal - currVal - 1
		if increment != 0 {
			// Temporarily change INCREMENT BY, get NEXTVAL, then restore
			if _, err := w.db.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE %s INCREMENT BY %d", qualifiedSeq, increment)); err != nil {
				logging.Warn("Failed to alter sequence %s increment: %v", seqName.String, err)
				return nil
			}
			if _, err := w.db.ExecContext(ctx, fmt.Sprintf("SELECT %s.NEXTVAL FROM DUAL", qualifiedSeq)); err != nil {
				logging.Warn("Failed to advance sequence %s: %v", seqName.String, err)
				// Try to restore INCREMENT BY 1 before returning
				w.db.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE %s INCREMENT BY 1", qualifiedSeq))
				return nil
			}
			if _, err := w.db.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE %s INCREMENT BY 1", qualifiedSeq)); err != nil {
				logging.Warn("Failed to restore sequence %s increment to 1: %v", seqName.String, err)
				return nil
			}
		}
		logging.Debug("Reset identity sequence %s to %d using INCREMENT BY workaround", seqName.String, nextVal)
	}

	return nil
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

	// Process in batches - larger batches perform better with godror.Batch
	batchSize := w.oracleBatchSize
	if batchSize <= 0 {
		batchSize = 5000 // Optimal batch size for Oracle bulk inserts
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

	// Use godror.Batch for efficient bulk inserts with native array binding
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, colList, strings.Join(placeholders, ", "))

	stmt, err := w.db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	// godror.Batch limit from config (default 5000 provides optimal throughput)
	batchLimit := w.oracleBatchSize
	if batchLimit <= 0 {
		batchLimit = 5000
	}
	batch := &godror.Batch{
		Stmt:  stmt,
		Limit: batchLimit,
	}

	for _, row := range rows {
		args := convertRowValues(row)
		if err := batch.Add(ctx, args...); err != nil {
			return fmt.Errorf("batch add: %w", err)
		}
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("batch flush: %w", err)
	}

	return nil
}

func (w *Writer) insertBatchRowByRow(ctx context.Context, tableName, colList string, columns []string, rows [][]any) error {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, colList, strings.Join(placeholders, ", "))

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
	batchSize := w.oracleBatchSize
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
