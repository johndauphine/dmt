package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// sqliteMaxVariables is the compile-time SQLITE_MAX_VARIABLE_NUMBER on
// modernc.org/sqlite (matches mainline SQLite ≥3.32). One placeholder per
// cell — multi-row INSERTs must chunk so numCols * rowsPerStatement stays
// strictly below this.
const sqliteMaxVariables = 32766

// Writer implements driver.Writer for SQLite.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	defaultBatchSize   int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper
	finalizationMapper driver.FinalizationDDLMapper
	dropDDLMapper      driver.TableDropDDLMapper
	dbContext          *driver.DatabaseContext
}

// NewWriter creates a new SQLite writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite connection: %w", err)
	}

	// Single-writer constraint — cap at one open connection so multiple
	// concurrent WriteBatch calls serialize through the pool rather than
	// hitting SQLITE_BUSY at the file lock.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging sqlite database: %w", err)
	}

	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper")
	}
	driver.LogTypeMapperInit(opts.TypeMapper)
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)
	dropDDLMapper, _ := opts.TypeMapper.(driver.TableDropDDLMapper)

	var version string
	db.QueryRow("SELECT sqlite_version()").Scan(&version)
	logging.Debug("Connected to SQLite target: %s (%s)", cfg.Database, version)

	w := &Writer{
		db:                 db,
		config:             cfg,
		maxConns:           1,
		defaultBatchSize:   opts.BatchSize,
		sourceType:         opts.SourceType,
		dialect:            dialect,
		typeMapper:         opts.TypeMapper,
		tableMapper:        tableMapper,
		finalizationMapper: finalizationMapper,
		dropDDLMapper:      dropDDLMapper,
	}
	w.dbContext = &driver.DatabaseContext{
		Version:                  "SQLite " + version,
		DatabaseName:             cfg.Database,
		IdentifierCase:           "preserve",
		CaseSensitiveIdentifiers: false,
		Charset:                  "UTF-8",
		Encoding:                 "UTF-8",
		MaxIdentifierLength:      0, // no documented limit
		VarcharSemantics:         "char",
		BytesPerChar:             4,
		Features:                 []string{"WAL", "FTS5", "JSON1"},
	}
	return w, nil
}

func (w *Writer) Close()                         { w.db.Close() }
func (w *Writer) Ping(ctx context.Context) error { return w.db.PingContext(ctx) }
func (w *Writer) DB() *sql.DB                    { return w.db }
func (w *Writer) MaxConns() int                  { return w.maxConns }
func (w *Writer) DBType() string                 { return "sqlite" }

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "sqlite",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// CreateSchema is a no-op for SQLite — schema names are not meaningful
// (a SQLite file IS the database).
func (w *Writer) CreateSchema(ctx context.Context, schema string) error { return nil }

// CreateTable creates a table from source metadata.
func (w *Writer) CreateTable(ctx context.Context, t *driver.Table, targetSchema string) error {
	return w.CreateTableWithOptions(ctx, t, targetSchema, driver.TableOptions{})
}

// CreateTableWithOptions creates a table with options.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  "sqlite",
		SourceTable:   t,
		TargetSchema:  "", // SQLite has no schemas
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}
	resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("DDL generation failed for table %s: %w", t.FullName(), err)
	}
	logging.Debug("Generated DDL for %s:\n%s", t.FullName(), resp.CreateTableDDL)
	for colName, colType := range resp.ColumnTypes {
		logging.Debug("  Column %s -> %s", colName, colType)
	}
	if _, err := w.db.ExecContext(ctx, resp.CreateTableDDL); err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, resp.CreateTableDDL)
	}
	return nil
}

// AddColumn adds a nullable column to an existing target table. It is
// idempotent so interrupted schema-evolution runs can be retried safely.
func (w *Writer) AddColumn(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return fmt.Errorf("table is required")
	}
	if column == nil {
		return fmt.Errorf("column is required")
	}

	exists, err := w.columnExists(ctx, t.Name, column.Name)
	if err != nil {
		return fmt.Errorf("checking column %s.%s: %w", t.Name, column.Name, err)
	}
	if exists {
		return nil
	}

	ddl, err := w.buildAddColumnSQL(t, column)
	if err != nil {
		return err
	}
	logging.Debug("Adding SQLite column with DDL: %s", ddl)
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) columnExists(ctx context.Context, table, column string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx,
		`SELECT 1 FROM pragma_table_info(?) WHERE name = ? LIMIT 1`,
		table, column).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (w *Writer) buildAddColumnSQL(t *driver.Table, column *driver.Column) (string, error) {
	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "sqlite", *column)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL",
		w.dialect.QualifyTable("", t.Name),
		w.dialect.QuoteIdentifier(column.Name),
		mappedType), nil
}

// DropColumnNotNull returns a clear unsupported error. SQLite cannot relax a
// NOT NULL constraint in place; doing it safely requires a table rebuild.
func (w *Writer) DropColumnNotNull(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return fmt.Errorf("table is required")
	}
	if column == nil {
		return fmt.Errorf("column is required")
	}
	return fmt.Errorf("sqlite cannot relax NOT NULL for %s.%s without rebuilding the table",
		t.Name, column.Name)
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	// Disable FK enforcement for the drop in case dependents exist.
	stmts := []string{
		"PRAGMA foreign_keys = OFF",
		fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable),
		"PRAGMA foreign_keys = ON",
	}
	for _, s := range stmts {
		if _, err := w.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("dropping %s: %w", table, err)
		}
	}
	return nil
}

// TruncateTable empties a table. SQLite has no TRUNCATE; DELETE without
// a WHERE clause is internally optimized to "TRUNCATE OPTIMIZATION" on
// modern SQLite (drops + recreates the b-tree).
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	if _, err := w.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s", qualifiedTable)); err != nil {
		return err
	}
	// Reset AUTOINCREMENT counter for the truncated table.
	_, _ = w.db.ExecContext(ctx,
		"DELETE FROM sqlite_sequence WHERE name = ?", table)
	return nil
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`,
		table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// SetTableLogged is a no-op for SQLite (no unlogged tables).
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error { return nil }

// CreatePrimaryKey is a no-op — PK is declared in CREATE TABLE for
// SQLite (ALTER TABLE in SQLite is restricted and can't add a PK after
// the fact).
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx,
		`SELECT 1 FROM pragma_table_info(?) WHERE pk > 0 LIMIT 1`,
		table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// GetTableDDL returns the CREATE TABLE DDL from sqlite_master.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	var ddl sql.NullString
	err := w.db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
		table).Scan(&ddl)
	if err != nil || !ddl.Valid {
		return ""
	}
	return ddl.String
}

func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return w.GetRowCountExact(ctx, schema, table, false)
}
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	return w.GetRowCountExact(ctx, schema, table, false)
}
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ResetSequence resets the AUTOINCREMENT counter for the table.
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

	var maxVal sql.NullInt64
	if err := w.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MAX(%s) FROM %s",
			w.dialect.QuoteIdentifier(identityCol),
			w.dialect.QualifyTable(schema, t.Name))).Scan(&maxVal); err != nil {
		return fmt.Errorf("getting max %s.%s: %w", t.Name, identityCol, err)
	}
	if !maxVal.Valid || maxVal.Int64 == 0 {
		return nil
	}

	// sqlite_sequence is created lazily — populated only after the first
	// row insert into an AUTOINCREMENT table. Update the existing row;
	// if no row exists yet, insert one.
	res, err := w.db.ExecContext(ctx,
		`UPDATE sqlite_sequence SET seq = ? WHERE name = ?`, maxVal.Int64, t.Name)
	if err != nil {
		return fmt.Errorf("resetting sequence for %s: %w", t.Name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		_, err = w.db.ExecContext(ctx,
			`INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)`, t.Name, maxVal.Int64)
		if err != nil {
			return fmt.Errorf("seeding sequence for %s: %w", t.Name, err)
		}
	}
	return nil
}

// CreateIndex creates an index on the target table.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for index creation")
	}
	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeIndex,
		SourceDBType:  w.sourceType,
		TargetDBType:  "sqlite",
		Table:         t,
		Index:         idx,
		TargetSchema:  "",
		TargetContext: w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("index DDL generation failed for %s.%s: %w", t.Name, idx.Name, err)
	}
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

// CreateForeignKey is a no-op with a warning for SQLite. SQLite cannot
// add foreign keys via ALTER TABLE — they must be declared inline at
// CREATE TABLE time. This driver targets test scenarios; users needing
// FK enforcement should either include the FK in the source-side CREATE
// TABLE (so the generated CREATE TABLE picks it up inline — possible
// future enhancement) or recreate the table.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	logging.Warn("sqlite: skipping FK %s.%s — SQLite cannot ADD CONSTRAINT FK via ALTER TABLE", t.Name, fk.Name)
	return nil
}

// CreateCheckConstraint is similarly a no-op with a warning. SQLite's
// ALTER TABLE doesn't support ADD CONSTRAINT CHECK.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	logging.Warn("sqlite: skipping CHECK %s.%s — SQLite cannot ADD CONSTRAINT CHECK via ALTER TABLE", t.Name, chk.Name)
	return nil
}

// WriteBatch writes rows using multi-row INSERTs inside a transaction.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	quoted := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		quoted[i] = w.dialect.QuoteIdentifier(c)
	}
	colList := strings.Join(quoted, ", ")
	fullTable := w.dialect.QualifyTable(opts.Schema, opts.Table)

	verb := "INSERT INTO"
	if opts.IdempotentOnDup {
		if len(opts.PKColumns) == 0 {
			return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
		}
		verb = "INSERT OR IGNORE INTO"
	}

	// Per-call BatchSize takes priority over the writer's default.
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Cap batch size to stay strictly below SQLITE_MAX_VARIABLE_NUMBER.
	maxRowsByVars := sqliteMaxVariables / len(opts.Columns)
	if maxRowsByVars < 1 {
		maxRowsByVars = 1
	}
	if batchSize > maxRowsByVars {
		batchSize = maxRowsByVars
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]
		query := buildInsertSQL(verb, fullTable, colList, len(opts.Columns), len(batch))
		args := make([]any, 0, len(batch)*len(opts.Columns))
		for _, row := range batch {
			args = append(args, convertRowValues(row)...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("inserting batch into %s: %w", opts.Table, err)
		}
	}

	return tx.Commit()
}

func buildInsertSQL(verb, tableName, colList string, numCols, numRows int) string {
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"
	rowPlaceholders := make([]string, numRows)
	for i := range rowPlaceholders {
		rowPlaceholders[i] = rowPlaceholder
	}
	return fmt.Sprintf("%s %s (%s) VALUES %s",
		verb, tableName, colList, strings.Join(rowPlaceholders, ", "))
}

// UpsertBatch performs upsert using INSERT ... ON CONFLICT DO UPDATE.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	quoted := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		quoted[i] = w.dialect.QuoteIdentifier(c)
	}
	colList := strings.Join(quoted, ", ")
	fullTable := w.dialect.QualifyTable(opts.Schema, opts.Table)

	pkSet := make(map[string]bool)
	pkQuoted := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		pkSet[strings.ToLower(pk)] = true
		pkQuoted[i] = w.dialect.QuoteIdentifier(pk)
	}

	var updateClauses []string
	for _, col := range opts.Columns {
		if pkSet[strings.ToLower(col)] {
			continue
		}
		qCol := w.dialect.QuoteIdentifier(col)
		// SQLite's UPSERT uses excluded.col for the new-row value.
		updateClauses = append(updateClauses, fmt.Sprintf("%s = excluded.%s", qCol, qCol))
	}

	upsertSuffix := ""
	if len(updateClauses) > 0 {
		upsertSuffix = fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(pkQuoted, ", "), strings.Join(updateClauses, ", "))
	} else {
		// PK-only table — no columns to update; treat as INSERT OR IGNORE.
		upsertSuffix = fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(pkQuoted, ", "))
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	maxRowsByVars := sqliteMaxVariables / len(opts.Columns)
	if maxRowsByVars < 1 {
		maxRowsByVars = 1
	}
	if batchSize > maxRowsByVars {
		batchSize = maxRowsByVars
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		placeholders := make([]string, len(opts.Columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"
		rowPlaceholders := make([]string, len(batch))
		for i := range rowPlaceholders {
			rowPlaceholders[i] = rowPlaceholder
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
			fullTable, colList, strings.Join(rowPlaceholders, ", "), upsertSuffix)

		args := make([]any, 0, len(batch)*len(opts.Columns))
		for _, row := range batch {
			args = append(args, convertRowValues(row)...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upserting batch into %s: %w", opts.Table, err)
		}
	}

	return tx.Commit()
}

// convertRowValues normalizes row values for the sqlite driver.
// modernc.org/sqlite accepts most Go types directly; booleans are
// stored as integers per SQLite convention.
func convertRowValues(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		switch val := v.(type) {
		case bool:
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

// ExecRaw executes a raw SQL query.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a single-row raw SQL query.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}
