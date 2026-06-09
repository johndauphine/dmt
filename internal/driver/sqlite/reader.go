package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	_ "modernc.org/sqlite" // SQLite driver (pure Go, no cgo)
	"strconv"
	"strings"
	"time"
)

// Reader implements driver.Reader for SQLite.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new SQLite reader. The file path is read from
// cfg.Database (SQLite has no host/port/user/password). Empty database
// opens an in-memory DB.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite connection: %w", err)
	}

	// SQLite tolerates multiple read connections fine. We cap at maxConns
	// to honor the caller's pool sizing.
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	if maxConns/4 < 1 {
		db.SetMaxIdleConns(1)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging sqlite database: %w", err)
	}

	logging.Debug("Connected to SQLite source: %s", cfg.Database)

	return &Reader{
		db:       db,
		config:   cfg,
		maxConns: maxConns,
		dialect:  dialect,
	}, nil
}

// Close closes the connection pool.
func (r *Reader) Close() error { return r.db.Close() }

// DB returns the underlying sql.DB.
func (r *Reader) DB() *sql.DB { return r.db }

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int { return r.maxConns }

// DBType returns the database type.
func (r *Reader) DBType() string { return "sqlite" }

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "sqlite",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ReadTable streams rows from a table via a channel.
func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	return shared.StreamTable(ctx, shared.StreamConfig{
		DB:      r.db,
		Dialect: r.dialect,
		Buffer:  4,
	}, opts)
}

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return r.GetRowCountExact(ctx, schema, table, false)
}

// GetRowCountFast returns an approximate row count. SQLite has no stats
// catalog; falls back to COUNT(*).
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	return r.GetRowCountExact(ctx, schema, table, false)
}

// GetRowCountExact returns the exact row count via COUNT(*). The
// strictConsistency flag has no meaning in SQLite (MVCC-like via WAL).
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, r.db, r.dialect, schema, table)
}

// GetPartitionBoundaries returns a single partition spanning the full PK
// range, regardless of the requested partition count. SQLite has no
// parallelism benefit from splitting a table (single-writer / single
// shared file).
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}
	pkCol := t.PrimaryKey[0]
	qPK := r.dialect.QuoteIdentifier(pkCol)
	qualifiedTable := r.dialect.QualifyTable(t.Schema, t.Name)

	row := r.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s", qPK, qPK, qualifiedTable))

	var p driver.Partition
	p.TableName = t.Name
	p.PartitionID = 1
	p.IsFirstPartition = true
	if err := row.Scan(&p.MinPK, &p.MaxPK, &p.RowCount); err != nil {
		return nil, fmt.Errorf("scanning partition boundaries: %w", err)
	}
	return []driver.Partition{p}, nil
}

// GetDateColumnInfo finds a date-like column among candidates.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt sql.NullString
		err := r.db.QueryRowContext(ctx,
			`SELECT type FROM pragma_table_info(?) WHERE name = ?`,
			table, col).Scan(&dt)
		if err != nil || !dt.Valid {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(dt.String))
		if r.dialect.ValidDateTypes()[t] {
			return col, t, true
		}
	}
	return "", "", false
}

// GetMaxDateColumnValue returns the current source-side high watermark for a
// date column. A nil value means the table has no non-NULL values in that
// column yet.
func (r *Reader) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
	)

	var raw any
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}

// SampleColumnValues retrieves sample values from a column.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		`SELECT DISTINCT CAST(%s AS TEXT) FROM %s WHERE %s IS NOT NULL LIMIT ?`,
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
		r.dialect.QuoteIdentifier(column),
	)

	samples, err := shared.QuerySampleColumnValues(ctx, r.db, query, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling column %s: %w", column, err)
	}
	return samples, nil
}

// SampleRows retrieves sample rows from a table.
func (r *Reader) SampleRows(ctx context.Context, schema, table string, columns []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 5
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	var quoted []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quoted = append(quoted, fmt.Sprintf("CAST(%s AS TEXT)", r.dialect.QuoteIdentifier(col)))
	}
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT ?",
		strings.Join(quoted, ", "),
		r.dialect.QualifyTable(schema, table))
	return shared.QuerySampleRows(ctx, r.db, query, columns, limit, limit)
}

// parseSqliteType splits a declared SQLite column type ("VARCHAR(255)",
// "NUMERIC(10, 2)", "INTEGER") into the lowercase base type and any
// numeric parameters. Returns:
//
//   - base: lowercase, trimmed, parameter-stripped type name (e.g.
//     "varchar", "numeric", "integer"). Empty when the source declared
//     no type — sqlite allows that, and the affinity rules pick BLOB,
//     but we surface empty so the canonical mapper can fall through to
//     its KindText default.
//   - maxLen: for single-parameter types (VARCHAR(255), CHAR(10)),
//     the parameter value. Zero when unparseable or absent.
//   - precision, scale: for two-parameter types (NUMERIC(10,2),
//     DECIMAL(p,s)), the two parameters. Zero when unparseable or
//     absent. SQLite stores these verbatim from the CREATE TABLE; the
//     canonical mapper uses them only for KindDecimal.
//
// The two-parameter form populates maxLen with the first parameter
// AND precision with the same value, so callers that only check
// CharacterMaximumLength still see a non-zero value for fixed-width
// numeric columns. This matches how MSSQL/PG drivers populate
// information_schema rows.
func parseSqliteType(decl string) (base string, maxLen, precision, scale int) {
	s := strings.TrimSpace(decl)
	open := strings.Index(s, "(")
	if open < 0 {
		return strings.ToLower(s), 0, 0, 0
	}
	close := strings.Index(s, ")")
	if close < 0 || close <= open {
		return strings.ToLower(strings.TrimSpace(s[:open])), 0, 0, 0
	}
	base = strings.ToLower(strings.TrimSpace(s[:open]))
	params := strings.Split(s[open+1:close], ",")
	for i, p := range params {
		params[i] = strings.TrimSpace(p)
	}
	switch len(params) {
	case 1:
		if v, err := strconv.Atoi(params[0]); err == nil {
			maxLen = v
		}
	case 2:
		if v, err := strconv.Atoi(params[0]); err == nil {
			precision = v
			maxLen = v
		}
		if v, err := strconv.Atoi(params[1]); err == nil {
			scale = v
		}
	}
	return base, maxLen, precision, scale
}

// ExtractSchema extracts table metadata from the SQLite database. The
// schema parameter is ignored — SQLite has no schemas distinct from
// attached databases.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	tables := []driver.Table{}

	rows, err := r.db.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, name := range names {
		t := driver.Table{Name: name}

		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, err
		}

		// Primary key is captured in loadColumns (via pragma table_info's
		// pk field) — sorted by the pk ordinal in PopulatePKColumns.
		t.PopulatePKColumns()

		count, err := r.GetRowCount(ctx, "", t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s: %v", t.Name, err)
		}
		t.RowCount = count
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}

	return tables, nil
}

// loadColumns reads column metadata from PRAGMA table_info. The pk field
// returned by the pragma is the 1-based ordinal of the column within the
// PK (0 means not part of PK); columns are appended to t.PrimaryKey in
// pk-ordinal order.
func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	// pragma_table_info is the table-valued form usable from a SELECT —
	// supported since SQLite 3.16. The named-parameter form is
	// pragma_table_info('table_name').
	rows, err := r.db.QueryContext(ctx,
		`SELECT cid, name, type, "notnull", dflt_value, pk FROM pragma_table_info(?)`,
		t.Name)
	if err != nil {
		return fmt.Errorf("querying columns for %s: %w", t.Name, err)
	}
	defer rows.Close()

	type pkEntry struct {
		name string
		ord  int
	}
	var pkCols []pkEntry

	for rows.Next() {
		var (
			cid      int
			name     string
			declType sql.NullString
			notnull  int
			dflt     sql.NullString
			pkOrd    int
		)
		if err := rows.Scan(&cid, &name, &declType, &notnull, &dflt, &pkOrd); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}

		// SQLite stores the column type verbatim from CREATE TABLE
		// ("VARCHAR(255)", "NUMERIC(10,2)", etc.). Strip and parse the
		// length/precision/scale so cross-engine type mapping preserves
		// bounded sizes — without this, a sqlite VARCHAR(255) source
		// would widen to unbounded TEXT on a postgres target because
		// MaxLength is zero.
		baseType, maxLen, prec, scale := parseSqliteType(declType.String)

		col := driver.Column{
			Name:         name,
			DataType:     baseType,
			MaxLength:    maxLen,
			Precision:    prec,
			Scale:        scale,
			IsNullable:   notnull == 0,
			DefaultValue: dflt.String,
			OrdinalPos:   cid + 1,
		}
		t.Columns = append(t.Columns, col)

		if pkOrd > 0 {
			pkCols = append(pkCols, pkEntry{name: name, ord: pkOrd})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Sort by pk ordinal and append to t.PrimaryKey.
	for i := 1; i <= len(pkCols); i++ {
		for _, pc := range pkCols {
			if pc.ord == i {
				t.PrimaryKey = append(t.PrimaryKey, pc.name)
				break
			}
		}
	}

	// Detect AUTOINCREMENT — sqlite_master.sql contains the original
	// CREATE TABLE statement. AUTOINCREMENT applies only to single-column
	// INTEGER PRIMARY KEY.
	if len(t.PrimaryKey) == 1 {
		var createSQL sql.NullString
		if err := r.db.QueryRowContext(ctx,
			`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
			t.Name).Scan(&createSQL); err == nil && createSQL.Valid {
			lower := strings.ToLower(createSQL.String)
			if strings.Contains(lower, "autoincrement") {
				pkName := t.PrimaryKey[0]
				for i := range t.Columns {
					if t.Columns[i].Name == pkName {
						t.Columns[i].IsIdentity = true
						break
					}
				}
			}
		}
	}

	return nil
}

// LoadIndexes loads index metadata for a table from sqlite_master plus
// PRAGMA index_info.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx,
		`SELECT name, "unique" FROM pragma_index_list(?) WHERE origin != 'pk'`,
		t.Name)
	if err != nil {
		return fmt.Errorf("querying indexes for %s: %w", t.Name, err)
	}
	defer rows.Close()

	type idxMeta struct {
		name   string
		unique bool
	}
	var metas []idxMeta
	for rows.Next() {
		var (
			name     string
			isUnique int
		)
		if err := rows.Scan(&name, &isUnique); err != nil {
			return err
		}
		metas = append(metas, idxMeta{name: name, unique: isUnique == 1})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range metas {
		idx := driver.Index{Name: m.name, IsUnique: m.unique}
		colRows, err := r.db.QueryContext(ctx,
			`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, m.name)
		if err != nil {
			return fmt.Errorf("loading index columns for %s: %w", m.name, err)
		}
		for colRows.Next() {
			var c string
			if err := colRows.Scan(&c); err != nil {
				colRows.Close()
				return err
			}
			idx.Columns = append(idx.Columns, c)
		}
		colRows.Close()
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

// LoadForeignKeys loads FK metadata for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, seq, "table", "from", "to", on_update, on_delete
		   FROM pragma_foreign_key_list(?) ORDER BY id, seq`,
		t.Name)
	if err != nil {
		return fmt.Errorf("querying FKs for %s: %w", t.Name, err)
	}
	defer rows.Close()

	// Group rows by FK id.
	type fkEntry struct {
		fk       *driver.ForeignKey
		fromCols []string
		toCols   []string
	}
	groups := make(map[int]*fkEntry)
	var order []int

	for rows.Next() {
		var (
			id, seq                  int
			refTable, fromCol, toCol string
			onUpdate, onDelete       sql.NullString
		)
		if err := rows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpdate, &onDelete); err != nil {
			return err
		}
		entry, ok := groups[id]
		if !ok {
			entry = &fkEntry{fk: &driver.ForeignKey{
				Name:     fmt.Sprintf("fk_%s_%d", t.Name, id),
				RefTable: refTable,
				OnUpdate: onUpdate.String,
				OnDelete: onDelete.String,
			}}
			groups[id] = entry
			order = append(order, id)
		}
		entry.fromCols = append(entry.fromCols, fromCol)
		entry.toCols = append(entry.toCols, toCol)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, id := range order {
		entry := groups[id]
		entry.fk.Columns = entry.fromCols
		entry.fk.RefColumns = entry.toCols
		t.ForeignKeys = append(t.ForeignKeys, *entry.fk)
	}
	return nil
}

// LoadCheckConstraints is a no-op for SQLite. CHECK constraints are
// stored inline in the CREATE TABLE SQL — parsing them out of
// sqlite_master.sql is non-trivial and not needed for the test scenarios
// the driver targets. CHECKs will round-trip when sqlite is both source
// and target via the original CREATE TABLE statement; cross-engine
// CHECK migration is out of scope.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	return nil
}
