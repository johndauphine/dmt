package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader from a catalog: introspection runs
// the catalog's queries against their canonical column shapes
// (IntrospectionSpec), everything else is the shared pipeline code.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	cat      *Catalog
	dialect  *Dialect
}

// readerWithDates adds the optional IncrementalDateReader capability
// (#476). NewReader returns it only when the catalog declares the
// capability, so type assertions — and the conformance capability
// matrix — see exactly what the catalog promises.
type readerWithDates struct {
	*Reader
}

var (
	_ driver.Reader                = (*Reader)(nil)
	_ driver.IncrementalDateReader = (*readerWithDates)(nil)
)

// NewReader opens the catalog's backend with the shared pool sizing.
func NewReader(cat *Catalog, cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	dialect := NewDialect(cat)
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open(knownBackends[cat.Connection.Backend], dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s connection: %w", cat.Name, err)
	}

	db.SetMaxOpenConns(maxConns)
	idle := maxConns / 4
	if idle < 1 {
		idle = 1
	}
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging %s database: %w", cat.Name, err)
	}

	logging.Debug("Connected to %s source (catalog-driven): %s", cat.Name, cfg.Database)

	r := &Reader{db: db, config: cfg, maxConns: maxConns, cat: cat, dialect: dialect}
	if cat.Capabilities.IncrementalDateReader {
		return &readerWithDates{r}, nil
	}
	return r, nil
}

func (r *Reader) Close() error  { return r.db.Close() }
func (r *Reader) DB() *sql.DB   { return r.db }
func (r *Reader) MaxConns() int { return r.maxConns }
func (r *Reader) DBType() string {
	return r.cat.Name
}

func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      r.cat.Name,
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// GetRowCount tries the stats-based count when the catalog declares
// one, otherwise exact.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	if r.cat.Queries.RowCountStats != "" {
		if n, err := r.GetRowCountFast(ctx, schema, table); err == nil && n > 0 {
			return n, nil
		}
	}
	return r.GetRowCountExact(ctx, schema, table, false)
}

func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	if r.cat.Queries.RowCountStats == "" {
		return r.GetRowCountExact(ctx, schema, table, false)
	}
	query := fmt.Sprintf(r.cat.Queries.RowCountStats, r.dialect.QualifyTable(schema, table))
	return shared.QueryExactRowCount(ctx, r.db, query)
}

func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, r.db, r.dialect, schema, table)
}

// GetPartitionBoundaries runs the catalog's partition query, which
// must return rows of (partition_id, min_pk, max_pk, row_count). A
// single-partition engine (sqlite) declares a query returning one row.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}
	q := strings.ReplaceAll(r.cat.Queries.PartitionBoundaries, "{n}", strconv.Itoa(numPartitions))
	q = strings.ReplaceAll(q, "{pk}", r.dialect.QuoteIdentifier(t.PrimaryKey[0]))
	q = strings.ReplaceAll(q, "{table}", r.dialect.QualifyTable(t.Schema, t.Name))

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying partition boundaries: %w", err)
	}
	defer rows.Close()

	var parts []driver.Partition
	for rows.Next() {
		var p driver.Partition
		p.TableName = t.Name
		if err := rows.Scan(&p.PartitionID, &p.MinPK, &p.MaxPK, &p.RowCount); err != nil {
			return nil, fmt.Errorf("scanning partition boundaries: %w", err)
		}
		p.IsFirstPartition = p.PartitionID == 1
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

// ExtractSchema walks the catalog's list_tables / describe_table
// queries and maps their canonical shapes onto driver.Table.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.ListTables)
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

	tables := make([]driver.Table, 0, len(names))
	for _, name := range names {
		t := driver.Table{Name: name, Schema: schemaOrEmpty(r.cat, schema)}
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, err
		}
		t.PopulatePKColumns()

		count, err := r.GetRowCount(ctx, t.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s: %v", t.Name, err)
		}
		t.RowCount = count
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}
	return tables, nil
}

func schemaOrEmpty(cat *Catalog, schema string) string {
	if cat.Quoting.SchemaIgnored {
		return ""
	}
	return schema
}

// loadColumns maps describe_table's canonical shape: (ordinal, name,
// decl_type, max_length, precision, scale, is_nullable, default_value,
// pk_ordinal).
func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.DescribeTable, t.Name)
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
			ordinal             int
			name                string
			declType            sql.NullString
			maxLen, prec, scale sql.NullInt64
			nullable            int
			dflt                sql.NullString
			pkOrd               int
		)
		if err := rows.Scan(&ordinal, &name, &declType, &maxLen, &prec, &scale, &nullable, &dflt, &pkOrd); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}

		col := driver.Column{
			Name:         name,
			DataType:     strings.ToLower(strings.TrimSpace(declType.String)),
			MaxLength:    int(maxLen.Int64),
			Precision:    int(prec.Int64),
			Scale:        int(scale.Int64),
			IsNullable:   nullable == 1,
			DefaultValue: dflt.String,
			OrdinalPos:   ordinal,
		}
		if r.cat.Introspection.ParseTypeParams {
			base, l, p, s := parseDeclaredType(declType.String)
			col.DataType = base
			col.MaxLength, col.Precision, col.Scale = l, p, s
		}
		t.Columns = append(t.Columns, col)

		if pkOrd > 0 {
			pkCols = append(pkCols, pkEntry{name: name, ord: pkOrd})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for i := 1; i <= len(pkCols); i++ {
		for _, pc := range pkCols {
			if pc.ord == i {
				t.PrimaryKey = append(t.PrimaryKey, pc.name)
				break
			}
		}
	}

	if s := r.cat.Introspection.IdentityStrategy; s != "" {
		identity, err := identityStrategies[s](ctx, r.db, t.Name, t.PrimaryKey)
		if err != nil {
			return fmt.Errorf("identity detection for %s: %w", t.Name, err)
		}
		for i := range t.Columns {
			if identity[t.Columns[i].Name] {
				t.Columns[i].IsIdentity = true
			}
		}
	}
	return nil
}

// parseDeclaredType splits "VARCHAR(255)" / "NUMERIC(10,2)" / "INTEGER"
// into base type and parameters. Two-parameter types populate both
// precision and max_length with the first parameter — the same shape
// information_schema engines report (see the hand-written sqlite
// reader's parseSqliteType, which this generalizes).
func parseDeclaredType(decl string) (base string, maxLen, precision, scale int) {
	s := strings.TrimSpace(decl)
	open := strings.Index(s, "(")
	if open < 0 {
		return strings.ToLower(s), 0, 0, 0
	}
	closeIdx := strings.Index(s, ")")
	if closeIdx < 0 || closeIdx <= open {
		return strings.ToLower(strings.TrimSpace(s[:open])), 0, 0, 0
	}
	base = strings.ToLower(strings.TrimSpace(s[:open]))
	params := strings.Split(s[open+1:closeIdx], ",")
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

// LoadIndexes maps index_list (index_name, is_unique) + index_columns
// (column_name, ordered).
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.IndexList, t.Name)
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
		colRows, err := r.db.QueryContext(ctx, r.cat.Introspection.IndexColumns, m.name)
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
		// A result set can fail after Next() returns false (context
		// cancellation, driver read error); without this check the
		// index would be appended with silently missing columns (codex).
		if err := colRows.Err(); err != nil {
			colRows.Close()
			return fmt.Errorf("reading index columns for %s: %w", m.name, err)
		}
		colRows.Close()
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

// LoadForeignKeys maps foreign_keys (fk_id, seq, ref_table,
// from_column, to_column, on_update, on_delete), grouping by fk_id in
// first-seen order.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.ForeignKeys, t.Name)
	if err != nil {
		return fmt.Errorf("querying FKs for %s: %w", t.Name, err)
	}
	defer rows.Close()

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

// LoadCheckConstraints maps check_constraints (name, expression), or
// is a documented no-op when the catalog declares no query (sqlite:
// CHECKs are inline in CREATE TABLE and not surfaced).
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	q := r.cat.Introspection.CheckConstraints
	if q == "" {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, q, t.Name)
	if err != nil {
		return fmt.Errorf("querying CHECK constraints for %s: %w", t.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cc driver.CheckConstraint
		if err := rows.Scan(&cc.Name, &cc.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, cc)
	}
	return rows.Err()
}

// GetDateColumnInfo finds a date-like column among candidates via the
// catalog's date_column query and date type list.
func (r *readerWithDates) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	valid := r.dialect.ValidDateTypes()
	for _, col := range candidates {
		var dt sql.NullString
		err := r.db.QueryRowContext(ctx, r.cat.Queries.DateColumn, table, col).Scan(&dt)
		if err != nil || !dt.Valid {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(dt.String))
		if valid[t] {
			return col, t, true
		}
	}
	return "", "", false
}

// GetMaxDateColumnValue returns the source-side high watermark for a
// date column.
func (r *readerWithDates) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table))
	var raw any
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}
