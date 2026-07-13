package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	poolMu   sync.RWMutex
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
	_ driver.ConnectionPoolResizer = (*Reader)(nil)
	_ driver.ConnectionPoolResizer = (*readerWithDates)(nil)
)

// NewReader opens the catalog's backend with the shared pool sizing.
func NewReader(cat *Catalog, cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	dialect := NewDialect(cat)
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open(knownBackends[cat.Connection.Backend], dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s connection: %w", cat.Name, logging.ScrubError(err))
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
		return nil, fmt.Errorf("pinging %s database: %w", cat.Name, logging.ScrubError(err))
	}

	if v := cat.Connection.ValidateStrategy; v != "" {
		if err := connectionValidators[v](db, cfg.Database); err != nil {
			db.Close()
			return nil, err
		}
	}

	logging.Debug("Connected to %s source (catalog-driven): %s", cat.Name, cfg.Database)

	r := &Reader{db: db, config: cfg, maxConns: maxConns, cat: cat, dialect: dialect}
	if cat.Capabilities.IncrementalDateReader {
		return &readerWithDates{r}, nil
	}
	return r, nil
}

func (r *Reader) Close() error { return r.db.Close() }
func (r *Reader) DB() *sql.DB  { return r.db }
func (r *Reader) MaxConns() int {
	r.poolMu.RLock()
	defer r.poolMu.RUnlock()
	return r.maxConns
}

// ResizeConnectionPool applies the source pool's tuned live limit while
// preserving the reader idle policy used at construction time: retain one
// idle connection per four open connections, with a floor of one.
func (r *Reader) ResizeConnectionPool(maxConns int) int {
	if maxConns < 0 {
		maxConns = 0
	}

	r.poolMu.Lock()
	defer r.poolMu.Unlock()

	r.db.SetMaxOpenConns(maxConns)
	idle := maxConns / 4
	if idle < 1 {
		idle = 1
	}
	r.db.SetMaxIdleConns(idle)

	actual := r.db.Stats().MaxOpenConnections
	r.maxConns = actual
	return actual
}

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
	stats := r.cat.Queries.RowCountStats
	if strings.Contains(stats, r.dialect.ParameterPlaceholder(1)) {
		// Parameterized form (mysql information_schema.TABLES, pg
		// pg_stat_user_tables) — binds (schema, table) like the
		// introspection queries. Detected via the engine's own
		// placeholder scheme ("?", "$1", ...); the unparameterized form
		// is a %s format string instead.
		var n sql.NullInt64
		if err := r.db.QueryRowContext(ctx, stats, r.introArgs(schema, table)...).Scan(&n); err != nil {
			return 0, err
		}
		return n.Int64, nil
	}
	query := fmt.Sprintf(stats, r.dialect.QualifyTable(schema, table))
	return shared.QueryExactRowCount(ctx, r.db, query)
}

// GetRowCountExact honors strict_consistency for hinted engines:
// mssql's relaxed default is a dirty NOLOCK count; strict drops the
// hint (#253). Engines with empty table_hints are unaffected.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, strictConsistency bool) (int64, error) {
	suffix := ""
	if hint := r.dialect.TableHint(strictConsistency); hint != "" {
		suffix = " " + hint
	}
	query, err := shared.ExactRowCountQueryWithSuffix(r.dialect, schema, table, suffix)
	if err != nil {
		return 0, err
	}
	return shared.QueryExactRowCount(ctx, r.db, query)
}

// GetPartitionBoundaries runs the catalog's partition query, which
// must return rows of (partition_id, min_pk, max_pk, row_count). A
// single-partition engine (sqlite) declares a query returning one row.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}
	if r.cat.Queries.PartitionStrategy == "min_max_even" {
		return r.partitionMinMaxEven(ctx, t, numPartitions)
	}
	// Single-pass replacement: sequential ReplaceAll could re-expand a
	// token appearing inside a quoted identifier (codex).
	q := strings.NewReplacer(
		"{n}", strconv.Itoa(numPartitions),
		"{pk}", r.dialect.QuoteIdentifier(t.PrimaryKey[0]),
		"{table}", r.dialect.QualifyTable(t.Schema, t.Name),
	).Replace(r.cat.Queries.PartitionBoundaries)

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

// introArgs prepends the schema for engines whose catalog tables are
// schema-keyed (IntrospectionSpec.UsesSchema). An empty schema falls
// back to the connection's database — engines like ClickHouse treat
// schema AS the database, and a config that only sets database is
// valid (codex on #507; mirrors the MySQL convention).
func (r *Reader) introArgs(schema string, rest ...any) []any {
	if !r.cat.Introspection.UsesSchema {
		return rest
	}
	if schema == "" {
		schema = r.config.Database
	}
	return append([]any{schema}, rest...)
}

// ExtractSchema walks the catalog's list_tables / describe_table
// queries and maps their canonical shapes onto driver.Table.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.ListTables, r.introArgs(schema)...)
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
		if q := r.cat.Introspection.AvgRowLength; q != "" {
			var avg sql.NullInt64
			// max(static, db): stats only widen the estimate (matches
			// the hand-written pg/mysql readers — an undercounting
			// stats view must not shrink the memory guardrail input).
			if err := r.db.QueryRowContext(ctx, q, r.introArgs(t.Schema, t.Name)...).Scan(&avg); err == nil && avg.Int64 > t.EstimatedRowSize {
				t.EstimatedRowSize = avg.Int64
			}
		}

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
// decl_type, [full_decl_type], max_length, precision, scale, is_nullable,
// default_value, pk_ordinal, [is_identity]).
func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.DescribeTable, r.introArgs(t.Schema, t.Name)...)
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
			fullDeclType        sql.NullString
			maxLen, prec, scale sql.NullInt64
			nullable            int
			dflt                sql.NullString
			pkOrd               int
			isIdentity          int
		)
		dests := []any{&ordinal, &name, &declType}
		if r.cat.Introspection.FullTypeInDescribe {
			dests = append(dests, &fullDeclType)
		}
		dests = append(dests, &maxLen, &prec, &scale, &nullable, &dflt, &pkOrd)
		if r.cat.Introspection.IdentityInDescribe {
			dests = append(dests, &isIdentity)
		}
		if err := rows.Scan(dests...); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}

		col := driver.Column{
			Name:         name,
			DataType:     strings.ToLower(strings.TrimSpace(declType.String)),
			FullDataType: strings.TrimSpace(fullDeclType.String),
			MaxLength:    int(maxLen.Int64),
			Precision:    int(prec.Int64),
			Scale:        int(scale.Int64),
			IsNullable:   nullable == 1,
			DefaultValue: dflt.String,
			OrdinalPos:   ordinal,
			IsIdentity:   isIdentity == 1,
		}
		if r.cat.Introspection.ParseTypeParams {
			base, l, p, s := parseDeclaredType(declType.String)
			col.DataType = base
			col.MaxLength, col.Precision, col.Scale = l, p, s
		}
		if col.FullDataType == "" {
			col.FullDataType = col.DataType
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
	if r.cat.Introspection.IndexListAgg != "" {
		return r.loadIndexesAgg(ctx, t)
	}
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.IndexList, r.introArgs(t.Schema, t.Name)...)
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
		colArgs := []any{m.name}
		if r.cat.Introspection.IndexColumnsByTable {
			colArgs = []any{t.Name, m.name}
		}
		colRows, err := r.db.QueryContext(ctx, r.cat.Introspection.IndexColumns, r.introArgs(t.Schema, colArgs...)...)
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
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.ForeignKeys, r.introArgs(t.Schema, t.Name)...)
	if err != nil {
		return fmt.Errorf("querying FKs for %s: %w", t.Name, err)
	}
	defer rows.Close()

	type fkEntry struct {
		fk       *driver.ForeignKey
		fromCols []string
		toCols   []string
	}
	groups := make(map[string]*fkEntry)
	var order []string

	for rows.Next() {
		var (
			id, seq                  int
			fkName, refSchema        sql.NullString
			refTable, fromCol, toCol string
			onUpdate, onDelete       sql.NullString
		)
		if err := rows.Scan(&id, &seq, &fkName, &refSchema, &refTable, &fromCol, &toCol, &onUpdate, &onDelete); err != nil {
			return err
		}
		name := fkName.String
		if name == "" {
			// Engines without constraint names in their catalog output
			// (sqlite pragma) get the synthesized form.
			name = fmt.Sprintf("fk_%s_%d", t.Name, id)
		}
		// Group by constraint name, not fk_id: named engines can then
		// emit a constant id, which keeps the query free of window
		// functions (MySQL 5.7 has none; codex on #509).
		entry, ok := groups[name]
		if !ok {
			entry = &fkEntry{fk: &driver.ForeignKey{
				Name:      name,
				RefSchema: refSchema.String,
				RefTable:  refTable,
				OnUpdate:  onUpdate.String,
				OnDelete:  onDelete.String,
			}}
			groups[name] = entry
			order = append(order, name)
		}
		entry.fromCols = append(entry.fromCols, fromCol)
		entry.toCols = append(entry.toCols, toCol)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, key := range order {
		entry := groups[key]
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
	rows, err := r.db.QueryContext(ctx, q, r.introArgs(t.Schema, t.Name)...)
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
		err := r.db.QueryRowContext(ctx, r.cat.Queries.DateColumn, r.introArgs(schema, table, col)...).Scan(&dt)
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
	if tmpl := r.cat.Queries.MaxDate; tmpl != "" {
		// mssql casts through datetime2(7): go-mssqldb scans
		// MAX(datetime) rounded to milliseconds, which can make an
		// equal source row compare greater than the persisted
		// watermark on the next run.
		query = strings.NewReplacer(
			"{column}", r.dialect.QuoteIdentifier(column),
			"{table}", r.dialect.QualifyTable(schema, table),
		).Replace(tmpl)
	}
	var raw any
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}

// partitionMinMaxEven is the min_max_even partition strategy (mssql's
// hand-written fast path): MIN/MAX index seeks plus even PK ranges
// instead of an NTILE full scan. Row counts are stats-estimated and
// spread evenly; the last partition takes the range remainder.
func (r *Reader) partitionMinMaxEven(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) != 1 {
		return nil, fmt.Errorf("partitioning requires single-column PK")
	}

	pkCol := r.dialect.QuoteIdentifier(t.PrimaryKey[0])
	qualifiedTable := r.dialect.QualifyTable(t.Schema, t.Name)
	hint := ""
	if h := r.dialect.TableHint(false); h != "" {
		hint = " " + h
	}

	var minPK, maxPK int64
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s%s", pkCol, pkCol, qualifiedTable, hint)
	if err := r.db.QueryRowContext(ctx, query).Scan(&minPK, &maxPK); err != nil {
		return nil, fmt.Errorf("getting MIN/MAX: %w", err)
	}

	rowCount, _ := r.GetRowCountFast(ctx, t.Schema, t.Name)
	if rowCount == 0 {
		rowCount = maxPK - minPK + 1
	}

	rangeSize := maxPK - minPK + 1
	partitionSize := rangeSize / int64(numPartitions)
	rowsPerPartition := rowCount / int64(numPartitions)

	var partitions []driver.Partition
	for i := 0; i < numPartitions; i++ {
		start := minPK + int64(i)*partitionSize
		end := minPK + int64(i+1)*partitionSize - 1
		if i == numPartitions-1 {
			end = maxPK
		}
		partitions = append(partitions, driver.Partition{
			TableName:        t.FullName(),
			PartitionID:      i + 1,
			MinPK:            start,
			MaxPK:            end,
			RowCount:         rowsPerPartition,
			IsFirstPartition: i == 0,
		})
	}

	logging.Debug("  %s: %d partitions via MIN/MAX (range %d-%d)", t.Name, numPartitions, minPK, maxPK)
	return partitions, nil
}

// indexAggDelimiter separates aggregated column names in
// index_list_agg results — CHAR(1) (SOH) cannot appear in a valid
// identifier on any supported engine.
const indexAggDelimiter = "\x01"

// loadIndexesAgg maps the single-query index form: one row per index
// of (name, is_unique, is_clustered, key columns, include columns),
// the column lists CHAR(1)-joined.
func (r *Reader) loadIndexesAgg(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, r.cat.Introspection.IndexListAgg, r.introArgs(t.Schema, t.Name)...)
	if err != nil {
		return fmt.Errorf("querying indexes for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			idx                 driver.Index
			isUnique, isClust   int
			colsStr, includeStr string
		)
		if err := rows.Scan(&idx.Name, &isUnique, &isClust, &colsStr, &includeStr); err != nil {
			return err
		}
		idx.IsUnique = isUnique == 1
		idx.IsClustered = isClust == 1
		idx.Columns = strings.Split(colsStr, indexAggDelimiter)
		if includeStr != "" {
			idx.IncludeCols = strings.Split(includeStr, indexAggDelimiter)
		}
		t.Indexes = append(t.Indexes, idx)
	}
	return rows.Err()
}
