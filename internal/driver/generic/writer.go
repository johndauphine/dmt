package generic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/smtddl"
	"github.com/johndauphine/dmt/internal/stats"
	typeddl "github.com/johndauphine/dmt/internal/typemap/ddl"
)

// Writer implements driver.Writer from a catalog: SMT owns the create-path
// SQL, catalog templates remain only for later DDL milestones, catalog queries
// provide existence checks, and named strategies own bulk/upsert/sequence
// behavior. Optional capabilities (Upserter,
// SequenceResetter) are exposed by wrapper types so the type-assertion
// surface matches the catalog's capability declarations exactly.
type Writer struct {
	db               *sql.DB
	config           *dbconfig.TargetConfig
	poolMu           sync.RWMutex
	maxConns         int
	defaultBatchSize int
	sourceType       string
	cat              *Catalog
	dialect          *Dialect

	// ident rewrites source-derived identifiers before quoting
	// (quoting.identifier_sanitizer); identity when unset.
	ident func(string) string

	pgState *pgBulkState
	myState *mysqlInfileState

	typeMapper         driver.TypeMapper
	finalizationMapper driver.FinalizationDDLMapper
	dbContext          *driver.DatabaseContext
}

// writerUpsertSeq is the {Upserter, SequenceResetter} capability combo
// (sqlite's). Further combos are added as catalogs need them —
// NewWriter rejects an undeclared combination loudly.
type writerUpsertSeq struct{ *Writer }

func (w *writerUpsertSeq) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	return w.upsertBatch(ctx, opts)
}

func (w *writerUpsertSeq) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
	return w.resetSequence(ctx, schema, t)
}

// writerFull is the all-capabilities combo (mysql/postgres/mssql).
type writerFull struct{ writerUpsertSeq }

func (w *writerFull) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	return w.finalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type: driver.DDLTypeForeignKey, Table: t, ForeignKey: fk, TargetSchema: targetSchema,
	}, "FK", fk.Name)
}

func (w *writerFull) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	return w.finalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type: driver.DDLTypeCheckConstraint, Table: t, CheckConstraint: chk, TargetSchema: targetSchema,
	}, "CHECK", chk.Name)
}

var (
	_ driver.Writer                = (*Writer)(nil)
	_ driver.Upserter              = (*writerUpsertSeq)(nil)
	_ driver.SequenceResetter      = (*writerUpsertSeq)(nil)
	_ driver.ConstraintWriter      = (*writerFull)(nil)
	_ driver.ConnectionPoolResizer = (*Writer)(nil)
	_ driver.ConnectionPoolResizer = (*writerUpsertSeq)(nil)
	_ driver.ConnectionPoolResizer = (*writerFull)(nil)
)

// NewWriter opens the catalog's backend as a write target.
func NewWriter(cat *Catalog, cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	dialect := NewDialect(cat)
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open(knownBackends[cat.Connection.Backend], dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s connection: %w", cat.Name, logging.ScrubError(err))
	}

	if cat.Connection.SingleWriter {
		// Single-writer engines (sqlite): serialize concurrent
		// WriteBatch calls through the pool rather than hitting the
		// file lock.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		maxConns = 1
	} else {
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging %s database: %w", cat.Name, logging.ScrubError(err))
	}

	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}
	// Preserve the existing writer-option contract even though SMT now owns the
	// create statement itself. TableTypeMapper remains part of DMT's public
	// internal wiring surface for telemetry and later DDL milestones.
	if _, ok := opts.TypeMapper.(driver.TableTypeMapper); !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper")
	}
	driver.LogTypeMapperInit(opts.TypeMapper)
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	var version string
	_ = db.QueryRow(cat.Context.VersionQuery).Scan(&version)
	logging.Debug("Connected to %s target (catalog-driven): %s (%s)", cat.Name, cfg.Database, version)

	sanitize := identifierSanitizers[cat.Quoting.IdentifierSanitizer]
	if sanitize == nil {
		sanitize = func(name string) string { return name }
	}
	w := &Writer{
		db:               db,
		ident:            sanitize,
		pgState:          &pgBulkState{},
		myState:          &mysqlInfileState{},
		config:           cfg,
		maxConns:         maxConns,
		defaultBatchSize: opts.BatchSize,
		sourceType:       opts.SourceType,
		cat:              cat,
		dialect:          dialect,

		typeMapper:         opts.TypeMapper,
		finalizationMapper: finalizationMapper,
	}
	w.dbContext = &driver.DatabaseContext{
		Version:                  cat.Context.VersionPrefix + version,
		DatabaseName:             cfg.Database,
		IdentifierCase:           cat.Context.IdentifierCase,
		CaseSensitiveIdentifiers: cat.Context.CaseSensitiveIdentifiers,
		Charset:                  cat.Context.Charset,
		Encoding:                 cat.Context.Encoding,
		MaxIdentifierLength:      cat.Context.MaxIdentifierLength,
		VarcharSemantics:         cat.Context.VarcharSemantics,
		BytesPerChar:             cat.Context.BytesPerChar,
		Features:                 cat.Context.Features,
	}

	caps := cat.Capabilities
	switch {
	case caps.Upserter && caps.SequenceResetter && caps.ConstraintWriter:
		return &writerFull{writerUpsertSeq{w}}, nil
	case caps.Upserter && caps.SequenceResetter && !caps.ConstraintWriter:
		return &writerUpsertSeq{w}, nil
	case !caps.Upserter && !caps.SequenceResetter && !caps.ConstraintWriter:
		return w, nil
	default:
		db.Close()
		return nil, fmt.Errorf("catalog %q declares a writer capability combination with no wrapper yet (upserter=%v sequence_resetter=%v constraint_writer=%v) — add one in writer.go",
			cat.Name, caps.Upserter, caps.SequenceResetter, caps.ConstraintWriter)
	}
}

func (w *Writer) Close()                         { w.db.Close() }
func (w *Writer) Ping(ctx context.Context) error { return w.db.PingContext(ctx) }
func (w *Writer) DB() *sql.DB                    { return w.db }
func (w *Writer) MaxConns() int {
	w.poolMu.RLock()
	defer w.poolMu.RUnlock()
	return w.maxConns
}

// ResizeConnectionPool applies the target pool's tuned live limit. Engines
// that require serialized writes remain clamped to one connection regardless
// of the requested limit; other writers retain their construction-time idle
// policy of allowing the full pool to remain idle.
func (w *Writer) ResizeConnectionPool(maxConns int) int {
	if maxConns < 0 {
		maxConns = 0
	}
	if w.cat.Connection.SingleWriter {
		maxConns = 1
	}

	w.poolMu.Lock()
	defer w.poolMu.Unlock()

	w.db.SetMaxOpenConns(maxConns)
	w.db.SetMaxIdleConns(maxConns)

	actual := w.db.Stats().MaxOpenConnections
	w.maxConns = actual
	return actual
}

func (w *Writer) DBType() string { return w.cat.Name }

func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      w.cat.Name,
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// CreateSchema selects the schema/database input and executes SMT's returned
// create-plan statement unchanged. The catalog flag is connection-selection
// policy for ClickHouse, not a SQL template.
func (w *Writer) CreateSchema(ctx context.Context, targetSchema string) error {
	schemaName := targetSchema
	if schemaName == "" && w.cat.DDL.CreateSchemaDefaultsToDatabase {
		// Schema-as-database engines (ClickHouse) accept configs that select
		// only target.database. SMT still owns the resulting CREATE DATABASE SQL.
		schemaName = w.config.Database
	}
	stmt, err := smtddl.RenderCreateSchema(smtddl.Request{
		SourceDialect: w.sourceType,
		TargetDialect: w.cat.Name,
		TargetSchema:  schemaName,
	})
	if err != nil {
		return fmt.Errorf("planning schema creation with SMT: %w", err)
	}
	if stmt == "" {
		return nil
	}
	logging.DebugEvent("Generated SMT schema DDL", "schema", schemaName, "ddl", stmt)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("creating schema %s: %w\nDDL: %s", schemaName, err, stmt)
	}
	return nil
}

func (w *Writer) CreateTable(ctx context.Context, t *driver.Table, targetSchema string) error {
	return w.CreateTableWithOptions(ctx, t, targetSchema, driver.TableOptions{})
}

func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	if w.cat.Quoting.SchemaIgnored {
		targetSchema = ""
	}
	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  w.cat.Name,
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}
	createDDL, err := driver.PlanCreateTable(req)
	if err != nil {
		return fmt.Errorf("DDL generation failed for table %s: %w", t.FullName(), err)
	}
	logging.DebugEvent("Generated SMT DDL", "table", t.FullName(), "ddl", createDDL)
	if _, err := w.db.ExecContext(ctx, createDDL); err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, createDDL)
	}
	return nil
}

// AddColumn adds a nullable column, idempotently.
func (w *Writer) AddColumn(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return errors.New("table is required")
	}
	if column == nil {
		return errors.New("column is required")
	}

	exists, err := w.columnExists(ctx, targetSchema, t.Name, column.Name)
	if err != nil {
		return fmt.Errorf("checking column %s.%s: %w", t.Name, column.Name, err)
	}
	if exists {
		return nil
	}

	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, w.cat.Name, *column)
	if err != nil {
		return err
	}
	ddl := strings.NewReplacer(
		"{table}", w.dialect.QualifyTable(targetSchema, w.ident(t.Name)),
		"{column}", w.dialect.QuoteIdentifier(w.ident(column.Name)),
		"{type}", mappedType,
	).Replace(w.cat.DDL.AddColumn)
	logging.Debug("Adding %s column with DDL: %s", w.cat.Name, ddl)
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

// introArgs mirrors the reader: schema-keyed engines bind the schema
// (falling back to the connection database) as the first parameter.
func (w *Writer) introArgs(schema string, rest ...any) []any {
	if !w.cat.Introspection.UsesSchema {
		return rest
	}
	if schema == "" {
		schema = w.config.Database
	}
	return append([]any{schema}, rest...)
}

func (w *Writer) columnExists(ctx context.Context, schema, table, column string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, w.cat.Introspection.ColumnExists, w.introArgs(schema, w.ident(table), w.ident(column))...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// DropColumnNotNull returns the uniform rebuild-required error when the
// catalog declares the engine can't relax NOT NULL in place.
func (w *Writer) DropColumnNotNull(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return errors.New("table is required")
	}
	if column == nil {
		return errors.New("column is required")
	}
	if !w.cat.DDL.CanDropNotNull {
		return fmt.Errorf("%s cannot relax NOT NULL for %s.%s without rebuilding the table",
			w.cat.Name, t.Name, column.Name)
	}
	return w.alterColumn(ctx, w.cat.DDL.DropNotNull, t, column, targetSchema, "NULL")
}

// AlterColumnType — same shape as DropColumnNotNull.
func (w *Writer) AlterColumnType(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return errors.New("table is required")
	}
	if column == nil {
		return errors.New("column is required")
	}
	if !w.cat.DDL.CanAlterColumnType {
		return fmt.Errorf("%s cannot alter type for %s.%s without rebuilding the table",
			w.cat.Name, t.Name, column.Name)
	}
	nullability := "NOT NULL"
	if column.IsNullable {
		nullability = "NULL"
	}
	return w.alterColumn(ctx, w.cat.DDL.AlterColumnType, t, column, targetSchema, nullability)
}

// alterColumn renders an in-place MODIFY/ALTER COLUMN template with
// the mapped type and a translated default clause (the hand-written
// mysql writer's shape, generalized).
func (w *Writer) alterColumn(ctx context.Context, tmpl string, t *driver.Table, column *driver.Column, targetSchema, nullability string) error {
	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, w.cat.Name, *column)
	if err != nil {
		return err
	}
	defaultClause := ""
	if column.DefaultValue != "" {
		isBool := false
		switch strings.ToLower(column.DataType) {
		case "bool", "boolean", "bit":
			isBool = true
		}
		expr := typeddl.FormatDDLDefault(column.DefaultValue,
			driver.Canonicalize(w.sourceType), w.cat.Name, isBool)
		if expr != "" {
			defaultClause = " " + typeddl.FormatDefaultClause(expr, mappedType, w.cat.Name)
		}
	}
	stmt := strings.NewReplacer(
		"{table}", w.dialect.QualifyTable(targetSchema, w.ident(t.Name)),
		"{column}", w.dialect.QuoteIdentifier(w.ident(column.Name)),
		"{type}", mappedType,
		"{nullability}", nullability,
		"{default_clause}", defaultClause,
	).Replace(tmpl)
	logging.Debug("Altering %s column with DDL: %s", w.cat.Name, stmt)
	_, err = w.db.ExecContext(ctx, stmt)
	return err
}

func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	qualified := w.dialect.QualifyTable(schema, w.ident(table))
	if err := w.execDDLStatementList(ctx, w.cat.DDL.DropTableStmts, qualified); err != nil {
		return fmt.Errorf("dropping %s: %w", table, err)
	}
	return nil
}

func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	qualified := w.dialect.QualifyTable(schema, w.ident(table))
	if err := w.execDDLStatementList(ctx, w.cat.DDL.TruncateStmts, qualified); err != nil {
		return err
	}
	if w.cat.DDL.TruncateCleanup != "" {
		// Best-effort, matching the hand-written writers (e.g. the
		// sqlite_sequence row may not exist yet).
		_, _ = w.db.ExecContext(ctx, w.cat.DDL.TruncateCleanup, table)
	}
	return nil
}

func (w *Writer) execDDLStatementList(ctx context.Context, templates []string, qualifiedTable string) error {
	if len(templates) == 0 {
		return nil
	}
	if len(templates) == 1 {
		stmt := strings.ReplaceAll(templates[0], "{table}", qualifiedTable)
		_, err := w.db.ExecContext(ctx, stmt)
		return err
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	fkChecksDisabled := false
	for _, tmpl := range templates {
		stmt := strings.ReplaceAll(tmpl, "{table}", qualifiedTable)
		if isMySQLDisableFKChecks(w.cat.Name, stmt) && !fkChecksDisabled {
			fkChecksDisabled = true
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := conn.ExecContext(cleanupCtx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
					logging.Warn("failed to restore mysql FOREIGN_KEY_CHECKS after DDL: %v", err)
				}
			}()
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func isMySQLDisableFKChecks(catalogName, stmt string) bool {
	return catalogName == "mysql" && strings.EqualFold(strings.TrimSpace(stmt), "SET FOREIGN_KEY_CHECKS = 0")
}

func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, w.cat.Introspection.TableExists, w.introArgs(schema, w.ident(table))...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// CreatePrimaryKey preserves DMT's idempotency check after a create plan, then
// executes SMT v1.3.0's standalone primary-key statement unchanged when a
// resumed/evolved target table still needs its key.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}
	hasPK, err := w.HasPrimaryKey(ctx, targetSchema, t.Name)
	if err != nil {
		return fmt.Errorf("checking for existing PK on %s: %w", t.Name, err)
	}
	if hasPK {
		return nil
	}
	if w.cat.Quoting.SchemaIgnored {
		targetSchema = ""
	}
	ddl, err := driver.PlanCreatePrimaryKey(driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  w.cat.Name,
		SourceTable:   t,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("primary-key DDL generation failed for %s: %w", t.FullName(), err)
	}
	logging.DebugEvent("Generated SMT primary-key DDL", "table", t.FullName(), "ddl", ddl)
	if _, err := w.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("creating primary key for %s: %w\nDDL: %s", t.FullName(), err, ddl)
	}
	return nil
}

func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, w.cat.Introspection.HasPrimaryKey, w.introArgs(schema, w.ident(table))...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// GetRowCount tries the stats-based count first, matching the
// hand-written writers — the validator's timeout fallback depends on
// GetRowCountFast being genuinely cheap (sys.partitions /
// pg_stat_user_tables), not a second unbounded COUNT(*).
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return shared.RowCountWithFallback(
		func() (int64, error) { return w.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return w.GetRowCountExact(ctx, schema, table, false) },
	)
}

func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	stats := w.cat.Queries.RowCountStats
	if stats == "" {
		return w.GetRowCountExact(ctx, schema, table, false)
	}
	if strings.Contains(stats, w.dialect.ParameterPlaceholder(1)) {
		var n sql.NullInt64
		if err := w.db.QueryRowContext(ctx, stats, w.introArgs(schema, w.ident(table))...).Scan(&n); err != nil {
			return 0, err
		}
		return n.Int64, nil
	}
	query := fmt.Sprintf(stats, w.dialect.QualifyTable(schema, w.ident(table)))
	return shared.QueryExactRowCount(ctx, w.db, query)
}

func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, w.db, w.dialect, schema, w.ident(table))
}

func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	if w.finalizationMapper == nil {
		return errors.New("finalization mapper not available for index creation")
	}
	if w.cat.Quoting.SchemaIgnored {
		targetSchema = ""
	}
	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeIndex,
		SourceDBType:  w.sourceType,
		TargetDBType:  w.cat.Name,
		Table:         t,
		Index:         idx,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("index DDL generation failed for %s.%s: %w", t.Name, idx.Name, err)
	}
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	return bulkStrategies[w.cat.Bulk.Strategy](ctx, w.bulkEnv(), opts)
}

func (w *Writer) upsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	return upsertStrategies[w.cat.Upsert.Strategy](ctx, w.bulkEnv(), opts)
}

func (w *Writer) resetSequence(ctx context.Context, schema string, t *driver.Table) error {
	return sequenceStrategies[w.cat.Sequence.Strategy](ctx, w.db, w.dialect, schema, t)
}

func (w *Writer) bulkEnv() bulkEnv {
	convert := func(row []any) []any { return row }
	if name := w.cat.Bulk.RowConverter; name != "" {
		convert = rowConverters[name]
	}
	return bulkEnv{
		db:               w.db,
		dialect:          w.dialect,
		batchSize:        w.defaultBatchSize,
		maxVars:          w.cat.Bulk.MaxBindVariables,
		convert:          convert,
		idempotentVerb:   w.cat.Bulk.IdempotentVerb,
		idempotentSuffix: w.cat.Bulk.IdempotentSuffix,
		transactional:    !w.cat.Bulk.NonTransactional,
		engine:           w.cat.Name,
		sourceType:       w.sourceType,
		pgState:          w.pgState,
		myState:          w.myState,
	}
}

// finalizationDDL generates and executes constraint DDL through the
// type-mapper engine — the same path CreateIndex uses.
func (w *Writer) finalizationDDL(ctx context.Context, req driver.FinalizationDDLRequest, kind, name string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for %s creation", kind)
	}
	if w.cat.Quoting.SchemaIgnored {
		req.TargetSchema = ""
	}
	req.SourceDBType = w.sourceType
	req.TargetDBType = w.cat.Name
	req.TargetContext = w.dbContext
	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("%s DDL generation failed for %s.%s: %w", kind, req.Table.Name, name, err)
	}
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	return shared.ExecRaw(ctx, w.db, query, args...)
}

func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return shared.QueryRowRaw(ctx, w.db, query, dest, args...)
}
