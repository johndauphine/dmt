// DeterministicMapper is the dmt-driver-side adapter for the
// deterministic typemap surface (#168 / #169). It implements the four
// type-mapper interfaces — TypeMapper, TableTypeMapper,
// FinalizationDDLMapper, TableDropDDLMapper — with SMT's public API for
// adopted create and side-object DDL and internal typemap/ddl only for later
// artifacts.
//
// History: 169c added the adapter without changing the default
// type mapper. #170 wires it as the default via GetTypeMapper, with
// AI as a registered fallback for Raw types and unadopted compatibility
// paths (see typemap_chain.go's FallbackChain).
//
// The adapter is stateless. The constructor exists for symmetry with
// NewAITypeMapper and to give #170's wiring code a stable factory.

package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/ident"
	"github.com/johndauphine/dmt/internal/smtddl"
	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/dmt/internal/typemap/ddl"
	"github.com/johndauphine/smt/schema"
)

// ErrUnsupportedDDL is retained for non-adopted finalization compatibility.
// Adopted create and side-object paths return SMT's typed policy errors
// directly and never use this sentinel to select an AI renderer.
var ErrUnsupportedDDL = errors.New("deterministic mapper: vendor-specific feature not supported by deterministic path")

// DeterministicMapper implements TypeMapper, TableTypeMapper,
// FinalizationDDLMapper, and TableDropDDLMapper. SMT owns create rendering;
// DMT's internal typemap remains for type metadata and later DDL artifacts.
// No I/O, no LLM calls, no shared state — same inputs always produce the same
// outputs.
type DeterministicMapper struct{}

// NewDeterministicMapper returns the deterministic mapper. The
// constructor takes no arguments today; it exists as a stable factory
// for #170's wiring code (and to match the shape of the AI mapper's
// constructor for the decorator pattern).
func NewDeterministicMapper() *DeterministicMapper {
	return &DeterministicMapper{}
}

// MapType implements TypeMapper. Looks up the column type via the
// canonical IR (#168) and returns the target dialect's DDL type
// string. Lossy translations (e.g. PG interval → MSSQL NVARCHAR(255))
// stay deterministic with an IsApproximate warning that #170's wiring
// can surface.
func (m *DeterministicMapper) MapType(info TypeInfo) string {
	col := typeInfoToTypemapColumn(info)
	return typemap.MapDDLType(col, info.SourceDBType, info.TargetDBType).SQLType
}

// CanMap implements TypeMapper. Returns true for any (source, target)
// pair where both dialects are in the deterministic mapper's catalog.
// Today that's the cross product of {postgres, mssql, mysql}.
func (m *DeterministicMapper) CanMap(sourceDBType, targetDBType string) bool {
	return isSupportedDialect(sourceDBType) && isSupportedDialect(targetDBType)
}

// SupportedTargets implements TypeMapper. Returns the list of target
// dialects this mapper can emit DDL for. Used by the registry/wiring
// layer to advertise capabilities.
func (m *DeterministicMapper) SupportedTargets() []string {
	return deterministicDDLDialects()
}

// GenerateTableDDL implements TableTypeMapper. Assembles the source
// table's columns + PRIMARY KEY into a complete CREATE TABLE statement
// for the target dialect. FK / UNIQUE / CHECK / INDEX go to the
// finalize phase via GenerateFinalizationDDL — matching dmt's
// existing TaskCreateFKs / TaskCreateChecks / TaskCreateIndexes
// orchestrator phases.
//
// The ColumnTypes map in the response is populated for telemetry /
// log reasoning so a reviewer can see the mapped target type per
// source column without re-parsing the DDL string.
func (m *DeterministicMapper) GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error) {
	createDDL, err := PlanCreateTable(req)
	if err != nil {
		return nil, err
	}

	columnTypes := make(map[string]string, len(req.SourceTable.Columns))
	for _, col := range req.SourceTable.Columns {
		columnTypes[col.Name] = m.MapType(TypeInfo{
			SourceDBType: req.SourceDBType,
			TargetDBType: req.TargetDBType,
			DataType:     col.DataType,
			FullDataType: col.FullDataType,
			MaxLength:    col.MaxLength,
			Precision:    col.Precision,
			Scale:        col.Scale,
		})
	}

	return &TableDDLResponse{
		CreateTableDDL: createDDL,
		ColumnTypes:    columnTypes,
		Notes:          "deterministic mapper (UVG-derived port; no AI)",
	}, nil
}

// PlanCreateTable converts DMT discovery metadata to SMT's public create plan
// and returns its table statement unchanged. It is the sole production create
// table boundary, shared by writers independently of any optional AI mapper.
func PlanCreateTable(req TableDDLRequest) (string, error) {
	if req.SourceTable == nil {
		return "", fmt.Errorf("PlanCreateTable: SourceTable is required")
	}
	createDDL, err := smtddl.RenderCreateTable(smtDDLRequest(req))
	if err != nil {
		// Create-path ownership belongs to SMT. Unsupported source features
		// deliberately propagate SMT's public typed policy instead of falling
		// back to DMT's legacy SQL renderer or local AI DDL generation.
		return "", fmt.Errorf("rendering CREATE TABLE with SMT: %w", err)
	}
	return createDDL, nil
}

// smtDDLRequest converts DMT discovery metadata to the narrow public SMT
// CREATE TABLE API. It also retains DMT-specific compatibility policies that
// sit outside deterministic rendering: target-schema suppression follows the
// active connection's database/schema, and keyed MySQL LOBs are bounded so
// they remain valid uniqueness keys.
func smtDDLRequest(req TableDDLRequest) smtddl.Request {
	targetSchema := smtDDLTargetSchema(req.TargetSchema, req.TargetDBType)
	table := smtddl.Table{
		// Keep CREATE TABLE identifiers aligned with DMT's writer and
		// finalization paths. In particular, PostgreSQL targets must use
		// ident.SanitizePG before SMT quotes the names; otherwise a
		// mixed-case source identifier would be created case-preserved but
		// later transfer operations would look it up lowercased.
		Name:       sanitizeForTarget(req.SourceTable.Name, req.TargetDBType),
		PrimaryKey: make([]string, len(req.SourceTable.PrimaryKey)),
		Columns:    make([]smtddl.Column, len(req.SourceTable.Columns)),
	}
	for i, name := range req.SourceTable.PrimaryKey {
		table.PrimaryKey[i] = sanitizeForTarget(name, req.TargetDBType)
	}
	for i, column := range req.SourceTable.Columns {
		dataType := column.FullDataType
		if dataType == "" {
			dataType = column.DataType
		}
		table.Columns[i] = smtddl.Column{
			Name:       sanitizeForTarget(column.Name, req.TargetDBType),
			DataType:   dataType,
			MaxLength:  column.MaxLength,
			Precision:  column.Precision,
			Scale:      column.Scale,
			IsNullable: column.IsNullable,
			IsIdentity: column.IsIdentity,
		}
	}

	// A MySQL TEXT/BLOB primary or unique key is invalid without a prefix;
	// DMT's established policy is a bounded 255-character/byte key, not a
	// prefix index that would weaken uniqueness semantics. Preserve that policy
	// before handing the table to SMT's deliberately generic public API.
	if Canonicalize(req.TargetDBType) == typemap.DialectMySQL {
		boundSMTMySQLUniqueLOBs(&table, req.SourceTable, req.SourceDBType)
	}

	return smtddl.Request{
		SourceDialect: req.SourceDBType,
		TargetDialect: req.TargetDBType,
		TargetSchema:  targetSchema,
		Table:         table,
	}
}

// smtDDLTargetSchema preserves DMT's existing qualification contract before
// invoking SMT. MySQL and SQLite use the connected database, while the
// PostgreSQL and MSSQL default schemas are intentionally unqualified.
func smtDDLTargetSchema(schema, targetDialect string) string {
	// The legacy DDL path sanitizes before QualifiedTableName applies its
	// target-default schema rule. Preserve that order so, for example,
	// PostgreSQL target schema "Public" follows the same unqualified
	// search-path behavior as "public".
	schema = sanitizeForTarget(schema, targetDialect)
	switch Canonicalize(targetDialect) {
	case typemap.DialectMySQL, typemap.DialectSQLite:
		return ""
	case typemap.DialectPostgres:
		if schema == "public" {
			return ""
		}
	case typemap.DialectMSSQL:
		if schema == "dbo" {
			return ""
		}
	}
	return schema
}

func boundSMTMySQLUniqueLOBs(table *smtddl.Table, source *Table, sourceDialect string) {
	if table == nil || source == nil {
		return
	}
	keyed := make(map[string]struct{}, len(source.PrimaryKey))
	for _, name := range source.PrimaryKey {
		keyed[name] = struct{}{}
	}
	for _, index := range source.Indexes {
		if !index.IsUnique {
			continue
		}
		for _, name := range index.Columns {
			keyed[name] = struct{}{}
		}
	}
	for i, sourceColumn := range source.Columns {
		if _, ok := keyed[sourceColumn.Name]; !ok {
			continue
		}
		mapped := typemap.MapDDLType(driverColumnToTypemapColumnInfo(sourceColumn), sourceDialect, typemap.DialectMySQL).SQLType
		switch strings.ToUpper(strings.TrimSpace(mapped)) {
		case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			table.Columns[i].DataType = "varchar"
			table.Columns[i].MaxLength = 255
		case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
			table.Columns[i].DataType = "varbinary"
			table.Columns[i].MaxLength = 255
		}
	}
}

// GenerateFinalizationDDL retains DMT's finalization scheduling boundary while
// delegating adopted side-object SQL to SMT's public API. Drop statements stay
// on their existing local path until a later SMT API milestone.
func (m *DeterministicMapper) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	if req.Table == nil {
		return "", fmt.Errorf("GenerateFinalizationDDL: Table is required")
	}
	if !m.CanMap(req.SourceDBType, req.TargetDBType) {
		return "", fmt.Errorf("deterministic mapper does not support %s → %s",
			req.SourceDBType, req.TargetDBType)
	}

	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return "", fmt.Errorf("DDLTypeIndex requires Index field")
		}
		sql, err := smtddl.RenderIndex(smtFinalizationRequest(req), smtIndex(*req.Index, req.TargetDBType))
		if err != nil {
			return "", fmt.Errorf("rendering index %q with SMT: %w", req.Index.Name, err)
		}
		return sql, nil

	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return "", fmt.Errorf("DDLTypeForeignKey requires ForeignKey field")
		}
		sql, err := smtddl.RenderForeignKey(smtFinalizationRequest(req), smtForeignKey(*req.ForeignKey, req.Table.Schema, req.TargetSchema, req.TargetDBType))
		if err != nil {
			return "", fmt.Errorf("rendering foreign key %q with SMT: %w", req.ForeignKey.Name, err)
		}
		return sql, nil

	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return "", fmt.Errorf("DDLTypeCheckConstraint requires CheckConstraint field")
		}
		sql, err := smtddl.RenderCheckConstraint(smtFinalizationRequest(req), smtddl.CheckConstraint{
			Name:       sanitizeForTarget(req.CheckConstraint.Name, req.TargetDBType),
			Expression: req.CheckConstraint.Definition,
		})
		if err != nil {
			return "", fmt.Errorf("rendering check constraint %q with SMT: %w", req.CheckConstraint.Name, err)
		}
		return sql, nil

	case DDLTypeDropTable:
		return generateDropTable(req.TargetSchema, req.Table.Name, req.TargetDBType), nil

	default:
		return "", fmt.Errorf("unknown DDLType %q", req.Type)
	}
}

// PlanCreatePrimaryKey returns SMT's standalone primary-key statement. DMT
// calls it only after its existing target-side idempotency check confirms that
// a pre-existing table still needs its key.
func PlanCreatePrimaryKey(req TableDDLRequest) (string, error) {
	if req.SourceTable == nil {
		return "", fmt.Errorf("PlanCreatePrimaryKey: SourceTable is required")
	}
	columns := make([]string, len(req.SourceTable.PrimaryKey))
	for i, column := range req.SourceTable.PrimaryKey {
		columns[i] = sanitizeForTarget(column, req.TargetDBType)
	}
	sql, err := smtddl.RenderPrimaryKey(smtDDLRequest(req), smtddl.PrimaryKey{Columns: columns})
	if err != nil {
		return "", fmt.Errorf("rendering standalone primary key with SMT: %w", err)
	}
	return sql, nil
}

func smtFinalizationRequest(req FinalizationDDLRequest) smtddl.Request {
	return smtDDLRequest(TableDDLRequest{
		SourceDBType:  req.SourceDBType,
		TargetDBType:  req.TargetDBType,
		SourceTable:   req.Table,
		TargetSchema:  req.TargetSchema,
		TargetContext: req.TargetContext,
	})
}

func smtIndex(index Index, targetDialect string) smtddl.Index {
	columns := make([]string, len(index.Columns))
	for i, column := range index.Columns {
		columns[i] = sanitizeForTarget(column, targetDialect)
	}
	includeColumns := make([]string, len(index.IncludeCols))
	for i, column := range index.IncludeCols {
		includeColumns[i] = sanitizeForTarget(column, targetDialect)
	}
	return smtddl.Index{
		Name:           sanitizeForTarget(index.Name, targetDialect),
		Columns:        columns,
		IsUnique:       index.IsUnique,
		IsClustered:    index.IsClustered,
		IncludeColumns: includeColumns,
		Filter:         index.Filter,
	}
}

func smtForeignKey(foreignKey ForeignKey, sourceSchema, targetSchema, targetDialect string) smtddl.ForeignKey {
	columns := make([]string, len(foreignKey.Columns))
	for i, column := range foreignKey.Columns {
		columns[i] = sanitizeForTarget(column, targetDialect)
	}
	refColumns := make([]string, len(foreignKey.RefColumns))
	for i, column := range foreignKey.RefColumns {
		refColumns[i] = sanitizeForTarget(column, targetDialect)
	}
	refSchema := foreignKey.RefSchema
	if refSchema == "" || refSchema == sourceSchema {
		refSchema = smtDDLTargetSchema(targetSchema, targetDialect)
	} else {
		refSchema = sanitizeForTarget(refSchema, targetDialect)
	}
	return smtddl.ForeignKey{
		Name:       sanitizeForTarget(foreignKey.Name, targetDialect),
		Columns:    columns,
		RefSchema:  refSchema,
		RefTable:   sanitizeForTarget(foreignKey.RefTable, targetDialect),
		RefColumns: refColumns,
		OnDelete:   schema.ReferentialAction(foreignKey.OnDelete),
		OnUpdate:   schema.ReferentialAction(foreignKey.OnUpdate),
	}
}

// GenerateDropTableDDL implements TableDropDDLMapper. Emits a single
// DROP TABLE IF EXISTS statement; the IF EXISTS handles the case where
// the table doesn't exist (e.g., first run of drop_recreate). FK
// drops are handled separately by the orchestrator's drop sequence,
// not bundled here.
func (m *DeterministicMapper) GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error) {
	if req.TableName == "" {
		return "", fmt.Errorf("GenerateDropTableDDL: TableName is required")
	}
	if !isSupportedDialect(req.TargetDBType) {
		return "", fmt.Errorf("deterministic mapper does not support target dialect %q", req.TargetDBType)
	}
	return generateDropTable(req.TargetSchema, req.TableName, req.TargetDBType), nil
}

// deterministicDDLDialects lists the dialects the deterministic mapper
// can generate FULL DDL for — type fragments AND quoting/drop/index
// syntax (internal/typemap/ddl). Deliberately NOT registry-backed
// (codex review on #479): typemap.Register only supplies type
// fragments, so a catalog engine that registered types alone must
// still route to AI fallback here until #191 wires catalog DDL
// templates into this gate as well.
func deterministicDDLDialects() []string {
	return []string{
		typemap.DialectPostgres,
		typemap.DialectMSSQL,
		typemap.DialectMySQL,
		typemap.DialectSQLite,
		typemap.DialectClickHouse,
	}
}

// isSupportedDialect returns true when the deterministic mapper can
// fully handle the dialect (types + DDL syntax). Anything else → false
// so wiring can route to AI fallback (#170) for unsupported dialects.
func isSupportedDialect(dialect string) bool {
	for _, d := range deterministicDDLDialects() {
		if d == dialect {
			return true
		}
	}
	return false
}

// generateDropTable emits the simple DROP TABLE IF EXISTS form.
//
// MSSQL behavior is special: the schema is ALWAYS qualified when
// non-empty, even when it matches the dialect's default ("dbo").
// SQL Server allows per-login default schemas, so an unqualified
// `DROP TABLE [users]` could resolve to a different schema than the
// subsequent CREATE / INSERT operations (which always qualify via
// Dialect.QualifyTable elsewhere in the MSSQL driver). The deterministic
// adapter MUST stay consistent with that convention or DROP/CREATE
// can target different objects (Copilot review on PR #190).
//
// PG and MySQL behave correctly under QualifiedTableName's default-
// suppression rules: PG's session search_path always puts the default
// schema first, and MySQL has no schema distinct from the database.
func generateDropTable(schema, tableName, targetDialect string) string {
	// Sanitize the table name for PG targets so DROP matches the
	// case-folded name the rest of dmt's PG flow uses (Writer.
	// CreatePrimaryKey, FK creation, etc. all sanitize via
	// ident.SanitizePG). Without this, DROP looks for "LinkTypes"
	// (case-preserved by quoting) while CREATE / INSERT use
	// "linktypes" (lowercased) and the operations target different
	// rows in pg_class.
	tableName = sanitizeForTarget(tableName, targetDialect)
	schema = sanitizeForTarget(schema, targetDialect)

	if targetDialect == typemap.DialectMSSQL && schema != "" {
		// Always qualify on MSSQL — see function doc.
		return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;",
			ddl.QuoteIdentifier(schema, targetDialect),
			ddl.QuoteIdentifier(tableName, targetDialect),
		)
	}
	qname := ddl.QualifiedTableName(schema, tableName, targetDialect)
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", qname)
}

// sanitizeForTarget runs the target-dialect's identifier sanitization.
// Postgres folds unquoted identifiers to lowercase, and the rest of
// dmt's PG writer code (CreatePrimaryKey, FK creation, etc.) sanitizes
// via ident.SanitizePG before emitting DDL. The deterministic typemap
// adapter MUST do the same on PG targets or the CREATE TABLE name
// (case-preserved by quoting) won't match what later phases look up.
//
// MSSQL is case-insensitive natively, so its identifier resolution
// works whether quoted or not — no sanitization needed. MySQL is
// case-sensitive on Linux but typically table names match source
// casing; pass through.
func sanitizeForTarget(name, targetDialect string) string {
	if targetDialect == typemap.DialectPostgres {
		return ident.SanitizePG(name)
	}
	return name
}

// typeInfoToTypemapColumn projects a TypeInfo (column-level request)
// down to the typemap.ColumnInfo the canonical mapper consumes.
// dmt's TypeInfo carries DataType for dispatch plus optional FullDataType for
// dialect-specific modifiers such as MySQL COLUMN_TYPE.
func typeInfoToTypemapColumn(info TypeInfo) typemap.ColumnInfo {
	fullType := info.FullDataType
	if fullType == "" {
		fullType = info.DataType
	}
	return typemap.ColumnInfo{
		UDTName:                info.DataType,
		DataType:               fullType,
		CharacterMaximumLength: nullableInt(info.MaxLength),
		NumericPrecision:       nullableInt(info.Precision),
		NumericScale:           nullableInt(info.Scale),
	}
}

// driverColumnToTypemapColumnInfo keeps the MySQL keyed-LOB compatibility
// preflight in DMT's metadata layer. It maps types but never emits SQL; SMT
// remains the sole create-path renderer.
func driverColumnToTypemapColumnInfo(col Column) typemap.ColumnInfo {
	return typeInfoToTypemapColumn(TypeInfo{
		DataType:     col.DataType,
		FullDataType: col.FullDataType,
		MaxLength:    col.MaxLength,
		Precision:    col.Precision,
		Scale:        col.Scale,
	})
}

// driverColumnToDDL projects a driver.Column to ddl.Column. DefaultValue is
// intentionally not passed through here: #305 uses source defaults for
// read-only drift reporting, while target DEFAULT preservation remains a
// separate DDL behavior decision. dmt's reader also doesn't currently expose
// identity start + increment / autoincrement flag / column comment, so those
// fields are left at zero values.
//
// IsIdentity flows through, so PG / MSSQL identity columns still get
// SERIAL / IDENTITY emission.
//
// Column name is sanitized for PG targets (lowercase) to match the
// rest of dmt's PG flow which uses ident.SanitizePG. Without this,
// CREATE TABLE column "CreatedAt" would survive case-preserved while
// later INSERT / index code looks up "createdat" — mismatch.
func driverColumnToDDL(col Column, targetDialect string) ddl.Column {
	fullType := col.FullDataType
	if fullType == "" {
		fullType = col.DataType
	}
	return ddl.Column{
		Name:                   sanitizeForTarget(col.Name, targetDialect),
		UDTName:                col.DataType,
		DataType:               fullType,
		CharacterMaximumLength: nullableInt(col.MaxLength),
		NumericPrecision:       nullableInt(col.Precision),
		NumericScale:           nullableInt(col.Scale),
		IsNullable:             col.IsNullable,
		IsIdentity:             col.IsIdentity,
	}
}

// driverTableToDDL projects a driver.Table to ddl.TableInfo. The PK
// constraint name is synthesized as `pk_<table>` since dmt's reader
// stores PK as a column-name list with no constraint name. The
// synthesized name does NOT match Postgres's actual default
// (Postgres uses `<table>_pkey`) — it's just a readable convention
// the deterministic mapper picks; introspecting the resulting target
// will show the synthesized name, not the source's PK name (Copilot
// review on PR #190).
//
// targetDialect drives identifier sanitization: PG targets get
// lowercased identifiers via ident.SanitizePG so the emitted CREATE
// TABLE matches the case-folded names the rest of dmt's PG flow
// (Writer.CreatePrimaryKey, FK creation, INSERT) uses.
//
// Secondary indexes, foreign keys, and checks are deliberately absent: SMT
// owns their standalone rendering through the finalization boundary.
func driverTableToDDL(t *Table, targetSchema, targetDialect string) ddl.TableInfo {
	columns := make([]ddl.Column, len(t.Columns))
	for i, c := range t.Columns {
		columns[i] = driverColumnToDDL(c, targetDialect)
	}

	tableName := sanitizeForTarget(t.Name, targetDialect)

	var constraints []ddl.Constraint
	if len(t.PrimaryKey) > 0 {
		pkColumns := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			pkColumns[i] = sanitizeForTarget(col, targetDialect)
		}
		constraints = append(constraints, ddl.Constraint{
			Name:    "pk_" + tableName,
			Type:    ddl.ConstraintPrimaryKey,
			Columns: pkColumns,
		})
	}

	return ddl.TableInfo{
		Schema:      sanitizeForTarget(targetSchema, targetDialect),
		Name:        tableName,
		Columns:     columns,
		Constraints: constraints,
	}
}

// nullableInt converts dmt's int (where 0 means "unset" by
// convention) to typemap/ddl's *int (where nil means "unset").
// MSSQL's -1 MAX sentinel is handled by typemap.ToCanonical (PR #185
// Codex-fix), so it doesn't need special treatment here.
func nullableInt(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}
