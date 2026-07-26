// DeterministicMapper is the dmt-driver-side adapter for the
// deterministic typemap surface (#168 / #169). It implements TypeMapper,
// TableTypeMapper, and FinalizationDDLMapper with SMT's public API for target
// schema DDL.
//
// History: 169c added the adapter without changing the default
// type mapper. #170 wires it as the default via GetTypeMapper, with
// AI as a registered fallback for Raw column types (see typemap_chain.go's
// FallbackChain).
//
// The adapter is stateless. The constructor exists for symmetry with
// NewAITypeMapper and to give #170's wiring code a stable factory.

package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/ident"
	"github.com/johndauphine/dmt/internal/smtddl"
	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/smt/schema"
)

// DeterministicMapper implements TypeMapper, TableTypeMapper,
// and FinalizationDDLMapper. SMT owns target-schema rendering; DMT's internal
// typemap remains only for type metadata and compatibility policy. No I/O, no
// LLM calls, no shared state — same inputs always produce the same outputs.
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
		table.Columns[i] = smtddl.Column{
			Name:         sanitizeForTarget(column.Name, req.TargetDBType),
			DataType:     column.DataType,
			FullDataType: column.FullDataType,
			MaxLength:    column.MaxLength,
			Precision:    column.Precision,
			Scale:        column.Scale,
			IsNullable:   column.IsNullable,
			IsIdentity:   column.IsIdentity,
		}
		if Canonicalize(req.SourceDBType) == typemap.DialectMySQL {
			table.Columns[i] = smtddl.ProjectMySQLFullDataType(table.Columns[i])
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

// GenerateFinalizationDDL preserves the mapper compatibility surface while
// hard-binding production side-object rendering to PlanFinalizationDDL.
func (m *DeterministicMapper) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	return PlanFinalizationDDL(req)
}

// PlanFinalizationDDL delegates side-object SQL directly to SMT's public API.
// Production writers call this function rather than an injected mapper so an
// AI mapper cannot become a target-schema renderer through WriterOptions.
func PlanFinalizationDDL(req FinalizationDDLRequest) (string, error) {
	if req.Table == nil {
		return "", fmt.Errorf("PlanFinalizationDDL: Table is required")
	}
	if !isSupportedDialect(req.SourceDBType) || !isSupportedDialect(req.TargetDBType) {
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

// deterministicDDLDialects lists the dialects whose type metadata and target
// schema DDL are supported by the SMT boundary.
func deterministicDDLDialects() []string {
	return []string{
		typemap.DialectPostgres,
		typemap.DialectMSSQL,
		typemap.DialectMySQL,
		typemap.DialectSQLite,
		typemap.DialectClickHouse,
	}
}

// isSupportedDialect returns true when the deterministic mapper can fully
// handle the dialect. Anything else returns false so column-level type mapping
// can route to AI fallback when configured.
func isSupportedDialect(dialect string) bool {
	for _, d := range deterministicDDLDialects() {
		if d == dialect {
			return true
		}
	}
	return false
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

// nullableInt converts dmt's int (where 0 means "unset" by convention) to
// typemap's pointer metadata.
func nullableInt(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}
