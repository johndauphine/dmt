// DeterministicMapper is the dmt-driver-side adapter for the
// deterministic typemap surface (#168 / #169). It implements the four
// type-mapper interfaces — TypeMapper, TableTypeMapper,
// FinalizationDDLMapper, TableDropDDLMapper — by delegating to
// internal/typemap (column-level mapping) and internal/typemap/ddl
// (full-table assembly + per-constraint DDL).
//
// 169c: this PR adds the adapter only. It does NOT change the default
// type mapper used by the orchestrator or the per-driver writers —
// they still construct an AITypeMapper via GetAITypeMapper(). Wiring
// the deterministic mapper as the default with AI fallback for Raw
// types is #170.
//
// The adapter is stateless. The constructor exists for symmetry with
// NewAITypeMapper and to give #170's wiring code a stable factory.

package driver

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/dmt/internal/typemap/ddl"
)

// DeterministicMapper implements TypeMapper, TableTypeMapper,
// FinalizationDDLMapper, and TableDropDDLMapper using the internal
// typemap + typemap/ddl packages. No I/O, no LLM calls, no shared
// state — same inputs always produce the same outputs.
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
	return []string{
		typemap.DialectPostgres,
		typemap.DialectMSSQL,
		typemap.DialectMySQL,
	}
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
	if !m.CanMap(req.SourceDBType, req.TargetDBType) {
		return nil, fmt.Errorf("deterministic mapper does not support %s → %s",
			req.SourceDBType, req.TargetDBType)
	}

	tbl := driverTableToDDL(req.SourceTable, req.TargetSchema)
	createDDL := ddl.GenerateCreateTable(tbl, req.SourceDBType, req.TargetDBType)

	columnTypes := make(map[string]string, len(req.SourceTable.Columns))
	for _, col := range req.SourceTable.Columns {
		columnTypes[col.Name] = m.MapType(TypeInfo{
			SourceDBType: req.SourceDBType,
			TargetDBType: req.TargetDBType,
			DataType:     col.DataType,
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

// GenerateFinalizationDDL implements FinalizationDDLMapper. Dispatches
// on req.Type to the right ddl.Generate* function and adapts the
// dmt-driver constraint shapes (driver.ForeignKey, driver.Index,
// driver.CheckConstraint) to the typemap/ddl shapes.
func (m *DeterministicMapper) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	if !m.CanMap(req.SourceDBType, req.TargetDBType) {
		return "", fmt.Errorf("deterministic mapper does not support %s → %s",
			req.SourceDBType, req.TargetDBType)
	}

	tbl := driverTableToDDL(req.Table, req.TargetSchema)

	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return "", fmt.Errorf("DDLTypeIndex requires Index field")
		}
		return ddl.GenerateIndex(tbl, driverIndexToDDL(*req.Index), req.SourceDBType, req.TargetDBType), nil

	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return "", fmt.Errorf("DDLTypeForeignKey requires ForeignKey field")
		}
		return ddl.GenerateAddForeignKey(tbl, driverFKToConstraint(*req.ForeignKey), req.SourceDBType, req.TargetDBType), nil

	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return "", fmt.Errorf("DDLTypeCheckConstraint requires CheckConstraint field")
		}
		return ddl.GenerateAddCheck(tbl, driverCheckToConstraint(*req.CheckConstraint), req.SourceDBType, req.TargetDBType), nil

	case DDLTypeDropTable:
		return generateDropTable(req.TargetSchema, req.Table.Name, req.TargetDBType), nil

	default:
		return "", fmt.Errorf("unknown DDLType %q", req.Type)
	}
}

// GenerateDropTableDDL implements TableDropDDLMapper. Emits a single
// DROP TABLE IF EXISTS statement; the IF EXISTS handles the case where
// the table doesn't exist (e.g., first run of drop_recreate). FK
// drops are handled separately by the orchestrator's drop sequence,
// not bundled here.
func (m *DeterministicMapper) GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error) {
	return generateDropTable(req.TargetSchema, req.TableName, req.TargetDBType), nil
}

// isSupportedDialect returns true when the dialect string is one of
// the three the deterministic mapper handles. Anything else → false
// so wiring can route to AI fallback (#170) for unsupported dialects.
func isSupportedDialect(dialect string) bool {
	switch dialect {
	case typemap.DialectPostgres, typemap.DialectMSSQL, typemap.DialectMySQL:
		return true
	}
	return false
}

// generateDropTable emits the simple DROP TABLE IF EXISTS form.
// Schema-qualified when the target schema is non-default. Targets all
// three dialects with the same ANSI-style syntax — all three accept it.
func generateDropTable(schema, tableName, targetDialect string) string {
	qname := ddl.QualifiedTableName(schema, tableName, targetDialect, targetDialect)
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", qname)
}

// typeInfoToTypemapColumn projects a TypeInfo (column-level request)
// down to the typemap.ColumnInfo the canonical mapper consumes.
// dmt's TypeInfo carries DataType only — typemap needs both UDTName
// (its primary dispatch key) and DataType. dmt's three readers all
// store udt_name-style values in DataType (PG udt_name, MSSQL/MySQL
// data_type), so passing DataType through as both fields works for
// the dispatch.
func typeInfoToTypemapColumn(info TypeInfo) typemap.ColumnInfo {
	return typemap.ColumnInfo{
		UDTName:                info.DataType,
		DataType:               info.DataType,
		CharacterMaximumLength: nullableInt(info.MaxLength),
		NumericPrecision:       nullableInt(info.Precision),
		NumericScale:           nullableInt(info.Scale),
	}
}

// driverColumnToDDL projects a driver.Column to ddl.Column. dmt's
// reader doesn't currently expose column_default / identity start +
// increment / autoincrement flag / column comment — those fields are
// left at zero values, which the ddl emitter handles correctly
// (no DEFAULT clause, no IDENTITY metadata, no inline COMMENT).
//
// IsIdentity flows through, so PG / MSSQL identity columns still get
// SERIAL / IDENTITY emission. PG legacy SERIAL detection (via
// nextval(...) in the default expression) won't fire because dmt
// doesn't carry the default — those columns will emit as plain INT
// with no auto-increment. Acceptable today; #170's AI fallback can
// pick up the legacy-serial case if it matters in practice.
func driverColumnToDDL(col Column) ddl.Column {
	return ddl.Column{
		Name:                   col.Name,
		UDTName:                col.DataType,
		DataType:               col.DataType,
		CharacterMaximumLength: nullableInt(col.MaxLength),
		NumericPrecision:       nullableInt(col.Precision),
		NumericScale:           nullableInt(col.Scale),
		IsNullable:             col.IsNullable,
		IsIdentity:             col.IsIdentity,
	}
}

// driverTableToDDL projects a driver.Table to ddl.TableInfo. The PK
// constraint name is synthesized from the table name (pk_<table>)
// since dmt's reader stores PK as a column-name list with no
// constraint name; the synthesized name matches PG's default naming
// convention and works on all three dialects.
//
// Indexes are passed through; FK and CHECK constraints are NOT
// included on the TableInfo because GenerateCreateTable emits PK only
// per dmt's contract — the orchestrator handles FK / CHECK in the
// finalize phase via GenerateFinalizationDDL.
func driverTableToDDL(t *Table, targetSchema string) ddl.TableInfo {
	columns := make([]ddl.Column, len(t.Columns))
	for i, c := range t.Columns {
		columns[i] = driverColumnToDDL(c)
	}

	var constraints []ddl.Constraint
	if len(t.PrimaryKey) > 0 {
		constraints = append(constraints, ddl.Constraint{
			Name:    "pk_" + t.Name,
			Type:    ddl.ConstraintPrimaryKey,
			Columns: t.PrimaryKey,
		})
	}

	indexes := make([]ddl.Index, len(t.Indexes))
	for i, idx := range t.Indexes {
		indexes[i] = driverIndexToDDL(idx)
	}

	return ddl.TableInfo{
		Schema:      targetSchema,
		Name:        t.Name,
		Columns:     columns,
		Constraints: constraints,
		Indexes:     indexes,
	}
}

// driverIndexToDDL projects driver.Index to ddl.Index. dmt's
// IsClustered / IncludeCols / Filter fields are dropped — they're
// vendor-specific (MSSQL clustered indexes, MSSQL covering indexes,
// MSSQL filtered indexes) and the deterministic emitter doesn't
// support them. Wiring routes those to AI fallback (#170).
func driverIndexToDDL(idx Index) ddl.Index {
	return ddl.Index{
		Name:     idx.Name,
		IsUnique: idx.IsUnique,
		Columns:  idx.Columns,
	}
}

// driverFKToConstraint projects driver.ForeignKey to a
// ddl.Constraint of type ConstraintForeignKey. The OnDelete /
// OnUpdate strings flow through to ddl which applies the
// cross-dialect translation rules (NO ACTION suppressed always,
// RESTRICT suppressed for MSSQL — see #189's Codex-fix).
func driverFKToConstraint(fk ForeignKey) ddl.Constraint {
	return ddl.Constraint{
		Name:    fk.Name,
		Type:    ddl.ConstraintForeignKey,
		Columns: fk.Columns,
		ForeignKey: &ddl.ForeignKey{
			RefSchema:  fk.RefSchema,
			RefTable:   fk.RefTable,
			RefColumns: fk.RefColumns,
			DeleteRule: fk.OnDelete,
			UpdateRule: fk.OnUpdate,
		},
	}
}

// driverCheckToConstraint projects driver.CheckConstraint to a
// ddl.Constraint of type ConstraintCheck. The expression passes
// through verbatim — cross-dialect translation of CHECK expressions
// (vendor functions, type casts) is the AI fallback's job.
func driverCheckToConstraint(c CheckConstraint) ddl.Constraint {
	return ddl.Constraint{
		Name:            c.Name,
		Type:            ddl.ConstraintCheck,
		CheckExpression: c.Definition,
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
