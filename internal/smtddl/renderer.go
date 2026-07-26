// Package smtddl is DMT's anti-corruption boundary for SMT's public DDL API.
//
// DMT owns migration planning, discovery, execution, and finalization. This
// package converts discovered metadata into SMT's public schema values and
// returns create statements and evolution batches without constructing or
// modifying SQL. Keeping the dependency here prevents SMT's application
// internals from leaking into the DMT driver and orchestrator packages.
package smtddl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/smt/schema"
	"github.com/johndauphine/smt/schema/canonical"
)

// Request contains DMT's table-rendering context in a dependency-neutral
// shape. A request with an empty Table renders only schema creation. The public
// SMT API intentionally does not know about DMT driver or orchestration types.
type Request struct {
	SourceDialect string
	TargetDialect string
	TargetSchema  string
	Table         Table
}

// Batch and Statement expose SMT's public execution contract through DMT's
// anti-corruption boundary without creating a second representation that
// could lose ordering, affinity, cleanup, or best-effort metadata.
type Batch = schema.Batch
type Statement = schema.Statement

// Table is DMT's table context for SMT create and standalone side-object
// rendering. DMT schedules indexes and constraints after data transfer; SMT
// renders their SQL from this metadata without taking over orchestration.
type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey []string
}

// Column is the source metadata SMT's public schema API needs to deterministically
// emit a target column definition.
type Column struct {
	Name              string
	DataType          string
	FullDataType      string
	MaxLength         int
	Precision         int
	Scale             int
	DatetimePrecision *int
	IsNullable        bool
	IsIdentity        bool
	IsUnsigned        bool
	DisplayWidth      int
	DefaultExpression string
	HasDefault        bool
	EnumValues        []string
	SRID              int
}

// Index is DMT's public-SMT-compatible secondary-index input. IsClustered is
// retained only so the boundary can return SMT's typed unsupported policy for
// the DMT-only source feature rather than silently dropping it or using AI.
type Index struct {
	Name           string
	Columns        []string
	IsUnique       bool
	IsClustered    bool
	IncludeColumns []string
	Filter         string
}

// PrimaryKey is DMT's standalone primary-key input. DMT does not retain a
// discovered primary-key constraint name, so Name is normally empty and SMT
// applies its deterministic pk_<table> convention.
type PrimaryKey struct {
	Name    string
	Columns []string
}

// ForeignKey is DMT's standalone foreign-key input after its migration-schema
// policy has selected the referenced schema.
type ForeignKey struct {
	Name       string
	Columns    []string
	RefSchema  string
	RefTable   string
	RefColumns []string
	OnDelete   schema.ReferentialAction
	OnUpdate   schema.ReferentialAction
}

// CheckConstraint is DMT's standalone check-constraint input.
type CheckConstraint struct {
	Name       string
	Expression string
}

// PlanCreate delegates deterministic create-path construction to SMT's public
// schema.Renderer.PlanCreate API. DMT retains control of when each returned
// statement runs, but must execute its SQL verbatim.
func PlanCreate(req Request) (schema.Plan, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Plan{}, err
	}

	if req.Table.Name == "" {
		plan, err := renderer.PlanCreate(nil)
		if err != nil {
			return schema.Plan{}, fmt.Errorf("plan CREATE schema: %w", err)
		}
		return plan, nil
	}

	columns, err := schemaColumns(req, renderer, "create-table", true)
	if err != nil {
		return schema.Plan{}, err
	}

	plan, err := renderer.PlanCreate([]schema.Table{{
		Name:       req.Table.Name,
		Columns:    columns,
		PrimaryKey: append([]string(nil), req.Table.PrimaryKey...),
	}})
	if err != nil {
		return schema.Plan{}, fmt.Errorf("plan CREATE TABLE: %w", err)
	}
	return plan, nil
}

// RenderIndex returns SMT's standalone index SQL unchanged. It is intentionally
// not a DMT formatter: SMT owns all quoting and dialect capability checks.
func RenderIndex(req Request, index Index) (string, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return "", err
	}
	if index.IsClustered {
		return "", unsupported(renderer, "clustered indexes")
	}
	table, err := tableRef(req, renderer, "index", index.Filter != "")
	if err != nil {
		return "", err
	}
	result, err := renderer.CreateIndex(table, schema.Index{
		Name:           index.Name,
		Columns:        append([]string(nil), index.Columns...),
		IsUnique:       index.IsUnique,
		IncludeColumns: append([]string(nil), index.IncludeColumns...),
		Filter:         index.Filter,
	})
	if err != nil {
		return "", fmt.Errorf("render index with SMT: %w", err)
	}
	return result.SQL, nil
}

// RenderPrimaryKey returns SMT's standalone primary-key SQL unchanged.
func RenderPrimaryKey(req Request, primaryKey PrimaryKey) (string, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return "", err
	}
	table, err := tableRef(req, renderer, "primary-key", false)
	if err != nil {
		return "", err
	}
	result, err := renderer.CreatePrimaryKey(table, schema.PrimaryKey{
		Name:    primaryKey.Name,
		Columns: append([]string(nil), primaryKey.Columns...),
	})
	if err != nil {
		return "", fmt.Errorf("render primary key with SMT: %w", err)
	}
	return result.SQL, nil
}

// RenderForeignKey returns SMT's standalone foreign-key SQL unchanged.
func RenderForeignKey(req Request, foreignKey ForeignKey) (string, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return "", err
	}
	table, err := tableRef(req, renderer, "foreign-key", false)
	if err != nil {
		return "", err
	}
	result, err := renderer.CreateForeignKey(table, schema.ForeignKey{
		Name:       foreignKey.Name,
		Columns:    append([]string(nil), foreignKey.Columns...),
		RefSchema:  foreignKey.RefSchema,
		RefTable:   foreignKey.RefTable,
		RefColumns: append([]string(nil), foreignKey.RefColumns...),
		OnDelete:   foreignKey.OnDelete,
		OnUpdate:   foreignKey.OnUpdate,
	})
	if err != nil {
		return "", fmt.Errorf("render foreign key with SMT: %w", err)
	}
	return result.SQL, nil
}

// RenderCheckConstraint returns SMT's standalone check SQL unchanged.
func RenderCheckConstraint(req Request, check CheckConstraint) (string, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return "", err
	}
	table, err := tableRef(req, renderer, "check-constraint", true)
	if err != nil {
		return "", err
	}
	result, err := renderer.CreateCheckConstraint(table, schema.CheckConstraint{
		Name:       check.Name,
		Expression: check.Expression,
	})
	if err != nil {
		return "", fmt.Errorf("render check constraint with SMT: %w", err)
	}
	return result.SQL, nil
}

// RenderAddColumn delegates one complete column-add operation to SMT's public
// evolution API and returns its ordered execution contract unchanged.
func RenderAddColumn(req Request, column Column) (schema.Batch, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Batch{}, err
	}
	table, err := tableRef(req, renderer, "add-column", true)
	if err != nil {
		return schema.Batch{}, err
	}
	schemaColumn, err := schemaColumn(req, renderer, "add-column", column, false)
	if err != nil {
		return schema.Batch{}, err
	}
	batch, err := renderer.AddColumn(table, schemaColumn)
	if err != nil {
		return schema.Batch{}, fmt.Errorf("render add column with SMT: %w", err)
	}
	return batch, nil
}

// RenderAlterColumnNullability delegates an in-place nullability transition
// to SMT. PostgreSQL can render this operation without a source type; targets
// that restate the type retain SMT's typed validation and capability policy.
func RenderAlterColumnNullability(req Request, column Column) (schema.Batch, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Batch{}, err
	}
	schemaColumn, err := schemaColumn(req, renderer, "alter-column-nullability", column, false)
	if err != nil {
		return schema.Batch{}, err
	}
	batch, err := renderer.AlterColumnNullability(schema.TableRef{Name: req.Table.Name}, schemaColumn)
	if err != nil {
		return schema.Batch{}, fmt.Errorf("render column nullability with SMT: %w", err)
	}
	return batch, nil
}

// RenderAlterColumnType delegates an in-place type transition to SMT and
// returns every statement and advisory flag without rewriting either.
func RenderAlterColumnType(req Request, column Column) (schema.Batch, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Batch{}, err
	}
	schemaColumn, err := schemaColumn(req, renderer, "alter-column-type", column, false)
	if err != nil {
		return schema.Batch{}, err
	}
	batch, err := renderer.AlterColumnType(schema.TableRef{Name: req.Table.Name}, schemaColumn)
	if err != nil {
		return schema.Batch{}, fmt.Errorf("render column type with SMT: %w", err)
	}
	return batch, nil
}

// RenderDropTable delegates an idempotent table drop to SMT. Cascade is an
// explicit DMT scheduling policy and is never inferred at this boundary.
func RenderDropTable(req Request, cascade bool) (schema.Batch, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Batch{}, err
	}
	batch, err := renderer.DropTable(schema.TableRef{Name: req.Table.Name}, schema.DropOptions{Cascade: cascade})
	if err != nil {
		return schema.Batch{}, fmt.Errorf("render table drop with SMT: %w", err)
	}
	return batch, nil
}

// RenderTruncateTable delegates a data-clearing operation to SMT, including
// any required session-affinity and best-effort cleanup contract.
func RenderTruncateTable(req Request, cascade bool) (schema.Batch, error) {
	renderer, err := newRenderer(req)
	if err != nil {
		return schema.Batch{}, err
	}
	batch, err := renderer.TruncateTable(schema.TableRef{Name: req.Table.Name}, schema.TruncateOptions{Cascade: cascade})
	if err != nil {
		return schema.Batch{}, fmt.Errorf("render table truncate with SMT: %w", err)
	}
	return batch, nil
}

func newRenderer(req Request) (schema.Renderer, error) {
	renderer, err := schema.NewRenderer(schema.Options{
		TargetDialect:     req.TargetDialect,
		Schema:            req.TargetSchema,
		SourceDialect:     req.SourceDialect,
		UnknownTypePolicy: schema.UnknownTypeFail,
	})
	if err != nil {
		return schema.Renderer{}, fmt.Errorf("create SMT renderer: %w", err)
	}
	return renderer, nil
}

func tableRef(req Request, renderer schema.Renderer, artifact string, withColumns bool) (schema.TableRef, error) {
	table := schema.TableRef{Name: req.Table.Name}
	if !withColumns {
		return table, nil
	}
	// Side-object renderers need the original columns only as expression context.
	// Unlike CREATE TABLE, an unrelated raw source type must not prevent SMT from
	// rendering an index filter or CHECK predicate that it can support.
	columns, err := schemaColumns(req, renderer, artifact, false)
	if err != nil {
		return schema.TableRef{}, err
	}
	table.Columns = columns
	return table, nil
}

func schemaColumns(req Request, renderer schema.Renderer, artifact string, rejectRaw bool) ([]schema.Column, error) {
	columns := make([]schema.Column, len(req.Table.Columns))
	for i, column := range req.Table.Columns {
		converted, err := schemaColumn(req, renderer, artifact, column, rejectRaw)
		if err != nil {
			return nil, err
		}
		columns[i] = converted
	}
	return columns, nil
}

func schemaColumn(req Request, renderer schema.Renderer, artifact string, column Column, rejectRaw bool) (schema.Column, error) {
	column = projectMySQLFullDataType(req.SourceDialect, column)
	ct := canonical.ToCanonical(column.DataType, canonical.TypeMeta{
		MaxLength:         column.MaxLength,
		Precision:         column.Precision,
		Scale:             column.Scale,
		DatetimePrecision: column.DatetimePrecision,
		IsUnsigned:        column.IsUnsigned,
		DisplayWidth:      column.DisplayWidth,
		EnumValues:        column.EnumValues,
		SRID:              column.SRID,
	}, req.SourceDialect)
	if rejectRaw && ct.Kind == canonical.Raw {
		return schema.Column{}, unsupported(renderer, fmt.Sprintf("source type %q for %s rendering", column.DataType, artifact))
	}
	return schema.Column{
		Name:              column.Name,
		DataType:          column.DataType,
		MaxLength:         column.MaxLength,
		Precision:         column.Precision,
		Scale:             column.Scale,
		DatetimePrecision: column.DatetimePrecision,
		IsNullable:        column.IsNullable,
		IsIdentity:        column.IsIdentity,
		IsUnsigned:        column.IsUnsigned,
		DisplayWidth:      column.DisplayWidth,
		DefaultExpression: column.DefaultExpression,
		HasDefault:        column.HasDefault,
		EnumValues:        append([]string(nil), column.EnumValues...),
		SRID:              column.SRID,
	}, nil
}

// projectMySQLFullDataType recovers the structured facts that MySQL exposes
// only through information_schema.COLUMNS.COLUMN_TYPE. Existing create-path
// callers already populate these fields; evolution callers can pass
// FullDataType and receive the same public SMT input without treating a full
// declaration such as varchar(100) as a catalog type name.
func projectMySQLFullDataType(sourceDialect string, column Column) Column {
	switch strings.ToLower(strings.TrimSpace(sourceDialect)) {
	case "mysql", "mariadb", "maria":
	default:
		return column
	}
	return ProjectMySQLFullDataType(column)
}

// ProjectMySQLFullDataType recovers structured MySQL type metadata from
// information_schema.COLUMNS.COLUMN_TYPE. MySQL reports temporal FSP only in
// that declaration, including the semantically significant implicit FSP 0.
func ProjectMySQLFullDataType(column Column) Column {
	fullDataType := strings.TrimSpace(column.FullDataType)
	lower := strings.ToLower(fullDataType)
	for _, modifier := range strings.Fields(lower) {
		if modifier == "unsigned" || modifier == "zerofill" {
			column.IsUnsigned = true
			break
		}
	}
	first, _, _ := strings.Cut(lower, " ")
	if first == "tinyint(1)" {
		column.DisplayWidth = 1
	}
	switch strings.ToLower(strings.TrimSpace(column.DataType)) {
	case "enum", "set":
		if len(column.EnumValues) == 0 {
			column.EnumValues = typemap.ParseMySQLEnumSetValues(fullDataType)
		}
	case "time", "datetime", "timestamp":
		column.DatetimePrecision = parseMySQLTemporalPrecision(column.DataType, fullDataType)
	}
	return column
}

func parseMySQLTemporalPrecision(dataType, fullDataType string) *int {
	base := strings.ToLower(strings.TrimSpace(dataType))
	decl := strings.ToLower(strings.TrimSpace(fullDataType))
	if decl == "" {
		decl = base
	}
	first, _, _ := strings.Cut(decl, " ")
	if first == base {
		precision := 0
		return &precision
	}
	prefix := base + "("
	if !strings.HasPrefix(first, prefix) || !strings.HasSuffix(first, ")") {
		return nil
	}
	precision, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(first, prefix), ")"))
	if err != nil || precision < 0 || precision > 6 {
		return nil
	}
	return &precision
}

func unsupported(renderer schema.Renderer, feature string) error {
	return &schema.UnsupportedFeatureError{Dialect: renderer.Dialect(), Feature: feature}
}

// RenderCreateSchema returns the schema statement from an SMT create plan. An
// empty result is the public no-op for targets such as SQLite or an omitted
// schema. The SQL is returned unchanged from SMT.
func RenderCreateSchema(req Request) (string, error) {
	plan, err := PlanCreate(req)
	if err != nil {
		return "", err
	}
	for _, statement := range plan.Statements {
		if statement.Kind == schema.StatementCreateSchema {
			return statement.SQL, nil
		}
	}
	return "", nil
}

// RenderCreateTable returns the table statement from an SMT create plan. It
// intentionally does not add a semicolon or otherwise rewrite SMT SQL.
func RenderCreateTable(req Request) (string, error) {
	plan, err := PlanCreate(req)
	if err != nil {
		return "", err
	}
	for _, statement := range plan.Statements {
		if statement.Kind == schema.StatementCreateTable {
			return statement.SQL, nil
		}
	}
	return "", fmt.Errorf("SMT create plan contains no table statement for %q", req.Table.Name)
}
