// Package smtddl is DMT's anti-corruption boundary for SMT's public DDL API.
//
// DMT owns migration planning, discovery, execution, and finalization. This
// package converts discovered metadata into SMT's public schema values and
// returns PlanCreate statements without constructing or modifying SQL. Keeping
// the dependency here prevents SMT's application internals from leaking into
// the DMT driver and orchestrator packages.
package smtddl

import (
	"fmt"

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
	Name         string
	DataType     string
	MaxLength    int
	Precision    int
	Scale        int
	IsNullable   bool
	IsIdentity   bool
	IsUnsigned   bool
	DisplayWidth int
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

	columns, err := schemaColumns(req, renderer, "create-table")
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
	columns, err := schemaColumns(req, renderer, artifact)
	if err != nil {
		return schema.TableRef{}, err
	}
	table.Columns = columns
	return table, nil
}

func schemaColumns(req Request, renderer schema.Renderer, artifact string) ([]schema.Column, error) {
	columns := make([]schema.Column, len(req.Table.Columns))
	for i, column := range req.Table.Columns {
		ct := canonical.ToCanonical(column.DataType, canonical.TypeMeta{
			MaxLength:    column.MaxLength,
			Precision:    column.Precision,
			Scale:        column.Scale,
			IsUnsigned:   column.IsUnsigned,
			DisplayWidth: column.DisplayWidth,
		}, req.SourceDialect)
		if ct.Kind == canonical.Raw {
			return nil, unsupported(renderer, fmt.Sprintf("source type %q for %s rendering", column.DataType, artifact))
		}
		columns[i] = schema.Column{
			Name:         column.Name,
			DataType:     column.DataType,
			MaxLength:    column.MaxLength,
			Precision:    column.Precision,
			Scale:        column.Scale,
			IsNullable:   column.IsNullable,
			IsIdentity:   column.IsIdentity,
			IsUnsigned:   column.IsUnsigned,
			DisplayWidth: column.DisplayWidth,
		}
	}
	return columns, nil
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
