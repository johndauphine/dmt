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

// Table is the CREATE TABLE subset that DMT passes to SMT. Secondary indexes,
// foreign keys, and checks remain in DMT's ordered finalization phase.
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

// PlanCreate delegates deterministic create-path construction to SMT's public
// schema.Renderer.PlanCreate API. DMT retains control of when each returned
// statement runs, but must execute its SQL verbatim.
func PlanCreate(req Request) (schema.Plan, error) {
	renderer, err := schema.NewRenderer(schema.Options{
		TargetDialect:     req.TargetDialect,
		Schema:            req.TargetSchema,
		SourceDialect:     req.SourceDialect,
		UnknownTypePolicy: schema.UnknownTypeFail,
	})
	if err != nil {
		return schema.Plan{}, fmt.Errorf("create SMT renderer: %w", err)
	}

	if req.Table.Name == "" {
		plan, err := renderer.PlanCreate(nil)
		if err != nil {
			return schema.Plan{}, fmt.Errorf("plan CREATE schema: %w", err)
		}
		return plan, nil
	}

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
			return schema.Plan{}, &schema.UnsupportedFeatureError{
				Dialect: renderer.Dialect(),
				Feature: fmt.Sprintf("source type %q for create-table rendering", column.DataType),
			}
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

// UnsupportedStandalonePrimaryKey returns SMT's public typed policy error for
// the rare resume/evolution case where a table already exists without its
// primary key. SMT v1.2.0 PlanCreate owns inline primary keys only; DMT must
// not synthesize an ALTER TABLE fallback locally.
func UnsupportedStandalonePrimaryKey(targetDialect string) error {
	return &schema.UnsupportedFeatureError{
		Dialect: targetDialect,
		Feature: "standalone primary-key creation; use PlanCreate with the table primary key",
	}
}
