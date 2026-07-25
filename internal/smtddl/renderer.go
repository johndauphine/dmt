// Package smtddl is DMT's anti-corruption boundary for SMT's public DDL API.
//
// DMT owns migration planning, discovery, execution, and finalization. This
// package deliberately owns only the value conversion needed to render a
// CREATE TABLE statement through github.com/johndauphine/smt/schema. Keeping
// the dependency here prevents SMT's application internals from leaking into
// the DMT driver and orchestrator packages.
package smtddl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/smt/schema"
	"github.com/johndauphine/smt/schema/canonical"
)

// ErrUnsupportedIdentity signals that SMT's selected public dialect declares
// no identity-column capability. DMT retains its established SQLite
// compatibility renderer for that narrow case.
var ErrUnsupportedIdentity = errors.New("SMT DDL dialect does not support identity columns")

// ErrUnsupportedSourceType signals that SMT's public canonical model cannot
// represent a source type. DMT retains its compatibility renderer for source
// types, such as PostgreSQL interval, that its existing typemap can render
// deterministically with an explicit approximation.
var ErrUnsupportedSourceType = errors.New("SMT canonical model does not support source type")

// Request contains DMT's table-rendering context in a dependency-neutral
// shape. The public SMT API intentionally does not know about DMT driver or
// orchestration types.
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

// RenderCreateTable delegates deterministic CREATE TABLE construction to SMT's
// supported public schema API. Unknown source types fail here; DMT's existing
// fallback chain performs the Raw-type gate before this boundary is reached.
func RenderCreateTable(req Request) (string, error) {
	renderer, err := schema.NewRenderer(schema.Options{
		TargetDialect:     req.TargetDialect,
		Schema:            req.TargetSchema,
		SourceDialect:     req.SourceDialect,
		UnknownTypePolicy: schema.UnknownTypeFail,
	})
	if err != nil {
		return "", fmt.Errorf("create SMT renderer: %w", err)
	}
	if !renderer.Capabilities().IdentityColumns {
		for _, column := range req.Table.Columns {
			if column.IsIdentity {
				return "", fmt.Errorf("%w: %s", ErrUnsupportedIdentity, req.TargetDialect)
			}
		}
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
			return "", fmt.Errorf("%w: %s", ErrUnsupportedSourceType, column.DataType)
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

	result, err := renderer.CreateTable(schema.Table{
		Name:       req.Table.Name,
		Columns:    columns,
		PrimaryKey: append([]string(nil), req.Table.PrimaryKey...),
	})
	if err != nil {
		return "", fmt.Errorf("render CREATE TABLE: %w", err)
	}
	sql := result.SQL
	// SMT v1.1.0's PostgreSQL implementation always qualifies table names;
	// with an intentionally empty schema it emits the invalid `""."table"`.
	// DMT uses an empty schema to mean the connection/search-path default, so
	// normalize just that public-renderer edge case at the compatibility seam.
	const emptyPostgresSchemaPrefix = `CREATE TABLE "".`
	if renderer.Dialect() == "postgres" && req.TargetSchema == "" && strings.HasPrefix(sql, emptyPostgresSchemaPrefix) {
		sql = "CREATE TABLE " + strings.TrimPrefix(sql, emptyPostgresSchemaPrefix)
	}
	return sql, nil
}
