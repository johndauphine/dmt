// Column-level CREATE TABLE DDL: type string + nullability + DEFAULT +
// auto-increment. Ported from UVG ddl.rs lines 266-370 plus the
// is_auto_increment_column / is_primary_key_column helpers from
// codegen/mod.rs.
//
// The column-level type string itself comes from the canonical type IR
// (typemap.MapDDLType from #168) — this file just wraps that with the
// surrounding column-DDL grammar (NOT NULL, DEFAULT, IDENTITY suffix,
// etc.).

package ddl

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/typemap"
)

// GenerateColumnDef returns the per-column line that goes inside a
// CREATE TABLE body, indented four spaces (matching UVG's output and
// the prevailing dmt formatter style).
//
// Form:
//
//	"<name>" <TYPE> [NOT NULL] [DEFAULT <expr>] [IDENTITY suffix]
//	                                          [COMMENT '<text>']  -- MySQL only
//
// Auto-increment columns get the dialect-specific encoding handled by
// autoIncrementType / autoIncrementSuffix (PG SERIAL/BIGSERIAL is type-
// shaped; MSSQL IDENTITY(s, i) is suffix-shaped; MySQL AUTO_INCREMENT
// is suffix-shaped). NOT NULL is suppressed for auto-increment PKs
// since the type-form already implies it.
func GenerateColumnDef(col Column, constraints []Constraint, sourceDialect, targetDialect string) string {
	quoted := QuoteIdentifier(col.Name, targetDialect)
	isAuto := isAutoIncrementColumn(col, sourceDialect)
	isPK := isPrimaryKeyColumn(col.Name, constraints)

	canonical := typemap.ToCanonical(toTypemapColumn(col), sourceDialect)
	isBoolean := canonical.Kind == typemap.KindBoolean

	typeStr := columnTypeString(col, canonical, sourceDialect, targetDialect, isAuto)

	parts := []string{fmt.Sprintf("    %s %s", quoted, typeStr)}

	if !col.IsNullable && !(isAuto && isPK) {
		parts = append(parts, "NOT NULL")
	}

	if !isAuto && col.ColumnDefault != "" {
		expr := FormatDDLDefault(col.ColumnDefault, sourceDialect, targetDialect, isBoolean)
		if expr != "" {
			parts = append(parts, "DEFAULT "+expr)
		}
	}

	if isAuto {
		if suffix := autoIncrementSuffix(col, targetDialect); suffix != "" {
			parts = append(parts, suffix)
		}
	}

	// MySQL's per-column COMMENT goes inline; PG / MSSQL emit COMMENT ON
	// as separate statements (handled by the comments generator in 169b).
	if targetDialect == DialectMySQL && col.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", strings.ReplaceAll(col.Comment, "'", "''")))
	}

	return strings.Join(parts, " ")
}

// columnTypeString returns the type string for the column. Auto-
// increment columns have dialect-specific type handling (PG returns
// SERIAL / BIGSERIAL which embeds the type); everything else delegates
// to the canonical type mapper from #168.
func columnTypeString(col Column, canonical typemap.CanonicalType, sourceDialect, targetDialect string, isAuto bool) string {
	if isAuto {
		return autoIncrementType(col, sourceDialect, targetDialect)
	}
	return typemap.FromCanonical(canonical, targetDialect).SQLType
}

// autoIncrementType returns the type string for an auto-increment
// column, possibly overriding the base type. Postgres uses the SERIAL
// family (SMALLSERIAL / SERIAL / BIGSERIAL) which is shorthand for
// SMALLINT / INTEGER / BIGINT plus a sequence — the type field carries
// the auto-increment-ness rather than a separate suffix.
//
// Picks the SERIAL-family member that matches the canonical type's
// width: a SMALLINT identity column maps to SMALLSERIAL (not SERIAL,
// which would silently widen the column to INTEGER — Codex review on
// PR #188). BIG > SMALL match order so an int8 with "SMALL" nowhere in
// its name still wins BIGSERIAL on the BIG check.
func autoIncrementType(col Column, sourceDialect, targetDialect string) string {
	baseType := typemap.MapDDLType(toTypemapColumn(col), sourceDialect, targetDialect).SQLType

	if targetDialect == DialectPostgres {
		switch {
		case strings.Contains(baseType, "BIG"):
			return "BIGSERIAL"
		case strings.Contains(baseType, "SMALL"):
			return "SMALLSERIAL"
		default:
			return "SERIAL"
		}
	}
	return baseType
}

// autoIncrementSuffix returns the trailing modifier that turns a plain
// integer column into an auto-increment one for dialects that don't use
// a SERIAL-style type (everything but Postgres).
//
//	MSSQL  → IDENTITY(start, increment) — values from col.Identity when
//	         present; (1, 1) is the documented MSSQL default.
//	MySQL  → AUTO_INCREMENT
//	PG     → empty (the type itself is SERIAL/BIGSERIAL)
func autoIncrementSuffix(col Column, targetDialect string) string {
	switch targetDialect {
	case DialectPostgres:
		return ""
	case DialectMySQL:
		return "AUTO_INCREMENT"
	case DialectMSSQL:
		start, inc := int64(1), int64(1)
		if col.Identity != nil {
			start, inc = col.Identity.Start, col.Identity.Increment
		}
		return fmt.Sprintf("IDENTITY(%d, %d)", start, inc)
	}
	return ""
}

// isAutoIncrementColumn unifies the four dialects' ways of marking a
// column as auto-incrementing:
//
//	PG / MSSQL → IsIdentity (information_schema.is_identity = YES)
//	MySQL      → Autoincrement (extra column contains "auto_increment")
//	PG SERIAL  → ColumnDefault begins with "nextval(" (legacy serial
//	             columns predate IDENTITY and don't set is_identity)
func isAutoIncrementColumn(col Column, sourceDialect string) bool {
	if col.IsIdentity {
		return true
	}
	if col.Autoincrement != nil && *col.Autoincrement {
		return true
	}
	if sourceDialect == DialectPostgres && strings.HasPrefix(col.ColumnDefault, "nextval(") {
		return true
	}
	return false
}

// isPrimaryKeyColumn returns true when the column is part of any PK
// constraint (single-column or composite). Used by GenerateColumnDef
// to suppress NOT NULL for auto-increment PKs (where the type form
// implies it).
func isPrimaryKeyColumn(colName string, constraints []Constraint) bool {
	for _, c := range constraints {
		if c.Type != ConstraintPrimaryKey {
			continue
		}
		for _, col := range c.Columns {
			if col == colName {
				return true
			}
		}
	}
	return false
}

// toTypemapColumn projects a ddl.Column down to the column-level
// typemap.ColumnInfo that the canonical type mapper consumes. Drops
// the DDL-only fields (Identity, Comment, ColumnDefault, IsIdentity,
// Autoincrement) since the type mapper doesn't need them.
func toTypemapColumn(col Column) typemap.ColumnInfo {
	return typemap.ColumnInfo{
		Name:                   col.Name,
		UDTName:                col.UDTName,
		DataType:               col.DataType,
		CharacterMaximumLength: col.CharacterMaximumLength,
		NumericPrecision:       col.NumericPrecision,
		NumericScale:           col.NumericScale,
		IsNullable:             col.IsNullable,
	}
}
