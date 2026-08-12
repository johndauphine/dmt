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

	"github.com/johndauphine/dmt/v5/internal/typemap"
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
func GenerateColumnDef(col Column, constraints []Constraint, indexes []Index, sourceDialect, targetDialect string) string {
	quoted := QuoteIdentifier(col.Name, targetDialect)
	isAuto := isAutoIncrementColumn(col, sourceDialect)
	isPK := isPrimaryKeyColumn(col.Name, constraints)

	canonical := typemap.ToCanonical(toTypemapColumn(col), sourceDialect)
	isBoolean := canonical.Kind == typemap.KindBoolean

	typeStr := columnTypeString(col, canonical, sourceDialect, targetDialect, isAuto)
	// Issue #196 / Codex P2 review on PR #207: when an unbounded-text
	// column participates in any uniqueness-enforcing structure on
	// MySQL — PRIMARY KEY, UNIQUE constraint, or unique INDEX — the
	// LONGTEXT widening would either fail at CREATE TABLE (TEXT/BLOB
	// can't be in a key without a prefix) or silently weaken
	// uniqueness (a 255-byte prefix index considers two values that
	// differ past byte 255 as duplicates — wrong vs source semantics).
	// Override the column type to bounded VARCHAR/VARBINARY(255) for
	// these columns: matches the prior pre-#196 emission for the
	// specific keyed columns, preserves uniqueness semantics, and
	// common non-keyed columns still get the LONGTEXT fidelity win.
	//
	// indexes is needed alongside constraints because the driver
	// adapter (driverTableToDDL) projects most source uniqueness as
	// Index{IsUnique:true} rather than ConstraintUnique — only PK is
	// synthesized as a Constraint. Without checking indexes, real
	// migrations from PG/MSSQL with UNIQUE on unbounded text would
	// silently fail at CREATE TABLE on MySQL (Codex review on PR #207).
	if shouldBoundForUniqueness(canonical, constraints, indexes, col.Name, targetDialect) {
		// Pick the binary or text bounded type based on the source
		// column's nature — KindBytes must NOT go through VARCHAR
		// (text encoding would corrupt arbitrary byte values), Codex
		// review on PR #207.
		if canonical.Kind == typemap.KindBytes {
			typeStr = "VARBINARY(255)"
		} else {
			typeStr = "VARCHAR(255)"
		}
	}

	// ClickHouse expresses nullability as a type wrapper, not a column
	// constraint (#507): wrap nullable columns, never emit NOT NULL
	// (non-nullable is already the default). Key columns are never
	// wrapped — the MergeTree sorting key rejects nullable columns,
	// and a PK is semantically non-null regardless of what the source
	// metadata claims (sqlite reports rowid-alias PKs as nullable).
	if targetDialect == DialectClickHouse && col.IsNullable && !isPK {
		typeStr = "Nullable(" + typeStr + ")"
	}

	parts := []string{fmt.Sprintf("    %s %s", quoted, typeStr)}

	// NOT NULL suppression: PG/MSSQL/MySQL all imply NOT NULL on PK
	// columns via the table-level PRIMARY KEY constraint, and identity
	// types (SERIAL, IDENTITY, AUTO_INCREMENT) imply it too. SQLite is
	// the exception: a column in a table-level composite PK is NOT
	// implicitly NOT NULL (only INTEGER PRIMARY KEY rowid aliases get
	// the implicit constraint). So on SQLite we only suppress when the
	// column will receive the inline `PRIMARY KEY AUTOINCREMENT` form
	// — autoIncrementSuffix returning a non-empty string is the signal.
	suppressNotNull := isAuto && isPK
	if targetDialect == DialectSQLite && suppressNotNull {
		if autoIncrementSuffix(col, constraints, targetDialect) == "" {
			suppressNotNull = false
		}
	}
	if !col.IsNullable && !suppressNotNull && targetDialect != DialectClickHouse {
		parts = append(parts, "NOT NULL")
	}

	if !isAuto && col.ColumnDefault != "" {
		expr := FormatDDLDefault(col.ColumnDefault, sourceDialect, targetDialect, isBoolean)
		if expr != "" {
			parts = append(parts, formatDefaultClause(expr, typeStr, targetDialect))
		}
	}

	if isAuto {
		if suffix := autoIncrementSuffix(col, constraints, targetDialect); suffix != "" {
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

// formatDefaultClause emits the `DEFAULT <expr>` clause, wrapping the
// expression in parens for MySQL TEXT/BLOB targets. MySQL <8.0.13
// rejects ordinary defaults on TEXT/BLOB entirely; 8.0.13+ accepts
// them only inside parens (DEFAULT (expr) syntax) — issue #196's
// LONGTEXT widening would otherwise produce DDL like
// `LONGTEXT DEFAULT 'foo'` that fails at CREATE TABLE on every
// MySQL version. dmt targets MySQL 8.0.13+ for this feature; older
// MySQL gets a clear CREATE TABLE error rather than silent
// truncation. (Codex review on PR #207.)
// FormatDefaultClause is the exported form for the generic writer's
// column-alter templates (#509).
func FormatDefaultClause(expr, targetType, targetDialect string) string {
	return formatDefaultClause(expr, targetType, targetDialect)
}

func formatDefaultClause(expr, targetType, targetDialect string) string {
	if targetDialect == DialectMySQL && needsMySQLKeyPrefix(targetType) {
		return "DEFAULT (" + expr + ")"
	}
	return "DEFAULT " + expr
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
	if targetDialect == DialectSQLite {
		// SQLite's autoincrement is INTEGER PRIMARY KEY AUTOINCREMENT.
		// The type MUST be exactly "INTEGER" (case-sensitive in some
		// builds) for the rowid alias to kick in; AUTOINCREMENT keyword
		// is appended in autoIncrementSuffix. The PRIMARY KEY clause
		// itself is emitted by formatPrimaryKey later, which is fine —
		// SQLite accepts both column-level and table-level PK
		// declarations.
		return "INTEGER"
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
func autoIncrementSuffix(col Column, constraints []Constraint, targetDialect string) string {
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
	case DialectSQLite:
		// SQLite's `INTEGER PRIMARY KEY AUTOINCREMENT` form is only
		// valid for a single-column integer primary key — sole PK,
		// integer-affinity type. For composite PKs or for an
		// auto-increment column that isn't part of the PK at all,
		// SQLite has no equivalent declaration (AUTOINCREMENT requires
		// a rowid alias). In those cases, emit no inline suffix; the
		// table-level PK constraint will still be emitted normally,
		// and the column behaves as a plain INTEGER. The
		// auto-increment semantics on SQLite are best-effort here —
		// inserting NULL into an INTEGER PK still gets a unique value
		// via SQLite's rowid mechanism (without the strict-monotonic
		// AUTOINCREMENT guarantee, which only matters for rolled-back
		// inserts).
		if isSoleColumnPK(col.Name, constraints) {
			return "PRIMARY KEY AUTOINCREMENT"
		}
		return ""
	}
	return ""
}

// isSoleColumnPK reports whether the named column is the only column
// in the table's primary-key constraint. Used by the SQLite branch of
// autoIncrementSuffix to decide whether the strict-rowid
// `INTEGER PRIMARY KEY AUTOINCREMENT` form is safe to emit.
func isSoleColumnPK(colName string, constraints []Constraint) bool {
	for _, c := range constraints {
		if c.Type != ConstraintPrimaryKey {
			continue
		}
		return len(c.Columns) == 1 && c.Columns[0] == colName
	}
	return false
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
