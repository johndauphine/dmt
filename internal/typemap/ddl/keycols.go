// Helpers for the issue #196 LONGTEXT/LONGBLOB fix on MySQL targets.
//
// Two distinct strategies handle MySQL's restriction that TEXT/BLOB
// columns can't appear in keys without a prefix length:
//
//  1. PRIMARY KEY / UNIQUE constraints / unique INDEX:
//     `shouldBoundForUniqueness` overrides the COLUMN TYPE itself to
//     bounded VARCHAR/VARBINARY(255). Used by GenerateColumnDef.
//     Preserves uniqueness semantics — a 255-byte prefix on a UNIQUE
//     index would silently weaken uniqueness (two values that differ
//     past byte 255 would conflict), which is worse than truncation.
//
//  2. Non-unique INDEX:
//     `keyColumnRefs` / `formatKeyColumnRef` append a `(255)` prefix
//     length to the column reference INSIDE the index clause. The
//     column type stays LONGTEXT (data fidelity preserved); only the
//     index uses the prefix. Indexes are lookup-only — semantics are
//     preserved. Used only by GenerateIndex for non-unique indexes.
//
// Codex review on PR #207 surfaced both strategies; the file header
// originally claimed `keyColumnRefs` was used for PK/UNIQUE too — that
// claim was wrong, the actual call sites are non-unique INDEX only.

package ddl

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/typemap"
)

// mysqlKeyPrefixLength is the prefix length used for non-unique
// INDEX clauses on MySQL TEXT/BLOB columns. The unit MySQL applies is
// **characters** for non-binary text types (TEXT/LONGTEXT/etc.) and
// **bytes** for BLOB/LONGBLOB — 255 in both cases. The character/byte
// distinction matters under utf8mb4 (4 bytes per character); 255
// chars = 1020 bytes is well under InnoDB's 3072-byte index-key
// limit (default page size). Matches the prior bounded VARCHAR(255)
// width so existing data that fit pre-#196 remains indexable.
const mysqlKeyPrefixLength = 255

// keyColumnRefs returns the column references to use inside a
// non-unique INDEX clause on MySQL. Columns whose mapped type is in
// the TEXT/BLOB family get a (255) key prefix appended; on other
// dialects (or when the type doesn't need a prefix) returns plain
// quoted names. PRIMARY KEY / UNIQUE callers use a different
// strategy (column-type bounding via shouldBoundForUniqueness in
// column.go) — see this file's package comment for why.
func keyColumnRefs(colNames []string, columns []Column, sourceDialect, targetDialect string) []string {
	out := make([]string, len(colNames))
	for i, name := range colNames {
		out[i] = formatKeyColumnRef(name, columns, sourceDialect, targetDialect)
	}
	return out
}

// formatKeyColumnRef returns one column reference for use inside a
// MySQL key clause. Looks up the column's mapped target type; if it's
// a TEXT/BLOB family type, appends `(255)` as a key prefix length.
// Defensive: if the column isn't found in `columns` (shouldn't happen
// in practice — the constraint emitter has the table's columns), the
// plain quoted name is returned.
func formatKeyColumnRef(name string, columns []Column, sourceDialect, targetDialect string) string {
	quoted := QuoteIdentifier(name, targetDialect)
	if targetDialect != DialectMySQL {
		return quoted
	}
	col := findColumnByName(name, columns)
	if col == nil {
		return quoted
	}
	targetType := typemap.MapDDLType(toTypemapColumn(*col), sourceDialect, targetDialect).SQLType
	if !needsMySQLKeyPrefix(targetType) {
		return quoted
	}
	return fmt.Sprintf("%s(%d)", quoted, mysqlKeyPrefixLength)
}

// needsMySQLKeyPrefix reports whether a MySQL DDL type requires a key
// prefix length when used in a non-unique INDEX. The same set of types
// also requires DEFAULT (expr) syntax instead of plain DEFAULT 'literal'
// on MySQL <8.0.13 (used by GenerateColumnDef too).
//
// PRIMARY KEY and UNIQUE callers should NOT use the prefix path —
// shouldBoundForUniqueness in column.go overrides the column type to
// bounded VARCHAR(255) for those cases, so when this function sees
// LONGTEXT in a PK/UNIQUE column reference, that's a bug somewhere.
func needsMySQLKeyPrefix(targetType string) bool {
	switch strings.ToUpper(strings.TrimSpace(targetType)) {
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
		"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return true
	}
	return false
}

// shouldBoundForUniqueness reports whether a column needs to be emitted
// as bounded VARCHAR/VARBINARY(255) on a MySQL target instead of
// LONGTEXT/LONGBLOB because it participates in a PRIMARY KEY constraint,
// a UNIQUE constraint, OR a unique INDEX (the driver adapter
// `driverTableToDDL` projects most source uniqueness as
// `Index{IsUnique:true}` rather than `ConstraintUnique` — only PK is
// synthesized as a Constraint, so checking constraints alone misses
// real-world UNIQUE structures).
//
// The 255-byte/char prefix workaround used by non-unique INDEX is fine
// (indexes are lookup-only, semantics-preserving) but on any
// uniqueness-enforcing structure it would silently weaken uniqueness —
// two values that differ past byte 255 would conflict on insert
// (reject valid source data) or, worse, be treated as duplicates after
// the migration completes (correctness violation). Bounded
// VARCHAR/VARBINARY(255) on the column itself avoids both failure modes.
//
// Trade-off: the column truncates source values >255 chars. This is a
// regression *for unbounded-text columns under uniqueness specifically*,
// restoring the pre-#196 emission for that narrow case. Common non-keyed
// unbounded text (e.g., Posts.Body) still gets the LONGTEXT fidelity
// win. Source schemas with PK/UNIQUE/unique-INDEX on unbounded text
// are rare; users who need full fidelity for keyed-text columns should
// add a length constraint to the source column.
//
// Codex review on PR #207 (post-#196 follow-up).
func shouldBoundForUniqueness(canonical typemap.CanonicalType, constraints []Constraint, indexes []Index, colName, targetDialect string) bool {
	if targetDialect != DialectMySQL {
		return false
	}
	// Only the specific canonical kinds that would map to TEXT/BLOB on
	// MySQL need the override; bounded varchar with a length already
	// emits VARCHAR(N) and never triggers this path.
	switch canonical.Kind {
	case typemap.KindVarchar, typemap.KindBytes:
		if canonical.Length != nil && *canonical.Length > 0 {
			return false
		}
	case typemap.KindText:
		// always
	default:
		return false
	}
	return isInUniquenessStructure(colName, constraints, indexes)
}

// isInUniquenessStructure reports whether the column is part of any
// PRIMARY KEY constraint, UNIQUE constraint, OR unique INDEX on the
// table. The driver adapter (driverTableToDDL) projects source
// uniqueness primarily as Index{IsUnique:true}, so checking constraints
// alone misses real-world UNIQUE structures (Codex review on PR #207).
func isInUniquenessStructure(name string, constraints []Constraint, indexes []Index) bool {
	for _, c := range constraints {
		if c.Type != ConstraintPrimaryKey && c.Type != ConstraintUnique {
			continue
		}
		for _, col := range c.Columns {
			if col == name {
				return true
			}
		}
	}
	for _, idx := range indexes {
		if !idx.IsUnique {
			continue
		}
		for _, col := range idx.Columns {
			if col == name {
				return true
			}
		}
	}
	return false
}

// findColumnByName returns the Column matching `name` from the slice,
// or nil when not found. Returns a pointer into the slice; the caller
// must not retain it past the slice's lifetime.
func findColumnByName(name string, columns []Column) *Column {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}
