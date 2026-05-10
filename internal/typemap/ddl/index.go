// CREATE INDEX emission. Ported from UVG ddl.rs lines 532-555
// (generate_indexes). Each index emits a single CREATE [UNIQUE] INDEX
// statement.
//
// Indexes that just back a UNIQUE constraint are skipped — they're
// already created implicitly when the unique constraint is added. The
// UVG `is_unique_constraint_index` helper drives this; the same logic
// is inlined here as IsUniqueConstraintIndex (exported so #169c's
// adapter can call it independently when filtering the index list).

package ddl

import (
	"fmt"
	"strings"
)

// GenerateIndex emits a CREATE INDEX statement for one index on the
// given table. The caller is responsible for filtering out indexes
// that back unique constraints (use IsUniqueConstraintIndex to test);
// this function emits unconditionally so it stays a single-purpose
// formatter.
//
// Form:
//
//	CREATE [UNIQUE] INDEX <name> ON <table> (<cols>);
//
// Index method (btree, hash, gin, etc.) and other vendor-specific
// kwargs from Index.Kwargs are NOT applied here — they're vendor-
// specific syntax (PG `USING gin`, MySQL prefix lengths) that needs
// per-dialect handling 169c will route through AI when present.
// Plain btree (the default) covers the common case without any kwargs.
func GenerateIndex(table TableInfo, idx Index, sourceDialect, targetDialect string) string {
	uniqueClause := ""
	if idx.IsUnique {
		uniqueClause = "UNIQUE "
	}

	tableName := QualifiedTableName(table.Schema, table.Name, targetDialect)
	// Non-unique INDEX on a TEXT/BLOB column on MySQL gets a (255) key
	// prefix — semantics-preserving (indexes are lookup-only). UNIQUE
	// INDEX on a TEXT/BLOB column does NOT get a prefix because the
	// prefix would silently weaken uniqueness (Codex P2 on PR #207);
	// emitting plain column refs causes MySQL to reject the CREATE
	// INDEX with a clear "used in key specification without a key
	// length" error, which is the right loud failure. PK/UNIQUE
	// CONSTRAINTS on the same shape are handled upstream by
	// shouldBoundForUniqueness in column.go (column type bounded to
	// VARCHAR(255)). Unique INDEX outside of a UNIQUE constraint is the
	// rare case where the source schema author chose unique-via-INDEX
	// rather than unique-via-CONSTRAINT — separate follow-up if it
	// shows up in real workloads.
	var cols []string
	if idx.IsUnique {
		cols = quoteColumnList(idx.Columns, targetDialect)
	} else {
		cols = keyColumnRefs(idx.Columns, table.Columns, sourceDialect, targetDialect)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX %s ON %s (%s);",
		uniqueClause,
		QuoteIdentifier(idx.Name, targetDialect),
		tableName,
		strings.Join(cols, ", "),
	)
}

// IsUniqueConstraintIndex returns true when the index is just the
// implicit one backing a UNIQUE constraint (same column set). The
// caller should skip emitting CREATE INDEX for these — the UNIQUE
// constraint creates them automatically.
//
// Same column comparison is order-sensitive: a UNIQUE (a, b) and an
// index on (b, a) are different indexes (different sort orders),
// even though both enforce the same uniqueness, so they're not
// considered redundant here.
func IsUniqueConstraintIndex(idx Index, constraints []Constraint) bool {
	if !idx.IsUnique {
		return false
	}
	for _, c := range constraints {
		if c.Type == ConstraintUnique && stringSlicesEqual(c.Columns, idx.Columns) {
			return true
		}
	}
	return false
}

// stringSlicesEqual returns true when the two slices have identical
// length and element-by-element equality. Inlined rather than pulled
// from slices.Equal so internal/typemap/ddl stays free of a slices
// import (the package's import surface is intentionally minimal).
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
