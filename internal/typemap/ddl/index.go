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
	// MySQL key-prefix handling for TEXT/BLOB columns differs by index
	// uniqueness:
	//
	//   - Non-unique INDEX: append (255) prefix to the column ref —
	//     semantics-preserving (indexes are lookup-only).
	//   - UNIQUE INDEX: emit plain column refs, no prefix. The column
	//     type was already bounded to VARCHAR/VARBINARY(255) by
	//     shouldBoundForUniqueness in column.go (which now checks
	//     unique indexes too — Codex P2 #5 on PR #207), so the
	//     CREATE UNIQUE INDEX succeeds without needing a prefix.
	//     If a unique index ever reaches here with the column type
	//     still LONGTEXT/LONGBLOB (shouldn't happen via the normal
	//     CREATE TABLE path), MySQL will fail loudly with "used in
	//     key specification without a key length" — the correct loud
	//     failure for an unhandled edge case.
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
