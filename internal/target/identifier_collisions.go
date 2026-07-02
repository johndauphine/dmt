package target

import (
	"fmt"
	"strings"
)

// PGIdentifierCollision describes a set of distinct source identifiers that
// all sanitize to the same PostgreSQL identifier.
type PGIdentifierCollision struct {
	Sanitized string
	Originals []string
}

// DetectPGIdentifierCollisions reports many-to-one sanitization collisions
// across the table set and within each table's columns. ident.SanitizePG is
// many-to-one (case-folding, non-alphanumeric → '_', 63-byte truncation), so
// distinct source identifiers can collapse to the same PostgreSQL name. In
// drop_recreate that silently destroys one colliding table's data; for
// columns it produces a CREATE TABLE with duplicate columns. Callers should
// invoke this before any DDL and fail the migration, so the operator can
// rename the source identifiers rather than lose data undetected (#553).
//
// Returns nil when there are no collisions.
func DetectPGIdentifierCollisions(tables []TableInfo) error {
	var msgs []string

	// Collisions across sanitized table names.
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.GetName()
	}
	for _, c := range findSanitizedCollisions(tableNames) {
		msgs = append(msgs, fmt.Sprintf("table names %s all sanitize to %q",
			quoteList(c.Originals), c.Sanitized))
	}

	// Collisions among sanitized column names, within each table.
	for _, t := range tables {
		for _, c := range findSanitizedCollisions(t.GetColumnNames()) {
			msgs = append(msgs, fmt.Sprintf("in table %q, columns %s all sanitize to %q",
				t.GetName(), quoteList(c.Originals), c.Sanitized))
		}
	}

	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf("PostgreSQL identifier sanitization collisions detected "+
		"(would silently overwrite table data or fail DDL); rename the source "+
		"identifiers to disambiguate:\n  - %s", strings.Join(msgs, "\n  - "))
}

// findSanitizedCollisions groups the given identifiers by their sanitized
// form and returns the groups that contain more than one distinct original.
// Groups and their originals are returned in first-seen order so callers get
// deterministic output.
func findSanitizedCollisions(names []string) []PGIdentifierCollision {
	order := make([]string, 0, len(names))
	groups := make(map[string][]string, len(names))

	for _, name := range names {
		s := SanitizePGIdentifier(name)
		if _, seen := groups[s]; !seen {
			order = append(order, s)
		}
		if !containsString(groups[s], name) {
			groups[s] = append(groups[s], name)
		}
	}

	var out []PGIdentifierCollision
	for _, s := range order {
		if len(groups[s]) > 1 {
			out = append(out, PGIdentifierCollision{Sanitized: s, Originals: groups[s]})
		}
	}
	return out
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// quoteList renders identifiers as a comma-separated, double-quoted list for
// error messages, e.g. `"Order Items", "Order-Items"`.
func quoteList(xs []string) string {
	quoted := make([]string, len(xs))
	for i, x := range xs {
		quoted[i] = fmt.Sprintf("%q", x)
	}
	return strings.Join(quoted, ", ")
}
