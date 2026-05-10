// Finalization-phase DDL: FOREIGN KEY, UNIQUE, CHECK constraint
// emission as standalone ALTER TABLE statements. dmt's orchestrator
// runs these in separate phases (TaskCreateFKs / TaskCreateChecks)
// after CREATE TABLE + data load + index creation, so they're emitted
// as ALTER ADD CONSTRAINT rather than inlined in CREATE TABLE.
//
// UVG inlines all constraint types in generate_create_table; the
// per-constraint extraction here matches dmt's
// FinalizationDDLMapper.GenerateFinalizationDDL surface, which calls
// per-constraint, not bundled.
//
// FK action strings (CASCADE, SET NULL, RESTRICT, NO ACTION) pass
// through verbatim from information_schema.referential_constraints —
// they're standardized across the three dialects so no translation is
// needed. NO ACTION is the default and gets suppressed (UVG pattern).

package ddl

import (
	"fmt"
	"strings"
)

// GenerateAddForeignKey emits a standalone ALTER TABLE ADD CONSTRAINT
// statement for a foreign-key constraint. The constraint argument must
// have Type == ConstraintForeignKey and a non-nil ForeignKey field;
// the function trusts the caller to filter (calls with the wrong type
// produce a malformed statement rather than panicking).
//
// Form:
//
//	ALTER TABLE <local> ADD CONSTRAINT <name>
//	    FOREIGN KEY (<local_cols>) REFERENCES <ref> (<ref_cols>)
//	    [ON DELETE <action>] [ON UPDATE <action>];
//
// The ON DELETE / ON UPDATE clauses are omitted when the action is
// NO ACTION (the SQL default; emitting it is redundant and noisier).
func GenerateAddForeignKey(table TableInfo, c Constraint, sourceDialect, targetDialect string) string {
	tableName := QualifiedTableName(table.Schema, table.Name, sourceDialect, targetDialect)
	localCols := quoteColumnList(c.Columns, targetDialect)

	fk := c.ForeignKey
	refTable := QualifiedTableName(fk.RefSchema, fk.RefTable, sourceDialect, targetDialect)
	refCols := quoteColumnList(fk.RefColumns, targetDialect)

	stmt := fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		tableName,
		QuoteIdentifier(c.Name, targetDialect),
		strings.Join(localCols, ", "),
		refTable,
		strings.Join(refCols, ", "),
	)

	if action := normalizeAction(fk.DeleteRule); action != "" {
		stmt += " ON DELETE " + action
	}
	if action := normalizeAction(fk.UpdateRule); action != "" {
		stmt += " ON UPDATE " + action
	}

	return stmt + ";"
}

// GenerateAddUnique emits a standalone ALTER TABLE ADD CONSTRAINT for
// a UNIQUE constraint. The constraint argument must have Type ==
// ConstraintUnique. Single- and multi-column unique constraints share
// the same form (composite is just longer column list).
func GenerateAddUnique(table TableInfo, c Constraint, sourceDialect, targetDialect string) string {
	tableName := QualifiedTableName(table.Schema, table.Name, sourceDialect, targetDialect)
	cols := quoteColumnList(c.Columns, targetDialect)

	return fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
		tableName,
		QuoteIdentifier(c.Name, targetDialect),
		strings.Join(cols, ", "),
	)
}

// GenerateAddCheck emits a standalone ALTER TABLE ADD CONSTRAINT for a
// CHECK constraint. The expression passes through verbatim — cross-
// dialect translation of CHECK expressions is genuinely hard (vendor
// functions, type-cast syntax, etc.) and is one of the surfaces #170's
// AI fallback handles when the expression isn't portable.
//
// CHECK expressions that ARE portable (boolean comparisons against
// literals, IN clauses on enum-style values, simple column references)
// pass through correctly via this verbatim emission.
func GenerateAddCheck(table TableInfo, c Constraint, sourceDialect, targetDialect string) string {
	tableName := QualifiedTableName(table.Schema, table.Name, sourceDialect, targetDialect)

	return fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
		tableName,
		QuoteIdentifier(c.Name, targetDialect),
		c.CheckExpression,
	)
}

// quoteColumnList is the common helper for FK / UNIQUE / index column
// lists — quotes each name for the target dialect.
func quoteColumnList(cols []string, targetDialect string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = QuoteIdentifier(c, targetDialect)
	}
	return out
}

// normalizeAction returns the FK referential-action string after
// suppressing NO ACTION (the SQL default — emitting it is redundant).
// Other actions pass through unchanged. Case-folded comparison so
// drivers reporting "no action" / "No Action" / "NO ACTION" all match.
func normalizeAction(rule string) string {
	trimmed := strings.TrimSpace(rule)
	if trimmed == "" || strings.EqualFold(trimmed, "NO ACTION") {
		return ""
	}
	return trimmed
}
