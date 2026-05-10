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
	tableName := QualifiedTableName(table.Schema, table.Name, targetDialect)
	localCols := quoteColumnList(c.Columns, targetDialect)

	fk := c.ForeignKey
	refTable := QualifiedTableName(fk.RefSchema, fk.RefTable, targetDialect)
	refCols := quoteColumnList(fk.RefColumns, targetDialect)

	stmt := fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		tableName,
		QuoteIdentifier(c.Name, targetDialect),
		strings.Join(localCols, ", "),
		refTable,
		strings.Join(refCols, ", "),
	)

	if action := normalizeActionForTarget(fk.DeleteRule, targetDialect); action != "" {
		stmt += " ON DELETE " + action
	}
	if action := normalizeActionForTarget(fk.UpdateRule, targetDialect); action != "" {
		stmt += " ON UPDATE " + action
	}

	return stmt + ";"
}

// GenerateAddUnique emits a standalone ALTER TABLE ADD CONSTRAINT for
// a UNIQUE constraint. The constraint argument must have Type ==
// ConstraintUnique. Single- and multi-column unique constraints share
// the same form (composite is just longer column list).
func GenerateAddUnique(table TableInfo, c Constraint, sourceDialect, targetDialect string) string {
	tableName := QualifiedTableName(table.Schema, table.Name, targetDialect)
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
	tableName := QualifiedTableName(table.Schema, table.Name, targetDialect)

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

// normalizeActionForTarget returns the FK referential-action string
// after applying two cross-dialect rules:
//
//  1. NO ACTION (the SQL default) is suppressed for ALL targets —
//     emitting it is redundant noise.
//  2. RESTRICT is suppressed for MSSQL targets — MSSQL does not accept
//     RESTRICT as a referential action (only CASCADE / SET NULL /
//     SET DEFAULT / NO ACTION are valid). PG and MySQL both report
//     RESTRICT in information_schema (PG treats it as the strict
//     immediate-check semantic; MySQL parses it as a synonym for
//     NO ACTION). Mapping to NO ACTION on MSSQL is the standard
//     cross-dialect translation — without this, PG/MySQL → MSSQL
//     migrations would emit invalid `ON DELETE RESTRICT` and FK
//     creation would fail (Codex review on PR #189).
//
// Other actions (CASCADE, SET NULL, SET DEFAULT) pass through
// unchanged. Case-folded comparison so drivers reporting "no action" /
// "No Action" / "NO ACTION" all match.
func normalizeActionForTarget(rule, targetDialect string) string {
	trimmed := strings.TrimSpace(rule)
	if trimmed == "" || strings.EqualFold(trimmed, "NO ACTION") {
		return ""
	}
	if targetDialect == DialectMSSQL && strings.EqualFold(trimmed, "RESTRICT") {
		return ""
	}
	return trimmed
}
