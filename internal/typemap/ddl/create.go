// CREATE TABLE assembly. Composes per-column DDL (column.go) with
// inline PRIMARY KEY into a complete CREATE TABLE statement. Matches
// dmt's existing pattern where FOREIGN KEY / UNIQUE / CHECK / INDEX
// constraints are emitted as separate ALTER TABLE statements during
// the orchestrator's finalize phase, NOT inline in CREATE TABLE.
//
// UVG's generate_create_table inlines all constraint types together;
// the per-constraint emission lives in finalize.go to fit dmt's
// FinalizationDDLMapper interface.
//
// Ported from UVG ddl.rs lines 139-263 (the CREATE TABLE portion;
// constraint emission split out per dmt's interface boundaries).

package ddl

import (
	"fmt"
	"strings"
)

// GenerateCreateTable returns a complete CREATE TABLE statement for the
// table in the target dialect. Includes columns + inline PRIMARY KEY.
// Excludes FK / UNIQUE / CHECK / INDEX — those are emitted separately
// by finalize.go (FK/UNIQUE/CHECK) and index.go (INDEX) and applied
// during the orchestrator's finalize phase.
//
// Form:
//
//	CREATE TABLE <qname> (
//	    <col1>,
//	    <col2>,
//	    ...
//	    CONSTRAINT <pk_name> PRIMARY KEY (<cols>)
//	)<comment-suffix-if-mysql>;
//
// Per-column DDL comes from GenerateColumnDef (169a), which handles
// type mapping, nullability, DEFAULT translation, auto-increment, and
// inline MySQL comments.
func GenerateCreateTable(table TableInfo, sourceDialect, targetDialect string) string {
	qname := QualifiedTableName(table.Schema, table.Name, targetDialect)

	parts := make([]string, 0, len(table.Columns)+1)
	for _, col := range table.Columns {
		parts = append(parts, GenerateColumnDef(col, table.Constraints, table.Indexes, sourceDialect, targetDialect))
	}

	if pk := primaryKeyConstraint(table.Constraints); pk != nil {
		// SQLite emits the PRIMARY KEY inline on auto-increment columns
		// (INTEGER PRIMARY KEY AUTOINCREMENT). Skip the table-level PK
		// constraint in that case to avoid a duplicate declaration.
		if !(targetDialect == DialectSQLite && hasInlineAutoIncPK(table, sourceDialect)) {
			parts = append(parts, formatPrimaryKey(*pk, targetDialect))
		}
	}

	body := strings.Join(parts, ",\n")
	tableSuffix := mysqlInlineTableComment(table, targetDialect)

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)%s;", qname, body, tableSuffix)
}

// hasInlineAutoIncPK returns true when at least one column in the table is
// both auto-increment and part of the PK. For SQLite, that column carries
// the PRIMARY KEY AUTOINCREMENT clause inline, so the table-level PK
// constraint must be suppressed.
func hasInlineAutoIncPK(table TableInfo, sourceDialect string) bool {
	pk := primaryKeyConstraint(table.Constraints)
	if pk == nil {
		return false
	}
	for _, col := range table.Columns {
		if !isAutoIncrementColumn(col, sourceDialect) {
			continue
		}
		for _, pkCol := range pk.Columns {
			if pkCol == col.Name {
				return true
			}
		}
	}
	return false
}

// primaryKeyConstraint returns the PK constraint from the table's
// constraint list, or nil when the table has no PK. Most tables in
// real schemas have a PK; the nil case here covers heap-style staging
// tables and CTAS targets.
func primaryKeyConstraint(constraints []Constraint) *Constraint {
	for i := range constraints {
		if constraints[i].Type == ConstraintPrimaryKey {
			return &constraints[i]
		}
	}
	return nil
}

// formatPrimaryKey emits the inline PRIMARY KEY constraint clause.
// Multi-column PKs are formatted as CONSTRAINT name PRIMARY KEY (a, b);
// single-column PKs use the same form for consistency (no special-case
// "PRIMARY KEY" on the column itself).
//
// Plain quoteColumnList is correct here: when a PK column on MySQL
// would otherwise map to LONGTEXT (issue #196), GenerateColumnDef's
// shouldBoundForUniqueness check overrides the column type to bounded
// VARCHAR(255) — preserving uniqueness semantics — so no key-prefix
// length is needed in this clause.
func formatPrimaryKey(pk Constraint, targetDialect string) string {
	cols := quoteColumnList(pk.Columns, targetDialect)
	return fmt.Sprintf("    CONSTRAINT %s PRIMARY KEY (%s)",
		QuoteIdentifier(pk.Name, targetDialect),
		strings.Join(cols, ", "),
	)
}

// mysqlInlineTableComment returns the MySQL inline COMMENT suffix for
// the CREATE TABLE statement (e.g., " COMMENT 'description'") when
// targeting MySQL and the source carries a comment. PG and MSSQL emit
// table comments as separate COMMENT ON / sp_addextendedproperty
// statements; that's deferred to the wiring layer (#169c) since dmt
// doesn't have a comment-emission interface today.
func mysqlInlineTableComment(table TableInfo, targetDialect string) string {
	if targetDialect != DialectMySQL || table.Comment == "" {
		return ""
	}
	escaped := strings.ReplaceAll(table.Comment, "'", "''")
	return fmt.Sprintf(" COMMENT '%s'", escaped)
}
