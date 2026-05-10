// Identifier quoting and qualified-name assembly. Per-dialect quoting
// rules and default-schema map. Ported verbatim from UVG ddl.rs:
// quote_identifier (line 130) + qualified_table_name (line 502) + the
// default_schema map from dialect.rs.

package ddl

import "strings"

// Dialect names. Match dmt's canonical driver names from
// internal/driver, kept here as string constants so internal/typemap/ddl
// stays import-cycle-clean from the driver package.
const (
	DialectPostgres = "postgres"
	DialectMSSQL    = "mssql"
	DialectMySQL    = "mysql"
)

// QuoteIdentifier wraps an identifier in the target dialect's quoting
// characters and escapes any embedded quote characters. Postgres uses
// double quotes (escape with another double quote), MySQL uses
// backticks (escape with another backtick), MSSQL uses square brackets
// (escape the closing bracket).
func QuoteIdentifier(name, dialect string) string {
	switch dialect {
	case DialectPostgres:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	case DialectMySQL:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case DialectMSSQL:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		// Unknown dialect → fall back to ANSI double-quote (matches
		// Postgres). Better than emitting an unquoted identifier that
		// could collide with a reserved word.
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

// QualifiedTableName returns the schema-qualified table name for the
// target dialect, suppressing the schema when it matches the target's
// default. Mirrors UVG's qualified_table_name with one notable behavior
// from the original: cross-dialect default schemas (PG "public", MSSQL
// "dbo", SQLite "main") are also suppressed when the target is a
// different dialect — the target writes into ITS default schema rather
// than re-emitting the source's default schema name.
//
// MySQL is a special case: its "schema" is the database name, which is
// always set at connection time. The schema prefix is unconditionally
// suppressed for MySQL targets.
func QualifiedTableName(schema, table, dialect string) string {
	if schema == "" || schema == defaultSchema(dialect) {
		return QuoteIdentifier(table, dialect)
	}

	// Cross-dialect default schemas — the source's default name doesn't
	// belong in the target DDL; the target uses its own default.
	if isSourceDefaultSchema(schema) {
		return QuoteIdentifier(table, dialect)
	}

	if dialect == DialectMySQL {
		return QuoteIdentifier(table, dialect)
	}

	return QuoteIdentifier(schema, dialect) + "." + QuoteIdentifier(table, dialect)
}

// defaultSchema returns the dialect's default schema name (the one
// implied when no schema prefix is supplied). MySQL has no schema
// distinct from the database itself, so its "default" is empty.
func defaultSchema(dialect string) string {
	switch dialect {
	case DialectPostgres:
		return "public"
	case DialectMSSQL:
		return "dbo"
	case DialectMySQL:
		return ""
	default:
		return ""
	}
}

// isSourceDefaultSchema returns true for any of the three dialects'
// default schema names. Used by QualifiedTableName to suppress the
// source default schema when emitting cross-dialect DDL.
func isSourceDefaultSchema(schema string) bool {
	return schema == "public" || schema == "dbo" || schema == "main"
}
