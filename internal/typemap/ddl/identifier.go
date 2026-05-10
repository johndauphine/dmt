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
// target dialect, suppressing the schema prefix when it would be
// redundant or wrong:
//
//   - empty schema, or schema matches the TARGET's default → suppress
//   - schema matches the SOURCE dialect's default → suppress (cross-
//     dialect default mapping: PG "public" → MSSQL writes into "dbo",
//     not into a literal "public" schema)
//   - MySQL target, always → suppress (MySQL's "schema" is the database
//     name, always set at connection time)
//
// The sourceDialect parameter is required for the second rule: without
// it, a real user-defined PG schema named "dbo" (or MSSQL schema named
// "public") would be silently suppressed and tables would merge into
// the target's default schema (Codex review on PR #188).
func QualifiedTableName(schema, table, sourceDialect, targetDialect string) string {
	if schema == "" || schema == defaultSchema(targetDialect) {
		return QuoteIdentifier(table, targetDialect)
	}

	if schema == defaultSchema(sourceDialect) {
		return QuoteIdentifier(table, targetDialect)
	}

	if targetDialect == DialectMySQL {
		return QuoteIdentifier(table, targetDialect)
	}

	return QuoteIdentifier(schema, targetDialect) + "." + QuoteIdentifier(table, targetDialect)
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

