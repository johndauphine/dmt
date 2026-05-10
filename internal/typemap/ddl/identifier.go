// Identifier quoting and qualified-name assembly. Per-dialect quoting
// rules and default-schema map. Ported verbatim from UVG ddl.rs:
// quote_identifier (line 130) + qualified_table_name (line 502) + the
// default_schema map from dialect.rs.

package ddl

import (
	"strings"

	"github.com/johndauphine/dmt/internal/typemap"
)

// Dialect names. Re-exported from internal/typemap to give the ddl
// subpackage a single self-contained import surface (callers can use
// ddl.DialectPostgres without separately importing typemap), while
// keeping the actual string values in one place to prevent drift
// (Copilot review on PR #188).
const (
	DialectPostgres = typemap.DialectPostgres
	DialectMSSQL    = typemap.DialectMSSQL
	DialectMySQL    = typemap.DialectMySQL
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
// target dialect, suppressing the schema prefix only when it would be
// redundant:
//
//   - empty schema → suppress (caller is opting out of qualification)
//   - schema matches the TARGET dialect's default → suppress (the
//     target dialect would resolve the same table either way)
//   - MySQL target, always → suppress (MySQL's "schema" is the
//     database name, always set at connection time)
//
// The schema argument is treated as the LITERAL target schema — what
// the caller wants to appear in the emitted DDL. No cross-dialect
// "source default → target default" mapping happens here. Callers
// that want that behavior must perform the mapping themselves before
// calling this function (typically by passing empty schema).
//
// History: PR #188 added a `sourceDialect` parameter and a
// "schema-matches-source-default → suppress" rule borrowed from UVG.
// That rule misfires when a caller passes the user's chosen TARGET
// schema and that name happens to collide with the source dialect's
// default — e.g., source=mssql with user-chosen TargetSchema="dbo"
// on a non-MSSQL target would be silently dropped (Copilot review on
// PR #190). The rule and the parameter are removed.
func QualifiedTableName(schema, table, targetDialect string) string {
	if schema == "" || schema == defaultSchema(targetDialect) {
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
