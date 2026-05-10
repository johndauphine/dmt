// Identifier-quoting and qualified-name tests. Ported from UVG ddl.rs
// `#[cfg(test)] mod tests` (lines 675-689) plus extra cross-dialect
// QualifiedTableName cases that UVG's tests imply but don't exercise
// directly.

package ddl

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		dialect string
		want    string
	}{
		{"pg_simple", "users", DialectPostgres, `"users"`},
		{"pg_escapes_double_quote", `my"table`, DialectPostgres, `"my""table"`},
		{"mysql_simple", "users", DialectMySQL, "`users`"},
		{"mysql_escapes_backtick", "my`tbl", DialectMySQL, "`my``tbl`"},
		{"mssql_simple", "users", DialectMSSQL, "[users]"},
		{"mssql_escapes_close_bracket", "my]tbl", DialectMSSQL, "[my]]tbl]"},
		{"unknown_dialect_falls_back_to_pg", "users", "oracle", `"users"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QuoteIdentifier(tc.input, tc.dialect); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQualifiedTableName_DefaultSchemaSuppressed(t *testing.T) {
	tests := []struct {
		name          string
		schema, table string
		target        string
		want          string
	}{
		{"pg_default_public_suppressed", "public", "users", DialectPostgres, `"users"`},
		{"mssql_default_dbo_suppressed", "dbo", "users", DialectMSSQL, "[users]"},
		{"mysql_schema_always_suppressed", "anything", "users", DialectMySQL, "`users`"},
		{"empty_schema_suppressed", "", "users", DialectPostgres, `"users"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QualifiedTableName(tc.schema, tc.table, tc.target); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQualifiedTableName_NonDefaultSchemaPreserved(t *testing.T) {
	// Custom non-default schema is preserved on PG and MSSQL.
	if got := QualifiedTableName("inventory", "items", DialectPostgres); got != `"inventory"."items"` {
		t.Errorf("PG custom schema: got %q, want %q", got, `"inventory"."items"`)
	}
	if got := QualifiedTableName("Sales", "orders", DialectMSSQL); got != "[Sales].[orders]" {
		t.Errorf("MSSQL custom schema: got %q, want %q", got, "[Sales].[orders]")
	}
}

// TestQualifiedTableName_PreservesUserSchemaNamedSourceDefault — Codex
// review on PR #190. The original PR #188 implementation suppressed
// any schema matching the SOURCE dialect's default, on the theory that
// the schema field carried the source's schema and we wanted the
// target to use its own default. That was wrong for dmt's use case
// where the schema field actually carries the user-chosen TARGET
// schema; the suppression silently dropped the user's choice when
// their target schema happened to match the source dialect's default
// name (e.g., source=mssql, TargetSchema="dbo" on a non-MSSQL target).
//
// The fix removed the rule and the sourceDialect parameter entirely.
// QualifiedTableName now treats the schema as the literal target
// schema; cross-dialect default mapping is the caller's responsibility
// (typically by passing empty schema). These cases would have been
// silently suppressed under the old rule:
func TestQualifiedTableName_PreservesUserSchemaNamedSourceDefault(t *testing.T) {
	// "dbo" is MSSQL's default schema name. A user targeting PG with
	// TargetSchema="dbo" wants a literal "dbo" schema in PG. The old
	// code silently dropped this when source happened to be MSSQL.
	if got := QualifiedTableName("dbo", "users", DialectPostgres); got != `"dbo"."users"` {
		t.Errorf("user schema 'dbo' → PG target: got %q, want %q (#190 regression)",
			got, `"dbo"."users"`)
	}
	// "public" is PG's default schema name. Same scenario inverted:
	// user targets MSSQL with TargetSchema="public".
	if got := QualifiedTableName("public", "users", DialectMSSQL); got != "[public].[users]" {
		t.Errorf("user schema 'public' → MSSQL target: got %q, want %q (#190 regression)",
			got, "[public].[users]")
	}
	// Same-dialect case still preserves a non-target-default schema.
	if got := QualifiedTableName("dbo", "users", DialectPostgres); got != `"dbo"."users"` {
		t.Errorf("PG → PG with non-default schema 'dbo': got %q, want %q",
			got, `"dbo"."users"`)
	}
}

// TestQualifiedTableName_MySQLSuppressesAlways verifies the third
// suppression rule still works after the parameter change.
func TestQualifiedTableName_MySQLSuppressesAlways(t *testing.T) {
	for _, schema := range []string{"any_schema", "dbo", "public", ""} {
		t.Run("schema_"+schema, func(t *testing.T) {
			if got := QualifiedTableName(schema, "users", DialectMySQL); got != "`users`" {
				t.Errorf("MySQL target should suppress schema %q; got %q",
					schema, got)
			}
		})
	}
}
