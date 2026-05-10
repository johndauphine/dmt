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
		name              string
		schema, table     string
		src, tgt          string
		want              string
	}{
		{"pg_default_public_suppressed", "public", "users", DialectPostgres, DialectPostgres, `"users"`},
		{"mssql_default_dbo_suppressed", "dbo", "users", DialectMSSQL, DialectMSSQL, "[users]"},
		{"mysql_schema_always_suppressed", "anything", "users", DialectMySQL, DialectMySQL, "`users`"},
		{"empty_schema_suppressed", "", "users", DialectPostgres, DialectPostgres, `"users"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QualifiedTableName(tc.schema, tc.table, tc.src, tc.tgt); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQualifiedTableName_CrossDialectDefaultMapping(t *testing.T) {
	// MSSQL "dbo" source → PG target: source default suppressed,
	// target uses its implicit default schema
	if got := QualifiedTableName("dbo", "users", DialectMSSQL, DialectPostgres); got != `"users"` {
		t.Errorf("MSSQL→PG dbo: got %q, want %q", got, `"users"`)
	}
	// PG "public" source → MSSQL target: same pattern
	if got := QualifiedTableName("public", "users", DialectPostgres, DialectMSSQL); got != "[users]" {
		t.Errorf("PG→MSSQL public: got %q, want %q", got, "[users]")
	}
}

func TestQualifiedTableName_NonDefaultSchemaPreserved(t *testing.T) {
	// Custom non-default schema is preserved on PG and MSSQL
	if got := QualifiedTableName("inventory", "items", DialectPostgres, DialectPostgres); got != `"inventory"."items"` {
		t.Errorf("PG custom schema: got %q, want %q", got, `"inventory"."items"`)
	}
	if got := QualifiedTableName("Sales", "orders", DialectMSSQL, DialectMSSQL); got != "[Sales].[orders]" {
		t.Errorf("MSSQL custom schema: got %q, want %q", got, "[Sales].[orders]")
	}
}

// TestQualifiedTableName_PreservesUserSchemaNamedDefault — Codex review
// catch on PR #188. The original implementation hard-coded a list
// (`dbo`, `public`, `main`) and suppressed any schema literally
// matching one regardless of source dialect — a legitimate
// user-defined PG schema named `dbo` would be silently dropped and the
// table would merge into the target's default schema (or, on a PG→PG
// migration, into "public" instead of "dbo").
//
// The fix takes sourceDialect as a parameter and only suppresses when
// the schema matches the SOURCE dialect's default (or the target's).
func TestQualifiedTableName_PreservesUserSchemaNamedDefault(t *testing.T) {
	// PG user schema literally named "dbo" → PG target. "dbo" is NOT
	// PG's default ("public" is), and NOT PG target's default either,
	// so the schema must be preserved.
	if got := QualifiedTableName("dbo", "users", DialectPostgres, DialectPostgres); got != `"dbo"."users"` {
		t.Errorf("PG user schema 'dbo' → PG: got %q, want %q (#188 regression)",
			got, `"dbo"."users"`)
	}
	// MSSQL user schema literally named "public" → MSSQL target. Same
	// pattern: not MSSQL's default ("dbo" is), not target default
	// either, must preserve.
	if got := QualifiedTableName("public", "users", DialectMSSQL, DialectMSSQL); got != "[public].[users]" {
		t.Errorf("MSSQL user schema 'public' → MSSQL: got %q, want %q (#188 regression)",
			got, "[public].[users]")
	}
	// PG user schema "dbo" → MySQL target. MySQL always suppresses.
	// (sanity check: the MySQL-suppression rule still applies even
	// when the schema is a non-default user name.)
	if got := QualifiedTableName("dbo", "users", DialectPostgres, DialectMySQL); got != "`users`" {
		t.Errorf("PG user schema 'dbo' → MySQL: got %q, want %q", got, "`users`")
	}
}
