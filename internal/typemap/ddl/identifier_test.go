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
		name             string
		schema, table, d string
		want             string
	}{
		{"pg_default_public_suppressed", "public", "users", DialectPostgres, `"users"`},
		{"mssql_default_dbo_suppressed", "dbo", "users", DialectMSSQL, "[users]"},
		{"mysql_schema_always_suppressed", "anything", "users", DialectMySQL, "`users`"},
		{"empty_schema_suppressed", "", "users", DialectPostgres, `"users"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QualifiedTableName(tc.schema, tc.table, tc.d); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQualifiedTableName_NonDefaultSchema(t *testing.T) {
	// MSSQL "dbo" → PG: source default name is suppressed; PG falls back
	// to its own default ("public", which is the implicit default)
	if got := QualifiedTableName("dbo", "users", DialectPostgres); got != `"users"` {
		t.Errorf("MSSQL→PG default schema mapping: got %q, want %q", got, `"users"`)
	}
	// PG "public" → MSSQL: same shape — source default suppressed
	if got := QualifiedTableName("public", "users", DialectMSSQL); got != "[users]" {
		t.Errorf("PG→MSSQL default schema mapping: got %q, want %q", got, "[users]")
	}
	// Custom non-default schema is preserved on PG and MSSQL
	if got := QualifiedTableName("inventory", "items", DialectPostgres); got != `"inventory"."items"` {
		t.Errorf("PG custom schema: got %q, want %q", got, `"inventory"."items"`)
	}
	if got := QualifiedTableName("Sales", "orders", DialectMSSQL); got != "[Sales].[orders]" {
		t.Errorf("MSSQL custom schema: got %q, want %q", got, "[Sales].[orders]")
	}
}
