package validation

import "testing"

func TestQualifiedName_DialectSpecific(t *testing.T) {
	tests := []struct {
		name        string
		driver      string
		schema      string
		table       string
		want        string
		description string
	}{
		{"pg with schema", "postgres", "public", "users", `"public"."users"`,
			"standard PG quote"},
		{"pg empty schema", "postgres", "", "users", `"users"`,
			"no schema → bare table"},
		{"mssql with schema", "mssql", "dbo", "Users", `[dbo].[Users]`,
			"square brackets preserve case"},
		{"mysql with schema", "mysql", "prod_db", "users", "`prod_db`.`users`",
			"MySQL cross-database qualifier preserved (Codex review on #226)"},
		{"mysql empty schema", "mysql", "", "users", "`users`",
			"empty schema → bare table"},
		{"pg name containing double-quote", "postgres", "public", `weird"name`, `"public"."weird""name"`,
			"embedded delimiter must be escaped (Codex review on #226)"},
		{"mssql name containing close-bracket", "mssql", "dbo", `weird]name`, `[dbo].[weird]]name]`,
			"embedded delimiter must be escaped"},
		{"mysql name containing backtick", "mysql", "db", "weird`name", "`db`.`weird``name`",
			"embedded delimiter must be escaped"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := qualifiedName(tc.driver, tc.schema, tc.table)
			if got != tc.want {
				t.Errorf("qualifiedName(%q, %q, %q) = %q, want %q\n  %s",
					tc.driver, tc.schema, tc.table, got, tc.want, tc.description)
			}
		})
	}
}

func TestQuoteIdent_EscapesDelimiters(t *testing.T) {
	tests := []struct {
		driver string
		name   string
		want   string
	}{
		{"postgres", "normal", `"normal"`},
		{"postgres", `weird"name`, `"weird""name"`},
		{"mssql", "normal", "[normal]"},
		{"mssql", "weird]name", "[weird]]name]"},
		{"mysql", "normal", "`normal`"},
		{"mysql", "weird`name", "`weird``name`"},
	}
	for _, tc := range tests {
		t.Run(tc.driver+"/"+tc.name, func(t *testing.T) {
			got := quoteIdent(tc.driver, tc.name)
			if got != tc.want {
				t.Errorf("quoteIdent(%q, %q) = %q, want %q", tc.driver, tc.name, got, tc.want)
			}
		})
	}
}

func TestEndpoint_QualifyTablePreservesSchemaCasing(t *testing.T) {
	// PG identifier sanitizer lowercases table names but the
	// configured target schema is used verbatim by the writer
	// (Codex review on #226). qualifyTable must mirror that.
	ep := Endpoint{
		Driver: "postgres",
		Schema: "Reporting", // mixed-case configured schema
		IdentifierFor: func(s string) string {
			// Apply PG sanitization (lowercase) — same shape as
			// target.SanitizePGIdentifier.
			out := make([]byte, len(s))
			for i := 0; i < len(s); i++ {
				c := s[i]
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				out[i] = c
			}
			return string(out)
		},
	}
	got := ep.qualifyTable("Users")
	// Want: schema unchanged, table lowercased.
	want := `"Reporting"."users"`
	if got != want {
		t.Errorf("qualifyTable preserved schema verbatim:\n  got  %q\n  want %q", got, want)
	}
}
