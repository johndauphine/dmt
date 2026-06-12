package ddl

import (
	"strings"
	"testing"
)

// #518: pg_get_expr wraps plain string comparisons in text-type casts
// (::text, ::character varying(n)) that only PostgreSQL parses.
// Cross-engine emission strips them; everything else passes verbatim.
func TestNormalizeCheckExpression(t *testing.T) {
	cases := []struct {
		name, expr, source, target, want string
	}{
		{"pg text casts stripped cross-engine",
			"((region)::text <> ''::text)", "postgres", "mssql",
			"((region) <> '')"},
		{"pg varchar cast with length stripped",
			"((code)::character varying(10) IN ('a'::character varying))", "postgres", "mysql",
			"((code) IN ('a'))"},
		{"same-engine passes verbatim",
			"((region)::text <> ''::text)", "postgres", "postgres",
			"((region)::text <> ''::text)"},
		{"non-text casts preserved (AI-fallback surface)",
			"((price)::numeric > 0::numeric)", "postgres", "mssql",
			"((price)::numeric > 0::numeric)"},
		{"non-pg sources untouched",
			"([Region]<>'')", "mssql", "postgres",
			"([Region]<>'')"},
		{"casts inside string literals preserved",
			"((tag)::text <> 'foo::text'::text)", "postgres", "mssql",
			"((tag) <> 'foo::text')"},
		{"escaped quotes keep literal boundaries",
			"((name)::text <> 'it''s::text ok'::text)", "postgres", "mssql",
			"((name) <> 'it''s::text ok')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeCheckExpression(tc.expr, tc.source, tc.target)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Non-portable casts that survive normalization must route to the AI
// fallback rather than emit DDL the target can't parse.
func TestCheckExpressionNeedsFallback(t *testing.T) {
	cases := []struct {
		name, expr, source, target string
		want                       bool
	}{
		{"numeric cast cross-engine", "((price)::numeric > (0)::numeric)", "postgres", "mssql", true},
		{"text casts strip clean", "((region)::text <> ''::text)", "postgres", "mssql", false},
		{"cast only inside literal", "(tag <> 'a::b')", "postgres", "mysql", false},
		{"same engine never falls back", "((price)::numeric > 0)", "postgres", "postgres", false},
		{"pg target parses pg casts", "((price)::numeric > 0)", "postgres", "pg", false},
		{"non-pg source untouched", "([n]>(0))", "mssql", "mysql", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CheckExpressionNeedsFallback(tc.expr, tc.source, tc.target); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// #518: the referenced table must be qualified with the TARGET schema —
// a pg source's "public" reaching an mssql target as
// REFERENCES [public].[users] fails because the data lives in [dbo].
func TestGenerateAddForeignKeyUsesTargetSchema(t *testing.T) {
	tbl := TableInfo{Schema: "dbo", Name: "orders"}
	c := Constraint{
		Name: "fk_orders_user", Type: ConstraintForeignKey,
		Columns: []string{"user_id"},
		ForeignKey: &ForeignKey{
			RefSchema: "dbo", RefTable: "users", RefColumns: []string{"id"},
			DeleteRule: "CASCADE",
		},
	}
	got := GenerateAddForeignKey(tbl, c, "postgres", "mssql")
	// QualifiedTableName suppresses the dialect default schema (dbo),
	// so the reference renders unqualified — resolving in the schema
	// the migration wrote to.
	if !strings.Contains(got, "REFERENCES [users] ([id])") {
		t.Errorf("unexpected ref table form: %s", got)
	}
	if strings.Contains(got, "public") {
		t.Errorf("source schema leaked into DDL: %s", got)
	}

	// Non-default target schema stays qualified.
	tbl.Schema = "sales"
	c.ForeignKey.RefSchema = "sales"
	got = GenerateAddForeignKey(tbl, c, "postgres", "mssql")
	if !strings.Contains(got, "REFERENCES [sales].[users]") {
		t.Errorf("non-default schema not qualified: %s", got)
	}
	// External-schema references pass through (the mapper only remaps
	// references into the migrated schema).
	c.ForeignKey.RefSchema = "auth"
	got = GenerateAddForeignKey(tbl, c, "postgres", "mssql")
	if !strings.Contains(got, "REFERENCES [auth].[users]") {
		t.Errorf("external schema not preserved: %s", got)
	}
}
