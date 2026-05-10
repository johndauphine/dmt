// Finalization-DDL tests: GenerateAddForeignKey / GenerateAddUnique /
// GenerateAddCheck. These match dmt's per-constraint
// FinalizationDDLMapper interface.

package ddl

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
)

func TestGenerateAddForeignKey_BasicCascade(t *testing.T) {
	table := TableInfo{Name: "messages"}
	c := Constraint{
		Name:    "fk_messages_users",
		Type:    ConstraintForeignKey,
		Columns: []string{"user_id"},
		ForeignKey: &ForeignKey{
			RefTable: "users", RefColumns: []string{"id"},
			DeleteRule: "CASCADE", UpdateRule: "NO ACTION",
		},
	}

	ddl := GenerateAddForeignKey(table, c, typemap.DialectPostgres, typemap.DialectPostgres)

	want := `ALTER TABLE "messages" ADD CONSTRAINT "fk_messages_users" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE;`
	if ddl != want {
		t.Errorf("\ngot:  %s\nwant: %s", ddl, want)
	}
}

func TestGenerateAddForeignKey_NoActionSuppressed(t *testing.T) {
	// NO ACTION is the SQL default — emitting it is redundant noise.
	// Both rules NO ACTION → no ON DELETE / ON UPDATE clause at all.
	table := TableInfo{Name: "messages"}
	c := Constraint{
		Name:    "fk_messages_users",
		Type:    ConstraintForeignKey,
		Columns: []string{"user_id"},
		ForeignKey: &ForeignKey{
			RefTable: "users", RefColumns: []string{"id"},
			DeleteRule: "NO ACTION", UpdateRule: "NO ACTION",
		},
	}

	ddl := GenerateAddForeignKey(table, c, typemap.DialectPostgres, typemap.DialectPostgres)

	if strings.Contains(ddl, "ON DELETE") || strings.Contains(ddl, "ON UPDATE") {
		t.Errorf("NO ACTION rules should be suppressed; got:\n%s", ddl)
	}
}

func TestGenerateAddForeignKey_BothRules(t *testing.T) {
	table := TableInfo{Name: "messages"}
	c := Constraint{
		Name:    "fk_messages_users",
		Type:    ConstraintForeignKey,
		Columns: []string{"user_id"},
		ForeignKey: &ForeignKey{
			RefTable: "users", RefColumns: []string{"id"},
			DeleteRule: "SET NULL", UpdateRule: "CASCADE",
		},
	}

	ddl := GenerateAddForeignKey(table, c, typemap.DialectPostgres, typemap.DialectPostgres)

	if !strings.Contains(ddl, "ON DELETE SET NULL") {
		t.Errorf("SET NULL delete rule missing; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "ON UPDATE CASCADE") {
		t.Errorf("CASCADE update rule missing; got:\n%s", ddl)
	}
}

func TestGenerateAddForeignKey_ActionCaseInsensitive(t *testing.T) {
	// Drivers report rules in different cases ("no action", "No Action",
	// "NO ACTION") — all three should be treated as the suppress-default.
	for _, variant := range []string{"no action", "No Action", "NO ACTION"} {
		t.Run(variant, func(t *testing.T) {
			table := TableInfo{Name: "messages"}
			c := Constraint{
				Name: "fk", Type: ConstraintForeignKey, Columns: []string{"user_id"},
				ForeignKey: &ForeignKey{
					RefTable: "users", RefColumns: []string{"id"},
					DeleteRule: variant, UpdateRule: variant,
				},
			}
			ddl := GenerateAddForeignKey(table, c, typemap.DialectPostgres, typemap.DialectPostgres)
			if strings.Contains(ddl, "ON DELETE") || strings.Contains(ddl, "ON UPDATE") {
				t.Errorf("%q should be treated as suppress-default; got:\n%s", variant, ddl)
			}
		})
	}
}

func TestGenerateAddForeignKey_CompositeFK(t *testing.T) {
	table := TableInfo{Name: "order_lines"}
	c := Constraint{
		Name:    "fk_order_lines",
		Type:    ConstraintForeignKey,
		Columns: []string{"order_id", "tenant_id"},
		ForeignKey: &ForeignKey{
			RefTable: "orders", RefColumns: []string{"id", "tenant_id"},
			DeleteRule: "RESTRICT",
		},
	}

	ddl := GenerateAddForeignKey(table, c, typemap.DialectPostgres, typemap.DialectMSSQL)
	if !strings.Contains(ddl, `([order_id], [tenant_id])`) {
		t.Errorf("composite local cols missing; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, `([id], [tenant_id])`) {
		t.Errorf("composite ref cols missing; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "ON DELETE RESTRICT") {
		t.Errorf("RESTRICT action missing; got:\n%s", ddl)
	}
}

func TestGenerateAddUnique(t *testing.T) {
	table := TableInfo{Name: "users"}
	c := Constraint{Name: "uq_users_email", Type: ConstraintUnique, Columns: []string{"email"}}

	ddl := GenerateAddUnique(table, c, typemap.DialectPostgres, typemap.DialectPostgres)
	want := `ALTER TABLE "users" ADD CONSTRAINT "uq_users_email" UNIQUE ("email");`
	if ddl != want {
		t.Errorf("\ngot:  %s\nwant: %s", ddl, want)
	}
}

func TestGenerateAddUnique_Composite(t *testing.T) {
	table := TableInfo{Name: "users"}
	c := Constraint{
		Name:    "uq_users_tenant_email",
		Type:    ConstraintUnique,
		Columns: []string{"tenant_id", "email"},
	}

	ddl := GenerateAddUnique(table, c, typemap.DialectPostgres, typemap.DialectMySQL)
	if !strings.Contains(ddl, "UNIQUE (`tenant_id`, `email`)") {
		t.Errorf("composite UNIQUE list missing; got:\n%s", ddl)
	}
}

func TestGenerateAddCheck(t *testing.T) {
	table := TableInfo{Name: "items"}
	c := Constraint{
		Name:            "ck_price_positive",
		Type:            ConstraintCheck,
		CheckExpression: "price > 0",
	}

	ddl := GenerateAddCheck(table, c, typemap.DialectPostgres, typemap.DialectPostgres)
	want := `ALTER TABLE "items" ADD CONSTRAINT "ck_price_positive" CHECK (price > 0);`
	if ddl != want {
		t.Errorf("\ngot:  %s\nwant: %s", ddl, want)
	}
}

func TestGenerateAddCheck_ExpressionPassThrough(t *testing.T) {
	// CHECK expressions pass through verbatim — no cross-dialect
	// translation. Caller (dmt's wiring layer) routes to AI fallback
	// for vendor-specific expressions; portable expressions
	// (boolean comparisons, IN clauses) work as-is.
	table := TableInfo{Name: "items"}
	c := Constraint{
		Name:            "ck_status",
		Type:            ConstraintCheck,
		CheckExpression: "status IN ('active', 'paused', 'closed')",
	}

	ddl := GenerateAddCheck(table, c, typemap.DialectPostgres, typemap.DialectPostgres)
	if !strings.Contains(ddl, "status IN ('active', 'paused', 'closed')") {
		t.Errorf("expression should pass through verbatim; got:\n%s", ddl)
	}
}
