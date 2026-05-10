// CREATE INDEX tests + IsUniqueConstraintIndex helper tests.

package ddl

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
)

func TestGenerateIndex_Plain(t *testing.T) {
	table := TableInfo{Name: "users"}
	idx := Index{Name: "idx_users_created_at", Columns: []string{"created_at"}}

	ddl := GenerateIndex(table, idx, typemap.DialectPostgres, typemap.DialectPostgres)
	want := `CREATE INDEX "idx_users_created_at" ON "users" ("created_at");`
	if ddl != want {
		t.Errorf("\ngot:  %s\nwant: %s", ddl, want)
	}
}

func TestGenerateIndex_Unique(t *testing.T) {
	table := TableInfo{Name: "users"}
	idx := Index{Name: "uq_users_email", IsUnique: true, Columns: []string{"email"}}

	ddl := GenerateIndex(table, idx, typemap.DialectPostgres, typemap.DialectPostgres)
	if !strings.HasPrefix(ddl, "CREATE UNIQUE INDEX") {
		t.Errorf("expected CREATE UNIQUE INDEX prefix; got:\n%s", ddl)
	}
}

func TestGenerateIndex_Composite(t *testing.T) {
	table := TableInfo{Name: "users"}
	idx := Index{Name: "idx_users_tenant_created", Columns: []string{"tenant_id", "created_at"}}

	ddl := GenerateIndex(table, idx, typemap.DialectPostgres, typemap.DialectMSSQL)
	if !strings.Contains(ddl, "([tenant_id], [created_at])") {
		t.Errorf("composite index column list wrong; got:\n%s", ddl)
	}
}

func TestGenerateIndex_QualifiedTable(t *testing.T) {
	// Non-default schema is preserved in the table reference.
	table := TableInfo{Schema: "inventory", Name: "items"}
	idx := Index{Name: "idx_items_sku", Columns: []string{"sku"}}

	ddl := GenerateIndex(table, idx, typemap.DialectPostgres, typemap.DialectPostgres)
	if !strings.Contains(ddl, `ON "inventory"."items"`) {
		t.Errorf("non-default schema should appear in ON clause; got:\n%s", ddl)
	}
}

func TestIsUniqueConstraintIndex_TrueForMatch(t *testing.T) {
	idx := Index{IsUnique: true, Columns: []string{"email"}}
	constraints := []Constraint{
		{Type: ConstraintUnique, Columns: []string{"email"}},
	}
	if !IsUniqueConstraintIndex(idx, constraints) {
		t.Error("expected true: unique index backs the unique constraint on the same column")
	}
}

func TestIsUniqueConstraintIndex_FalseForNonUniqueIndex(t *testing.T) {
	// Non-unique indexes are never "backing" a unique constraint —
	// they're separate query-acceleration artifacts.
	idx := Index{IsUnique: false, Columns: []string{"email"}}
	constraints := []Constraint{
		{Type: ConstraintUnique, Columns: []string{"email"}},
	}
	if IsUniqueConstraintIndex(idx, constraints) {
		t.Error("non-unique index should never be classified as backing a unique constraint")
	}
}

func TestIsUniqueConstraintIndex_FalseForDifferentColumns(t *testing.T) {
	// Same column SET but different ORDER → considered different
	// indexes (different sort orders, different uniqueness behavior).
	idx := Index{IsUnique: true, Columns: []string{"a", "b"}}
	constraints := []Constraint{
		{Type: ConstraintUnique, Columns: []string{"b", "a"}},
	}
	if IsUniqueConstraintIndex(idx, constraints) {
		t.Error("differently-ordered columns should NOT match the unique constraint")
	}
}

func TestIsUniqueConstraintIndex_FalseWhenNoUniqueConstraint(t *testing.T) {
	idx := Index{IsUnique: true, Columns: []string{"email"}}
	constraints := []Constraint{
		{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
	}
	if IsUniqueConstraintIndex(idx, constraints) {
		t.Error("PK constraint shouldn't fool the check")
	}
}
