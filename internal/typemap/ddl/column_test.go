// Column DDL emission tests. Some cases ported from UVG ddl.rs
// (lines 705-723) but using the Go ddl.Column shape directly rather
// than UVG's table-builder DSL.

package ddl

import (
	"strings"
	"testing"
)

// boolPtr is a small test helper for the *bool autoincrement field.
func boolPtr(v bool) *bool { return &v }

func TestGenerateColumnDef_BasicInteger_PG(t *testing.T) {
	col := Column{
		Name:       "id",
		UDTName:    "int4",
		IsNullable: false,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectPostgres)
	if got != `    "id" INTEGER NOT NULL` {
		t.Errorf("got %q", got)
	}
}

func TestGenerateColumnDef_VarcharWithLength_PG_to_MySQL(t *testing.T) {
	length := 100
	col := Column{
		Name:                   "name",
		UDTName:                "varchar",
		CharacterMaximumLength: &length,
		IsNullable:             false,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectMySQL)
	if got != "    `name` VARCHAR(100) NOT NULL" {
		t.Errorf("got %q", got)
	}
}

func TestGenerateColumnDef_NullableText_PG_to_MySQL(t *testing.T) {
	col := Column{
		Name:       "bio",
		UDTName:    "text",
		IsNullable: true,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectMySQL)
	if got != "    `bio` TEXT" {
		t.Errorf("got %q", got)
	}
}

func TestGenerateColumnDef_NumericWithPrecision_PG_to_PG(t *testing.T) {
	prec, scale := 10, 2
	col := Column{
		Name:             "price",
		UDTName:          "numeric",
		NumericPrecision: &prec,
		NumericScale:     &scale,
		IsNullable:       false,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectPostgres)
	if got != `    "price" NUMERIC(10, 2) NOT NULL` {
		t.Errorf("got %q", got)
	}
}

func TestGenerateColumnDef_DefaultExpression(t *testing.T) {
	col := Column{
		Name:          "created_at",
		UDTName:       "timestamptz",
		ColumnDefault: "now()",
		IsNullable:    false,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectMySQL)
	if !strings.Contains(got, "DEFAULT CURRENT_TIMESTAMP") {
		t.Errorf("expected MySQL DEFAULT CURRENT_TIMESTAMP, got %q", got)
	}
}

func TestGenerateColumnDef_BooleanDefault_PG_to_MSSQL(t *testing.T) {
	// PG bool DEFAULT true → MSSQL bit DEFAULT 1
	col := Column{
		Name:          "active",
		UDTName:       "bool",
		ColumnDefault: "true",
		IsNullable:    false,
	}
	got := GenerateColumnDef(col, nil, DialectPostgres, DialectMSSQL)
	if !strings.Contains(got, "DEFAULT 1") {
		t.Errorf("expected DEFAULT 1, got %q", got)
	}
	if !strings.Contains(got, "BIT") {
		t.Errorf("expected BIT type, got %q", got)
	}
}

func TestGenerateColumnDef_AutoIncrementPK_PG(t *testing.T) {
	// PG identity / serial → SERIAL type (no separate suffix).
	// NOT NULL is suppressed for auto-increment PK.
	col := Column{
		Name:       "id",
		UDTName:    "int4",
		IsIdentity: true,
		IsNullable: false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectPostgres)
	if !strings.Contains(got, "SERIAL") {
		t.Errorf("expected SERIAL type, got %q", got)
	}
	if strings.Contains(got, "NOT NULL") {
		t.Errorf("auto-increment PK should suppress NOT NULL; got %q", got)
	}
}

func TestGenerateColumnDef_AutoIncrementPK_BIGSERIAL(t *testing.T) {
	col := Column{
		Name:       "id",
		UDTName:    "int8",
		IsIdentity: true,
		IsNullable: false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectPostgres)
	if !strings.Contains(got, "BIGSERIAL") {
		t.Errorf("expected BIGSERIAL type, got %q", got)
	}
}

func TestGenerateColumnDef_AutoIncrementPK_MySQL(t *testing.T) {
	// MySQL: type stays INT, AUTO_INCREMENT goes as suffix.
	col := Column{
		Name:       "id",
		UDTName:    "int4",
		IsIdentity: true,
		IsNullable: false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectMySQL)
	if !strings.Contains(got, "AUTO_INCREMENT") {
		t.Errorf("expected AUTO_INCREMENT suffix, got %q", got)
	}
	if !strings.Contains(got, "INT") {
		t.Errorf("expected INT type, got %q", got)
	}
}

func TestGenerateColumnDef_AutoIncrementPK_MSSQL_DefaultSeed(t *testing.T) {
	// No Identity metadata → MSSQL defaults (1, 1)
	col := Column{
		Name:       "id",
		UDTName:    "int4",
		IsIdentity: true,
		IsNullable: false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectMSSQL)
	if !strings.Contains(got, "IDENTITY(1, 1)") {
		t.Errorf("expected IDENTITY(1, 1), got %q", got)
	}
}

func TestGenerateColumnDef_AutoIncrementPK_MSSQL_CustomSeed(t *testing.T) {
	col := Column{
		Name:       "id",
		UDTName:    "int4",
		IsIdentity: true,
		Identity:   &IdentityInfo{Start: 100, Increment: 10},
		IsNullable: false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectMSSQL)
	if !strings.Contains(got, "IDENTITY(100, 10)") {
		t.Errorf("expected IDENTITY(100, 10), got %q", got)
	}
}

func TestGenerateColumnDef_PGSerialFromNextval(t *testing.T) {
	// Legacy PG SERIAL columns predate IDENTITY and don't set
	// is_identity; they're detected via nextval(...) in the default.
	col := Column{
		Name:          "id",
		UDTName:       "int4",
		ColumnDefault: "nextval('users_id_seq'::regclass)",
		IsNullable:    false,
	}
	pk := []Constraint{{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectPostgres, DialectPostgres)
	if !strings.Contains(got, "SERIAL") {
		t.Errorf("expected SERIAL (nextval-detected), got %q", got)
	}
	// DEFAULT clause must be suppressed for auto-increment columns —
	// SERIAL implies the sequence, re-emitting nextval would double up.
	if strings.Contains(got, "DEFAULT") {
		t.Errorf("DEFAULT should be suppressed for auto-increment column; got %q", got)
	}
}

func TestGenerateColumnDef_MySQLAutoincrementFlag(t *testing.T) {
	// MySQL marks columns via the autoincrement flag (extra='auto_increment').
	col := Column{
		Name:          "id",
		UDTName:       "int",
		Autoincrement: boolPtr(true),
		IsNullable:    false,
	}
	pk := []Constraint{{Name: "pk_t", Type: ConstraintPrimaryKey, Columns: []string{"id"}}}
	got := GenerateColumnDef(col, pk, DialectMySQL, DialectMySQL)
	if !strings.Contains(got, "AUTO_INCREMENT") {
		t.Errorf("expected AUTO_INCREMENT suffix, got %q", got)
	}
}

func TestGenerateColumnDef_MySQLInlineComment(t *testing.T) {
	// MySQL emits column comments inline; PG / MSSQL use COMMENT ON.
	col := Column{
		Name:       "name",
		UDTName:    "varchar",
		IsNullable: false,
		Comment:    "user's display name",
	}
	got := GenerateColumnDef(col, nil, DialectMySQL, DialectMySQL)
	if !strings.Contains(got, "COMMENT 'user''s display name'") {
		t.Errorf("expected escaped MySQL inline comment, got %q", got)
	}

	// Non-MySQL targets must NOT inline comments
	got = GenerateColumnDef(col, nil, DialectMySQL, DialectPostgres)
	if strings.Contains(got, "COMMENT") {
		t.Errorf("PG target should not inline comments; got %q", got)
	}
}

func TestIsPrimaryKeyColumn_CompositePK(t *testing.T) {
	pk := []Constraint{{
		Name:    "pk_orders",
		Type:    ConstraintPrimaryKey,
		Columns: []string{"customer_id", "order_id"},
	}}
	if !isPrimaryKeyColumn("customer_id", pk) {
		t.Error("customer_id should be in PK")
	}
	if !isPrimaryKeyColumn("order_id", pk) {
		t.Error("order_id should be in PK")
	}
	if isPrimaryKeyColumn("created_at", pk) {
		t.Error("created_at should NOT be in PK")
	}
}

func TestIsAutoIncrementColumn_AllVariants(t *testing.T) {
	// Identity (PG / MSSQL)
	if !isAutoIncrementColumn(Column{IsIdentity: true}, DialectPostgres) {
		t.Error("IsIdentity=true should be auto-increment")
	}
	// Autoincrement flag (MySQL)
	if !isAutoIncrementColumn(Column{Autoincrement: boolPtr(true)}, DialectMySQL) {
		t.Error("Autoincrement=true should be auto-increment")
	}
	// nextval default (PG legacy SERIAL)
	if !isAutoIncrementColumn(Column{ColumnDefault: "nextval('seq'::regclass)"}, DialectPostgres) {
		t.Error("PG nextval default should be auto-increment")
	}
	// nextval default on MSSQL is NOT auto-increment (different mechanism)
	if isAutoIncrementColumn(Column{ColumnDefault: "nextval('seq'::regclass)"}, DialectMSSQL) {
		t.Error("MSSQL nextval default should NOT be treated as auto-increment")
	}
	// Plain column
	if isAutoIncrementColumn(Column{}, DialectPostgres) {
		t.Error("empty column should not be auto-increment")
	}
}
