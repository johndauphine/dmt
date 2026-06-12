// Tests for the deterministic typemap adapter (#169c). Covers all
// four interface methods plus the conversion helpers. Production code
// is in deterministic_mapper.go.

package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/dmt/internal/typemap/ddl"
)

// ---------- MapType (column-level) ----------

func TestDeterministicMapper_MapType_PG_to_MSSQL(t *testing.T) {
	m := NewDeterministicMapper()
	got := m.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		DataType:     "int4",
	})
	if got != "INT" {
		t.Errorf("PG int4 → MSSQL: got %q, want INT", got)
	}
}

func TestDeterministicMapper_MapType_MSSQL_to_PG(t *testing.T) {
	m := NewDeterministicMapper()
	got := m.MapType(TypeInfo{
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectPostgres,
		DataType:     "uniqueidentifier",
	})
	if got != "UUID" {
		t.Errorf("MSSQL uniqueidentifier → PG: got %q, want UUID", got)
	}
}

func TestDeterministicMapper_MapType_VarcharWithLength(t *testing.T) {
	m := NewDeterministicMapper()
	got := m.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMySQL,
		DataType:     "varchar",
		MaxLength:    100,
	})
	if got != "VARCHAR(100)" {
		t.Errorf("PG varchar(100) → MySQL: got %q, want VARCHAR(100)", got)
	}
}

func TestDeterministicMapper_MapType_DecimalWithPrecision(t *testing.T) {
	m := NewDeterministicMapper()
	got := m.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		DataType:     "numeric",
		Precision:    10,
		Scale:        2,
	})
	if got != "NUMERIC(10, 2)" {
		t.Errorf("PG numeric(10,2): got %q, want NUMERIC(10, 2)", got)
	}
}

// ---------- CanMap / SupportedTargets ----------

func TestDeterministicMapper_CanMap(t *testing.T) {
	m := NewDeterministicMapper()
	tests := []struct {
		source, target string
		want           bool
	}{
		{typemap.DialectPostgres, typemap.DialectMSSQL, true},
		{typemap.DialectMySQL, typemap.DialectMSSQL, true},
		{typemap.DialectPostgres, typemap.DialectPostgres, true},
		{"oracle", typemap.DialectMSSQL, false},
		{typemap.DialectPostgres, "snowflake", false},
		{"oracle", "snowflake", false},
	}
	for _, tc := range tests {
		t.Run(tc.source+"_to_"+tc.target, func(t *testing.T) {
			if got := m.CanMap(tc.source, tc.target); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeterministicMapper_SupportedTargets(t *testing.T) {
	m := NewDeterministicMapper()
	got := m.SupportedTargets()
	want := map[string]bool{
		typemap.DialectPostgres:   true,
		typemap.DialectMSSQL:      true,
		typemap.DialectMySQL:      true,
		typemap.DialectSQLite:     true,
		typemap.DialectClickHouse: true,
	}
	if len(got) != len(want) {
		t.Errorf("got %d targets, want %d (%v)", len(got), len(want), got)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("unexpected target %q in supported list", d)
		}
	}
}

// ---------- GenerateTableDDL ----------

func TestDeterministicMapper_GenerateTableDDL_BasicTable(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{
		Schema: "public", Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false},
			{Name: "bio", DataType: "text", IsNullable: true},
		},
		PrimaryKey: []string{"id"},
	}

	resp, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  table,
		TargetSchema: "public",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		"CREATE TABLE",
		`"users"`,
		"SERIAL", // identity column on PG → SERIAL
		`"email" VARCHAR(255) NOT NULL`,
		`"bio" TEXT`,
		`CONSTRAINT "pk_users" PRIMARY KEY ("id")`,
	} {
		if !strings.Contains(resp.CreateTableDDL, want) {
			t.Errorf("expected DDL to contain %q; got:\n%s", want, resp.CreateTableDDL)
		}
	}

	// ColumnTypes map populated for telemetry
	if len(resp.ColumnTypes) != 3 {
		t.Errorf("expected ColumnTypes with 3 entries; got %d", len(resp.ColumnTypes))
	}
	if resp.ColumnTypes["email"] != "VARCHAR(255)" {
		t.Errorf("ColumnTypes[email]: got %q, want VARCHAR(255)", resp.ColumnTypes["email"])
	}
}

func TestDeterministicMapper_GenerateTableDDL_CrossDialect_PG_to_MySQL(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{
		Schema: "public", Name: "products",
		Columns: []Column{
			{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
			{Name: "price", DataType: "numeric", Precision: 10, Scale: 2, IsNullable: false},
		},
		PrimaryKey: []string{"id"},
	}

	resp, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMySQL,
		SourceTable:  table,
		TargetSchema: "shop",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		"CREATE TABLE `products`", // MySQL backticks; schema suppressed (always on MySQL)
		// NOT NULL suppressed for auto-increment PK — the AUTO_INCREMENT
		// suffix already implies NOT NULL semantics on MySQL.
		"`id` INT AUTO_INCREMENT",
		"`price` DECIMAL(10, 2) NOT NULL",
	} {
		if !strings.Contains(resp.CreateTableDDL, want) {
			t.Errorf("expected DDL to contain %q; got:\n%s", want, resp.CreateTableDDL)
		}
	}
}

func TestDeterministicMapper_GenerateTableDDL_UnsupportedDialect(t *testing.T) {
	m := NewDeterministicMapper()
	_, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: "oracle",
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  &Table{Name: "x"},
	})
	if err == nil {
		t.Error("expected error for unsupported source dialect")
	}
}

// ---------- GenerateFinalizationDDL ----------

func TestDeterministicMapper_GenerateFinalizationDDL_Index(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{Schema: "public", Name: "users"}
	idx := &Index{Name: "idx_users_created_at", Columns: []string{"created_at"}}

	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        table, TargetSchema: "public",
		Index: idx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `CREATE INDEX "idx_users_created_at" ON "users" ("created_at");`
	if got != want {
		t.Errorf("\ngot:  %s\nwant: %s", got, want)
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_UniqueIndex(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{Schema: "public", Name: "users"}
	idx := &Index{Name: "uq_users_email", Columns: []string{"email"}, IsUnique: true}

	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        table, TargetSchema: "public",
		Index: idx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(got, "CREATE UNIQUE INDEX") {
		t.Errorf("expected CREATE UNIQUE INDEX prefix; got:\n%s", got)
	}
}

// TestDeterministicMapper_GenerateFinalizationDDL_UnsupportedIndexFeatures
// — Codex review on PR #190. Without this guard, indexes with vendor-
// specific features (MSSQL clustered, covering, filtered) were silently
// emitted as plain btree, dropping the user's specified behavior. The
// adapter must instead return ErrUnsupportedDDL so the wiring layer
// (#170) routes to AI fallback.
func TestDeterministicMapper_GenerateFinalizationDDL_UnsupportedIndexFeatures(t *testing.T) {
	cases := []struct {
		name string
		idx  *Index
	}{
		{
			"clustered_index",
			&Index{Name: "ci_t", Columns: []string{"id"}, IsClustered: true},
		},
		{
			"covering_index",
			&Index{Name: "covi_t", Columns: []string{"a"}, IncludeCols: []string{"b", "c"}},
		},
		{
			"filtered_index",
			&Index{Name: "fi_t", Columns: []string{"id"}, Filter: "deleted_at IS NULL"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewDeterministicMapper()
			_, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
				Type:         DDLTypeIndex,
				SourceDBType: typemap.DialectMSSQL,
				TargetDBType: typemap.DialectMSSQL,
				Table:        &Table{Name: "t"},
				TargetSchema: "dbo",
				Index:        tc.idx,
			})
			if err == nil {
				t.Fatalf("%s: expected ErrUnsupportedDDL; got nil error and emitted DDL — silent feature drop (#190 regression)", tc.name)
			}
			if !errors.Is(err, ErrUnsupportedDDL) {
				t.Errorf("%s: expected errors.Is(err, ErrUnsupportedDDL) to match; got %v", tc.name, err)
			}
		})
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_FK_Cascade(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{Schema: "public", Name: "messages"}
	fk := &ForeignKey{
		Name: "fk_messages_users", Columns: []string{"user_id"},
		RefSchema: "public", RefTable: "users", RefColumns: []string{"id"},
		OnDelete: "CASCADE", OnUpdate: "NO ACTION",
	}

	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeForeignKey,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        table, TargetSchema: "public",
		ForeignKey: fk,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		`ALTER TABLE "messages"`,
		`ADD CONSTRAINT "fk_messages_users"`,
		`FOREIGN KEY ("user_id")`,
		`REFERENCES "users" ("id")`,
		"ON DELETE CASCADE",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected FK DDL to contain %q; got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "ON UPDATE") {
		t.Errorf("NO ACTION update rule should be suppressed; got:\n%s", got)
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_FK_RestrictToMSSQL(t *testing.T) {
	// PG → MSSQL with RESTRICT — must suppress (MSSQL doesn't accept RESTRICT).
	// This is the wiring-layer regression guard for the #189 fix.
	m := NewDeterministicMapper()
	table := &Table{Schema: "dbo", Name: "messages"}
	fk := &ForeignKey{
		Name: "fk_messages_users", Columns: []string{"user_id"},
		RefSchema: "dbo", RefTable: "users", RefColumns: []string{"id"},
		OnDelete: "RESTRICT",
	}

	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeForeignKey,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		Table:        table, TargetSchema: "dbo",
		ForeignKey: fk,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "RESTRICT") {
		t.Errorf("RESTRICT must not appear on MSSQL targets; got:\n%s", got)
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_Check(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{Schema: "public", Name: "items"}
	check := &CheckConstraint{
		Name: "ck_price_positive", Definition: "price > 0",
	}

	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeCheckConstraint,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        table, TargetSchema: "public",
		CheckConstraint: check,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `ALTER TABLE "items" ADD CONSTRAINT "ck_price_positive" CHECK (price > 0);`
	if got != want {
		t.Errorf("\ngot:  %s\nwant: %s", got, want)
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_DropTable(t *testing.T) {
	m := NewDeterministicMapper()
	got, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeDropTable,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        &Table{Name: "users"},
		TargetSchema: "public",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `DROP TABLE IF EXISTS "users";`
	if got != want {
		t.Errorf("\ngot:  %s\nwant: %s", got, want)
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_NilPointers(t *testing.T) {
	m := NewDeterministicMapper()
	cases := []struct {
		name    string
		ddlType DDLType
	}{
		{"missing_index", DDLTypeIndex},
		{"missing_fk", DDLTypeForeignKey},
		{"missing_check", DDLTypeCheckConstraint},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
				Type:         tc.ddlType,
				SourceDBType: typemap.DialectPostgres,
				TargetDBType: typemap.DialectPostgres,
				Table:        &Table{Name: "x"},
			})
			if err == nil {
				t.Errorf("expected error when %s payload pointer is nil", tc.name)
			}
		})
	}
}

func TestDeterministicMapper_GenerateFinalizationDDL_UnknownType(t *testing.T) {
	m := NewDeterministicMapper()
	_, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         "bogus",
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        &Table{Name: "x"},
	})
	if err == nil {
		t.Error("expected error for unknown DDLType")
	}
}

// ---------- GenerateDropTableDDL ----------

func TestDeterministicMapper_GenerateDropTableDDL(t *testing.T) {
	m := NewDeterministicMapper()
	cases := []struct {
		name          string
		schema, table string
		dialect       string
		want          string
	}{
		{"pg_default_schema_suppressed", "public", "users", typemap.DialectPostgres, `DROP TABLE IF EXISTS "users";`},
		{"pg_custom_schema_qualified", "inventory", "items", typemap.DialectPostgres, `DROP TABLE IF EXISTS "inventory"."items";`},
		// MSSQL must ALWAYS qualify when schema is non-empty — even
		// "dbo" — to stay consistent with Dialect.QualifyTable used
		// elsewhere in the MSSQL driver. SQL Server allows per-login
		// default schemas; an unqualified DROP could resolve to a
		// different schema than the corresponding CREATE/INSERT
		// (Copilot review on PR #190).
		{"mssql_default_dbo_qualified", "dbo", "users", typemap.DialectMSSQL, "DROP TABLE IF EXISTS [dbo].[users];"},
		{"mssql_custom_schema_qualified", "Sales", "orders", typemap.DialectMSSQL, "DROP TABLE IF EXISTS [Sales].[orders];"},
		{"mssql_empty_schema_unqualified", "", "users", typemap.DialectMSSQL, "DROP TABLE IF EXISTS [users];"},
		{"mysql_always_suppressed", "any", "users", typemap.DialectMySQL, "DROP TABLE IF EXISTS `users`;"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := m.GenerateDropTableDDL(context.Background(), DropTableDDLRequest{
				TargetSchema: tc.schema,
				TableName:    tc.table,
				TargetDBType: tc.dialect,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDeterministicMapper_GenerateDropTableDDL_Validation — Copilot
// review on PR #190. The mapper should error on missing required
// fields and on unsupported dialects rather than emit invalid SQL.
func TestDeterministicMapper_GenerateDropTableDDL_Validation(t *testing.T) {
	m := NewDeterministicMapper()

	if _, err := m.GenerateDropTableDDL(context.Background(), DropTableDDLRequest{
		TargetDBType: typemap.DialectPostgres,
		// TableName missing
	}); err == nil {
		t.Error("expected error when TableName is empty")
	}

	if _, err := m.GenerateDropTableDDL(context.Background(), DropTableDDLRequest{
		TableName:    "users",
		TargetDBType: "oracle",
	}); err == nil {
		t.Error("expected error for unsupported target dialect")
	}
}

// TestDeterministicMapper_GenerateTableDDL_NilSourceTable — Copilot
// review on PR #190. Nil source must produce a user-facing error,
// not a panic.
func TestDeterministicMapper_GenerateTableDDL_NilSourceTable(t *testing.T) {
	m := NewDeterministicMapper()
	_, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		// SourceTable: nil
	})
	if err == nil {
		t.Fatal("expected error for nil SourceTable; got nil — would have panicked")
	}
	if !strings.Contains(err.Error(), "SourceTable") {
		t.Errorf("error should mention SourceTable; got %q", err.Error())
	}
}

// TestDeterministicMapper_GenerateFinalizationDDL_NilTable — Copilot
// review on PR #190. Same nil-validation guard for the finalization
// path.
func TestDeterministicMapper_GenerateFinalizationDDL_NilTable(t *testing.T) {
	m := NewDeterministicMapper()
	_, err := m.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		// Table: nil
	})
	if err == nil {
		t.Fatal("expected error for nil Table; got nil — would have panicked")
	}
}

// TestDeterministicMapper_GenerateTableDDL_TargetSchemaPreservedAcrossSourceDefault
// — Copilot review on PR #190 (regression guard for the schema-
// suppression fix). When the user explicitly sets a TargetSchema whose
// name happens to match the SOURCE dialect's default ("dbo" for an
// MSSQL source), the deterministic adapter must preserve it on the
// target — not silently suppress because the name collides with the
// source's default.
func TestDeterministicMapper_GenerateTableDDL_TargetSchemaPreservedAcrossSourceDefault(t *testing.T) {
	m := NewDeterministicMapper()
	table := &Table{
		Schema: "myapp", Name: "users",
		Columns:    []Column{{Name: "id", DataType: "int", IsNullable: false}},
		PrimaryKey: []string{"id"},
	}
	resp, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectMSSQL, // mssql default = "dbo"
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  table,
		TargetSchema: "dbo", // PG schema literally named "dbo"
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// PG quoting + schema preserved
	if !strings.Contains(resp.CreateTableDDL, `CREATE TABLE "dbo"."users"`) {
		t.Errorf("target schema 'dbo' must be preserved on PG even though source is MSSQL; got:\n%s",
			resp.CreateTableDDL)
	}
}

// ---------- Conversion helpers ----------

func TestDriverColumnToDDL_PreservesFields(t *testing.T) {
	col := Column{
		Name: "email", DataType: "varchar", MaxLength: 255,
		Precision: 0, Scale: 0,
		IsNullable: false, IsIdentity: false,
	}
	got := driverColumnToDDL(col, typemap.DialectPostgres)

	if got.Name != "email" || got.UDTName != "varchar" || got.DataType != "varchar" {
		t.Errorf("name/udt/datatype: %+v", got)
	}
	if got.CharacterMaximumLength == nil || *got.CharacterMaximumLength != 255 {
		t.Errorf("CharacterMaximumLength: %+v", got.CharacterMaximumLength)
	}
	if got.NumericPrecision != nil || got.NumericScale != nil {
		t.Errorf("zero numeric fields should map to nil; got %+v / %+v",
			got.NumericPrecision, got.NumericScale)
	}
	if got.IsNullable {
		t.Error("IsNullable should be false")
	}
}

func TestNullableInt_ZeroIsNil(t *testing.T) {
	if nullableInt(0) != nil {
		t.Error("nullableInt(0) should be nil")
	}
	v := nullableInt(42)
	if v == nil || *v != 42 {
		t.Errorf("nullableInt(42): got %+v, want pointer-to-42", v)
	}
}

func TestDriverTableToDDL_SynthesizesPKName(t *testing.T) {
	// dmt's reader stores PK as a column-name list with no constraint
	// name. The adapter synthesizes "pk_<table>" using the (sanitized)
	// table name.
	t1 := &Table{
		Name:       "users",
		Columns:    []Column{{Name: "id", DataType: "int4"}},
		PrimaryKey: []string{"id"},
	}
	got := driverTableToDDL(t1, "public", typemap.DialectPostgres)
	if len(got.Constraints) != 1 {
		t.Fatalf("expected 1 PK constraint; got %d", len(got.Constraints))
	}
	if got.Constraints[0].Name != "pk_users" {
		t.Errorf("synthesized PK name: got %q, want pk_users", got.Constraints[0].Name)
	}
	if got.Constraints[0].Type != ddl.ConstraintPrimaryKey {
		t.Errorf("synthesized PK type: got %d, want ConstraintPrimaryKey", got.Constraints[0].Type)
	}
}

// TestDriverTableToDDL_PG_SanitizesIdentifiers — regression guard for
// the case-mismatch issue caught when running SO2010 mssql→pg on PR
// #192. The deterministic emitter MUST lowercase identifiers when
// targeting PG so the CREATE TABLE name matches what the rest of
// dmt's PG flow (Writer.CreatePrimaryKey, FK creation, INSERT) uses
// after running ident.SanitizePG.
func TestDriverTableToDDL_PG_SanitizesIdentifiers(t *testing.T) {
	t1 := &Table{
		Schema:     "dbo",
		Name:       "LinkTypes",
		Columns:    []Column{{Name: "Id", DataType: "int"}, {Name: "Name", DataType: "nvarchar", MaxLength: 50}},
		PrimaryKey: []string{"Id"},
	}
	got := driverTableToDDL(t1, "public", typemap.DialectPostgres)

	if got.Name != "linktypes" {
		t.Errorf("table name: got %q, want lowercased linktypes", got.Name)
	}
	for _, col := range got.Columns {
		if col.Name != "id" && col.Name != "name" {
			t.Errorf("column name not lowercased: %q", col.Name)
		}
	}
	if got.Constraints[0].Name != "pk_linktypes" {
		t.Errorf("PK constraint name: got %q, want pk_linktypes", got.Constraints[0].Name)
	}
	if got.Constraints[0].Columns[0] != "id" {
		t.Errorf("PK column: got %q, want lowercased id", got.Constraints[0].Columns[0])
	}
}

// TestDriverTableToDDL_NonPG_PreservesIdentifiers — sanity guard that
// the sanitization only applies to PG targets. MSSQL is case-
// insensitive natively; preserving source casing is correct.
func TestDriverTableToDDL_NonPG_PreservesIdentifiers(t *testing.T) {
	t1 := &Table{
		Name:       "LinkTypes",
		Columns:    []Column{{Name: "Id", DataType: "int"}},
		PrimaryKey: []string{"Id"},
	}
	got := driverTableToDDL(t1, "dbo", typemap.DialectMSSQL)

	if got.Name != "LinkTypes" {
		t.Errorf("MSSQL target should preserve case; got %q", got.Name)
	}
	if got.Columns[0].Name != "Id" {
		t.Errorf("MSSQL target should preserve column case; got %q", got.Columns[0].Name)
	}
}

func TestDriverTableToDDL_NoPK_NoConstraint(t *testing.T) {
	t1 := &Table{
		Name:    "audit_log",
		Columns: []Column{{Name: "ts", DataType: "timestamptz"}},
	}
	got := driverTableToDDL(t1, "public", typemap.DialectPostgres)
	if len(got.Constraints) != 0 {
		t.Errorf("table with no PK should produce no constraints; got %d", len(got.Constraints))
	}
}
