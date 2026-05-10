// CREATE TABLE assembly tests. Cases adapted from UVG ddl.rs
// `#[cfg(test)] mod tests` (lines 705-739) using the Go ddl.TableInfo
// shape directly rather than UVG's table-builder DSL.

package ddl

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
)

// intPtr is a small test helper for *int fields on Column.
func intPtr(v int) *int { return &v }

func TestGenerateCreateTable_PG_to_MySQL(t *testing.T) {
	table := TableInfo{
		Schema: "",
		Name:   "users",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsNullable: false},
			{Name: "name", UDTName: "varchar", CharacterMaximumLength: intPtr(100), IsNullable: false},
			{Name: "bio", UDTName: "text", IsNullable: true},
		},
		Constraints: []Constraint{
			{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectMySQL)

	for _, want := range []string{
		"CREATE TABLE `users`",
		"`id` INT NOT NULL",
		"`name` VARCHAR(100) NOT NULL",
		// Issue #196: PG TEXT (unbounded) → MySQL LONGTEXT, not TEXT.
		"`bio` LONGTEXT",
		"CONSTRAINT `pk_users` PRIMARY KEY (`id`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("expected DDL to contain %q; got:\n%s", want, ddl)
		}
	}
}

func TestGenerateCreateTable_PG_to_PG(t *testing.T) {
	table := TableInfo{
		Name: "items",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsNullable: false},
			{Name: "price", UDTName: "numeric", NumericPrecision: intPtr(10), NumericScale: intPtr(2), IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_items", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)

	for _, want := range []string{
		`"id" INTEGER NOT NULL`,
		`"price" NUMERIC(10, 2) NOT NULL`,
		`CONSTRAINT "pk_items" PRIMARY KEY ("id")`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("expected DDL to contain %q; got:\n%s", want, ddl)
		}
	}
}

func TestGenerateCreateTable_CompositePrimaryKey(t *testing.T) {
	table := TableInfo{
		Name: "order_items",
		Columns: []Column{
			{Name: "order_id", UDTName: "int4", IsNullable: false},
			{Name: "line_no", UDTName: "int4", IsNullable: false},
			{Name: "qty", UDTName: "int4", IsNullable: false},
		},
		Constraints: []Constraint{{
			Name: "pk_order_items", Type: ConstraintPrimaryKey,
			Columns: []string{"order_id", "line_no"},
		}},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)

	if !strings.Contains(ddl, `PRIMARY KEY ("order_id", "line_no")`) {
		t.Errorf("composite PK should list both columns; got:\n%s", ddl)
	}
}

func TestGenerateCreateTable_NoPrimaryKey(t *testing.T) {
	// Heap-style table with no PK — emit columns only, no constraint.
	table := TableInfo{
		Name: "audit_log",
		Columns: []Column{
			{Name: "ts", UDTName: "timestamptz", IsNullable: false},
			{Name: "msg", UDTName: "text", IsNullable: true},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)

	if strings.Contains(ddl, "PRIMARY KEY") {
		t.Errorf("no PK should produce no PRIMARY KEY clause; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, `"ts"`) || !strings.Contains(ddl, `"msg"`) {
		t.Errorf("columns missing; got:\n%s", ddl)
	}
}

func TestGenerateCreateTable_ExcludesFKAndCheckAndUnique(t *testing.T) {
	// dmt's contract: CREATE TABLE includes columns + PK only.
	// FK / UNIQUE / CHECK go to finalize.go for the Finalize phase.
	// This test is the regression guard — if anyone later tries to
	// inline these in CREATE TABLE, the test fails.
	table := TableInfo{
		Name: "messages",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsNullable: false},
			{Name: "user_id", UDTName: "int4", IsNullable: false},
			{Name: "status", UDTName: "varchar", CharacterMaximumLength: intPtr(20), IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_messages", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			{
				Name: "fk_messages_users", Type: ConstraintForeignKey,
				Columns: []string{"user_id"},
				ForeignKey: &ForeignKey{
					RefTable: "users", RefColumns: []string{"id"},
					DeleteRule: "CASCADE",
				},
			},
			{Name: "uq_messages_id", Type: ConstraintUnique, Columns: []string{"id"}},
			{Name: "ck_status", Type: ConstraintCheck, CheckExpression: "status IN ('open','closed')"},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)

	for _, mustNotContain := range []string{"FOREIGN KEY", "UNIQUE", "CHECK"} {
		if strings.Contains(ddl, mustNotContain) {
			t.Errorf("CREATE TABLE must not include %s constraints (separate finalize phase); got:\n%s",
				mustNotContain, ddl)
		}
	}
	// PK still inline, columns present.
	if !strings.Contains(ddl, "PRIMARY KEY") {
		t.Errorf("PK should remain inline; got:\n%s", ddl)
	}
}

func TestGenerateCreateTable_MySQLInlineComment(t *testing.T) {
	// MySQL-only: table-level COMMENT goes inline as a CREATE TABLE
	// suffix. Other dialects emit COMMENT ON / sp_addextendedproperty
	// separately (deferred to wiring layer).
	table := TableInfo{
		Name:    "users",
		Comment: "all registered accounts",
		Columns: []Column{{Name: "id", UDTName: "int4", IsNullable: false}},
		Constraints: []Constraint{
			{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	mysqlDDL := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectMySQL)
	if !strings.Contains(mysqlDDL, "COMMENT 'all registered accounts'") {
		t.Errorf("MySQL target should inline table COMMENT; got:\n%s", mysqlDDL)
	}

	// PG / MSSQL must not inline (they use separate COMMENT ON).
	pgDDL := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)
	if strings.Contains(pgDDL, "COMMENT") {
		t.Errorf("PG target must not inline table comment; got:\n%s", pgDDL)
	}
}

func TestGenerateCreateTable_SchemaPrefix(t *testing.T) {
	// User-defined schema (non-default) is preserved on PG/MSSQL.
	// Default schemas suppressed (covered in identifier_test.go).
	table := TableInfo{
		Schema: "inventory", Name: "items",
		Columns: []Column{{Name: "id", UDTName: "int4", IsNullable: false}},
		Constraints: []Constraint{
			{Name: "pk_items", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)
	if !strings.Contains(ddl, `CREATE TABLE "inventory"."items"`) {
		t.Errorf("should preserve non-default schema; got:\n%s", ddl)
	}
}

// TestGenerateCreateTable_PG_VarcharPK_to_MySQL_BoundsColumn is the
// Codex P2 #1+#3 regression guard from PR #207. Pre-fix, the #196
// LONGTEXT widening would have produced `LONGTEXT PRIMARY KEY` which
// MySQL rejects. The first fix attempt added a `(255)` key prefix,
// which Codex correctly flagged as a uniqueness-semantics violation
// (two values that differ past byte 255 would be treated as
// duplicates). Final fix: the column TYPE itself is bounded to
// VARCHAR(255) when an unbounded-text column ends up in a PK or
// UNIQUE constraint on MySQL — matches the prior pre-#196 emission
// for these specific columns, preserves uniqueness semantics, and
// the common non-keyed columns still get the LONGTEXT win.
func TestGenerateCreateTable_PG_VarcharPK_to_MySQL_BoundsColumn(t *testing.T) {
	table := TableInfo{
		Name: "items",
		Columns: []Column{
			// PG `varchar PRIMARY KEY` (no length): the rare case that
			// triggers the post-#196 regression Codex caught.
			{Name: "id", UDTName: "varchar", IsNullable: false},
			{Name: "name", UDTName: "varchar", CharacterMaximumLength: intPtr(100), IsNullable: false},
			// A non-keyed unbounded varchar must still get LONGTEXT —
			// the override only fires for PK/UNIQUE columns.
			{Name: "bio", UDTName: "varchar", IsNullable: true},
		},
		Constraints: []Constraint{
			{Name: "pk_items", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectMySQL)

	// PK column: bounded to VARCHAR(255) for uniqueness preservation.
	if !strings.Contains(ddl, "`id` VARCHAR(255) NOT NULL") {
		t.Errorf("expected `id VARCHAR(255)` for unbounded-varchar PK column; got:\n%s", ddl)
	}
	// PK clause: plain column ref, no prefix length needed.
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("expected plain `PRIMARY KEY (id)` (no prefix); got:\n%s", ddl)
	}
	if strings.Contains(ddl, "`id`(255)") {
		t.Errorf("PK should NOT use prefix (uniqueness-semantics issue); got:\n%s", ddl)
	}
	// Non-keyed unbounded varchar: full LONGTEXT (data fidelity preserved).
	if !strings.Contains(ddl, "`bio` LONGTEXT") {
		t.Errorf("expected `bio LONGTEXT` for non-keyed unbounded varchar; got:\n%s", ddl)
	}
}

// TestGenerateCreateTable_PG_VarcharUnique_to_MySQL_BoundsColumn — same
// override applies for UNIQUE constraints, not just PK.
func TestGenerateCreateTable_PG_VarcharUnique_to_MySQL_BoundsColumn(t *testing.T) {
	table := TableInfo{
		Name: "items",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsNullable: false},
			{Name: "slug", UDTName: "varchar", IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_items", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			{Name: "uq_slug", Type: ConstraintUnique, Columns: []string{"slug"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectMySQL)

	if !strings.Contains(ddl, "`slug` VARCHAR(255) NOT NULL") {
		t.Errorf("UNIQUE column should be bounded to VARCHAR(255); got:\n%s", ddl)
	}
}

// TestGenerateCreateTable_PG_VarcharUniqueIndex_to_MySQL_BoundsColumn
// is the Codex P2 #5 regression guard from PR #207. The driver adapter
// (driverTableToDDL) only synthesizes a Constraint for PRIMARY KEY;
// other source uniqueness — UNIQUE constraints from the source schema
// — comes through as Index{IsUnique:true}. Without checking indexes
// in shouldBoundForUniqueness, real PG/MSSQL → MySQL migrations of
// schemas with UNIQUE on unbounded text would silently fail at
// CREATE TABLE / CREATE INDEX time.
func TestGenerateCreateTable_PG_VarcharUniqueIndex_to_MySQL_BoundsColumn(t *testing.T) {
	table := TableInfo{
		Name: "items",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsNullable: false},
			{Name: "slug", UDTName: "varchar", IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_items", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
		// Driver-adapter shape: UNIQUE represented as a unique INDEX,
		// not a constraint. shouldBoundForUniqueness must catch this.
		Indexes: []Index{
			{Name: "uq_slug", IsUnique: true, Columns: []string{"slug"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectMySQL)

	if !strings.Contains(ddl, "`slug` VARCHAR(255) NOT NULL") {
		t.Errorf("column under unique INDEX should bound to VARCHAR(255); got:\n%s", ddl)
	}
}

// TestGenerateCreateTable_MSSQL_VarbinaryMaxPK_to_MySQL_BoundsToVarbinary
// is the Codex P2 #4 regression guard from PR #207. KindBytes columns
// in a PK/UNIQUE must bound to VARBINARY(255), NOT VARCHAR(255) —
// text encoding would corrupt arbitrary byte values.
func TestGenerateCreateTable_MSSQL_VarbinaryMaxPK_to_MySQL_BoundsToVarbinary(t *testing.T) {
	maxLen := -1
	table := TableInfo{
		Name: "blobs",
		Columns: []Column{
			// MSSQL varbinary(max) PK — the rare case Codex flagged.
			{Name: "fingerprint", UDTName: "varbinary", CharacterMaximumLength: &maxLen, IsNullable: false},
			{Name: "label", UDTName: "varchar", CharacterMaximumLength: intPtr(50)},
		},
		Constraints: []Constraint{
			{Name: "pk_blobs", Type: ConstraintPrimaryKey, Columns: []string{"fingerprint"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectMSSQL, typemap.DialectMySQL)

	if !strings.Contains(ddl, "`fingerprint` VARBINARY(255) NOT NULL") {
		t.Errorf("KindBytes PK should bound to VARBINARY(255), NOT VARCHAR; got:\n%s", ddl)
	}
	if strings.Contains(ddl, "`fingerprint` VARCHAR") {
		t.Errorf("KindBytes PK must not become a text column; got:\n%s", ddl)
	}
}

func TestGenerateCreateTable_AutoIncrementPK_PG(t *testing.T) {
	// Auto-increment PK ends with SERIAL (not INTEGER + IDENTITY) on
	// PG, NOT NULL suppressed, but the explicit PK constraint clause
	// is still emitted (matches dmt's pattern of always emitting a
	// named PK constraint, not relying on inline `PRIMARY KEY`).
	table := TableInfo{
		Name: "users",
		Columns: []Column{
			{Name: "id", UDTName: "int4", IsIdentity: true, IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	ddl := GenerateCreateTable(table, typemap.DialectPostgres, typemap.DialectPostgres)
	if !strings.Contains(ddl, "SERIAL") {
		t.Errorf("PG identity → SERIAL; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, `CONSTRAINT "pk_users" PRIMARY KEY`) {
		t.Errorf("PK constraint clause should still be emitted; got:\n%s", ddl)
	}
}
