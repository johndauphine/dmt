package ddl

import (
	"strings"
	"testing"
)

// ClickHouse CREATE TABLE differs structurally from the row-stores
// (#507): nullability is a Nullable(T) type wrapper, the PRIMARY KEY
// becomes the MergeTree ORDER BY key instead of a constraint, and the
// statement carries an ENGINE clause.
func TestGenerateCreateTable_ClickHouse(t *testing.T) {
	intp := func(n int) *int { return &n }
	table := TableInfo{
		Schema: "analytics",
		Name:   "events",
		Columns: []Column{
			{Name: "id", UDTName: "int8", DataType: "bigint", IsNullable: false},
			{Name: "kind", UDTName: "varchar", DataType: "varchar", CharacterMaximumLength: intp(40), IsNullable: false},
			{Name: "note", UDTName: "text", DataType: "text", IsNullable: true},
			{Name: "at", UDTName: "timestamp", DataType: "timestamp", IsNullable: false},
		},
		Constraints: []Constraint{
			{Name: "pk_events", Type: ConstraintPrimaryKey, Columns: []string{"id", "at"}},
		},
	}

	ddl := GenerateCreateTable(table, DialectPostgres, DialectClickHouse)

	for _, want := range []string{
		"ENGINE = MergeTree ORDER BY (`id`, `at`)",
		"`note` Nullable(String)",
		"`id` Int64",
		"`at` DateTime64(3)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
	for _, reject := range []string{"NOT NULL", "PRIMARY KEY", "CONSTRAINT"} {
		if strings.Contains(ddl, reject) {
			t.Errorf("DDL must not contain %q on clickhouse:\n%s", reject, ddl)
		}
	}
}

// A PK-less table is valid in ClickHouse via ORDER BY tuple().
func TestGenerateCreateTable_ClickHouseNoPK(t *testing.T) {
	table := TableInfo{
		Name: "log_lines",
		Columns: []Column{
			{Name: "line", UDTName: "text", DataType: "text", IsNullable: false},
		},
	}
	ddl := GenerateCreateTable(table, DialectPostgres, DialectClickHouse)
	if !strings.Contains(ddl, "ENGINE = MergeTree ORDER BY tuple()") {
		t.Errorf("DDL missing tuple() ORDER BY:\n%s", ddl)
	}
}
