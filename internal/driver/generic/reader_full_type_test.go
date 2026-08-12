package generic

import (
	"context"
	"database/sql"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/dbconfig"
	coredriver "github.com/johndauphine/dmt/v5/internal/driver"
)

func TestReaderLoadColumnsPreservesFullDataTypeCase(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	describeTable := `
SELECT
  1,
  'status',
  'enum',
  'enum(''Active'',''InActive'',''PENDING'')',
  0,
  0,
  0,
  1,
  NULL,
  0
WHERE ? = 'users'
`
	reader := &Reader{
		db:     db,
		config: &dbconfig.SourceConfig{},
		cat: &Catalog{
			Introspection: IntrospectionSpec{
				DescribeTable:      describeTable,
				FullTypeInDescribe: true,
			},
		},
	}
	table := &coredriver.Table{Name: "users"}

	if err := reader.loadColumns(context.Background(), table); err != nil {
		t.Fatalf("loadColumns: %v", err)
	}
	if len(table.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(table.Columns))
	}
	col := table.Columns[0]
	if col.DataType != "enum" {
		t.Fatalf("DataType = %q, want enum", col.DataType)
	}
	if col.FullDataType != "enum('Active','InActive','PENDING')" {
		t.Fatalf("FullDataType = %q, want mixed-case enum declaration", col.FullDataType)
	}
}
