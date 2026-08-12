package shared

import (
	"context"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

func TestQueryStandardColumns(t *testing.T) {
	db := openSharedSQLite(t)
	ctx := context.Background()

	got, err := QueryStandardColumns(ctx, db, `
		SELECT 'id', 'integer', 0, 0, 0, false, true, '', 1
		UNION ALL
		SELECT 'email', 'varchar', 255, 0, 0, false, false, '''unknown''', 2
		UNION ALL
		SELECT 'price', 'numeric', 0, 10, 2, true, false, '0', 3
	`, "users")
	if err != nil {
		t.Fatalf("QueryStandardColumns returned error: %v", err)
	}

	want := []driver.Column{
		{Name: "id", DataType: "integer", IsIdentity: true, OrdinalPos: 1},
		{Name: "email", DataType: "varchar", MaxLength: 255, DefaultValue: "'unknown'", OrdinalPos: 2},
		{Name: "price", DataType: "numeric", Precision: 10, Scale: 2, IsNullable: true, DefaultValue: "0", OrdinalPos: 3},
	}

	if len(got) != len(want) {
		t.Fatalf("QueryStandardColumns returned %d columns, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("column %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestQueryStandardColumnsNilDB(t *testing.T) {
	_, err := QueryStandardColumns(context.Background(), nil, "SELECT 1", "users")
	if err == nil {
		t.Fatal("QueryStandardColumns nil db returned nil error")
	}
}
