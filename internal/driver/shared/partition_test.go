package shared

import (
	"context"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestQueryPartitionBoundaries(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	query := `
		SELECT 1 AS partition_id, 10 AS min_pk, 19 AS max_pk, 10 AS row_count
		UNION ALL
		SELECT 2 AS partition_id, 20 AS min_pk, 29 AS max_pk, 10 AS row_count
	`
	got, err := QueryPartitionBoundaries(ctx, db, query, "orders")
	if err != nil {
		t.Fatalf("QueryPartitionBoundaries returned error: %v", err)
	}

	want := []driver.Partition{
		{TableName: "orders", PartitionID: 1, MinPK: int64(10), MaxPK: int64(19), RowCount: 10},
		{TableName: "orders", PartitionID: 2, MinPK: int64(20), MaxPK: int64(29), RowCount: 10},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QueryPartitionBoundaries() = %#v, want %#v", got, want)
	}
}
