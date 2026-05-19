package shared

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestStreamTableFullTable(t *testing.T) {
	ctx := context.Background()
	db := openStreamDB(t)

	opts := streamReadOptions(nil, 3)
	batches, err := StreamTable(ctx, StreamConfig{DB: db, Dialect: streamDialect{}}, opts)
	if err != nil {
		t.Fatalf("StreamTable returned error: %v", err)
	}

	got := collectStreamBatches(t, batches)
	if len(got) != 2 {
		t.Fatalf("batch count = %d, want 2", len(got))
	}
	if len(got[0].Rows) != 3 || got[0].Done {
		t.Fatalf("first batch rows/done = %d/%v, want 3/false", len(got[0].Rows), got[0].Done)
	}
	if len(got[1].Rows) != 1 || !got[1].Done {
		t.Fatalf("second batch rows/done = %d/%v, want 1/true", len(got[1].Rows), got[1].Done)
	}
}

func TestStreamTableKeyset(t *testing.T) {
	ctx := context.Background()
	db := openStreamDB(t)

	partition := &driver.Partition{MinPK: int64(0), MaxPK: int64(3)}
	opts := streamReadOptions(partition, 2)
	batches, err := StreamTable(ctx, StreamConfig{DB: db, Dialect: streamDialect{}}, opts)
	if err != nil {
		t.Fatalf("StreamTable returned error: %v", err)
	}

	got := collectStreamBatches(t, batches)
	if len(got) != 2 {
		t.Fatalf("batch count = %d, want 2", len(got))
	}
	if got[0].LastKey != int64(2) || got[0].Done {
		t.Fatalf("first batch last/done = %#v/%v, want 2/false", got[0].LastKey, got[0].Done)
	}
	if got[1].LastKey != int64(3) || !got[1].Done {
		t.Fatalf("second batch last/done = %#v/%v, want 3/true", got[1].LastKey, got[1].Done)
	}
}

func TestStreamTableRowNumber(t *testing.T) {
	ctx := context.Background()
	db := openStreamDB(t)

	partition := &driver.Partition{StartRow: 1, EndRow: 4}
	opts := streamReadOptions(partition, 2)
	batches, err := StreamTable(ctx, StreamConfig{DB: db, Dialect: streamDialect{}}, opts)
	if err != nil {
		t.Fatalf("StreamTable returned error: %v", err)
	}

	got := collectStreamBatches(t, batches)
	if len(got) != 2 {
		t.Fatalf("batch count = %d, want 2", len(got))
	}
	if got[0].RowNum != 1 || len(got[0].Rows) != 2 || got[0].Done {
		t.Fatalf("first batch rownum/rows/done = %d/%d/%v, want 1/2/false", got[0].RowNum, len(got[0].Rows), got[0].Done)
	}
	if got[1].RowNum != 3 || len(got[1].Rows) != 1 || !got[1].Done {
		t.Fatalf("second batch rownum/rows/done = %d/%d/%v, want 3/1/true", got[1].RowNum, len(got[1].Rows), got[1].Done)
	}
}

func openStreamDB(t *testing.T) SQLQuerier {
	t.Helper()

	db := openSharedSQLite(t)
	ctx := context.Background()
	if _, err := ExecRaw(ctx, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create users: %v", err)
	}
	if _, err := ExecRaw(ctx, db,
		`INSERT INTO users (id, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?)`,
		1, "Ada", 2, "Grace", 3, "Katherine", 4, "Dorothy"); err != nil {
		t.Fatalf("insert users: %v", err)
	}
	return db
}

func streamReadOptions(partition *driver.Partition, chunkSize int) driver.ReadOptions {
	return driver.ReadOptions{
		Table: driver.Table{
			Schema:     "",
			Name:       "users",
			PrimaryKey: []string{"id"},
		},
		Columns:   []string{"id", "name"},
		Partition: partition,
		ChunkSize: chunkSize,
	}
}

func collectStreamBatches(t *testing.T, batches <-chan driver.Batch) []driver.Batch {
	t.Helper()

	var got []driver.Batch
	for batch := range batches {
		if batch.Error != nil {
			t.Fatalf("batch error: %v", batch.Error)
		}
		got = append(got, batch)
	}
	return got
}

type streamDialect struct{}

func (streamDialect) DBType() string { return "stream" }

func (streamDialect) QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

func (d streamDialect) QualifyTable(schema, table string) string {
	if schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (streamDialect) ParameterPlaceholder(int) string { return "?" }

func (streamDialect) BuildDSN(string, int, string, string, string, map[string]any) string {
	return ""
}

func (streamDialect) TableHint(bool) string { return "" }

func (d streamDialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = d.QuoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}

func (d streamDialect) ColumnListForSelect(cols, _ []string, _ string) string {
	return d.ColumnList(cols)
}

func (d streamDialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, _ *driver.DateFilter) string {
	qPK := d.QuoteIdentifier(pkCol)
	if hasMaxPK {
		return fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s > ? AND %s <= ? ORDER BY %s LIMIT ?",
			cols, d.QualifyTable(schema, table), qPK, qPK, qPK)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
		cols, d.QualifyTable(schema, table), qPK, qPK)
}

func (streamDialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, _ *driver.DateFilter) []any {
	if hasMaxPK {
		return []any{lastPK, maxPK, limit}
	}
	return []any{lastPK, limit}
}

func (d streamDialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string, _ *driver.DateFilter) string {
	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) AS __rn
			FROM %s
		)
		WHERE __rn > ? AND __rn <= ?
		ORDER BY __rn
	`, cols, cols, orderBy, d.QualifyTable(schema, table))
}

func (streamDialect) BuildRowNumberArgs(rowNum int64, limit int, _ *driver.DateFilter) []any {
	return []any{rowNum, rowNum + int64(limit)}
}

func (streamDialect) PartitionBoundariesQuery(string, string, string, int) string { return "" }
func (streamDialect) RowCountQuery(bool) string                                   { return "" }
func (streamDialect) DateColumnQuery() string                                     { return "" }
func (streamDialect) ValidDateTypes() map[string]bool                             { return nil }
func (streamDialect) AIPromptAugmentation() string                                { return "" }
func (streamDialect) AIDropTablePromptAugmentation() string                       { return "" }
