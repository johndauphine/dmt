package generic

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/sqlite"
)

func testTable() *driver.Table {
	return &driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsIdentity: true, OrdinalPos: 1},
			{Name: "name", DataType: "varchar", MaxLength: 80, IsNullable: false, OrdinalPos: 2},
			{Name: "active", DataType: "boolean", IsNullable: true, OrdinalPos: 3},
			{Name: "price", DataType: "numeric", Precision: 10, Scale: 2, IsNullable: true, OrdinalPos: 4},
		},
		PrimaryKey: []string{"id"},
	}
}

func openWriters(t *testing.T) (gen, ref driver.Writer, genPath, refPath string) {
	t.Helper()
	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	wopts := driver.WriterOptions{BatchSize: 1000, SourceType: "sqlite", TypeMapper: tm}

	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	genPath = filepath.Join(t.TempDir(), "gen.db")
	gen, err = NewWriter(cat, &dbconfig.TargetConfig{Type: "sqlite", Database: genPath, ChunkSize: 1000}, 1, wopts)
	if err != nil {
		t.Fatalf("generic NewWriter: %v", err)
	}
	t.Cleanup(gen.Close)

	refPath = filepath.Join(t.TempDir(), "ref.db")
	refW, err := sqlite.NewWriter(&dbconfig.TargetConfig{Type: "sqlite", Database: refPath, ChunkSize: 1000}, 1, wopts)
	if err != nil {
		t.Fatalf("sqlite NewWriter: %v", err)
	}
	t.Cleanup(refW.Close)
	return gen, refW, genPath, refPath
}

// dump reads observable database state: the verbatim DDL of every
// object, every table's rows, and the sqlite_sequence content.
func dump(t *testing.T, path string) map[string]any {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	state := map[string]any{}
	rows, err := db.Query(`SELECT name, COALESCE(sql,'') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		state["ddl:"+name] = ddl
		if !strings.Contains(ddl, "CREATE INDEX") && !strings.Contains(ddl, "CREATE UNIQUE INDEX") {
			tables = append(tables, name)
		}
	}
	rows.Close()

	for _, tbl := range tables {
		dataRows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" ORDER BY 1`, tbl))
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := dataRows.Columns()
		var data [][]any
		for dataRows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := dataRows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			data = append(data, vals)
		}
		dataRows.Close()
		state["data:"+tbl] = data
	}

	var seqTable, seqVal sql.NullString
	_ = db.QueryRow(`SELECT name, seq FROM sqlite_sequence LIMIT 1`).Scan(&seqTable, &seqVal)
	state["sequence"] = []string{seqTable.String, seqVal.String}
	return state
}

// The PR-3 acceptance bar: an identical operation sequence through the
// catalog-driven and hand-written writers leaves byte-identical
// observable database state (DDL text, data, sequence counters).
func TestSQLiteCatalogMatchesHandWrittenWriter(t *testing.T) {
	ctx := context.Background()
	gen, ref, genPath, refPath := openWriters(t)
	tbl := testTable()

	// Each step runs against both writers; state is compared after the
	// full sequence and at checkpoints marked compare().
	compare := func(stage string) {
		t.Helper()
		got, want := dump(t, genPath), dump(t, refPath)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("state diverges after %s:\n  generic: %v\n  sqlite:  %v", stage, got, want)
		}
	}

	for _, w := range []driver.Writer{gen, ref} {
		if err := w.CreateSchema(ctx, "ignored"); err != nil {
			t.Fatal(err)
		}
		if exists, err := w.TableExists(ctx, "", "items"); err != nil || exists {
			t.Fatalf("TableExists pre-create: %v %v", exists, err)
		}
		if err := w.CreateTable(ctx, tbl, ""); err != nil {
			t.Fatal(err)
		}
		if exists, err := w.TableExists(ctx, "", "items"); err != nil || !exists {
			t.Fatalf("TableExists post-create: %v %v", exists, err)
		}
		if hasPK, err := w.HasPrimaryKey(ctx, "", "items"); err != nil || !hasPK {
			t.Fatalf("HasPrimaryKey: %v %v", hasPK, err)
		}
		if err := w.CreatePrimaryKey(ctx, tbl, ""); err != nil {
			t.Fatal(err) // no-op on sqlite
		}
	}
	compare("create table")

	cols := []string{"id", "name", "active", "price"}
	rows := [][]any{
		{int64(1), "a", true, 1.50},
		{int64(2), "b", false, 2.25},
		{int64(3), "c", nil, nil},
	}
	for _, w := range []driver.Writer{gen, ref} {
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Table: "items", Columns: cols, Rows: rows,
		}); err != nil {
			t.Fatal(err)
		}
		// Idempotent replay (#227): the same chunk again must be a
		// silent no-op, not duplicates or overwrites.
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Table: "items", Columns: cols, Rows: rows,
			IdempotentOnDup: true, PKColumns: []string{"id"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	compare("write batch + idempotent replay")

	for _, w := range []driver.Writer{gen, ref} {
		up, ok := w.(driver.Upserter)
		if !ok {
			t.Fatal("writer must expose Upserter (catalog declares it)")
		}
		if err := up.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Table: "items", Columns: cols, PKColumns: []string{"id"},
			Rows: [][]any{
				{int64(2), "b-updated", true, 9.99}, // update
				{int64(4), "d", false, 4.00},        // insert
			},
		}); err != nil {
			t.Fatal(err)
		}
		seq, ok := w.(driver.SequenceResetter)
		if !ok {
			t.Fatal("writer must expose SequenceResetter (catalog declares it)")
		}
		if err := seq.ResetSequence(ctx, "", tbl); err != nil {
			t.Fatal(err)
		}
	}
	compare("upsert + sequence reset")

	newCol := &driver.Column{Name: "note", DataType: "text", IsNullable: true}
	for _, w := range []driver.Writer{gen, ref} {
		if err := w.AddColumn(ctx, tbl, newCol, ""); err != nil {
			t.Fatal(err)
		}
		// Idempotency: second AddColumn is a no-op.
		if err := w.AddColumn(ctx, tbl, newCol, ""); err != nil {
			t.Fatal(err)
		}
		// Unsupported in-place changes return errors, not silent no-ops.
		if err := w.DropColumnNotNull(ctx, tbl, &tbl.Columns[1], ""); err == nil {
			t.Fatal("DropColumnNotNull must error on sqlite")
		}
		if err := w.AlterColumnType(ctx, tbl, &tbl.Columns[1], ""); err == nil {
			t.Fatal("AlterColumnType must error on sqlite")
		}
	}
	compare("add column")

	for _, w := range []driver.Writer{gen, ref} {
		n, err := w.GetRowCount(ctx, "", "items")
		if err != nil || n != 4 {
			t.Fatalf("GetRowCount: %d %v", n, err)
		}
		if err := w.TruncateTable(ctx, "", "items"); err != nil {
			t.Fatal(err)
		}
		if err := w.DropTable(ctx, "", "items"); err != nil {
			t.Fatal(err)
		}
	}
	compare("truncate + drop")
}
