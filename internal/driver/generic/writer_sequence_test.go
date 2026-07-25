package generic

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
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

func openWriter(t *testing.T) (gen driver.Writer, genPath string) {
	t.Helper()
	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	genPath = filepath.Join(t.TempDir(), "gen.db")
	gen, err = NewWriter(cat, &dbconfig.TargetConfig{Type: "sqlite", Database: genPath, ChunkSize: 1000}, 1,
		driver.WriterOptions{BatchSize: 1000, SourceType: "sqlite", TypeMapper: tm})
	if err != nil {
		t.Fatalf("generic NewWriter: %v", err)
	}
	t.Cleanup(gen.Close)
	return gen, genPath
}

// TestSQLiteCatalogWriterExecutesSMTCreatePlanVerbatim is a real database
// create-path migration check. sqlite_master retains the submitted CREATE
// TABLE text, allowing this test to prove that the writer executes DMT's SMT
// plan statement unchanged while retaining DMT's schema/PK idempotency flow.
func TestSQLiteCatalogWriterExecutesSMTCreatePlanVerbatim(t *testing.T) {
	ctx := context.Background()
	gen, genPath := openWriter(t)
	table := testTable()

	expected, err := driver.PlanCreateTable(driver.TableDDLRequest{
		SourceDBType: "sqlite",
		TargetDBType: "sqlite",
		SourceTable:  table,
	})
	if err != nil {
		t.Fatalf("planning SMT create table: %v", err)
	}
	if err := gen.CreateSchema(ctx, "ignored_by_sqlite"); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	if err := gen.CreateTable(ctx, table, "ignored_by_sqlite"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := gen.CreatePrimaryKey(ctx, table, ""); err != nil {
		t.Fatalf("CreatePrimaryKey inline idempotency: %v", err)
	}

	db, err := sql.Open("sqlite", genPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'items'`).Scan(&got); err != nil {
		t.Fatalf("read sqlite_master: %v", err)
	}
	if got != expected {
		t.Fatalf("writer rewrote SMT create SQL:\n got: %s\nwant: %s", got, expected)
	}
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

// The full writer operation sequence with literal state assertions.
// These expectations were proven byte-identical to the hand-written
// sqlite writer by the differential test that ran until #506.
func TestSQLiteCatalogWriterSequence(t *testing.T) {
	ctx := context.Background()
	gen, genPath := openWriter(t)
	tbl := testTable()

	expect := func(stage string, check func(state map[string]any) string) {
		t.Helper()
		state := dump(t, genPath)
		if msg := check(state); msg != "" {
			t.Fatalf("after %s: %s\nstate: %v", stage, msg, state)
		}
	}

	for _, w := range []driver.Writer{gen} {
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
	expect("create table", func(st map[string]any) string {
		ddl, _ := st["ddl:items"].(string)
		for _, frag := range []string{"CREATE TABLE", "AUTOINCREMENT"} {
			if !strings.Contains(ddl, frag) {
				return "DDL missing " + frag + ": " + ddl
			}
		}
		return ""
	})

	cols := []string{"id", "name", "active", "price"}
	rows := [][]any{
		{int64(1), "a", true, 1.50},
		{int64(2), "b", false, 2.25},
		{int64(3), "c", nil, nil},
	}
	for _, w := range []driver.Writer{gen} {
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
	expect("write batch + idempotent replay", func(st map[string]any) string {
		rows, _ := st["data:items"].([][]any)
		if len(rows) != 3 {
			return fmt.Sprintf("rows = %d, want 3 (replay must not duplicate)", len(rows))
		}
		if rows[0][1] != "a" || rows[0][2] != int64(1) || rows[0][3] != 1.5 {
			return fmt.Sprintf("row1 = %v (bool→int and values)", rows[0])
		}
		if rows[2][2] != nil || rows[2][3] != nil {
			return fmt.Sprintf("row3 NULLs = %v", rows[2])
		}
		return ""
	})

	for _, w := range []driver.Writer{gen} {
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
	expect("upsert + sequence reset", func(st map[string]any) string {
		rows, _ := st["data:items"].([][]any)
		if len(rows) != 4 {
			return fmt.Sprintf("rows = %d, want 4", len(rows))
		}
		if rows[1][1] != "b-updated" || rows[1][3] != 9.99 {
			return fmt.Sprintf("upsert update row = %v", rows[1])
		}
		seq, _ := st["sequence"].([]string)
		if len(seq) != 2 || seq[0] != "items" || seq[1] != "4" {
			return fmt.Sprintf("sequence = %v, want [items 4]", seq)
		}
		return ""
	})

	newCol := &driver.Column{Name: "note", DataType: "text", IsNullable: true}
	for _, w := range []driver.Writer{gen} {
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
	expect("add column", func(st map[string]any) string {
		ddl, _ := st["ddl:items"].(string)
		if !strings.Contains(ddl, `"note"`) {
			return "added column missing from DDL: " + ddl
		}
		return ""
	})

	for _, w := range []driver.Writer{gen} {
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
	expect("truncate + drop", func(st map[string]any) string {
		if _, ok := st["ddl:items"]; ok {
			return "items table still exists after drop"
		}
		return ""
	})
}
