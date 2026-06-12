package generic

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	mysqldrv "github.com/johndauphine/dmt/internal/driver/mysql"
)

// Differential proof for the mysql conversion (#509): the catalog
// engine's view and effects against a live MySQL server must match the
// hand-written driver's, with the hand-written implementation as the
// oracle — the same bar the sqlite conversion cleared.
//
// Requires the mysql-test container (localhost:3306, root/TestPass2024).
// Skips when unreachable unless MYSQL_REQUIRED=1.

func mysqlBootstrap(t *testing.T, dbName string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	raw, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.Ping(); err != nil {
		if os.Getenv("MYSQL_REQUIRED") == "1" {
			t.Fatalf("mysql required but unreachable: %v", err)
		}
		t.Skipf("mysql not reachable: %v", err)
	}
	if _, err := raw.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s", dbName, dbName)); err != nil {
		t.Fatal(err)
	}
}

const mysqlFixtureDDL = `
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    bio TEXT,
    balance DECIMAL(10,2) DEFAULT 0.00,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_users_name ON users(name);
CREATE UNIQUE INDEX idx_users_name_created ON users(name, created_at);

CREATE TABLE orders (
    region VARCHAR(10) NOT NULL,
    order_no INT NOT NULL,
    user_id INT,
    note TEXT,
    PRIMARY KEY (order_no, region),
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE ON UPDATE SET NULL,
    CONSTRAINT chk_region CHECK (region <> '')
);

INSERT INTO users (name, balance) VALUES ('a', 1.50), ('b', 2.50), ('c', 0);
INSERT INTO orders (region, order_no, user_id) VALUES ('us', 1, 1), ('eu', 2, 2);
`

func TestMySQLCatalogMatchesHandWrittenReader(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_diff_src"
	mysqlBootstrap(t, dbName)

	raw, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName+"?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(mysqlFixtureDDL); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	raw.Close()

	cfg := &dbconfig.SourceConfig{
		Type: "mysql", Host: "localhost", Port: 3306,
		Database: dbName, User: "root", Password: "TestPass2024",
		Schema: dbName, SSLMode: "disable",
	}
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("generic NewReader: %v", err)
	}
	defer gen.Close()
	ref, err := mysqldrv.NewReader(cfg, 4)
	if err != nil {
		t.Fatalf("mysql NewReader: %v", err)
	}
	defer ref.Close()

	genTables, err := gen.ExtractSchema(ctx, dbName)
	if err != nil {
		t.Fatalf("generic ExtractSchema: %v", err)
	}
	refTables, err := ref.ExtractSchema(ctx, dbName)
	if err != nil {
		t.Fatalf("mysql ExtractSchema: %v", err)
	}
	if len(genTables) != len(refTables) {
		t.Fatalf("table count: %d != %d", len(genTables), len(refTables))
	}

	for i := range refTables {
		genT, refT := &genTables[i], &refTables[i]
		for name, fn := range map[string]func(context.Context, *driver.Table) error{
			"gen indexes": gen.LoadIndexes, "gen fks": gen.LoadForeignKeys, "gen checks": gen.LoadCheckConstraints,
		} {
			if err := fn(ctx, genT); err != nil {
				t.Fatalf("%s(%s): %v", name, genT.Name, err)
			}
		}
		for name, fn := range map[string]func(context.Context, *driver.Table) error{
			"ref indexes": ref.LoadIndexes, "ref fks": ref.LoadForeignKeys, "ref checks": ref.LoadCheckConstraints,
		} {
			if err := fn(ctx, refT); err != nil {
				t.Fatalf("%s(%s): %v", name, refT.Name, err)
			}
		}
		if !reflect.DeepEqual(genT, refT) {
			t.Errorf("table %s diverges:\n  generic: %+v\n  mysql:   %+v", refT.Name, genT, refT)
		}
	}

	t.Run("counts and partitions", func(t *testing.T) {
		for _, tbl := range refTables {
			g, err := gen.GetRowCountExact(ctx, dbName, tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ref.GetRowCountExact(ctx, dbName, tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			if g != r {
				t.Errorf("count(%s): %d != %d", tbl.Name, g, r)
			}
		}
		gp, err := gen.GetPartitionBoundaries(ctx, &refTables[1], 2)
		if err != nil {
			t.Fatal(err)
		}
		rp, err := ref.GetPartitionBoundaries(ctx, &refTables[1], 2)
		if err != nil {
			t.Fatal(err)
		}
		// The hand-written mysql reader never sets IsFirstPartition —
		// every other engine does, and it gates partial-data cleanup on
		// resume. The generic engine's true-on-partition-1 is the
		// correct shared semantic; normalize the oracle's defect here
		// rather than replicate it (noted in the #509 PR).
		for i := range rp {
			rp[i].IsFirstPartition = rp[i].PartitionID == 1
		}
		if !reflect.DeepEqual(gp, rp) {
			t.Errorf("partitions: %+v != %+v", gp, rp)
		}
	})

	t.Run("incremental dates", func(t *testing.T) {
		gd := gen.(driver.IncrementalDateReader)
		var rd driver.IncrementalDateReader = ref
		gc, gt, gf := gd.GetDateColumnInfo(ctx, dbName, "users", []string{"missing", "created_at"})
		rc, rt, rf := rd.GetDateColumnInfo(ctx, dbName, "users", []string{"missing", "created_at"})
		if gc != rc || gt != rt || gf != rf {
			t.Errorf("date info: (%q,%q,%v) != (%q,%q,%v)", gc, gt, gf, rc, rt, rf)
		}
	})
}

// Writer equivalence: identical operation sequences into two databases
// leave identical observable state (information_schema shape + data).
func TestMySQLCatalogMatchesHandWrittenWriter(t *testing.T) {
	ctx := context.Background()
	mysqlBootstrap(t, "dmt_diff_gen")
	mysqlBootstrap(t, "dmt_diff_ref")

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	wopts := driver.WriterOptions{BatchSize: 1000, SourceType: "mysql", TypeMapper: tm}

	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	genW, err := NewWriter(cat, &dbconfig.TargetConfig{
		Type: "mysql", Host: "localhost", Port: 3306,
		Database: "dmt_diff_gen", User: "root", Password: "TestPass2024",
		SSLMode: "disable", ChunkSize: 1000,
	}, 4, wopts)
	if err != nil {
		t.Fatalf("generic NewWriter: %v", err)
	}
	defer genW.Close()
	refW, err := mysqldrv.NewWriter(&dbconfig.TargetConfig{
		Type: "mysql", Host: "localhost", Port: 3306,
		Database: "dmt_diff_ref", User: "root", Password: "TestPass2024",
		SSLMode: "disable", ChunkSize: 1000,
	}, 4, wopts)
	if err != nil {
		t.Fatalf("mysql NewWriter: %v", err)
	}
	defer refW.Close()

	tbl := &driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "int", IsIdentity: true, OrdinalPos: 1},
			{Name: "name", DataType: "varchar", MaxLength: 80, IsNullable: false, OrdinalPos: 2},
			{Name: "active", DataType: "tinyint", IsNullable: true, OrdinalPos: 3},
			{Name: "price", DataType: "decimal", Precision: 10, Scale: 2, IsNullable: true, OrdinalPos: 4},
		},
		PrimaryKey: []string{"id"},
	}

	cols := []string{"id", "name", "active", "price"}
	rows := [][]any{
		{int64(1), "a", true, 1.50},
		{int64(2), "b", false, 2.25},
		{int64(3), "c", nil, nil},
	}

	run := func(w driver.Writer, schema string) {
		t.Helper()
		if err := w.CreateTable(ctx, tbl, schema); err != nil {
			t.Fatalf("CreateTable(%s): %v", schema, err)
		}
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: schema, Table: "items", Columns: cols, Rows: rows,
		}); err != nil {
			t.Fatalf("WriteBatch(%s): %v", schema, err)
		}
		// #227 idempotent replay is a no-op.
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: schema, Table: "items", Columns: cols, Rows: rows,
			IdempotentOnDup: true, PKColumns: []string{"id"},
		}); err != nil {
			t.Fatalf("idempotent WriteBatch(%s): %v", schema, err)
		}
		up := w.(driver.Upserter)
		if err := up.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Schema: schema, Table: "items", Columns: cols, PKColumns: []string{"id"},
			Rows: [][]any{
				{int64(2), "b-updated", true, 9.99},
				{int64(4), "d", false, 4.00},
			},
		}); err != nil {
			t.Fatalf("UpsertBatch(%s): %v", schema, err)
		}
		seq := w.(driver.SequenceResetter)
		if err := seq.ResetSequence(ctx, schema, tbl); err != nil {
			t.Fatalf("ResetSequence(%s): %v", schema, err)
		}
		newCol := &driver.Column{Name: "note", DataType: "text", IsNullable: true}
		if err := w.AddColumn(ctx, tbl, newCol, schema); err != nil {
			t.Fatalf("AddColumn(%s): %v", schema, err)
		}
		if err := w.AddColumn(ctx, tbl, newCol, schema); err != nil {
			t.Fatalf("AddColumn idempotent(%s): %v", schema, err)
		}
		relax := &driver.Column{Name: "name", DataType: "varchar", MaxLength: 80, IsNullable: true}
		if err := w.DropColumnNotNull(ctx, tbl, relax, schema); err != nil {
			t.Fatalf("DropColumnNotNull(%s): %v", schema, err)
		}
	}
	run(genW, "dmt_diff_gen")
	run(refW, "dmt_diff_ref")

	got := dumpMySQL(t, "dmt_diff_gen")
	want := dumpMySQL(t, "dmt_diff_ref")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state diverges:\n  generic: %v\n  mysql:   %v", got, want)
	}
}

// dumpMySQL reads observable state: column shapes, auto_increment
// counter, and full data.
func dumpMySQL(t *testing.T, dbName string) map[string]any {
	t.Helper()
	db, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state := map[string]any{}

	rows, err := db.Query(`SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items'
		ORDER BY ORDINAL_POSITION`, dbName)
	if err != nil {
		t.Fatal(err)
	}
	var colShapes []string
	for rows.Next() {
		var n, ct, nl string
		var def, extra sql.NullString
		if err := rows.Scan(&n, &ct, &nl, &def, &extra); err != nil {
			t.Fatal(err)
		}
		colShapes = append(colShapes, fmt.Sprintf("%s %s %s %s %s", n, ct, nl, def.String, extra.String))
	}
	rows.Close()
	state["columns"] = colShapes

	var autoInc sql.NullInt64
	_ = db.QueryRow(`SELECT AUTO_INCREMENT FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items'`, dbName).Scan(&autoInc)
	state["auto_increment"] = autoInc.Int64

	dataRows, err := db.Query("SELECT id, name, active, price FROM items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var data []string
	for dataRows.Next() {
		var id int64
		var name string
		var active, price sql.NullString
		if err := dataRows.Scan(&id, &name, &active, &price); err != nil {
			t.Fatal(err)
		}
		data = append(data, fmt.Sprintf("%d|%s|%s|%s", id, name, active.String, price.String))
	}
	dataRows.Close()
	state["data"] = data
	return state
}
