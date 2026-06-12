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
	pgdrv "github.com/johndauphine/dmt/internal/driver/postgres"
)

// Differential proof for the postgres conversion (#509): the catalog
// engine's view and effects against a live PostgreSQL server must match
// the hand-written driver's, with the hand-written implementation as
// the oracle — the same bar the sqlite and mysql conversions cleared.
//
// Requires the pg-bench container (localhost:5432, postgres/TestPass2024).
// Skips when unreachable unless PG_REQUIRED=1.

const pgDSNBase = "postgres://postgres:TestPass2024@localhost:5432/"

func pgBootstrap(t *testing.T, dbName string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	raw, err := sql.Open("pgx", pgDSNBase+"postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.Ping(); err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but unreachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	// pgx uses the extended protocol (one statement per Exec), and
	// CREATE/DROP DATABASE cannot run inside a transaction anyway.
	if _, err := raw.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
}

func pgExecAll(t *testing.T, dbName string, stmts []string) {
	t.Helper()
	db, err := sql.Open("pgx", pgDSNBase+dbName+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

var pgFixtureDDL = []string{
	`CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(120) NOT NULL,
		bio TEXT,
		balance NUMERIC(10,2) DEFAULT 0.00,
		active BOOLEAN NOT NULL DEFAULT true,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE INDEX idx_users_name ON users(name)`,
	`CREATE UNIQUE INDEX idx_users_name_created ON users(name, created_at)`,
	`CREATE TABLE orders (
		region VARCHAR(10) NOT NULL,
		order_no INT NOT NULL,
		user_id INT,
		note TEXT,
		PRIMARY KEY (order_no, region),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
		CONSTRAINT chk_region CHECK (region <> '')
	)`,
	`INSERT INTO users (name, balance) VALUES ('a', 1.50), ('b', 2.50), ('c', 0)`,
	`INSERT INTO orders (region, order_no, user_id) VALUES ('us', 1, 1), ('eu', 2, 2)`,
}

func TestPostgresCatalogMatchesHandWrittenReader(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_pgdiff_src"
	pgBootstrap(t, dbName)
	pgExecAll(t, dbName, pgFixtureDDL)

	cfg := &dbconfig.SourceConfig{
		Type: "postgres", Host: "localhost", Port: 5432,
		Database: dbName, User: "postgres", Password: "TestPass2024",
		Schema: "public", SSLMode: "disable",
	}
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("generic NewReader: %v", err)
	}
	defer gen.Close()
	ref, err := pgdrv.NewReader(cfg, 4)
	if err != nil {
		t.Fatalf("postgres NewReader: %v", err)
	}
	defer ref.Close()

	genTables, err := gen.ExtractSchema(ctx, "public")
	if err != nil {
		t.Fatalf("generic ExtractSchema: %v", err)
	}
	refTables, err := ref.ExtractSchema(ctx, "public")
	if err != nil {
		t.Fatalf("postgres ExtractSchema: %v", err)
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
			t.Errorf("table %s diverges:\n  generic:  %+v\n  postgres: %+v", refT.Name, genT, refT)
		}
	}

	t.Run("counts and partitions", func(t *testing.T) {
		for _, tbl := range refTables {
			g, err := gen.GetRowCountExact(ctx, "public", tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ref.GetRowCountExact(ctx, "public", tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			if g != r {
				t.Errorf("count(%s): %d != %d", tbl.Name, g, r)
			}
		}
		gp, err := gen.GetPartitionBoundaries(ctx, &refTables[len(refTables)-1], 2)
		if err != nil {
			t.Fatal(err)
		}
		rp, err := ref.GetPartitionBoundaries(ctx, &refTables[len(refTables)-1], 2)
		if err != nil {
			t.Fatal(err)
		}
		// shared.QueryPartitionBoundaries (used by the hand-written pg
		// reader) never sets IsFirstPartition — the same oracle defect
		// the mysql differential normalized. The generic engine's
		// true-on-partition-1 is the correct shared semantic: it gates
		// partial-data cleanup on resume.
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
		gc, gt, gf := gd.GetDateColumnInfo(ctx, "public", "users", []string{"missing", "created_at"})
		rc, rt, rf := rd.GetDateColumnInfo(ctx, "public", "users", []string{"missing", "created_at"})
		if gc != rc || gt != rt || gf != rf {
			t.Errorf("date info: (%q,%q,%v) != (%q,%q,%v)", gc, gt, gf, rc, rt, rf)
		}
	})
}

// Writer equivalence: identical operation sequences into two databases
// leave identical observable state (information_schema shape, sequence
// position, and data).
func TestPostgresCatalogMatchesHandWrittenWriter(t *testing.T) {
	ctx := context.Background()
	pgBootstrap(t, "dmt_pgdiff_gen")
	pgBootstrap(t, "dmt_pgdiff_ref")

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	wopts := driver.WriterOptions{BatchSize: 1000, SourceType: "postgres", TypeMapper: tm}

	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	genW, err := NewWriter(cat, &dbconfig.TargetConfig{
		Type: "postgres", Host: "localhost", Port: 5432,
		Database: "dmt_pgdiff_gen", User: "postgres", Password: "TestPass2024",
		Schema: "public", SSLMode: "disable", ChunkSize: 1000,
	}, 4, wopts)
	if err != nil {
		t.Fatalf("generic NewWriter: %v", err)
	}
	defer genW.Close()
	refW, err := pgdrv.NewWriter(&dbconfig.TargetConfig{
		Type: "postgres", Host: "localhost", Port: 5432,
		Database: "dmt_pgdiff_ref", User: "postgres", Password: "TestPass2024",
		Schema: "public", SSLMode: "disable", ChunkSize: 1000,
	}, 4, wopts)
	if err != nil {
		t.Fatalf("postgres NewWriter: %v", err)
	}
	defer refW.Close()

	// Mixed-case source names (the MSSQL shape): every writer surface
	// must sanitize to the lowercase names CreateTable emits.
	tbl := &driver.Table{
		Name: "ItemEvents",
		Columns: []driver.Column{
			{Name: "Id", DataType: "int4", IsIdentity: true, OrdinalPos: 1},
			{Name: "Name", DataType: "varchar", MaxLength: 80, IsNullable: false, OrdinalPos: 2},
			{Name: "Active", DataType: "bool", IsNullable: true, OrdinalPos: 3},
			{Name: "Price", DataType: "numeric", Precision: 10, Scale: 2, IsNullable: true, OrdinalPos: 4},
		},
		PrimaryKey: []string{"Id"},
	}

	cols := []string{"Id", "Name", "Active", "Price"}
	rows := [][]any{
		{int64(1), "a", true, 1.50},
		{int64(2), "b", false, 2.25},
		{int64(3), "c", nil, nil},
	}

	run := func(w driver.Writer, label string) {
		t.Helper()
		if err := w.CreateTable(ctx, tbl, "public"); err != nil {
			t.Fatalf("CreateTable(%s): %v", label, err)
		}
		// drop_recreate rerun shape: DropTable must hit the sanitized
		// name or the second CreateTable fails with "already exists".
		if err := w.DropTable(ctx, "public", tbl.Name); err != nil {
			t.Fatalf("DropTable(%s): %v", label, err)
		}
		if err := w.CreateTable(ctx, tbl, "public"); err != nil {
			t.Fatalf("CreateTable after drop (%s): %v", label, err)
		}
		// The orchestrator calls CreatePrimaryKey right after transfer
		// even though CreateTable emitted the PK inline — must no-op.
		if err := w.CreatePrimaryKey(ctx, tbl, "public"); err != nil {
			t.Fatalf("CreatePrimaryKey(%s): %v", label, err)
		}
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "public", Table: "ItemEvents", Columns: cols, Rows: rows,
		}); err != nil {
			t.Fatalf("WriteBatch(%s): %v", label, err)
		}
		// #227 idempotent replay is a no-op.
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "public", Table: "ItemEvents", Columns: cols, Rows: rows,
			IdempotentOnDup: true, PKColumns: []string{"Id"},
		}); err != nil {
			t.Fatalf("idempotent WriteBatch(%s): %v", label, err)
		}
		up := w.(driver.Upserter)
		// Oracle defect normalization: the hand-written UpsertBatch is
		// the one writer surface that never sanitizes identifiers, so
		// mixed-case upserts fail on it outright. Feed it pre-sanitized
		// names; the generic engine gets the raw ones and must sanitize
		// itself.
		upTable, upCols, upPK := "ItemEvents", cols, []string{"Id"}
		if label == "postgres" {
			upTable, upPK = "itemevents", []string{"id"}
			upCols = []string{"id", "name", "active", "price"}
		}
		if err := up.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Schema: "public", Table: upTable, Columns: upCols, PKColumns: upPK,
			Rows: [][]any{
				{int64(2), "b-updated", true, 9.99},
				{int64(4), "d", false, 4.00},
			},
		}); err != nil {
			t.Fatalf("UpsertBatch(%s): %v", label, err)
		}
		seq := w.(driver.SequenceResetter)
		if err := seq.ResetSequence(ctx, "public", tbl); err != nil {
			t.Fatalf("ResetSequence(%s): %v", label, err)
		}
		newCol := &driver.Column{Name: "Note", DataType: "text", IsNullable: true}
		if err := w.AddColumn(ctx, tbl, newCol, "public"); err != nil {
			t.Fatalf("AddColumn(%s): %v", label, err)
		}
		if err := w.AddColumn(ctx, tbl, newCol, "public"); err != nil {
			t.Fatalf("AddColumn idempotent(%s): %v", label, err)
		}
		relax := &driver.Column{Name: "Name", DataType: "varchar", MaxLength: 80, IsNullable: true}
		if err := w.DropColumnNotNull(ctx, tbl, relax, "public"); err != nil {
			t.Fatalf("DropColumnNotNull(%s): %v", label, err)
		}
	}
	run(genW, "generic")
	run(refW, "postgres")

	got := dumpPG(t, "dmt_pgdiff_gen")
	want := dumpPG(t, "dmt_pgdiff_ref")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state diverges:\n  generic:  %v\n  postgres: %v", got, want)
	}
}

// dumpPG reads observable state: column shapes, sequence position, and
// full data.
func dumpPG(t *testing.T, dbName string) map[string]any {
	t.Helper()
	db, err := sql.Open("pgx", pgDSNBase+dbName+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state := map[string]any{}

	rows, err := db.Query(`SELECT column_name, udt_name,
			COALESCE(character_maximum_length, 0), COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0), is_nullable, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'itemevents'
		ORDER BY ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	var colShapes []string
	for rows.Next() {
		var name, udt, nullable, def string
		var maxLen, prec, scale int
		if err := rows.Scan(&name, &udt, &maxLen, &prec, &scale, &nullable, &def); err != nil {
			t.Fatal(err)
		}
		colShapes = append(colShapes, fmt.Sprintf("%s %s %d %d %d %s %s", name, udt, maxLen, prec, scale, nullable, def))
	}
	rows.Close()
	state["columns"] = colShapes

	// Both identity and serial columns back onto a sequence reachable
	// via pg_get_serial_sequence; its position proves ResetSequence ran.
	var lastVal sql.NullInt64
	_ = db.QueryRow(`SELECT last_value FROM pg_sequences
		WHERE schemaname = 'public'
		AND sequencename = replace(replace(pg_get_serial_sequence('public.itemevents','id'), 'public.', ''), '"', '')`).Scan(&lastVal)
	state["sequence_last_value"] = lastVal.Int64

	dataRows, err := db.Query("SELECT id, name, active, price FROM itemevents ORDER BY id")
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
