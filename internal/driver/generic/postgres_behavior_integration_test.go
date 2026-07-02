package generic

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// Live postgres behavior tests (#509 cleanup). These replace the
// differential proof that compared the catalog engine against the
// hand-written driver: with the oracle removed, the expectations the
// differential established are pinned here as literals.
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
		PRIMARY KEY (order_no, region),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
		CONSTRAINT chk_region CHECK (region <> '')
	)`,
	`INSERT INTO users (name, balance) VALUES ('a', 1.50), ('b', 2.50), ('c', 0)`,
	`INSERT INTO orders (region, order_no, user_id) VALUES ('us', 1, 1), ('eu', 2, 2)`,
}

func TestPostgresCatalogReaderBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_pgbehav_src"
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
	r, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, "public")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "orders" || tables[1].Name != "users" {
		t.Fatalf("tables = %+v", tables)
	}
	users, orders := &tables[1], &tables[0]
	if !reflect.DeepEqual(users.PrimaryKey, []string{"id"}) {
		t.Errorf("users PK = %v", users.PrimaryKey)
	}
	// serial → nextval default → identity.
	if !users.Columns[0].IsIdentity {
		t.Errorf("id should be identity: %+v", users.Columns[0])
	}

	if err := r.LoadIndexes(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(users.Indexes) != 2 ||
		users.Indexes[0].Name != "idx_users_name" || users.Indexes[0].IsUnique ||
		users.Indexes[1].Name != "idx_users_name_created" || !users.Indexes[1].IsUnique ||
		!reflect.DeepEqual(users.Indexes[1].Columns, []string{"name", "created_at"}) {
		t.Errorf("indexes = %+v", users.Indexes)
	}

	// #518: pg-as-source FK/CHECK introspection. The hand-written
	// reader never implemented these (no-op stubs); the catalog fixes
	// that, so pg sources now round-trip constraints.
	if err := r.LoadForeignKeys(ctx, orders); err != nil {
		t.Fatal(err)
	}
	if err := r.LoadCheckConstraints(ctx, orders); err != nil {
		t.Fatal(err)
	}
	if len(orders.ForeignKeys) != 1 {
		t.Fatalf("orders FKs: %+v", orders.ForeignKeys)
	}
	fk := orders.ForeignKeys[0]
	if fk.Name != "fk_orders_user" || fk.RefTable != "users" || fk.RefSchema != "public" ||
		!reflect.DeepEqual(fk.Columns, []string{"user_id"}) ||
		!reflect.DeepEqual(fk.RefColumns, []string{"id"}) ||
		fk.OnDelete != "CASCADE" || fk.OnUpdate != "NO ACTION" {
		t.Errorf("fk_orders_user wrong: %+v", fk)
	}
	if len(orders.CheckConstraints) != 1 || orders.CheckConstraints[0].Name != "chk_region" {
		t.Fatalf("orders checks: %+v", orders.CheckConstraints)
	}
	// Bare expression — the finalization DDL generator adds the
	// CHECK (...) wrapper, so a leading CHECK here would double it.
	def := orders.CheckConstraints[0].Definition
	if !strings.Contains(def, "region") || strings.Contains(def, "CHECK") {
		t.Errorf("chk_region definition = %q, want bare expression", def)
	}

	n, err := r.GetRowCountExact(ctx, "public", "users", false)
	if err != nil || n != 3 {
		t.Errorf("count = %d (%v), want 3", n, err)
	}

	parts, err := r.GetPartitionBoundaries(ctx, users, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || !parts[0].IsFirstPartition || parts[1].IsFirstPartition {
		t.Errorf("partitions: %+v", parts)
	}

	rd := r.(driver.IncrementalDateReader)
	col, typ, found := rd.GetDateColumnInfo(ctx, "public", "users", []string{"missing", "created_at"})
	if col != "created_at" || typ != "timestamp" || !found {
		t.Errorf("date info: (%q,%q,%v)", col, typ, found)
	}
}

// TestPostgresCatalogDetectsGeneratedIdentity pins #546: columns declared
// GENERATED ALWAYS/BY DEFAULT AS IDENTITY (PG 10+, NULL default) must be
// flagged IsIdentity, not only legacy serial/nextval columns — otherwise the
// target CREATE TABLE silently drops auto-generation and TaskResetSequences
// skips them.
func TestPostgresCatalogDetectsGeneratedIdentity(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_pgbehav_identity"
	pgBootstrap(t, dbName)
	pgExecAll(t, dbName, []string{
		`CREATE TABLE idcols (
			always_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			bydefault_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
			serial_id SERIAL,
			plain_col INT,
			computed_col INT GENERATED ALWAYS AS (plain_col + 1) STORED
		)`,
	})

	cfg := &dbconfig.SourceConfig{
		Type: "postgres", Host: "localhost", Port: 5432,
		Database: dbName, User: "postgres", Password: "TestPass2024",
		Schema: "public", SSLMode: "disable",
	}
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, "public")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "idcols" {
		t.Fatalf("tables = %+v", tables)
	}

	identity := map[string]bool{}
	for _, c := range tables[0].Columns {
		identity[c.Name] = c.IsIdentity
	}
	// GENERATED ALWAYS / BY DEFAULT AS IDENTITY and legacy serial are all identity.
	for _, col := range []string{"always_id", "bydefault_id", "serial_id"} {
		if !identity[col] {
			t.Errorf("%s should be IsIdentity=true, got false (columns=%+v)", col, tables[0].Columns)
		}
	}
	// A plain column and a computed (GENERATED ALWAYS AS (expr) STORED) column
	// are NOT identity — the OR clause must not mis-flag computed columns
	// (is_identity='NO' there; the value column is is_generated='ALWAYS').
	for _, col := range []string{"plain_col", "computed_col"} {
		if identity[col] {
			t.Errorf("%s should be IsIdentity=false", col)
		}
	}
}

// Writer behavior: the op sequence the differential ran (including the
// mixed-case MSSQL-shaped names that exercise identifier_sanitizer),
// with the observable state asserted directly.
func TestPostgresCatalogWriterBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_pgbehav_tgt"
	pgBootstrap(t, dbName)

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(cat, &dbconfig.TargetConfig{
		Type: "postgres", Host: "localhost", Port: 5432,
		Database: dbName, User: "postgres", Password: "TestPass2024",
		Schema: "public", SSLMode: "disable", ChunkSize: 1000,
	}, 4, driver.WriterOptions{BatchSize: 1000, SourceType: "postgres", TypeMapper: tm})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

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

	if err := w.CreateTable(ctx, tbl, "public"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// drop_recreate rerun: DropTable must hit the sanitized name or
	// the second CreateTable fails with "already exists".
	if err := w.DropTable(ctx, "public", tbl.Name); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if err := w.CreateTable(ctx, tbl, "public"); err != nil {
		t.Fatalf("CreateTable after drop: %v", err)
	}
	// Orchestrator calls CreatePrimaryKey after transfer — must no-op.
	if err := w.CreatePrimaryKey(ctx, tbl, "public"); err != nil {
		t.Fatalf("CreatePrimaryKey: %v", err)
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "public", Table: "ItemEvents", Columns: cols, Rows: rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	// #227 idempotent replay is a no-op.
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "public", Table: "ItemEvents", Columns: cols, Rows: rows,
		IdempotentOnDup: true, PKColumns: []string{"Id"},
	}); err != nil {
		t.Fatalf("idempotent WriteBatch: %v", err)
	}
	if err := w.(driver.Upserter).UpsertBatch(ctx, driver.UpsertBatchOptions{
		Schema: "public", Table: "ItemEvents", Columns: cols, PKColumns: []string{"Id"},
		Rows: [][]any{
			{int64(2), "b-updated", true, 9.99},
			{int64(4), "d", false, 4.00},
		},
	}); err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}
	if err := w.(driver.SequenceResetter).ResetSequence(ctx, "public", tbl); err != nil {
		t.Fatalf("ResetSequence: %v", err)
	}
	newCol := &driver.Column{Name: "Note", DataType: "text", IsNullable: true}
	if err := w.AddColumn(ctx, tbl, newCol, "public"); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if err := w.AddColumn(ctx, tbl, newCol, "public"); err != nil {
		t.Fatalf("AddColumn idempotent: %v", err)
	}
	relax := &driver.Column{Name: "Name", DataType: "varchar", MaxLength: 80, IsNullable: true}
	if err := w.DropColumnNotNull(ctx, tbl, relax, "public"); err != nil {
		t.Fatalf("DropColumnNotNull: %v", err)
	}

	db, err := sql.Open("pgx", pgDSNBase+dbName+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var data []string
	dataRows, err := db.Query("SELECT id, name, active, price FROM itemevents ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
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
	want := []string{"1|a|true|1.50", "2|b-updated|true|9.99", "3|c||", "4|d|false|4.00"}
	if !reflect.DeepEqual(data, want) {
		t.Errorf("data = %v, want %v", data, want)
	}

	// ResetSequence positioned the backing sequence at MAX(id).
	var lastVal sql.NullInt64
	_ = db.QueryRow(`SELECT last_value FROM pg_sequences
		WHERE schemaname = 'public'
		AND sequencename = replace(replace(pg_get_serial_sequence('public.itemevents','id'), 'public.', ''), '"', '')`).Scan(&lastVal)
	if lastVal.Int64 != 4 {
		t.Errorf("sequence last_value = %d, want 4", lastVal.Int64)
	}

	var nameNullable string
	if err := db.QueryRow(`SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'itemevents' AND column_name = 'name'`).Scan(&nameNullable); err != nil {
		t.Fatal(err)
	}
	if nameNullable != "YES" {
		t.Errorf("name nullable = %q after DropColumnNotNull, want YES", nameNullable)
	}
	var noteType string
	if err := db.QueryRow(`SELECT udt_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'itemevents' AND column_name = 'note'`).Scan(&noteType); err != nil {
		t.Fatal(err)
	}
	if noteType != "text" {
		t.Errorf("note type = %q, want text", noteType)
	}
}
