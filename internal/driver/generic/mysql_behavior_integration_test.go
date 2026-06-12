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
)

// Live mysql behavior tests (#509 cleanup). These replace the
// differential proof that compared the catalog engine against the
// hand-written driver: with the oracle removed, the expectations the
// differential established are pinned here as literals.
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

func TestMySQLCatalogReaderBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_behav_src"
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
	r, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, dbName)
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "orders" || tables[1].Name != "users" {
		t.Fatalf("tables = %v", tables)
	}
	users, orders := &tables[1], &tables[0]

	if !reflect.DeepEqual(users.PrimaryKey, []string{"id"}) || !users.Columns[0].IsIdentity {
		t.Errorf("users PK/identity wrong: %+v", users)
	}
	if !reflect.DeepEqual(orders.PrimaryKey, []string{"order_no", "region"}) {
		t.Errorf("orders composite PK wrong: %v", orders.PrimaryKey)
	}

	for _, tbl := range tables {
		tt := tbl
		for name, fn := range map[string]func(context.Context, *driver.Table) error{
			"indexes": r.LoadIndexes, "fks": r.LoadForeignKeys, "checks": r.LoadCheckConstraints,
		} {
			if err := fn(ctx, &tt); err != nil {
				t.Fatalf("%s(%s): %v", name, tt.Name, err)
			}
		}
		switch tt.Name {
		case "users":
			if len(tt.Indexes) != 2 {
				t.Fatalf("users indexes: %+v", tt.Indexes)
			}
			if tt.Indexes[0].Name != "idx_users_name" || tt.Indexes[0].IsUnique {
				t.Errorf("idx_users_name wrong: %+v", tt.Indexes[0])
			}
			if tt.Indexes[1].Name != "idx_users_name_created" || !tt.Indexes[1].IsUnique ||
				!reflect.DeepEqual(tt.Indexes[1].Columns, []string{"name", "created_at"}) {
				t.Errorf("idx_users_name_created wrong: %+v", tt.Indexes[1])
			}
		case "orders":
			if len(tt.ForeignKeys) != 1 {
				t.Fatalf("orders FKs: %+v", tt.ForeignKeys)
			}
			fk := tt.ForeignKeys[0]
			if fk.Name != "fk_orders_user" || fk.RefTable != "users" ||
				!reflect.DeepEqual(fk.Columns, []string{"user_id"}) ||
				!reflect.DeepEqual(fk.RefColumns, []string{"id"}) ||
				fk.OnDelete != "CASCADE" || fk.OnUpdate != "SET NULL" {
				t.Errorf("fk_orders_user wrong: %+v", fk)
			}
			found := false
			for _, c := range tt.CheckConstraints {
				if c.Name == "chk_region" {
					found = true
				}
			}
			if !found {
				t.Errorf("chk_region missing: %+v", tt.CheckConstraints)
			}
		}
	}

	t.Run("counts and partitions", func(t *testing.T) {
		n, err := r.GetRowCountExact(ctx, dbName, "users", false)
		if err != nil || n != 3 {
			t.Errorf("users count = %d (%v), want 3", n, err)
		}
		parts, err := r.GetPartitionBoundaries(ctx, users, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != 2 || !parts[0].IsFirstPartition || parts[1].IsFirstPartition {
			t.Errorf("partitions: %+v", parts)
		}
		if parts[0].MinPK != int64(1) || parts[1].MaxPK != int64(3) {
			t.Errorf("partition bounds: %+v", parts)
		}
	})

	t.Run("incremental dates", func(t *testing.T) {
		rd := r.(driver.IncrementalDateReader)
		col, typ, found := rd.GetDateColumnInfo(ctx, dbName, "users", []string{"missing", "created_at"})
		if col != "created_at" || typ != "datetime" || !found {
			t.Errorf("date info: (%q,%q,%v)", col, typ, found)
		}
	})
}

// Writer behavior: the op sequence the differential ran, with the
// observable state asserted directly.
func TestMySQLCatalogWriterBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_behav_tgt"
	mysqlBootstrap(t, dbName)

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(cat, &dbconfig.TargetConfig{
		Type: "mysql", Host: "localhost", Port: 3306,
		Database: dbName, User: "root", Password: "TestPass2024",
		SSLMode: "disable", ChunkSize: 1000,
	}, 4, driver.WriterOptions{BatchSize: 1000, SourceType: "mysql", TypeMapper: tm})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

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

	if err := w.CreateTable(ctx, tbl, dbName); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: dbName, Table: "items", Columns: cols, Rows: rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	// #227 idempotent replay is a no-op.
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: dbName, Table: "items", Columns: cols, Rows: rows,
		IdempotentOnDup: true, PKColumns: []string{"id"},
	}); err != nil {
		t.Fatalf("idempotent WriteBatch: %v", err)
	}
	if err := w.(driver.Upserter).UpsertBatch(ctx, driver.UpsertBatchOptions{
		Schema: dbName, Table: "items", Columns: cols, PKColumns: []string{"id"},
		Rows: [][]any{
			{int64(2), "b-updated", true, 9.99},
			{int64(4), "d", false, 4.00},
		},
	}); err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}
	if err := w.(driver.SequenceResetter).ResetSequence(ctx, dbName, tbl); err != nil {
		t.Fatalf("ResetSequence: %v", err)
	}
	newCol := &driver.Column{Name: "note", DataType: "text", IsNullable: true}
	if err := w.AddColumn(ctx, tbl, newCol, dbName); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if err := w.AddColumn(ctx, tbl, newCol, dbName); err != nil {
		t.Fatalf("AddColumn idempotent: %v", err)
	}
	relax := &driver.Column{Name: "name", DataType: "varchar", MaxLength: 80, IsNullable: true}
	if err := w.DropColumnNotNull(ctx, tbl, relax, dbName); err != nil {
		t.Fatalf("DropColumnNotNull: %v", err)
	}

	db, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var data []string
	dataRows, err := db.Query("SELECT id, name, active, price FROM items ORDER BY id")
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
	want := []string{"1|a|1|1.50", "2|b-updated|1|9.99", "3|c||", "4|d|0|4.00"}
	if !reflect.DeepEqual(data, want) {
		t.Errorf("data = %v, want %v", data, want)
	}

	var autoInc sql.NullInt64
	_ = db.QueryRow(`SELECT AUTO_INCREMENT FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items'`, dbName).Scan(&autoInc)
	if autoInc.Int64 != 5 {
		t.Errorf("auto_increment = %d, want 5 (MAX(id)+1 after ResetSequence)", autoInc.Int64)
	}

	var nameNullable, noteType string
	if err := db.QueryRow(`SELECT IS_NULLABLE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items' AND COLUMN_NAME = 'name'`, dbName).Scan(&nameNullable); err != nil {
		t.Fatal(err)
	}
	if nameNullable != "YES" {
		t.Errorf("name nullable = %q after DropColumnNotNull, want YES", nameNullable)
	}
	if err := db.QueryRow(`SELECT DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items' AND COLUMN_NAME = 'note'`, dbName).Scan(&noteType); err != nil {
		t.Fatal(err)
	}
	if noteType != "text" {
		t.Errorf("note type = %q, want text", noteType)
	}
}
