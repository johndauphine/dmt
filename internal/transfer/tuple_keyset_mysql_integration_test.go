package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlTupleSourcePool wraps a real MySQL connection behind the transfer
// test stub so executeCompositeKeysetPagination runs its actual generated
// SQL against a live server.
type mysqlTupleSourcePool struct{ keysetRuntimeSourcePool }

func (p *mysqlTupleSourcePool) DBType() string { return "mysql" }

// TestTupleKeysetMySQLCollation is the #629 collation regression test: a
// varchar PK under utf8mb4_0900_ai_ci, seeded with values whose byte order
// diverges from collation order ('B' < 'a' bytewise, 'a' < 'B' under ci),
// paged with chunk_size=1 so every boundary is a watermark comparison. A
// bytewise comparison would skip rows; the test asserts every row arrives
// exactly once in the column collation's order — including across a
// simulated crash-resume through the typed watermark codec.
//
// Requires the mysql test container (localhost:3306, root/TestPass2024).
// Skips when unreachable unless MYSQL_REQUIRED=1.
func TestTupleKeysetMySQLCollation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/?multiStatements=true")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MYSQL_REQUIRED") == "1" {
			t.Fatalf("mysql required but not reachable: %v", err)
		}
		t.Skipf("mysql not reachable: %v", err)
	}
	defer admin.Close()

	const dbName = "dmt_tuple_ci_test"
	if _, err := admin.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName) })

	// Same DSN params dmt's mysql catalog sets (parseTime/loc/charset).
	db, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName+
		"?parseTime=true&multiStatements=true&charset=utf8mb4&loc=UTC")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE ci (
		code VARCHAR(40) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY,
		val INT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// Byte order: 'Ab'(0x41) < 'B'(0x42) < 'a'(0x61) < 'z'(0x7a).
	// ci collation order: 'a' < 'Ab' < 'B' < 'z'.
	for i, code := range []string{"a", "B", "z", "Ab"} {
		if _, err := db.Exec("INSERT INTO ci VALUES (?, ?)", code, i); err != nil {
			t.Fatalf("insert %q: %v", code, err)
		}
	}
	wantOrder := []string{"a", "Ab", "B", "z"} // server collation order

	table := driver.Table{
		Name:   "ci",
		Schema: dbName,
		Columns: []driver.Column{
			{Name: "code", DataType: "varchar", IsNullable: false},
			{Name: "val", DataType: "int"},
		},
		PrimaryKey:       []string{"code"},
		RowCount:         4,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	if !driver.TupleKeysetRoutable(&table, "mysql") {
		t.Fatal("precondition: mysql varchar PK must be tuple-routable")
	}

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         1, // every row boundary is a watermark comparison
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	run := func(resumeTuple []any) []string {
		src := &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}
		tgt := &compositeTextTargetPool{}
		_, err := executeCompositeKeysetPagination(
			context.Background(), src, tgt, cfg, Job{Table: table},
			[]string{"code", "val"}, []string{"code", "val"},
			[]string{"varchar", "int"}, []int{0, 0},
			progress.New(), resumeTuple, 0, "ci", nil, nil,
		)
		if err != nil {
			t.Fatalf("executeCompositeKeysetPagination: %v", err)
		}
		return tgt.codes()
	}

	got := run(nil)
	if fmt.Sprint(got) != fmt.Sprint(wantOrder) {
		t.Fatalf("paged order %v, want collation order %v — a bytewise comparison skipped/reordered rows", got, wantOrder)
	}

	// Crash-resume through the typed codec from after 'a': remaining rows
	// must be exactly Ab, B, z (bytewise would return only z).
	wm := decodeCompositeTuple(encodeCompositeTuple([]any{"a"}))
	if len(wm) != 1 || wm[0] != "a" {
		t.Fatalf("watermark round-trip = %v", wm)
	}
	rest := run(wm)
	if fmt.Sprint(rest) != fmt.Sprint([]string{"Ab", "B", "z"}) {
		t.Fatalf("resume from 'a' returned %v, want [Ab B z] — collation-inconsistent watermark comparison", rest)
	}
}

// TestParallelTupleKeysetMySQLCollation runs #667's leading-integer range
// split against a mixed composite key. The text component deliberately uses a
// case-insensitive collation, so every page must preserve MySQL's own tuple
// comparison semantics while the reader workers split only tenant_id.
func TestParallelTupleKeysetMySQLCollation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/?multiStatements=true")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MYSQL_REQUIRED") == "1" {
			t.Fatalf("mysql required but not reachable: %v", err)
		}
		t.Skipf("mysql not reachable: %v", err)
	}
	defer admin.Close()

	const dbName = "dmt_tuple_parallel_ci_test"
	if _, err := admin.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName) })

	db, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName+
		"?parseTime=true&multiStatements=true&charset=utf8mb4&loc=UTC")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE tenant_names (
		tenant_id BIGINT NOT NULL,
		name VARCHAR(40) COLLATE utf8mb4_0900_ai_ci NOT NULL,
		val INT NOT NULL,
		PRIMARY KEY (tenant_id, name))`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for tenant := 1; tenant <= 32; tenant++ {
		for i, name := range []string{"a", "B", "z", "Ab"} {
			if _, err := db.Exec(`INSERT INTO tenant_names VALUES (?, ?, ?)`, tenant, name, i); err != nil {
				t.Fatalf("insert tenant=%d name=%q: %v", tenant, name, err)
			}
		}
	}

	table := driver.Table{
		Name:   "tenant_names",
		Schema: dbName,
		Columns: []driver.Column{
			{Name: "tenant_id", DataType: "bigint", IsNullable: false},
			{Name: "name", DataType: "varchar", IsNullable: false},
			{Name: "val", DataType: "int"},
		},
		PrimaryKey:       []string{"tenant_id", "name"},
		RowCount:         32 * 4,
		EstimatedRowSize: 48,
	}
	table.PopulatePKColumns()
	if !driver.TupleKeysetRoutable(&table, "mysql") {
		t.Fatal("precondition: MySQL mixed composite PK must be tuple-routable")
	}
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: 3, ParallelReaders: 4, WriteAheadWriters: 1, TargetMode: "drop_recreate",
	}}
	tgt := newCompositeAnyTargetPool()
	stats, used, err := executeParallelCompositeKeysetPagination(
		context.Background(), &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}, tgt, cfg, Job{Table: table},
		[]string{"tenant_id", "name", "val"}, []string{"tenant_id", "name", "val"}, []string{"bigint", "varchar", "int"}, []int{0, 0, 0},
		progress.New(), nil, 0, table.Name, nil, nil,
	)
	if err != nil || !used {
		t.Fatalf("parallel MySQL tuple path = (used=%v, err=%v), want success", used, err)
	}
	if stats.Rows != table.RowCount {
		t.Fatalf("rows = %d, want %d", stats.Rows, table.RowCount)
	}
	tgt.assertExact(t, int(table.RowCount))
}

// compositeTextTargetPool captures text PKs in write order.
type compositeTextTargetPool struct {
	keysetRuntimeTargetPool
	muT      sync.Mutex
	gotCodes []string
}

func (p *compositeTextTargetPool) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.muT.Lock()
	for _, row := range opts.Rows {
		switch v := row[0].(type) {
		case string:
			p.gotCodes = append(p.gotCodes, v)
		case []byte:
			p.gotCodes = append(p.gotCodes, string(v))
		}
	}
	p.muT.Unlock()
	return nil
}

func (p *compositeTextTargetPool) codes() []string {
	p.muT.Lock()
	defer p.muT.Unlock()
	return append([]string(nil), p.gotCodes...)
}
