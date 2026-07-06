package transfer

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

	_ "modernc.org/sqlite"
)

// TestCompositeIntegerPKClassification pins which tables take the tuple
// keyset path (#616): multi-column all-integer PKs only.
func TestCompositeIntegerPKClassification(t *testing.T) {
	mk := func(pk []string, types ...string) *driver.Table {
		cols := make([]driver.Column, len(types))
		for i := range types {
			cols[i] = driver.Column{Name: pk[i], DataType: types[i]}
		}
		tbl := &driver.Table{Columns: cols, PrimaryKey: pk}
		tbl.PopulatePKColumns()
		return tbl
	}
	if !mk([]string{"a", "b"}, "int", "bigint").CompositeIntegerPK() {
		t.Error("(int,bigint) composite PK should be CompositeIntegerPK")
	}
	if mk([]string{"a", "b"}, "int", "varchar").CompositeIntegerPK() {
		t.Error("(int,varchar) composite PK must NOT be tuple-keyset (has non-integer component)")
	}
	if mk([]string{"a"}, "int").CompositeIntegerPK() {
		t.Error("single-column PK is not composite")
	}
	// Unsigned BIGINT (MySQL) can exceed MaxInt64 and can't bind as a param.
	unsigned := mk([]string{"a", "b"}, "bigint", "bigint")
	unsigned.PKColumns[1].FullDataType = "bigint unsigned"
	if unsigned.CompositeIntegerPK() {
		t.Error("composite PK with an unsigned BIGINT component must stay on ROW_NUMBER")
	}
	// INT UNSIGNED (<= 2^32) fits int64 and stays eligible.
	uint32PK := mk([]string{"a", "b"}, "int", "int")
	uint32PK.PKColumns[1].FullDataType = "int unsigned"
	if !uint32PK.CompositeIntegerPK() {
		t.Error("composite PK with INT UNSIGNED components is safe and should use tuple keyset")
	}
	// Nullable component (SQLite composite PK without NOT NULL).
	nullable := mk([]string{"a", "b"}, "int", "int")
	nullable.PKColumns[0].IsNullable = true
	if nullable.CompositeIntegerPK() {
		t.Error("composite PK with a nullable component must stay on ROW_NUMBER")
	}
	if !mk([]string{"a", "b", "c"}, "int", "smallint", "bigint").CompositeIntegerPK() {
		t.Error("3-column all-integer PK should be CompositeIntegerPK")
	}
}

// TestCompositeTuplePreservesBigInt verifies the checkpoint tuple round-trips
// through JSON without losing int64 precision past float64's exact range
// (#616) — a naive unmarshal would round a BIGINT watermark and shift resume.
func TestCompositeTuplePreservesBigInt(t *testing.T) {
	const big = int64(1) << 60 // beyond float64 exact-integer range
	tuple := []any{big + 1, int64(7)}
	got := decodeCompositeTuple(encodeCompositeTuple(tuple))
	if len(got) != 2 {
		t.Fatalf("decoded %d elements, want 2", len(got))
	}
	if got[0] != big+1 {
		t.Fatalf("first component = %v (%T), want %d (int64) — BIGINT precision lost", got[0], got[0], big+1)
	}
	if got[1] != int64(7) {
		t.Fatalf("second component = %v, want int64(7)", got[1])
	}
}

func TestCompositeTupleEmptyAndMalformed(t *testing.T) {
	if encodeCompositeTuple(nil) != "" {
		t.Error("encode(nil) should be empty")
	}
	if decodeCompositeTuple("") != nil {
		t.Error("decode(\"\") should be nil")
	}
	if decodeCompositeTuple("not json") != nil {
		t.Error("decode(malformed) should be nil")
	}
}

// TestCompositeKeysetTransfersAllRows drives executeCompositeKeysetPagination
// directly on a sqlite composite table (with non-null PK metadata, since
// sqlite introspection alone reports composite PK columns nullable) and
// asserts every row — including the smallest tuple, via the unbounded first
// chunk — is transferred exactly once (#616).
func TestCompositeKeysetTransfersAllRows(t *testing.T) {
	db := seedCompositeKeysetDB(t, 30, 20) // 30 orders × 20 lines = 600 rows
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}

	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"order_id", "line_no"},
		RowCount:         600,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	if !table.CompositeIntegerPK() {
		t.Fatal("precondition: non-null all-integer composite PK must be tuple-keyset eligible")
	}

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         70, // forces many chunks + a boundary mid-order
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	stats, err := executeCompositeKeysetPagination(
		context.Background(), srcPool, tgtPool, cfg, Job{Table: table},
		[]string{"order_id", "line_no", "qty"}, []string{"order_id", "line_no", "qty"},
		[]string{"integer", "integer", "integer"}, []int{0, 0, 0},
		progress.New(), nil, 0, "lines", nil, nil,
	)
	if err != nil {
		t.Fatalf("executeCompositeKeysetPagination: %v", err)
	}
	if stats.Rows != 600 {
		t.Fatalf("stats.Rows = %d, want 600", stats.Rows)
	}
	keys := tgtPool.keys()
	if len(keys) != 600 {
		t.Fatalf("wrote %d rows, want 600", len(keys))
	}
	// Exactly-once + smallest tuple present.
	seen := make(map[[2]int64]bool, 600)
	var hasMin bool
	for _, k := range keys {
		if seen[k] {
			t.Fatalf("duplicate tuple %v — keyset skipped or re-read", k)
		}
		seen[k] = true
		if k == [2]int64{1, 1} {
			hasMin = true
		}
	}
	if !hasMin {
		t.Fatal("smallest tuple (1,1) missing — unbounded first chunk dropped it")
	}
}

func seedCompositeKeysetDB(t *testing.T, orders, linesPer int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "ck.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE lines (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, qty INTEGER, PRIMARY KEY(order_id, line_no))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO lines VALUES (?,?,?)`)
	for o := 1; o <= orders; o++ {
		for l := 1; l <= linesPer; l++ {
			if _, err := stmt.Exec(o, l, o*1000+l); err != nil {
				t.Fatalf("insert (%d,%d): %v", o, l, err)
			}
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return db
}

type compositeTargetPool struct {
	keysetRuntimeTargetPool
	mu3     sync.Mutex
	gotKeys [][2]int64
}

func (p *compositeTargetPool) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu3.Lock()
	for _, row := range opts.Rows {
		a, _ := keysetRuntimeInt(row[0])
		b, _ := keysetRuntimeInt(row[1])
		p.gotKeys = append(p.gotKeys, [2]int64{int64(a), int64(b)})
	}
	p.mu3.Unlock()
	return nil
}

func (p *compositeTargetPool) keys() [][2]int64 {
	p.mu3.Lock()
	defer p.mu3.Unlock()
	return append([][2]int64(nil), p.gotKeys...)
}
