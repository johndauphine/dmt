package transfer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

	_ "modernc.org/sqlite"
)

// TestTupleKeysetEligibleClassification pins which tables take the tuple
// keyset path (#616/#629): composite PKs and single-column PKs whose every
// component is a safe type on the given source engine and that are not already
// owned by the legacy parallel keyset path.
func TestTupleKeysetEligibleClassification(t *testing.T) {
	mk := func(pk []string, types ...string) *driver.Table {
		cols := make([]driver.Column, len(types))
		for i := range types {
			cols[i] = driver.Column{Name: pk[i], DataType: types[i]}
		}
		tbl := &driver.Table{Columns: cols, PrimaryKey: pk}
		tbl.PopulatePKColumns()
		return tbl
	}

	// Composite integer keys (the #628 case) remain eligible.
	if !mk([]string{"a", "b"}, "int", "bigint").TupleKeysetEligible("postgres") {
		t.Error("(int,bigint) composite PK should be tuple-eligible")
	}
	if !mk([]string{"a", "b", "c"}, "int", "smallint", "bigint").TupleKeysetEligible("mysql") {
		t.Error("3-column all-integer PK should be tuple-eligible")
	}
	// Mixed integer+text composite keys are now eligible (#629).
	if !mk([]string{"a", "b"}, "int", "varchar").TupleKeysetEligible("mysql") {
		t.Error("(int,varchar) composite PK should be tuple-eligible on mysql")
	}
	// Single-column non-integer PKs are eligible as 1-tuples (#629).
	for _, dt := range []string{"varchar", "text", "uuid", "numeric", "decimal"} {
		if !mk([]string{"k"}, dt).TupleKeysetEligible("postgres") {
			t.Errorf("single %s PK should be tuple-eligible on postgres", dt)
		}
	}
	for _, dt := range []string{"date", "timestamptz", "timestamp", "timestamp without time zone", "timestamp with time zone"} {
		if mk([]string{"k"}, dt).TupleKeysetEligible("postgres") {
			t.Errorf("single %s PK must stay on ROW_NUMBER", dt)
		}
	}
	// Single-column INTEGER PKs already owned by the parallel keyset path
	// stay there.
	for _, dt := range []string{"int", "bigint", "integer", "int8"} {
		if mk([]string{"k"}, dt).TupleKeysetEligible("mysql") {
			t.Errorf("single %s PK must stay on the parallel integer keyset path", dt)
		}
	}
	// mediumint must not be added to SupportsKeysetPagination because that
	// would reinterpret old ROW_NUMBER checkpoints as PK watermarks, but it
	// can route through the new tuple path whose resume guard safely replays.
	if !mk([]string{"k"}, "mediumint").TupleKeysetEligible("mysql") {
		t.Error("single mediumint PK should be tuple-eligible, not legacy-keyset eligible")
	}
	// mssql text: only Unicode types are safe (varchar binds as nvarchar and
	// legacy SQL_* collations sort non-Unicode vs Unicode differently).
	if mk([]string{"k"}, "varchar").TupleKeysetEligible("mssql") {
		t.Error("mssql single varchar PK must stay on ROW_NUMBER (SQL_* collation hazard)")
	}
	if !mk([]string{"k"}, "nvarchar").TupleKeysetEligible("mssql") {
		t.Error("mssql single nvarchar PK should be tuple-eligible")
	}
	if mk([]string{"a", "b"}, "int", "varchar").TupleKeysetEligible("mssql") {
		t.Error("mssql (int,varchar) composite must stay on ROW_NUMBER")
	}
	if !mk([]string{"a", "b"}, "int", "nvarchar").TupleKeysetEligible("mssql") {
		t.Error("mssql (int,nvarchar) composite should be tuple-eligible")
	}
	// Converter-covered / order-hazard types are excluded everywhere.
	for _, dt := range []string{"uniqueidentifier", "datetime2", "datetime", "smalldatetime", "datetimeoffset", "float", "real", "varbinary", "bit", "time"} {
		if mk([]string{"a", "b"}, "int", dt).TupleKeysetEligible("mssql") {
			t.Errorf("(int,%s) composite must NOT be tuple-eligible", dt)
		}
	}
	// Unsigned BIGINT (MySQL) can exceed MaxInt64 and cannot bind as a param.
	unsigned := mk([]string{"a", "b"}, "bigint", "bigint")
	unsigned.PKColumns[1].FullDataType = "bigint unsigned"
	if unsigned.TupleKeysetEligible("mysql") {
		t.Error("composite PK with an unsigned BIGINT component must stay on ROW_NUMBER")
	}
	// INT UNSIGNED (<= 2^32) fits int64 and stays eligible.
	uint32PK := mk([]string{"a", "b"}, "int", "int")
	uint32PK.PKColumns[1].FullDataType = "int unsigned"
	if !uint32PK.TupleKeysetEligible("mysql") {
		t.Error("composite PK with INT UNSIGNED components is safe and should use tuple keyset")
	}
	// Nullable component (SQLite composite PK without NOT NULL).
	nullable := mk([]string{"a", "b"}, "int", "int")
	nullable.PKColumns[0].IsNullable = true
	if nullable.TupleKeysetEligible("sqlite") {
		t.Error("composite PK with a nullable component must stay on ROW_NUMBER")
	}
	nullText := mk([]string{"k"}, "text")
	nullText.PKColumns[0].IsNullable = true
	if nullText.TupleKeysetEligible("sqlite") {
		t.Error("nullable single text PK (sqlite default) must stay on ROW_NUMBER")
	}
	// No PK at all.
	if (&driver.Table{Columns: []driver.Column{{Name: "x", DataType: "int"}}}).TupleKeysetEligible("postgres") {
		t.Error("PK-less table must not be tuple-eligible")
	}
}

// TestConvertersTouchPK pins the runtime safety gate (#629): a value
// converter on any PK column forces the ROW_NUMBER fallback, because the
// watermark is extracted post-conversion and may no longer match the source.
func TestConvertersTouchPK(t *testing.T) {
	d := driver.GetDialect("sqlite")
	if d == nil {
		t.Fatal("sqlite dialect not registered")
	}
	cols := []string{"id", "when"}
	// sqlite "datetime" gets a DefaultValueConverter; "integer"/"text" do not.
	if convertersTouchPK(d, cols, []string{"integer", "datetime"}, "sqlite", []string{"id"}) {
		t.Error("integer PK with converter only on a non-PK column must pass the gate")
	}
	if !convertersTouchPK(d, cols, []string{"integer", "datetime"}, "sqlite", []string{"when"}) {
		t.Error("datetime PK (converter-covered) must trip the gate")
	}
	if !convertersTouchPK(nil, cols, []string{"integer", "text"}, "sqlite", []string{"id"}) {
		t.Error("nil dialect must fail safe (gate trips)")
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
	if !table.TupleKeysetEligible("sqlite") {
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

func TestCompositeParallelKeysetKeepsIneligiblePathsSingleReader(t *testing.T) {
	db := seedCompositeKeysetDB(t, 2, 2)
	if _, err := db.Exec(`CREATE TABLE text_lines (order_id TEXT NOT NULL, line_no INTEGER NOT NULL, PRIMARY KEY(order_id, line_no))`); err != nil {
		t.Fatalf("create text_lines: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO text_lines VALUES ('a', 1), ('b', 1)`); err != nil {
		t.Fatalf("insert text_lines: %v", err)
	}
	srcPool := &keysetRuntimeSourcePool{db: db}
	table := driver.Table{
		Name: "text_lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "text", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
		},
		PrimaryKey: []string{"order_id", "line_no"},
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Migration: config.MigrationConfig{ChunkSize: 2, ParallelReaders: 4}}
	_, used, err := executeParallelCompositeKeysetPagination(context.Background(), srcPool, &compositeTargetPool{}, cfg, Job{Table: table}, []string{"order_id", "line_no"}, []string{"order_id", "line_no"}, []string{"text", "integer"}, []int{0, 0}, progress.New(), nil, 0, "lines", nil, nil)
	if err != nil {
		t.Fatalf("ineligible leading component returned error: %v", err)
	}
	if used {
		t.Fatal("text leading component must keep the single-reader tuple path")
	}
	// Digit-only text values must be just as ineligible: their source
	// collation order is not the numeric order used for range splitting.
	if _, err := db.Exec(`CREATE TABLE numeric_text_lines (order_id TEXT NOT NULL, line_no INTEGER NOT NULL, PRIMARY KEY(order_id, line_no))`); err != nil {
		t.Fatalf("create numeric_text_lines: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO numeric_text_lines VALUES ('2', 1), ('10', 1)`); err != nil {
		t.Fatalf("insert numeric_text_lines: %v", err)
	}
	table.Name = "numeric_text_lines"
	_, used, err = executeParallelCompositeKeysetPagination(context.Background(), srcPool, &compositeTargetPool{}, cfg, Job{Table: table}, []string{"order_id", "line_no"}, []string{"order_id", "line_no"}, []string{"text", "integer"}, []int{0, 0}, progress.New(), nil, 0, "lines", nil, nil)
	if err != nil || used {
		t.Fatalf("numeric-text leading component = (used=%v, err=%v), want single-reader fallback", used, err)
	}
	// Strict mode remains explicitly on today's snapshot-safe path even when
	// the leading component itself is numeric and otherwise eligible.
	strictTable := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey: []string{"order_id", "line_no"},
	}
	strictTable.PopulatePKColumns()
	cfg.Migration.StrictConsistency = true
	_, used, err = executeParallelCompositeKeysetPagination(context.Background(), srcPool, &compositeTargetPool{}, cfg, Job{Table: strictTable}, []string{"order_id", "line_no", "qty"}, []string{"order_id", "line_no", "qty"}, []string{"integer", "integer", "integer"}, []int{0, 0, 0}, progress.New(), nil, 0, "lines", nil, nil)
	if err != nil || used {
		t.Fatalf("strict tuple path = (used=%v, err=%v), want single-reader fallback", used, err)
	}
}

func TestCompositeParallelRangeResumeRestoresTupleWatermarks(t *testing.T) {
	const (
		orders   = 8
		perOrder = 3
	)
	db := seedCompositeKeysetDB(t, orders, perOrder)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	// These are exactly the tuples covered by the two range checkpoints. The
	// resumed ranges must fill only the suffixes and produce the full key set.
	for order := int64(1); order <= 2; order++ {
		for line := int64(1); line <= perOrder; line++ {
			tgtPool.gotKeys = append(tgtPool.gotKeys, [2]int64{order, line})
		}
	}
	for order := int64(5); order <= 6; order++ {
		for line := int64(1); line <= perOrder; line++ {
			tgtPool.gotKeys = append(tgtPool.gotKeys, [2]int64{order, line})
		}
	}

	rangeState := encodeCompositeRangeState([]compositeResumeRange{
		{min: 1, max: 4, minInclusive: true, tuple: []any{int64(2), int64(3)}},
		{min: 4, max: 8, minInclusive: false, tuple: []any{int64(6), int64(3)}},
	})
	saver := &foreignTupleProgressSaver{
		resumeLastPK:   []any{int64(2), int64(3)},
		resumeRowsDone: 12,
		rangeState:     rangeState,
	}
	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"order_id", "line_no"},
		RowCount:         orders * perOrder,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: 2, ParallelReaders: 2, WriteAheadWriters: 1, TargetMode: "drop_recreate", CheckpointFrequency: 1,
	}}

	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table, TaskID: 667, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute resume: %v", err)
	}
	if stats.Rows != orders*perOrder {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, orders*perOrder)
	}
	keys := tgtPool.keys()
	if len(keys) != orders*perOrder {
		t.Fatalf("final key count = %d, want %d", len(keys), orders*perOrder)
	}
	seen := make(map[[2]int64]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("resume wrote duplicate tuple %v", key)
		}
		seen[key] = struct{}{}
	}
	last, ok := saver.last()
	if !ok {
		t.Fatal("expected final range checkpoint")
	}
	finalRanges := decodeCompositeRangeState(last.rangeState)
	if len(finalRanges) != 2 || !finalRanges[0].complete || !finalRanges[1].complete {
		t.Fatalf("final range state = %q, want two completed ranges", last.rangeState)
	}
}

// A #667 range envelope is sufficient resume evidence even when a foreign
// saver did not retain legacy last_pk. Execute must preserve target rows in
// completed ranges before the producer skips them; truncating first would
// silently lose their keys.
func TestCompositeParallelRangeResumeWithoutLegacyLastPKPreservesCompletedRanges(t *testing.T) {
	const (
		orders   = 8
		perOrder = 3
	)
	db := seedCompositeKeysetDB(t, orders, perOrder)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	for order := int64(1); order <= 4; order++ {
		for line := int64(1); line <= perOrder; line++ {
			tgtPool.gotKeys = append(tgtPool.gotKeys, [2]int64{order, line})
		}
	}
	saver := &foreignTupleProgressSaver{
		resumeRowsDone: 4 * perOrder,
		rangeState: encodeCompositeRangeState([]compositeResumeRange{
			{min: 1, max: 4, minInclusive: true, tuple: []any{int64(4), int64(3)}, complete: true},
			{min: 4, max: 8, minInclusive: false},
		}),
	}
	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"order_id", "line_no"},
		RowCount:         orders * perOrder,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: 2, ParallelReaders: 2, WriteAheadWriters: 1, TargetMode: "drop_recreate", CheckpointFrequency: 1,
	}}

	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table, TaskID: 667, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute range-only resume: %v", err)
	}
	if stats.Rows != orders*perOrder {
		t.Fatalf("rows = %d, want %d", stats.Rows, orders*perOrder)
	}
	if tgtPool.truncates != 0 {
		t.Fatalf("range-only resume truncated target %d time(s)", tgtPool.truncates)
	}
	keys := tgtPool.keys()
	if len(keys) != orders*perOrder {
		t.Fatalf("final key count = %d, want %d", len(keys), orders*perOrder)
	}
	seen := make(map[[2]int64]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate tuple %v", key)
		}
		seen[key] = struct{}{}
	}
}

// An old tuple checkpoint carries one watermark, not #667's range envelope.
// The new binary deliberately retains the single-reader resume in that case:
// it continues at the old tuple instead of range-splitting from the table
// minimum and replaying the prefix.
func TestCompositeParallelKeysetRetainsLegacyTupleResume(t *testing.T) {
	db := seedCompositeKeysetDB(t, 6, 2)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	for order := int64(1); order <= 2; order++ {
		for line := int64(1); line <= 2; line++ {
			tgtPool.gotKeys = append(tgtPool.gotKeys, [2]int64{order, line})
		}
	}
	legacyTuple := []any{int64(2), int64(2)}
	saver := &foreignTupleProgressSaver{
		resumeLastPK:   legacyTuple,
		resumeRowsDone: 4,
		rangeState:     encodeCompositeTuple(legacyTuple),
	}
	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"order_id", "line_no"},
		RowCount:         12,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: 2, ParallelReaders: 4, WriteAheadWriters: 1, TargetMode: "drop_recreate", CheckpointFrequency: 1,
	}}

	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table, TaskID: 667, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute legacy tuple resume: %v", err)
	}
	if stats.Rows != 12 {
		t.Fatalf("rows = %d, want 12", stats.Rows)
	}
	keys := tgtPool.keys()
	if len(keys) != 12 {
		t.Fatalf("key count = %d, want 12; a range split replayed the legacy prefix", len(keys))
	}
	last, ok := saver.last()
	if !ok || decodeCompositeRangeState(last.rangeState) != nil {
		t.Fatalf("legacy resume persisted #667 range state: %#v", last)
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
	mu3       sync.Mutex
	gotKeys   [][2]int64
	truncates int
}

func (p *compositeTargetPool) TruncateTable(context.Context, string, string) error {
	p.mu3.Lock()
	p.gotKeys = nil
	p.truncates++
	p.mu3.Unlock()
	return nil
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

// TestTupleCodecTypedRoundTrip verifies watermark component types round-trip
// exactly through the range_state JSON (#629) — the crash-resume fidelity
// that plain JSON would break (float64 rounding for BIGINT, raw bytes for
// SQLite storage classes and invalid-UTF-8 TEXT).
func TestTupleCodecTypedRoundTrip(t *testing.T) {
	const big = int64(1) << 60
	loc := time.FixedZone("X", -5*3600)
	rawText := "a\x80"
	rawBytes := []byte("raw-bytes-defensive")
	in := []any{
		big + 1,
		"aou-ÄÖÜ-собака-犬", // unicode text watermark
		rawText,
		time.Date(2024, 6, 15, 12, 30, 45, 123456789, time.UTC),
		time.Date(2024, 11, 3, 1, 30, 0, 0, loc), // DST-fold-ish local time with offset
		1.5,
		rawBytes,
	}
	enc := encodeCompositeTuple(in)
	if enc == "" {
		t.Fatal("encode returned empty")
	}
	got := decodeCompositeTuple(enc)
	if len(got) != len(in) {
		t.Fatalf("decoded %d components, want %d", len(got), len(in))
	}
	if got[0] != big+1 {
		t.Fatalf("int64 component = %v (%T), want %d — BIGINT precision lost", got[0], got[0], big+1)
	}
	if got[1] != in[1] {
		t.Fatalf("string component = %q, want %q", got[1], in[1])
	}
	if got[2] != rawText {
		t.Fatalf("raw string component bytes = % x, want % x", []byte(got[2].(string)), []byte(rawText))
	}
	tm0, ok := got[3].(time.Time)
	if !ok || !tm0.Equal(in[3].(time.Time)) {
		t.Fatalf("time component = %v (%T), want equal instant to %v", got[3], got[3], in[3])
	}
	tm1, ok := got[4].(time.Time)
	if !ok || !tm1.Equal(in[4].(time.Time)) {
		t.Fatalf("offset time component = %v, want equal instant to %v", got[4], in[4])
	}
	if got[5] != 1.5 {
		t.Fatalf("float component = %v, want 1.5", got[5])
	}
	gotBytes, ok := got[6].([]byte)
	if !ok || !bytes.Equal(gotBytes, rawBytes) {
		t.Fatalf("[]byte component = %v (%T), want %v", got[6], got[6], rawBytes)
	}
}

// TestTupleCodecLegacyAndForeign pins backward/foreign-input handling:
// plain-number arrays (the #628 integer-composite checkpoint format) decode
// as int64s; the integer-keyset per-range watermark blob (objects without a
// "t" tag) and malformed input decode to nil.
func TestTupleCodecLegacyAndForeign(t *testing.T) {
	got := decodeCompositeTuple("[14100,10]")
	if len(got) != 2 || got[0] != int64(14100) || got[1] != int64(10) {
		t.Fatalf("legacy #628 array decoded as %v, want [14100 10] int64s", got)
	}
	if decodeCompositeTuple(`[{"last":5,"max":10,"complete":false}]`) != nil {
		t.Error("keyset per-range watermark blob must decode to nil (foreign format)")
	}
	if decodeCompositeTuple("") != nil || decodeCompositeTuple("not json") != nil {
		t.Error("empty/malformed must decode to nil")
	}
	if decodeCompositeTuple(`[{"t":"i","v":"NaN-ish"}]`) != nil {
		t.Error("typed component with wrong value kind must decode to nil")
	}
	if enc := encodeCompositeTuple([]any{struct{ X int }{1}}); enc != "" {
		t.Errorf("unknown component type must encode to \"\" (legacy fallback), got %q", enc)
	}
}

// TestNormalizeTupleValue pins source-specific []byte normalization (#629).
func TestNormalizeTupleValue(t *testing.T) {
	if v := normalizeTupleValue([]byte("abc"), "mysql"); v != "abc" {
		t.Fatalf("mysql []byte = %v (%T), want string \"abc\"", v, v)
	}
	if v := normalizeTupleValue([]byte("abc"), "postgres"); v != "abc" {
		t.Fatalf("postgres []byte = %v (%T), want string \"abc\"", v, v)
	}
	sqliteBytes, ok := normalizeTupleValue([]byte("abc"), "sqlite").([]byte)
	if !ok || !bytes.Equal(sqliteBytes, []byte("abc")) {
		t.Fatalf("sqlite []byte = %v (%T), want raw bytes", sqliteBytes, sqliteBytes)
	}
	tm := time.Now()
	if v := normalizeTupleValue(tm, "sqlite"); v != tm {
		t.Fatal("time.Time must pass through")
	}
	if v := normalizeTupleValue(int64(5), "mysql"); v != int64(5) {
		t.Fatal("int64 must pass through")
	}
}

func TestTupleKeysetSQLiteBlobStorageClassWatermark(t *testing.T) {
	db := seedSQLiteBlobPKDB(t)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTextTargetPool{}
	table := sqliteTextPKTable("items", 4)
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	stats, err := executeCompositeKeysetPagination(
		context.Background(), srcPool, tgtPool, cfg, Job{Table: table},
		[]string{"k", "val"}, []string{"k", "val"}, []string{"text", "integer"}, []int{0, 0},
		progress.New(), nil, 0, "items", nil, nil,
	)
	if err != nil {
		t.Fatalf("executeCompositeKeysetPagination: %v", err)
	}
	if stats.Rows != 4 {
		t.Fatalf("stats.Rows = %d, want 4", stats.Rows)
	}
	want := []string{"a", "b", "\x01", "\x02"}
	if got := tgtPool.codes(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sqlite mixed TEXT/BLOB PK order = %q, want %q", got, want)
	}
}

func TestTupleKeysetSQLiteInvalidUTF8ResumeWatermark(t *testing.T) {
	db := seedSQLiteInvalidUTF8PKDB(t)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTextTargetPool{}
	table := sqliteTextPKTable("bad_utf8", 2)
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}
	resumeTuple := decodeCompositeTuple(encodeCompositeTuple([]any{"a\x80"}))
	if len(resumeTuple) != 1 {
		t.Fatalf("resume tuple decode failed: %v", resumeTuple)
	}

	_, err := executeCompositeKeysetPagination(
		context.Background(), srcPool, tgtPool, cfg, Job{Table: table},
		[]string{"k", "val"}, []string{"k", "val"}, []string{"text", "integer"}, []int{0, 0},
		progress.New(), resumeTuple, 1, "bad_utf8", nil, nil,
	)
	if err != nil {
		t.Fatalf("executeCompositeKeysetPagination: %v", err)
	}
	got := tgtPool.codes()
	if len(got) != 1 {
		t.Fatalf("resume returned %d rows, want 1: %q", len(got), got)
	}
	if !bytes.Equal([]byte(got[0]), []byte("a\xc3\xa9")) {
		t.Fatalf("resume row bytes = % x, want % x", []byte(got[0]), []byte("a\xc3\xa9"))
	}
}

func TestTupleKeysetForeignCheckpointReplaysWithoutRowsDoneOvercount(t *testing.T) {
	db := seedVarcharPKDB(t, 4)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	saver := &foreignTupleProgressSaver{
		resumeLastPK:   int64(2), // old ROW_NUMBER checkpoint, not a tuple
		resumeRowsDone: 2,
	}
	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "code", DataType: "varchar", IsNullable: false},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"code"},
		RowCount:         5,
		EstimatedRowSize: 48,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         2,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}
	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg,
		Job{Table: table, TaskID: 629, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != 5 {
		t.Fatalf("stats.Rows = %d, want replayed full table count 5", stats.Rows)
	}
	last, ok := saver.last()
	if !ok {
		t.Fatal("no final checkpoint saved")
	}
	if last.rowsDone != 5 {
		t.Fatalf("final rowsDone = %d, want 5; foreign checkpoint rows_done was incorrectly carried forward", last.rowsDone)
	}
}

// TestTupleKeysetSingleVarcharViaExecute drives the FULL Execute routing
// (#629): a single varchar NOT NULL PK must take the tuple keyset path and
// transfer every row exactly once — including the lexicographically smallest
// (the unbounded first chunk) — with a typed string watermark persisted.
func TestTupleKeysetSingleVarcharViaExecute(t *testing.T) {
	db := seedVarcharPKDB(t, 900)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	saver := &keysetRuntimeProgressSaver{}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "code", DataType: "varchar", IsNullable: false},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"code"},
		RowCount:         901,
		EstimatedRowSize: 48,
	}
	table.PopulatePKColumns()
	if !driver.TupleKeysetRoutable(&table, "sqlite") {
		t.Fatal("precondition: single varchar NOT NULL PK must be tuple-routable on sqlite")
	}

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:           100,
			WriteAheadWriters:   1,
			TargetMode:          "drop_recreate",
			CheckpointFrequency: 1,
		},
	}
	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg,
		Job{Table: table, TaskID: 629, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != 901 {
		t.Fatalf("stats.Rows = %d, want 901", stats.Rows)
	}
	keys := tgtPool.keys()
	if len(keys) != 901 {
		t.Fatalf("wrote %d rows, want 901", len(keys))
	}

	// The watermark must have persisted as a TYPED STRING tuple — proving
	// the tuple path ran (not ROW_NUMBER) and the codec kept the type.
	last, ok := saver.last()
	if !ok {
		t.Fatal("no checkpoint persisted — did the tuple path run?")
	}
	tuple, ok := last.lastPK.([]any)
	if !ok || len(tuple) != 1 {
		t.Fatalf("final checkpoint lastPK = %v (%T), want a 1-tuple", last.lastPK, last.lastPK)
	}
	if _, isStr := tuple[0].(string); !isStr {
		t.Fatalf("watermark component = %T, want string (normalized)", tuple[0])
	}
}

// TestTupleKeysetMixedCompositeViaExecute drives Execute end-to-end for a
// mixed (int, text) composite PK (#629): every row exactly once, including
// chunks that end mid-group, with the smallest tuple present.
func TestTupleKeysetMixedCompositeViaExecute(t *testing.T) {
	db := seedMixedPKDB(t, 40, 15) // 40 groups × 15 codes = 600 rows
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}

	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "grp", DataType: "integer", IsNullable: false},
			{Name: "code", DataType: "varchar", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"grp", "code"},
		RowCount:         600,
		EstimatedRowSize: 40,
	}
	table.PopulatePKColumns()
	if !driver.TupleKeysetRoutable(&table, "sqlite") {
		t.Fatal("precondition: (int,varchar) composite must be tuple-routable on sqlite")
	}

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         37, // prime → many chunk boundaries mid-group
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}
	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg,
		Job{Table: table}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != 600 {
		t.Fatalf("stats.Rows = %d, want 600", stats.Rows)
	}
	keys := tgtPool.keys()
	if len(keys) != 600 {
		t.Fatalf("wrote %d rows, want 600", len(keys))
	}
}

func seedVarcharPKDB(t *testing.T, n int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "vk629.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE items (code TEXT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO items VALUES (?,?)`)
	for i := 1; i <= n; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("k%08d", i), fmt.Sprintf("p-%d", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := stmt.Exec("aaa-FIRST", "min"); err != nil { // strict minimum
		t.Fatalf("insert min: %v", err)
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return db
}

func seedMixedPKDB(t *testing.T, groups, per int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "mixed629.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE lines (grp INTEGER NOT NULL, code TEXT NOT NULL, qty INTEGER, PRIMARY KEY(grp, code))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO lines VALUES (?,?,?)`)
	for g := 1; g <= groups; g++ {
		for c := 1; c <= per; c++ {
			if _, err := stmt.Exec(g, fmt.Sprintf("c%03d", c), g*1000+c); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return db
}

func sqliteTextPKTable(name string, rows int64) driver.Table {
	table := driver.Table{
		Name: name,
		Columns: []driver.Column{
			{Name: "k", DataType: "text", IsNullable: false},
			{Name: "val", DataType: "integer"},
		},
		PrimaryKey:       []string{"k"},
		RowCount:         rows,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	return table
}

func seedSQLiteBlobPKDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "blob-pk.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE items (k TEXT NOT NULL PRIMARY KEY, val INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, v := range []any{"a", "b", []byte{0x01}, []byte{0x02}} {
		if _, err := db.Exec(`INSERT INTO items VALUES (?, ?)`, v, i); err != nil {
			t.Fatalf("insert %v: %v", v, err)
		}
	}
	return db
}

func seedSQLiteInvalidUTF8PKDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "bad-utf8-pk.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE bad_utf8 (k TEXT NOT NULL PRIMARY KEY, val INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO bad_utf8 VALUES (CAST(X'6180' AS TEXT), 1), (CAST(X'61C3A9' AS TEXT), 2)`); err != nil {
		t.Fatalf("insert invalid utf8 text: %v", err)
	}
	return db
}

type foreignTupleProgressSaver struct {
	keysetRuntimeProgressSaver
	resumeLastPK   any
	resumeRowsDone int64
	rangeState     string
}

func (s *foreignTupleProgressSaver) GetProgress(int64) (any, int64, string, error) {
	return s.resumeLastPK, s.resumeRowsDone, s.rangeState, nil
}
