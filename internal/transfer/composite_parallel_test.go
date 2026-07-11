package transfer

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

	_ "modernc.org/sqlite"
)

func TestCompositeParallelKeysetTransfersEveryRange(t *testing.T) {
	db := seedCompositeKeysetDB(t, 80, 12)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &compositeTargetPool{}
	saver := &keysetRuntimeProgressSaver{}
	table := driver.Table{
		Name: "lines",
		Columns: []driver.Column{
			{Name: "order_id", DataType: "integer", IsNullable: false},
			{Name: "line_no", DataType: "integer", IsNullable: false},
			{Name: "qty", DataType: "integer"},
		},
		PrimaryKey:       []string{"order_id", "line_no"},
		RowCount:         960,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: 11, ParallelReaders: 4, WriteAheadWriters: 1, TargetMode: "drop_recreate",
	}}

	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table, TaskID: 667, Saver: saver}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != 960 {
		t.Fatalf("stats.Rows = %d, want 960", stats.Rows)
	}
	keys := tgtPool.keys()
	if len(keys) != 960 {
		t.Fatalf("wrote %d keys, want 960", len(keys))
	}
	seen := make(map[[2]int64]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate tuple %v", key)
		}
		seen[key] = struct{}{}
	}
	if _, ok := seen[[2]int64{1, 1}]; !ok {
		t.Fatal("smallest tuple missing")
	}
	if _, ok := seen[[2]int64{80, 12}]; !ok {
		t.Fatal("largest tuple missing")
	}
	last, ok := saver.last()
	if !ok || len(decodeCompositeRangeState(last.rangeState)) < 2 {
		t.Fatalf("normal Execute did not persist parallel tuple ranges: %#v", last)
	}
}

func TestCompositeRangeStateCodecRoundTripAndLegacyFallback(t *testing.T) {
	const big = int64(1) << 60
	in := []compositeResumeRange{
		{min: big + 1, max: big + 9, tuple: []any{big + 2, "middle"}},
		{min: big + 10, max: big + 19, tuple: []any{big + 18, "last"}, complete: true},
	}
	encoded := encodeCompositeRangeState(in)
	got := decodeCompositeRangeState(encoded)
	if len(got) != len(in) {
		t.Fatalf("decoded ranges = %d, want %d (%s)", len(got), len(in), encoded)
	}
	if got[0].min != in[0].min || got[0].max != in[0].max || got[0].tuple[0] != big+2 || got[1].tuple[1] != "last" || !got[1].complete {
		t.Fatalf("round trip = %#v, want %#v", got, in)
	}
	if decodeCompositeRangeState(encodeCompositeTuple([]any{int64(1), int64(2)})) != nil {
		t.Fatal("legacy single tuple must not be mistaken for range state")
	}
	if decodeCompositeRangeState(encodeKeysetRangeState([]readerCheckpointState{{lastPK: int64(1), maxPK: int64(2)}})) != nil {
		t.Fatal("legacy integer-keyset ranges must not be mistaken for tuple ranges")
	}
}

// TestCompositeParallelKeysetWorkStealsSkewedRanges covers the workload #667
// is intended to fix: most composite rows inhabit a narrow high-leading-key
// band. Oversubscribed leading-component ranges let several readers pull that
// band instead of leaving a static final reader with essentially all rows.
func TestCompositeParallelKeysetWorkStealsSkewedRanges(t *testing.T) {
	const (
		numReaders = 4
		chunkSize  = 200
		bandStart  = 760_001
		bandEnd    = 1_000_000
		bandStep   = 40
	)
	db, wantRows := seedSkewedCompositeKeysetDB(t, bandStart, bandEnd, bandStep)
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
		RowCount:         int64(wantRows),
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		ChunkSize: chunkSize, ParallelReaders: numReaders, WriteAheadWriters: 1, TargetMode: "drop_recreate",
	}}

	producer, stats, err := runParallelCompositeForTest(t, srcPool, tgtPool, cfg, Job{Table: table})
	if err != nil {
		t.Fatalf("parallel composite keyset: %v", err)
	}
	if stats.Rows != int64(wantRows) {
		t.Fatalf("rows = %d, want %d", stats.Rows, wantRows)
	}
	keys := tgtPool.keys()
	if len(keys) != wantRows {
		t.Fatalf("key count = %d, want %d", len(keys), wantRows)
	}
	seen := make(map[[2]int64]struct{}, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate tuple %v", key)
		}
		seen[key] = struct{}{}
	}
	if len(producer.ranges) <= numReaders {
		t.Fatalf("expected oversubscribed range work, got %d ranges for %d readers", len(producer.ranges), numReaders)
	}
	if producer.readerCount() != numReaders {
		t.Fatalf("reader count = %d, want configured %d", producer.readerCount(), numReaders)
	}
	for workerID, ranges := range producer.rangesPerWorker {
		if ranges == 0 {
			t.Fatalf("reader %d received no range; want all %d configured readers active (%v)", workerID, numReaders, producer.rangesPerWorker)
		}
	}
	rows := append([]int64(nil), producer.rowsPerWorker...)
	sort.Slice(rows, func(i, j int) bool { return rows[i] > rows[j] })
	if rows[0] == stats.Rows {
		t.Fatalf("one reader handled all %d rows; range work was not stolen (%v)", stats.Rows, producer.rowsPerWorker)
	}
	if share := float64(rows[1]) / float64(stats.Rows); share < 0.15 {
		t.Fatalf("second-busiest reader handled %.1f%% of rows; want a material share (%v)", share*100, producer.rowsPerWorker)
	}
}

// runParallelCompositeForTest mirrors #667's fresh-range setup and returns
// the producer so the skew test can inspect its per-worker accounting.
func runParallelCompositeForTest(t *testing.T, srcPool *keysetRuntimeSourcePool, tgtPool *compositeTargetPool, cfg *config.Config, job Job) (*compositeParallelProducer, *TransferStats, error) {
	t.Helper()
	ctx := context.Background()
	dialect := driver.GetDialect("sqlite")
	min, max, eligible, err := compositeLeadingBounds(ctx, srcPool.DB(), dialect, job, dialect.TableHint(false))
	if err != nil || !eligible {
		return nil, nil, err
	}
	numRanges := keysetWorkRangeCount(min, max, cfg.Migration.ParallelReaders, cfg.Migration.ChunkSize)
	ranges := make([]compositeResumeRange, 0, numRanges)
	for _, r := range splitPKRange(min, max, numRanges, true) {
		lo, okLo := parseNumericPK(r.minPK)
		hi, okHi := parseNumericPK(r.maxPK)
		if !okLo || !okHi {
			return nil, nil, nil
		}
		ranges = append(ranges, compositeResumeRange{min: lo, max: hi, minInclusive: r.minInclusive})
	}
	cols := []string{"order_id", "line_no", "qty"}
	colTypes := []string{"integer", "integer", "integer"}
	producer := &compositeParallelProducer{
		db:         srcPool.DB(),
		dialect:    dialect,
		colList:    dialect.ColumnListForSelect(cols, colTypes, "sqlite"),
		job:        job,
		pkCols:     job.Table.PrimaryKey,
		pkIdxs:     []int{0, 1},
		srcDBType:  "sqlite",
		valueConvs: dialect.ValueConverters(colTypes, "sqlite"),
		numCols:    len(cols),
		ranges:     ranges,
		numReaders: cfg.Migration.ParallelReaders,
	}
	producer.convIdx = buildConvIdx(producer.valueConvs)
	stats, err := runPipeline(ctx, pipelineConfig{
		cfg:             cfg,
		job:             job,
		tgtPool:         tgtPool,
		prog:            progress.New(),
		producer:        producer,
		targetTableName: job.Table.Name,
		targetCols:      cols,
		colTypes:        colTypes,
		colSRIDs:        []int{0, 0, 0},
	})
	return producer, stats, err
}

func seedSkewedCompositeKeysetDB(t *testing.T, bandStart, bandEnd, bandStep int) (*sql.DB, int) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "skewed-composite.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE lines (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, qty INTEGER NOT NULL, PRIMARY KEY (order_id, line_no))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO lines VALUES (?, 1, ?)`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rows := 0
	if _, err := stmt.Exec(1, 1); err != nil {
		t.Fatalf("insert low sentinel: %v", err)
	}
	rows++
	for id := bandStart; id <= bandEnd; id += bandStep {
		if _, err := stmt.Exec(id, id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
		rows++
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("close statement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return db, rows
}
