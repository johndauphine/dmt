package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

	_ "modernc.org/sqlite"
)

// TestKeysetWorkRangeCount pins the sub-range sizing decisions (#615).
func TestKeysetWorkRangeCount(t *testing.T) {
	tests := []struct {
		name       string
		minPK      any
		maxPK      any
		numReaders int
		chunkSize  int
		want       int
	}{
		{"single reader never splits", int64(1), int64(1_000_000), 1, 1000, 1},
		{"non-numeric bounds fall back to readers", "a", "z", 4, 1000, 4},
		{"tiny table stays at readers", int64(1), int64(100), 4, 1000, 4},
		{"dense-narrow stays at readers", int64(1), int64(50_000), 4, 10_000, 4},
		// distance 4_000_000 / (10_000*4) = 100 sub-ranges, under the 4*8=32 cap? no: capped at 32.
		{"large table oversubscribes to the cap", int64(1), int64(4_000_001), 4, 10_000, 32},
		// distance 800_000 / (10_000*4) = 20, between readers(4) and cap(32).
		{"mid table scales between readers and cap", int64(1), int64(800_001), 4, 10_000, 20},
		// Cap holds even with a huge reader count: 100*8=800 → maxWorkRanges 256.
		{"cap holds against huge reader count", int64(1), int64(1_000_000_000), 100, 1000, maxWorkRanges},
		{"zero chunk size falls back to readers", int64(1), int64(1_000_000), 4, 0, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := keysetWorkRangeCount(tt.minPK, tt.maxPK, tt.numReaders, tt.chunkSize)
			if got != tt.want {
				t.Fatalf("keysetWorkRangeCount(%v,%v,readers=%d,chunk=%d) = %d, want %d",
					tt.minPK, tt.maxPK, tt.numReaders, tt.chunkSize, got, tt.want)
			}
		})
	}
}

// TestKeysetWorkStealingRebalancesSkewedLoad seeds a table whose rows are
// clustered in the top static-quarter of the ID space — the case where a
// one-range-per-reader static split makes a single reader do ~all the work
// while the others idle (#615). It asserts (a) every row is transferred
// correctly and (b) the heavy band's rows are spread across multiple
// workers rather than concentrated in one.
func TestKeysetWorkStealingRebalancesSkewedLoad(t *testing.T) {
	const (
		numReaders = 4
		chunkSize  = 2000
		// A wide, sparse tail forces MIN=1, MAX≈1_000_000 so the static
		// 4-way split is [1..250k][250k..500k][500k..750k][750k..1M].
		bandStart = 760_001
		bandEnd   = 1_000_000
		bandStep  = 8 // 30_000 rows densely packed in the top quarter
	)

	db := seedSkewedKeysetDB(t, bandStart, bandEnd, bandStep)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true} // updated:true → no tuner callback

	var wantIDs []int
	for id := bandStart; id <= bandEnd; id += bandStep {
		wantIDs = append(wantIDs, id)
	}
	// Plus two sentinels at the extremes to pin MIN/MAX and prove the
	// sparse low ranges are visited and empty-fast.
	if _, err := db.Exec(`INSERT INTO items (id, payload) VALUES (1, 'lo'), (1000000, 'hi')`); err != nil {
		t.Fatalf("insert sentinels: %v", err)
	}
	wantIDs = append([]int{1}, wantIDs...)
	if wantIDs[len(wantIDs)-1] != bandEnd {
		wantIDs = append(wantIDs, 1_000_000)
	}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         int64(len(wantIDs)),
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         chunkSize,
			ParallelReaders:   numReaders,
			WriteAheadWriters: 2,
			TargetMode:        "drop_recreate",
		},
	}
	job := Job{Table: table}

	producer, stats, err := runKeysetForTest(t, srcPool, tgtPool, cfg, job, chunkSize)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}

	// (a) Correctness: every clustered row plus sentinels landed exactly once.
	gotIDs, _ := tgtPool.snapshot()
	sort.Ints(gotIDs)
	sort.Ints(wantIDs)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("transferred %d rows, want %d", len(gotIDs), len(wantIDs))
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("row[%d] = %d, want %d", i, gotIDs[i], wantIDs[i])
		}
	}
	if stats.Rows != int64(len(wantIDs)) {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, len(wantIDs))
	}

	// (b) Oversubscription actually happened.
	if len(producer.pkRanges) <= numReaders {
		t.Fatalf("expected work-stealing oversubscription (>%d ranges), got %d", numReaders, len(producer.pkRanges))
	}

	// (c) Rebalancing: the heavy band was NOT drained by a single worker.
	// With a static split one worker would read ~100% of rows; here the top
	// two workers by row count should each carry a real share.
	rows := append([]int64(nil), producer.rowsPerWorker...)
	sort.Slice(rows, func(i, j int) bool { return rows[i] > rows[j] })
	total := stats.Rows
	if rows[0] == total {
		t.Fatalf("one worker read all %d rows — work-stealing did not rebalance (per-worker: %v)", total, producer.rowsPerWorker)
	}
	// At least two workers each read >15% of the rows — impossible under a
	// static split where the single heavy static-quarter is owned by one reader.
	share := func(n int64) float64 { return float64(n) / float64(total) }
	if share(rows[1]) < 0.15 {
		t.Fatalf("second-busiest worker read only %.1f%% of rows; expected the band spread across workers (per-worker: %v)",
			share(rows[1])*100, producer.rowsPerWorker)
	}
	t.Logf("work-stealing spread %d rows across %d ranges; per-worker rows=%v", total, len(producer.pkRanges), producer.rowsPerWorker)
}

// runKeysetForTest builds the producer the way executeKeysetPagination does
// and runs the pipeline, returning the producer so tests can inspect its
// per-worker accounting. It mirrors the boundary/split logic rather than
// duplicating executeKeysetPagination's plumbing.
func runKeysetForTest(t *testing.T, srcPool *keysetRuntimeSourcePool, tgtPool *keysetRuntimeTargetPool, cfg *config.Config, job Job, chunkSize int) (*keysetProducer, *TransferStats, error) {
	t.Helper()
	ctx := context.Background()
	srcDialect := driver.GetDialect("sqlite")

	cols := []string{"id", "payload"}
	colTypes := []string{"integer", "text"}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, "sqlite")
	valueConvs := srcDialect.ValueConverters(colTypes, "sqlite")

	var minPKVal, maxPKVal any
	err := srcPool.DB().QueryRowContext(ctx, "SELECT MIN(id), MAX(id) FROM items").Scan(&minPKVal, &maxPKVal)
	if err != nil {
		return nil, nil, err
	}

	numReaders := cfg.Migration.ParallelReaders
	numRanges := keysetWorkRangeCount(minPKVal, maxPKVal, numReaders, chunkSize)
	pkRanges := splitPKRange(minPKVal, maxPKVal, numRanges)

	producer := &keysetProducer{
		db:         srcPool.DB(),
		dialect:    srcDialect,
		colList:    colList,
		job:        job,
		pkCol:      "id",
		pkIdx:      0,
		valueConvs: valueConvs,
		convIdx:    buildConvIdx(valueConvs),
		numCols:    len(cols),
		pkRanges:   pkRanges,
		numReaders: numReaders,
	}

	var coord *keysetCheckpointCoordinator
	stats, err := runPipeline(ctx, pipelineConfig{
		cfg:             cfg,
		job:             job,
		tgtPool:         tgtPool,
		prog:            progress.New(),
		producer:        producer,
		targetTableName: "items",
		targetCols:      cols,
		colTypes:        colTypes,
		colSRIDs:        []int{0, 0},
		newAckHandler: func(wp *writerPool, cb tunerCallbacks) func(writeAck) {
			coord = newKeysetCheckpointCoordinator(job, pkRanges, nil, 0, wp.TotalWrittenPtr(), cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
	})
	return producer, stats, err
}

// seedSkewedKeysetDB creates an items table with rows densely packed in
// [bandStart, bandEnd] (step bandStep) and nothing else. Sentinels at the
// ID-space extremes are added by the caller.
func seedSkewedKeysetDB(t *testing.T, bandStart, bandEnd, bandStep int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "skewed.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	db.SetMaxOpenConns(numReadersForSeed)
	db.SetMaxIdleConns(numReadersForSeed)
	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO items (id, payload) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for id := bandStart; id <= bandEnd; id += bandStep {
		if _, err := stmt.Exec(id, fmt.Sprintf("row-%d", id)); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("close stmt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return db
}

const numReadersForSeed = 8

// TestKeysetWorkStealingPersistsManyRangesForResume verifies a fresh
// transfer now persists K > numReaders per-range watermarks (#615) and that
// every range converges to complete, so a resume can restore all K ranges
// verbatim. Paired with TestKeysetResumeSkipsCompletedRanges (which drives
// resume from persisted ranges), this covers the K>numReaders resume path.
func TestKeysetWorkStealingPersistsManyRangesForResume(t *testing.T) {
	const (
		totalRows  = 48_000
		numReaders = 4
		chunkSize  = 1000
	)
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}
	saver := &rangeStateRecordingSaver{}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         totalRows,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:           chunkSize,
			ParallelReaders:     numReaders,
			WriteAheadWriters:   2,
			TargetMode:          "drop_recreate",
			CheckpointFrequency: 1,
		},
	}
	job := Job{Table: table, TaskID: 615, Saver: saver}

	stats, err := executeKeysetPagination(
		context.Background(), srcPool, tgtPool, cfg, job,
		[]string{"id", "payload"}, []string{"id", "payload"},
		[]string{"integer", "text"}, []int{0, 0},
		nil, nil, 0, nil, "items", nil, nil,
	)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}
	if stats.Rows != totalRows {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, totalRows)
	}

	saver.mu.Lock()
	finalState := saver.lastRangeState
	saver.mu.Unlock()

	ranges := decodeKeysetRangeState(finalState)
	if len(ranges) <= numReaders {
		t.Fatalf("persisted %d ranges, expected work-stealing oversubscription (>%d)", len(ranges), numReaders)
	}
	for i, rr := range ranges {
		if !rr.complete {
			t.Errorf("range %d not complete after full transfer: %+v", i, rr)
		}
	}
	t.Logf("full transfer persisted %d complete ranges (numReaders=%d)", len(ranges), numReaders)
}
