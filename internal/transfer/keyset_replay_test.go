package transfer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/progress"
	"github.com/johndauphine/dmt/v5/internal/source"
)

// recordingSaver wraps a real ProgressSaver and records every save so tests
// can assert invariants on each periodic checkpoint, not just the final row.
type recordingSaver struct {
	inner ProgressSaver
	mu    sync.Mutex
	saves []savedProgress
}

func (r *recordingSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	r.mu.Lock()
	r.saves = append(r.saves, savedProgress{lastPK: lastPK, rowsDone: rowsDone, rowsTotal: rowsTotal, rangeState: rangeState})
	r.mu.Unlock()
	return r.inner.SaveProgress(taskID, tableName, partitionID, lastPK, rowsDone, rowsTotal, rangeState)
}

func (r *recordingSaver) GetProgress(taskID int64) (any, int64, string, error) {
	return r.inner.GetProgress(taskID)
}

// Regression test for #632 (SO2010 Posts:p2): a keyset partition retry that
// replays rows committed after the last checkpoint must not inflate
// rows_done. The staged state mirrors a timeout mid-attempt: per-range
// watermarks persisted with a rows_done that exactly covers them, plus rows
// already committed to the target beyond each watermark. The replay must
// clean those up, re-transfer them, and land with rows_done equal to the
// real row count — while every periodic checkpoint along the way keeps
// rows_done consistent with the watermarks saved next to it.
func TestKeysetPartitionReplayDoesNotInflateRowsDone(t *testing.T) {
	ctx := context.Background()

	src := newSQLiteTestReader(t, "source")
	defer src.Close()
	tgt := newSQLiteTestWriter(t, "target")
	defer tgt.Close()

	createKeysetReplayTable(t, ctx, src.DB())
	createKeysetReplayTable(t, ctx, tgt.DB())
	insertKeysetReplayRows(t, ctx, src.DB(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	// Interrupted-attempt target state: range (0,5] checkpointed through
	// id 3 with id 4 committed after the checkpoint; range (5,10]
	// checkpointed through id 7 with id 8 committed after it.
	insertKeysetReplayRows(t, ctx, tgt.DB(), 1, 2, 3, 4, 6, 7, 8)

	table := keysetReplayTable()
	if !table.SupportsKeysetPagination() {
		t.Fatal("test table must route to keyset pagination")
	}

	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()
	saver := &recordingSaver{inner: checkpoint.NewProgressSaver(state)}
	if err := state.CreateRun("run-632", "main", "main", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	taskID, err := state.CreateTask("run-632", "transfer", "transfer:main.events:p2")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	partID := 2
	rangeState := encodeKeysetRangeState([]readerCheckpointState{
		{lastPK: int64(3), maxPK: int64(5)},
		{lastPK: int64(7), maxPK: int64(10)},
	})
	// rows_done = 5 covers exactly ids 1..3 and 6..7 — what the watermarks
	// represent. Ids 4 and 8 were committed but never checkpointed, so they
	// are NOT in rows_done; the replay must count them exactly once.
	if err := state.SaveTransferProgress(taskID, table.Name, &partID, int64(3), 5, 10, rangeState); err != nil {
		t.Fatalf("SaveTransferProgress: %v", err)
	}

	cfg := &config.Config{
		Source: config.SourceConfig{Type: "sqlite"},
		Target: config.TargetConfig{Type: "sqlite", ChunkSize: 2},
		Migration: config.MigrationConfig{
			ChunkSize:            2,
			TargetMode:           "drop_recreate",
			WriteAheadWriters:    2,
			ParallelReaders:      2,
			ReadAheadBuffers:     2,
			CheckpointFrequency:  1,
			UpsertMergeChunkSize: 2,
			MaxSourceConnections: 2,
			MaxTargetConnections: 2,
			Workers:              1,
			LargeTableThreshold:  1,
			MaxPartitions:        2,
		},
	}

	job := Job{
		Table: table,
		Partition: &source.Partition{
			TableName:   table.Name,
			PartitionID: partID,
			MinPK:       int64(1),
			MaxPK:       int64(10),
			RowCount:    10,
		},
		TaskID:         taskID,
		Saver:          saver,
		ReplayPossible: true,
	}

	stats, err := Execute(ctx, src, tgt, cfg, job, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute() replay error: %v", err)
	}
	if stats.Rows != 10 {
		t.Fatalf("stats.Rows = %d, want 10", stats.Rows)
	}

	assertKeysetReplayTarget(t, ctx, tgt.DB())

	final, err := state.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress: %v", err)
	}
	if final == nil {
		t.Fatal("GetTransferProgress = nil, want progress")
	}
	if final.RowsDone != 10 {
		t.Fatalf("final rows_done = %d, want exactly 10 (replayed rows counted twice)", final.RowsDone)
	}

	// Every checkpoint written during the replay must keep rows_done in
	// lock-step with its own watermarks. For dense ids 1..N split at 5,
	// coverage is last0 + (last1 - 5). Any excess means write-attempt
	// counting leaked back into rows_done (#632).
	saver.mu.Lock()
	saves := append([]savedProgress(nil), saver.saves...)
	saver.mu.Unlock()
	if len(saves) == 0 {
		t.Fatal("expected periodic checkpoint saves during replay")
	}
	for i, save := range saves {
		ranges := decodeKeysetRangeState(save.rangeState)
		if len(ranges) != 2 {
			t.Fatalf("save %d: range_state ranges = %d, want 2 (%q)", i, len(ranges), save.rangeState)
		}
		last0, ok0 := parseNumericPK(ranges[0].lastPK)
		last1, ok1 := parseNumericPK(ranges[1].lastPK)
		if !ok0 || !ok1 {
			t.Fatalf("save %d: non-numeric watermarks in %q", i, save.rangeState)
		}
		if want := last0 + (last1 - 5); save.rowsDone != want {
			t.Fatalf("save %d: rows_done = %d, want %d for watermarks (%d, %d) — rows_done drifted from checkpoint coverage",
				i, save.rowsDone, want, last0, last1)
		}
	}
}

func keysetReplayTable() source.Table {
	t := source.Table{
		Schema: "main",
		Name:   "events",
		Columns: []source.Column{
			{Name: "id", DataType: "integer", IsNullable: false, OrdinalPos: 1},
			{Name: "payload", DataType: "varchar", MaxLength: 64, IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         10,
		EstimatedRowSize: 64,
	}
	t.PopulatePKColumns()
	return t
}

func createKeysetReplayTable(t *testing.T, ctx context.Context, db sqliteTestDB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			payload TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("creating events: %v", err)
	}
}

func insertKeysetReplayRows(t *testing.T, ctx context.Context, db sqliteTestDB, ids ...int) {
	t.Helper()
	for _, id := range ids {
		if _, err := db.ExecContext(ctx, `INSERT INTO events (id, payload) VALUES (?, ?)`, id, fmt.Sprintf("row-%d", id)); err != nil {
			t.Fatalf("inserting id %d: %v", id, err)
		}
	}
}

func assertKeysetReplayTarget(t *testing.T, ctx context.Context, db sqliteTestDB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT id FROM events ORDER BY id`)
	if err != nil {
		t.Fatalf("querying target: %v", err)
	}
	defer rows.Close()

	var got []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning id: %v", err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating target: %v", err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(got) != len(want) {
		t.Fatalf("target ids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("target ids = %v, want %v", got, want)
		}
	}
}
