package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestKeysetPartitionReplayDoesNotInflateRowsDonePostgres is the live-database
// end-to-end analogue of TestKeysetPartitionReplayDoesNotInflateRowsDone
// (#632). It drives a real keyset partition replay through the generic
// PostgreSQL source and target pools and the real checkpoint saver — not
// in-memory fakes — against the engine where the bug was observed (SO2010
// MSSQL→Postgres: Posts:p2 over-reported by 67,289 rows after two COPY timeout
// retries), to confirm the whole pipeline lands correct results on Postgres.
//
// It stages an interrupted partition attempt: per-range watermarks persisted
// with a rows_done that exactly covers them, plus rows already committed to the
// target beyond each watermark (what a crash mid-flush leaves behind). The
// replay must clean those up, re-transfer them, and land with the target exact
// and the final persisted rows_done equal to the real row count.
//
// Scope note: the deterministic, mutation-verified regression proof — that
// every persisted checkpoint's rows_done stays in lock-step with the watermarks
// saved beside it — lives in the SQLite sibling and the coordinator unit tests,
// which use a synchronous saver. Here the production async saver (#620) sits
// between the coordinator and this saver and legitimately coalesces periodic
// snapshots latest-wins, so the count and timing of mid-replay saves this test
// observes is nondeterministic and must not be asserted on. What this test
// checks deterministically: the real-engine result is correct, and every
// checkpoint that IS observed is internally consistent.
//
// Both source and target live in uniquely named schemas the test owns, so it
// never touches an application table on a shared instance. Requires the pg test
// container (localhost:5432, postgres/TestPass2024); skips when unreachable
// unless PG_REQUIRED=1.
func TestKeysetPartitionReplayDoesNotInflateRowsDonePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	const dsn = "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	// Registered first so it runs last (t.Cleanup is LIFO): the schema drops
	// below register later and therefore execute before the handle closes.
	// A plain `defer db.Close()` would close the handle before any t.Cleanup
	// runs, silently failing the drops and leaking test schemas.
	t.Cleanup(func() { _ = db.Close() })

	const (
		srcSchema = "dmt632_src"
		tgtSchema = "dmt632_tgt"
		table     = "events"
		partID    = 2 // mirrors the observed SO2010 Posts:p2 inflation
		total     = 2000
		splitAt   = 1000 // range boundary: (0,1000] and (1000,2000]
		wm0       = 400  // range 0 watermark
		wm1       = 1400 // range 1 watermark
		committed = 50   // rows committed past each watermark before the "crash"
	)

	ctx := context.Background()
	setup := []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, srcSchema),
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, tgtSchema),
		fmt.Sprintf(`CREATE SCHEMA %s`, srcSchema),
		fmt.Sprintf(`CREATE SCHEMA %s`, tgtSchema),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, srcSchema, table),
		fmt.Sprintf(`INSERT INTO %s.%s SELECT g, 'row-'||g FROM generate_series(1,%d) g`, srcSchema, table, total),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, tgtSchema, table),
		// Interrupted-attempt target state: each range checkpointed through its
		// watermark, with `committed` rows past it that never made a checkpoint.
		fmt.Sprintf(`INSERT INTO %s.%s SELECT g, 'row-'||g FROM generate_series(1,%d) g`, tgtSchema, table, wm0+committed),
		fmt.Sprintf(`INSERT INTO %s.%s SELECT g, 'row-'||g FROM generate_series(%d,%d) g`, tgtSchema, table, splitAt+1, wm1+committed),
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, srcSchema))
		_, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, tgtSchema))
	})

	tbl := driver.Table{
		Schema: srcSchema,
		Name:   table,
		Columns: []driver.Column{
			{Name: "id", DataType: "int4", IsNullable: false, OrdinalPos: 1},
			{Name: "payload", DataType: "text", IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         total,
		EstimatedRowSize: 32,
	}
	tbl.PopulatePKColumns()
	if !tbl.SupportsKeysetPagination() {
		t.Fatal("test table must route to keyset pagination")
	}

	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "postgres", Host: "localhost", Port: 5432, Database: "postgres",
			User: "postgres", Password: "TestPass2024", Schema: srcSchema, SSLMode: "disable",
		},
		Target: config.TargetConfig{
			Type: "postgres", Host: "localhost", Port: 5432, Database: "postgres",
			User: "postgres", Password: "TestPass2024", Schema: tgtSchema, SSLMode: "disable",
			ChunkSize: 50,
		},
		Migration: config.MigrationConfig{
			ChunkSize:            50,
			TargetMode:           "drop_recreate",
			WriteAheadWriters:    2,
			ParallelReaders:      2,
			ReadAheadBuffers:     2,
			CheckpointFrequency:  1,
			UpsertMergeChunkSize: 50,
			MaxSourceConnections: 4,
			MaxTargetConnections: 4,
			Workers:              1,
			LargeTableThreshold:  1,
		},
	}

	typeMapper, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatalf("GetTypeMapper: %v", err)
	}
	srcPool, err := pool.NewSourcePool(&cfg.Source, cfg.Migration.MaxSourceConnections)
	if err != nil {
		t.Fatalf("NewSourcePool: %v", err)
	}
	defer srcPool.Close()
	tgtPool, err := pool.NewTargetPool(&cfg.Target, cfg.Migration.MaxTargetConnections, "postgres", typeMapper)
	if err != nil {
		t.Fatalf("NewTargetPool: %v", err)
	}
	defer tgtPool.Close()

	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()
	saver := &recordingSaver{inner: checkpoint.NewProgressSaver(state)}
	if err := state.CreateRun("run-632-pg", srcSchema, tgtSchema, nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	taskID, err := state.CreateTask("run-632-pg", "transfer", fmt.Sprintf("transfer:%s.%s:p%d", srcSchema, table, partID))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	// Two per-range watermarks; rows_done covers exactly ids 1..wm0 and
	// (splitAt+1)..wm1 — the committed-but-uncheckpointed rows past each
	// watermark are NOT in rows_done, so the replay must count them once.
	partIDVal := partID
	rangeState := encodeKeysetRangeState([]readerCheckpointState{
		{lastPK: int64(wm0), maxPK: int64(splitAt)},
		{lastPK: int64(wm1), maxPK: int64(total)},
	})
	stagedRowsDone := int64(wm0 + (wm1 - splitAt))
	if err := state.SaveTransferProgress(taskID, table, &partIDVal, int64(wm0), stagedRowsDone, total, rangeState); err != nil {
		t.Fatalf("SaveTransferProgress: %v", err)
	}

	job := Job{
		Table: tbl,
		Partition: &source.Partition{
			TableName:   table,
			PartitionID: partID,
			MinPK:       int64(1),
			MaxPK:       int64(total),
			RowCount:    total,
		},
		TaskID:         taskID,
		Saver:          saver,
		ReplayPossible: true,
	}

	stats, err := Execute(ctx, srcPool, tgtPool, cfg, job, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute() replay error: %v", err)
	}

	// Target must hold exactly the source rows, no duplicates, no gaps.
	var tgtCount, tgtDistinct, tgtMin, tgtMax int
	if err := db.QueryRow(fmt.Sprintf(
		`SELECT count(*), count(DISTINCT id), coalesce(min(id),0), coalesce(max(id),0) FROM %s.%s`, tgtSchema, table,
	)).Scan(&tgtCount, &tgtDistinct, &tgtMin, &tgtMax); err != nil {
		t.Fatalf("target count query: %v", err)
	}
	if tgtCount != total || tgtDistinct != total || tgtMin != 1 || tgtMax != total {
		t.Fatalf("target = {count:%d distinct:%d min:%d max:%d}, want {%d %d 1 %d}",
			tgtCount, tgtDistinct, tgtMin, tgtMax, total, total, total)
	}

	final, err := state.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress: %v", err)
	}
	if final == nil {
		t.Fatal("GetTransferProgress = nil, want progress")
	}
	if final.RowsDone != total {
		t.Fatalf("final rows_done = %d, want exactly %d (replayed rows counted twice)", final.RowsDone, total)
	}
	if stats.Rows != total {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, total)
	}

	// Every checkpoint written during the replay must keep rows_done in
	// lock-step with its own watermarks. For dense ids split at splitAt,
	// coverage is last0 + (last1 - splitAt). Any excess means write-attempt
	// counting leaked back into rows_done (#632). This assertion is the one
	// that fails under the pre-fix accounting (mutation-verified against the
	// live DB and on the SQLite sibling test).
	// Internal-consistency check over whatever checkpoints were observed
	// (their count is nondeterministic — see the scope note): every persisted
	// rows_done must equal the coverage of the watermarks saved alongside it.
	// For dense ids split at splitAt, coverage is last0 + (last1 - splitAt).
	// This never flakes — it asserts a per-save invariant, not that any
	// particular save survived async coalescing — and it holds at completion
	// too (last0=splitAt, last1=total → rows_done=total).
	saver.mu.Lock()
	saves := append([]savedProgress(nil), saver.saves...)
	saver.mu.Unlock()
	if len(saves) == 0 {
		t.Fatal("expected at least the final checkpoint save")
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
		if want := last0 + (last1 - splitAt); save.rowsDone != want {
			t.Fatalf("save %d: rows_done = %d, want %d for watermarks (%d, %d) — rows_done not in lock-step with checkpoint coverage",
				i, save.rowsDone, want, last0, last1)
		}
	}
	t.Logf("live pg partition replay: target exact, final rows_done=%d, %d checkpoint(s) observed and internally consistent", final.RowsDone, len(saves))
}
