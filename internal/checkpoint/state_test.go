package checkpoint

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestCleanupOldRuns(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	oldSuccess := "old-success"
	oldFailed := "old-failed"
	recentSuccess := "recent-success"
	running := "running"

	for _, runID := range []string{oldSuccess, oldFailed, recentSuccess, running} {
		if err := state.CreateRun(runID, "dbo", "public", map[string]string{"run": runID}, "", ""); err != nil {
			t.Fatalf("CreateRun(%s) error: %v", runID, err)
		}
	}

	if err := state.CompleteRun(oldSuccess, "success", ""); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", oldSuccess, err)
	}
	if err := state.CompleteRun(oldFailed, "failed", "boom"); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", oldFailed, err)
	}
	if err := state.CompleteRun(recentSuccess, "success", ""); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", recentSuccess, err)
	}

	oldTime := time.Now().AddDate(0, 0, -31).Format("2006-01-02 15:04:05")
	if _, err := state.db.Exec(`UPDATE runs SET completed_at = ? WHERE id IN (?, ?)`, oldTime, oldSuccess, oldFailed); err != nil {
		t.Fatalf("update old completed_at error: %v", err)
	}

	recentTime := time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05")
	if _, err := state.db.Exec(`UPDATE runs SET completed_at = ? WHERE id = ?`, recentTime, recentSuccess); err != nil {
		t.Fatalf("update recent completed_at error: %v", err)
	}

	taskIDs := make(map[string]int64)
	for _, runID := range []string{oldSuccess, oldFailed, recentSuccess, running} {
		taskID, err := state.CreateTask(runID, "transfer", "transfer:dbo.Table")
		if err != nil {
			t.Fatalf("CreateTask(%s) error: %v", runID, err)
		}
		taskIDs[runID] = taskID
		if err := state.SaveTransferProgress(taskID, "Table", nil, int64(1), 10, 100); err != nil {
			t.Fatalf("SaveTransferProgress(%s) error: %v", runID, err)
		}
		if _, err := state.db.Exec(`INSERT INTO task_outputs (task_id, key, value) VALUES (?, ?, ?)`, taskID, "k", "v"); err != nil {
			t.Fatalf("insert task_outputs(%s) error: %v", runID, err)
		}
	}

	deleted, err := state.CleanupOldRuns(30)
	if err != nil {
		t.Fatalf("CleanupOldRuns error: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted runs = %d, want 2", deleted)
	}

	if got := countRows(t, state.db, `SELECT COUNT(*) FROM runs`); got != 2 {
		t.Fatalf("runs remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM runs WHERE id = ?`, running); got != 1 {
		t.Fatalf("running run missing after cleanup")
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM tasks`); got != 2 {
		t.Fatalf("tasks remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM transfer_progress`); got != 2 {
		t.Fatalf("transfer_progress remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM task_outputs`); got != 2 {
		t.Fatalf("task_outputs remaining = %d, want 2", got)
	}
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count query error: %v", err)
	}
	return count
}

func TestAITuningThroughputFeedback(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Verify new columns exist by saving a record
	record := AITuningRecord{
		Timestamp:    time.Now(),
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		TotalTables:  5,
		TotalRows:    1000000,
		Workers:      6,
		ChunkSize:    50000,
		WasAIUsed:    true,
	}
	if err := state.SaveAITuning(record); err != nil {
		t.Fatalf("SaveAITuning() error: %v", err)
	}

	// Update with throughput result
	if err := state.UpdateAITuningResult(450000, 240.5, 0); err != nil {
		t.Fatalf("UpdateAITuningResult() error: %v", err)
	}

	// Verify throughput is returned by GetAITuningHistory (filtered by direction)
	history, err := state.GetAITuningHistory(5, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory() error: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(history))
	}
	if math.Abs(history[0].FinalThroughput-450000) > 1 {
		t.Errorf("FinalThroughput = %f, want 450000", history[0].FinalThroughput)
	}
	if math.Abs(history[0].FinalDurationSecs-240.5) > 0.1 {
		t.Errorf("FinalDurationSecs = %f, want 240.5", history[0].FinalDurationSecs)
	}

	// Test that UpdateAITuningResult only targets records with NULL throughput
	record2 := AITuningRecord{
		Timestamp:    time.Now().Add(time.Second), // Ensure distinct timestamp (stored as seconds)
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		TotalTables:  5,
		TotalRows:    1000000,
		Workers:      6,
		ChunkSize:    100000,
		WasAIUsed:    true,
	}
	if err := state.SaveAITuning(record2); err != nil {
		t.Fatalf("SaveAITuning(record2) error: %v", err)
	}
	if err := state.UpdateAITuningResult(300000, 350.0, 2); err != nil {
		t.Fatalf("UpdateAITuningResult(record2) error: %v", err)
	}

	history, err = state.GetAITuningHistory(5, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory() error: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(history))
	}
	// history[0] is newest (chunk_size=100000), history[1] is oldest (chunk_size=50000)
	if history[0].ChunkSize != 100000 {
		t.Errorf("Newest record chunk_size = %d, want 100000", history[0].ChunkSize)
	}
	if math.Abs(history[0].FinalThroughput-300000) > 1 {
		t.Errorf("Newest FinalThroughput = %f, want 300000", history[0].FinalThroughput)
	}
	// First record should still have its original throughput (not overwritten)
	if math.Abs(history[1].FinalThroughput-450000) > 1 {
		t.Errorf("Oldest FinalThroughput = %f, want 450000 (should not be overwritten)", history[1].FinalThroughput)
	}

	// Newest record should reflect the chunk retry count we passed in
	if history[0].ChunkRetryCount != 2 {
		t.Errorf("Newest ChunkRetryCount = %d, want 2", history[0].ChunkRetryCount)
	}
	// Oldest record was updated with chunkRetryCount=0
	if history[1].ChunkRetryCount != 0 {
		t.Errorf("Oldest ChunkRetryCount = %d, want 0", history[1].ChunkRetryCount)
	}
}

func TestAITuningHistoryDirectionFilter(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Save records for two different directions
	mssqlToPG := AITuningRecord{
		Timestamp:    time.Now(),
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		TotalTables:  9,
		TotalRows:    106000000,
		Workers:      10,
		ChunkSize:    12000,
		WasAIUsed:    true,
	}
	pgToPG := AITuningRecord{
		Timestamp:    time.Now(),
		SourceDBType: "postgres",
		TargetDBType: "postgres",
		TotalTables:  9,
		TotalRows:    106000000,
		Workers:      12,
		ChunkSize:    14000,
		WasAIUsed:    true,
	}
	if err := state.SaveAITuning(mssqlToPG); err != nil {
		t.Fatalf("SaveAITuning(mssql→pg) error: %v", err)
	}
	if err := state.SaveAITuning(pgToPG); err != nil {
		t.Fatalf("SaveAITuning(pg→pg) error: %v", err)
	}

	// Filtered by mssql→postgres should return only 1
	mssqlOnly, err := state.GetAITuningHistory(5, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory(mssql→pg) error: %v", err)
	}
	if len(mssqlOnly) != 1 {
		t.Fatalf("Expected 1 mssql→pg record, got %d", len(mssqlOnly))
	}
	if mssqlOnly[0].Workers != 10 {
		t.Errorf("Expected workers=10 for mssql→pg, got %d", mssqlOnly[0].Workers)
	}

	// Filtered by postgres→postgres should return only 1
	pgOnly, err := state.GetAITuningHistory(5, "postgres", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory(pg→pg) error: %v", err)
	}
	if len(pgOnly) != 1 {
		t.Fatalf("Expected 1 pg→pg record, got %d", len(pgOnly))
	}
	if pgOnly[0].Workers != 12 {
		t.Errorf("Expected workers=12 for pg→pg, got %d", pgOnly[0].Workers)
	}

	// Filtered by nonexistent direction should return empty
	none, err := state.GetAITuningHistory(5, "mysql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory(mysql→pg) error: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("Expected 0 mysql→pg records, got %d", len(none))
	}
}

func TestAITuningHistoryLimitZeroReturnsAll(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Insert 7 records for the same direction
	for i := 0; i < 7; i++ {
		rec := AITuningRecord{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
			SourceDBType: "mssql",
			TargetDBType: "postgres",
			TotalTables:  9,
			TotalRows:    int64(1000 * (i + 1)),
			Workers:      i + 1,
			ChunkSize:    50000,
			WasAIUsed:    true,
		}
		if err := state.SaveAITuning(rec); err != nil {
			t.Fatalf("SaveAITuning(%d) error: %v", i, err)
		}
	}

	// limit=0 should return all 7
	all, err := state.GetAITuningHistory(0, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory(0) error: %v", err)
	}
	if len(all) != 7 {
		t.Fatalf("limit=0: expected 7 records, got %d", len(all))
	}

	// limit=3 should return exactly 3
	limited, err := state.GetAITuningHistory(3, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetAITuningHistory(3) error: %v", err)
	}
	if len(limited) != 3 {
		t.Fatalf("limit=3: expected 3 records, got %d", len(limited))
	}

	// Both should return most recent first (highest Workers value)
	if all[0].Workers != 7 {
		t.Errorf("limit=0: expected most recent first (workers=7), got workers=%d", all[0].Workers)
	}
	if limited[0].Workers != 7 {
		t.Errorf("limit=3: expected most recent first (workers=7), got workers=%d", limited[0].Workers)
	}
}

func TestAIAdjustmentsLimitZeroReturnsAll(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Create a run to attach adjustments to
	if err := state.CreateRun("test-run", "dbo", "public", "", "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	// Insert 4 adjustment records
	for i := 0; i < 4; i++ {
		rec := AIAdjustmentRecord{
			AdjustmentNumber: i + 1,
			Timestamp:        time.Now().Add(time.Duration(i) * time.Second),
			Action:           "scale_up",
			Adjustments:      map[string]int{"workers": i + 2},
		}
		if err := state.SaveAIAdjustment("test-run", rec); err != nil {
			t.Fatalf("SaveAIAdjustment(%d) error: %v", i, err)
		}
	}

	// limit=0 should return all 4
	all, err := state.GetAIAdjustments(0)
	if err != nil {
		t.Fatalf("GetAIAdjustments(0) error: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("limit=0: expected 4 records, got %d", len(all))
	}

	// limit=2 should return exactly 2
	limited, err := state.GetAIAdjustments(2)
	if err != nil {
		t.Fatalf("GetAIAdjustments(2) error: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("limit=2: expected 2 records, got %d", len(limited))
	}
}

func TestSyncTimestamps(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	sourceSchema := "dbo"
	tableName := "Orders"
	targetSchema := "public"

	// Test: Get timestamp for table with no prior sync
	ts, err := state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for first sync, got %v", ts)
	}

	// Test: Update sync timestamp
	syncTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	if err := state.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, syncTime); err != nil {
		t.Fatalf("UpdateSyncTimestamp() error: %v", err)
	}

	// Test: Get updated timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts == nil {
		t.Fatal("Expected non-nil timestamp after update")
	}
	if !ts.Equal(syncTime) {
		t.Errorf("Timestamp mismatch: got %v, want %v", ts, syncTime)
	}

	// Test: Update with newer timestamp (upsert)
	newerTime := time.Date(2024, 6, 16, 12, 0, 0, 0, time.UTC)
	if err := state.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, newerTime); err != nil {
		t.Fatalf("UpdateSyncTimestamp() error: %v", err)
	}

	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if !ts.Equal(newerTime) {
		t.Errorf("Timestamp not updated: got %v, want %v", ts, newerTime)
	}

	// Test: Different table should have no timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, "OtherTable", targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for different table, got %v", ts)
	}

	// Test: Same table, different target schema should have no timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, "other_schema")
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for different target schema, got %v", ts)
	}
}

func TestUpdateRunConfig(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	const runID = "run-update-config"
	original := map[string]any{"workers": 12, "chunk_size": 113510}
	if err := state.CreateRun(runID, "dbo", "public", original, "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	updated := map[string]any{"workers": 6, "chunk_size": 100000}
	if err := state.UpdateRunConfig(runID, updated); err != nil {
		t.Fatalf("UpdateRunConfig error: %v", err)
	}

	run, err := state.GetRunByID(runID)
	if err != nil {
		t.Fatalf("GetRunByID error: %v", err)
	}
	if run == nil {
		t.Fatalf("run not found after UpdateRunConfig")
	}
	if !strings.Contains(run.Config, `"workers":6`) || !strings.Contains(run.Config, `"chunk_size":100000`) {
		t.Errorf("config not updated, got: %s", run.Config)
	}
	if strings.Contains(run.Config, `"workers":12`) {
		t.Errorf("config still contains pre-update workers=12: %s", run.Config)
	}

	// Unknown run id should return an error rather than silently succeeding.
	if err := state.UpdateRunConfig("does-not-exist", updated); err == nil {
		t.Errorf("expected error for unknown run id, got nil")
	}
}

func TestCountPartitionTasks(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	runID := "test-run"
	if err := state.CreateRun(runID, "public", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun() error: %v", err)
	}

	// Create partition tasks for a table
	for i := 1; i <= 6; i++ {
		key := fmt.Sprintf("transfer:public.votes:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}

	// Create a non-partition task for the same table (should not be counted)
	if _, err := state.CreateTask(runID, "transfer", "transfer:public.votes"); err != nil {
		t.Fatalf("CreateTask() error: %v", err)
	}

	// Create partition tasks for a different table
	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("transfer:public.posts:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}

	// Count votes partitions
	count, err := state.CountPartitionTasks(runID, "transfer:public.votes")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 6 {
		t.Errorf("expected 6 partition tasks for votes, got %d", count)
	}

	// Count posts partitions
	count, err = state.CountPartitionTasks(runID, "transfer:public.posts")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 partition tasks for posts, got %d", count)
	}

	// Table with underscores — underscore must not act as wildcard
	for i := 1; i <= 2; i++ {
		key := fmt.Sprintf("transfer:public.order_items:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}
	count, err = state.CountPartitionTasks(runID, "transfer:public.order_items")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 partition tasks for order_items, got %d", count)
	}

	// Non-existent table
	count, err = state.CountPartitionTasks(runID, "transfer:public.nonexistent")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 partition tasks for nonexistent table, got %d", count)
	}
}
