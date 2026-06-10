package checkpoint

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestFileState_CreateAndResumeRun(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.yaml")

	// Create file state
	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	// Create a run
	err = fs.CreateRun("test123", "dbo", "public", map[string]string{"key": "value"}, "myprofile", "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Check state file exists
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatal("state file not created")
	}

	// Read file contents
	data, _ := os.ReadFile(stateFile)
	t.Logf("State file contents:\n%s", string(data))

	// Get last incomplete run
	run, err := fs.GetLastIncompleteRun()
	if err != nil {
		t.Fatalf("GetLastIncompleteRun: %v", err)
	}
	if run == nil {
		t.Fatal("expected incomplete run")
	}
	if run.ID != "test123" {
		t.Errorf("run ID = %q, want %q", run.ID, "test123")
	}
	if run.Status != "running" {
		t.Errorf("run status = %q, want %q", run.Status, "running")
	}

	// Create task
	taskID, err := fs.CreateTask("test123", "transfer", "transfer:dbo.Users")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if taskID == 0 {
		t.Error("expected non-zero task ID")
	}

	// Save progress
	err = fs.SaveTransferProgress(taskID, "Users", nil, 12345, 50000, 100000, "")
	if err != nil {
		t.Fatalf("SaveTransferProgress: %v", err)
	}

	// Read progress
	prog, err := fs.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress: %v", err)
	}
	if prog == nil {
		t.Fatal("expected progress")
	}
	if prog.RowsDone != 50000 {
		t.Errorf("RowsDone = %d, want %d", prog.RowsDone, 50000)
	}

	// Complete the task
	err = fs.MarkTaskComplete("test123", "transfer:dbo.Users")
	if err != nil {
		t.Fatalf("MarkTaskComplete: %v", err)
	}

	// Check completed tables
	completed, err := fs.GetCompletedTables("test123")
	if err != nil {
		t.Fatalf("GetCompletedTables: %v", err)
	}
	if !completed["transfer:dbo.Users"] {
		t.Error("expected transfer:dbo.Users to be completed")
	}

	// Complete the run
	err = fs.CompleteRun("test123", "success", "")
	if err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}

	// Verify no incomplete run
	run, err = fs.GetLastIncompleteRun()
	if err != nil {
		t.Fatalf("GetLastIncompleteRun after complete: %v", err)
	}
	if run != nil {
		t.Error("expected no incomplete run after completion")
	}

	// Read final file contents
	data, _ = os.ReadFile(stateFile)
	t.Logf("Final state file:\n%s", string(data))
}

func TestFileState_ClearTransferProgress(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.yaml")

	// Create file state
	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	// Create a run and task
	err = fs.CreateRun("test456", "dbo", "public", nil, "", "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	taskID, err := fs.CreateTask("test456", "transfer", "transfer:dbo.Users")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Save progress
	err = fs.SaveTransferProgress(taskID, "Users", nil, 12345, 50000, 100000, "")
	if err != nil {
		t.Fatalf("SaveTransferProgress: %v", err)
	}

	// Verify progress exists
	prog, err := fs.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress: %v", err)
	}
	if prog == nil {
		t.Fatal("expected progress before clear")
	}
	if prog.RowsDone != 50000 {
		t.Errorf("RowsDone = %d, want 50000", prog.RowsDone)
	}

	// Clear progress
	err = fs.ClearTransferProgress(taskID)
	if err != nil {
		t.Fatalf("ClearTransferProgress: %v", err)
	}

	// Verify progress is cleared
	prog, err = fs.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress after clear: %v", err)
	}
	if prog != nil {
		t.Errorf("expected no progress after clear, got: %+v", prog)
	}

	// Verify task status is reset to pending
	total, pending, _, _, _, err := fs.GetRunStats("test456")
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1", total)
	}
	if pending != 1 {
		t.Errorf("pending = %d, want 1", pending)
	}
}

// TestFileState_ClearPartitionTransferProgress mirrors the SQLite test of the
// same name for the file backend (#227). The file backend is used by Airflow
// runs without SQLite, so its resume preflight needs the same partition-aware
// clear behavior: clearing for one table must wipe only that table's
// partitions and leave both the table-level record and sibling tables intact.
func TestFileState_ClearPartitionTransferProgress(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.yaml")

	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	const runID = "test-run-227"
	if err := fs.CreateRun(runID, "public", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// votes: 3 partitions + table-level
	votesPartIDs := []int64{}
	for i := 1; i <= 3; i++ {
		key := "transfer:public.votes:p" + itoa(i)
		taskID, err := fs.CreateTask(runID, "transfer", key)
		if err != nil {
			t.Fatalf("CreateTask(%s): %v", key, err)
		}
		votesPartIDs = append(votesPartIDs, taskID)
		pid := i
		if err := fs.SaveTransferProgress(taskID, "votes", &pid, int64(1000*i), 1000, 5000, ""); err != nil {
			t.Fatalf("SaveTransferProgress votes p%d: %v", i, err)
		}
	}
	votesTableID, err := fs.CreateTask(runID, "transfer", "transfer:public.votes")
	if err != nil {
		t.Fatalf("CreateTask(votes table): %v", err)
	}
	if err := fs.SaveTransferProgress(votesTableID, "votes", nil, int64(42), 42, 5000, ""); err != nil {
		t.Fatalf("SaveTransferProgress votes table: %v", err)
	}

	// posts: 2 partitions (sibling table — must remain untouched)
	postsPartIDs := []int64{}
	for i := 1; i <= 2; i++ {
		key := "transfer:public.posts:p" + itoa(i)
		taskID, err := fs.CreateTask(runID, "transfer", key)
		if err != nil {
			t.Fatalf("CreateTask(%s): %v", key, err)
		}
		postsPartIDs = append(postsPartIDs, taskID)
		pid := i
		if err := fs.SaveTransferProgress(taskID, "posts", &pid, int64(2000*i), 2000, 10000, ""); err != nil {
			t.Fatalf("SaveTransferProgress posts p%d: %v", i, err)
		}
	}

	if err := fs.ClearPartitionTransferProgress(runID, "transfer:public.votes"); err != nil {
		t.Fatalf("ClearPartitionTransferProgress: %v", err)
	}

	for _, id := range votesPartIDs {
		p, err := fs.GetTransferProgress(id)
		if err != nil {
			t.Fatalf("GetTransferProgress votes part %d: %v", id, err)
		}
		if p != nil {
			t.Errorf("expected votes partition %d cleared, got %+v", id, p)
		}
	}
	tp, err := fs.GetTransferProgress(votesTableID)
	if err != nil {
		t.Fatalf("GetTransferProgress votes table: %v", err)
	}
	if tp == nil {
		t.Error("expected votes table-level progress to remain")
	}
	for _, id := range postsPartIDs {
		p, err := fs.GetTransferProgress(id)
		if err != nil {
			t.Fatalf("GetTransferProgress posts part %d: %v", id, err)
		}
		if p == nil {
			t.Errorf("expected posts partition %d to remain, got nil", id)
		}
	}
}

func TestFileState_GetPartitionTransferProgressSummary(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.yaml")
	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	const runID = "test-run-267"
	if err := fs.CreateRun(runID, "public", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	for i, rowsDone := range []int64{100, 250, 400} {
		pid := i + 1
		key := "transfer:public.votes:p" + itoa(pid)
		taskID, err := fs.CreateTask(runID, "transfer", key)
		if err != nil {
			t.Fatalf("CreateTask(%s): %v", key, err)
		}
		if err := fs.SaveTransferProgress(taskID, "votes", &pid, int64(pid*1000), rowsDone, 1000, ""); err != nil {
			t.Fatalf("SaveTransferProgress(%s): %v", key, err)
		}
	}

	tableTaskID, err := fs.CreateTask(runID, "transfer", "transfer:public.votes")
	if err != nil {
		t.Fatalf("CreateTask(votes table): %v", err)
	}
	if err := fs.SaveTransferProgress(tableTaskID, "votes", nil, int64(999), 999, 1000, ""); err != nil {
		t.Fatalf("SaveTransferProgress(votes table): %v", err)
	}

	emptyProgressID, err := fs.CreateTask(runID, "transfer", "transfer:public.votes:p4")
	if err != nil {
		t.Fatalf("CreateTask(votes empty partition): %v", err)
	}
	pid := 4
	if err := fs.SaveTransferProgress(emptyProgressID, "votes", &pid, nil, 999, 1000, ""); err != nil {
		t.Fatalf("SaveTransferProgress(votes empty partition): %v", err)
	}

	otherTaskID, err := fs.CreateTask(runID, "transfer", "transfer:public.posts:p1")
	if err != nil {
		t.Fatalf("CreateTask(posts): %v", err)
	}
	pid = 1
	if err := fs.SaveTransferProgress(otherTaskID, "posts", &pid, int64(500), 500, 1000, ""); err != nil {
		t.Fatalf("SaveTransferProgress(posts): %v", err)
	}

	summary, err := fs.GetPartitionTransferProgressSummary(runID, "transfer:public.votes")
	if err != nil {
		t.Fatalf("GetPartitionTransferProgressSummary: %v", err)
	}
	if summary.RowsDone != 750 {
		t.Errorf("RowsDone = %d, want 750", summary.RowsDone)
	}
	if summary.PartitionsWithProgress != 3 {
		t.Errorf("PartitionsWithProgress = %d, want 3", summary.PartitionsWithProgress)
	}
}

// itoa is a tiny local helper so the test stays free of strconv imports.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestFileState_ConfigHash(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.yaml")

	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	// Create a run with config (hash will be computed)
	config := map[string]interface{}{
		"source": map[string]string{"host": "localhost"},
		"target": map[string]string{"host": "postgres"},
	}
	err = fs.CreateRun("hash123", "dbo", "public", config, "", "/path/to/config.yaml")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Get the run and verify config hash is set
	run, err := fs.GetLastIncompleteRun()
	if err != nil {
		t.Fatalf("GetLastIncompleteRun: %v", err)
	}
	if run == nil {
		t.Fatal("expected incomplete run")
	}
	if run.ConfigHash == "" {
		t.Error("expected config hash to be set")
	}
	t.Logf("Config hash: %s", run.ConfigHash)

	// Verify hash is deterministic (same config = same hash)
	fs2, _ := NewFileState(filepath.Join(tmpDir, "state2.yaml"))
	fs2.CreateRun("hash456", "dbo", "public", config, "", "")
	run2, _ := fs2.GetLastIncompleteRun()
	if run.ConfigHash != run2.ConfigHash {
		t.Errorf("config hashes differ for same config: %s != %s", run.ConfigHash, run2.ConfigHash)
	}

	// Verify different config = different hash
	config2 := map[string]interface{}{
		"source": map[string]string{"host": "other-host"},
		"target": map[string]string{"host": "postgres"},
	}
	fs3, _ := NewFileState(filepath.Join(tmpDir, "state3.yaml"))
	fs3.CreateRun("hash789", "dbo", "public", config2, "", "")
	run3, _ := fs3.GetLastIncompleteRun()
	if run.ConfigHash == run3.ConfigHash {
		t.Errorf("config hashes should differ for different configs: %s == %s", run.ConfigHash, run3.ConfigHash)
	}
}

func TestFileState_LoadExisting(t *testing.T) {
	// Create temp file with existing state
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.yaml")

	// Write existing state
	existingState := `run_id: existing123
started_at: 2025-12-20T10:00:00Z
status: running
source_schema: dbo
target_schema: public
tables:
  transfer:dbo.Users:
    status: success
    task_id: 1001
  transfer:dbo.Posts:
    status: running
    last_pk: 5000
    rows_done: 25000
    rows_total: 50000
    task_id: 1002
`
	if err := os.WriteFile(stateFile, []byte(existingState), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Load state
	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	// Check run
	run, err := fs.GetLastIncompleteRun()
	if err != nil {
		t.Fatalf("GetLastIncompleteRun: %v", err)
	}
	if run == nil {
		t.Fatal("expected incomplete run")
	}
	if run.ID != "existing123" {
		t.Errorf("run ID = %q, want %q", run.ID, "existing123")
	}

	// Check completed tables
	completed, err := fs.GetCompletedTables("existing123")
	if err != nil {
		t.Fatalf("GetCompletedTables: %v", err)
	}
	if !completed["transfer:dbo.Users"] {
		t.Error("expected Users to be completed")
	}
	if completed["transfer:dbo.Posts"] {
		t.Error("expected Posts to NOT be completed")
	}

	// Check run stats
	total, pending, running, success, failed, err := fs.GetRunStats("existing123")
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if success != 1 {
		t.Errorf("success = %d, want 1", success)
	}
	if running != 1 {
		t.Errorf("running = %d, want 1", running)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0", pending)
	}
	if failed != 0 {
		t.Errorf("failed = %d, want 0", failed)
	}
}

// TestAtomicWriteFile_CreatesMissingDir covers the directory-missing
// branch of #254's acceptance criteria: NewFileState may be pointed
// at a nested path whose parent dirs don't exist yet. atomicWriteFile
// must MkdirAll them with safe permissions. POSIX-only perm checks
// run only on Linux/macOS since Windows ignores the mode bits passed
// to MkdirAll and Mode().Perm() doesn't reliably reflect ACLs.
func TestAtomicWriteFile_CreatesMissingDir(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "missing", "subdir", "state.yaml")

	if err := atomicWriteFile(nested, []byte("hello"), 0600); err != nil {
		t.Fatalf("atomicWriteFile into missing dir: %v", err)
	}

	got, err := os.ReadFile(nested)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("contents = %q, want %q", got, "hello")
	}

	if runtime.GOOS == "windows" {
		return // perm assertions below are POSIX-only
	}

	// Final file has the requested perms.
	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("file perm = %o, want 0600", info.Mode().Perm())
	}

	// Parent dir has the restrictive perms MkdirAll requested.
	dirInfo, err := os.Stat(filepath.Dir(nested))
	if err != nil {
		t.Fatalf("stat parent: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("parent dir perm = %o, want 0700", dirInfo.Mode().Perm())
	}
}

// TestAtomicWriteFile_FailedWriteLeavesExistingFileIntact simulates
// the "crash mid-write" failure mode #254 cares about. Failure is
// injected by chmod'ing the directory read-only so CreateTemp fails;
// the safety property to verify is that the previous file's
// contents are left intact, not torn. Skipped where chmod can't
// enforce that restriction (root, Windows).
func TestAtomicWriteFile_FailedWriteLeavesExistingFileIntact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod 0500 doesn't restrict writes on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks; failure injection won't fire")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "state.yaml")

	if err := os.WriteFile(path, []byte("ORIGINAL"), 0600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	err := atomicWriteFile(path, []byte("NEW DATA"), 0600)
	if err == nil {
		t.Fatal("atomicWriteFile succeeded into a read-only dir; expected error")
	}

	// Restore so we can read the file back.
	_ = os.Chmod(dir, 0700)

	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read back: %v", readErr)
	}
	if string(got) != "ORIGINAL" {
		t.Errorf("existing file was modified or truncated despite failed write: got %q, want %q", got, "ORIGINAL")
	}
}

// TestAtomicWriteFile_NoLeftoverTempFiles guards the success path:
// after a clean write, no .tmp siblings should be left behind. The
// failure-path cleanup is covered indirectly by
// TestAtomicWriteFile_FailedWriteLeavesExistingFileIntact, which
// exercises the defer-Remove on a CreateTemp-failure code path.
func TestAtomicWriteFile_NoLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.yaml")

	if err := atomicWriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if e.Name() == filepath.Base(path) {
			continue
		}
		t.Errorf("unexpected leftover file: %s", e.Name())
	}
}

// TestFileState_SyncTimestamps_BasicLifecycle is the core #255
// regression guard: the file backend's Get/UpdateSyncTimestamp
// pair used to be no-ops, silently degrading date-based
// incremental sync to a full sync on every run. Mirrors the
// shape of the SQLite test in state_test.go.
func TestFileState_SyncTimestamps_BasicLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileState(filepath.Join(tmpDir, "state.yaml"))
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	const sourceSchema = "dbo"
	const tableName = "Orders"
	const targetSchema = "public"

	// First lookup returns no timestamp.
	ts, err := fs.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp (pre-update): %v", err)
	}
	if ts != nil {
		t.Errorf("expected nil timestamp pre-sync, got %v", ts)
	}

	syncTime := time.Date(2026, 5, 12, 10, 30, 0, 123456789, time.UTC)
	if err := fs.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, syncTime); err != nil {
		t.Fatalf("UpdateSyncTimestamp: %v", err)
	}

	ts, err = fs.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp (post-update): %v", err)
	}
	if ts == nil {
		t.Fatal("expected non-nil timestamp after update")
	}
	if !ts.Equal(syncTime) {
		t.Errorf("timestamp mismatch: got %v, want %v", *ts, syncTime)
	}

	// Newer timestamp overwrites.
	newer := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	if err := fs.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, newer); err != nil {
		t.Fatalf("UpdateSyncTimestamp (overwrite): %v", err)
	}
	ts, err = fs.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp (post-overwrite): %v", err)
	}
	if ts == nil {
		t.Fatal("expected non-nil timestamp after overwrite")
	}
	if !ts.Equal(newer) {
		t.Errorf("timestamp not overwritten: got %v, want %v", *ts, newer)
	}

	// Different table → no timestamp.
	ts, _ = fs.GetLastSyncTimestamp(sourceSchema, "OtherTable", targetSchema)
	if ts != nil {
		t.Errorf("different table should have nil timestamp, got %v", ts)
	}

	// Different target schema → no timestamp (same source table can
	// be synced to multiple targets independently).
	ts, _ = fs.GetLastSyncTimestamp(sourceSchema, tableName, "other_schema")
	if ts != nil {
		t.Errorf("different target schema should have nil timestamp, got %v", ts)
	}
}

// TestFileState_SyncTimestamps_NoDelimiterCollision guards against
// the Codex review finding on this PR: an earlier flat-key encoding
// joined the (source schema, table, target schema) triple with '|',
// which collided on quoted identifiers containing pipes. The
// nested-map encoding makes the storage unambiguous regardless of
// identifier contents.
func TestFileState_SyncTimestamps_NoDelimiterCollision(t *testing.T) {
	fs, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	// Two distinct triples that would collide under a "join with |"
	// encoding: "a|b|c|d" results from ("a", "b|c", "d") AND from
	// ("a|b", "c", "d"). The two writes must remain independent.
	tsA := time.Date(2026, 5, 12, 8, 0, 0, 0, time.UTC)
	tsB := time.Date(2026, 5, 13, 8, 0, 0, 0, time.UTC)
	if err := fs.UpdateSyncTimestamp("a", "b|c", "d", tsA); err != nil {
		t.Fatalf("UpdateSyncTimestamp A: %v", err)
	}
	if err := fs.UpdateSyncTimestamp("a|b", "c", "d", tsB); err != nil {
		t.Fatalf("UpdateSyncTimestamp B: %v", err)
	}

	got, _ := fs.GetLastSyncTimestamp("a", "b|c", "d")
	if got == nil || !got.Equal(tsA) {
		t.Errorf("triple A: got %v, want %v", got, tsA)
	}
	got, _ = fs.GetLastSyncTimestamp("a|b", "c", "d")
	if got == nil || !got.Equal(tsB) {
		t.Errorf("triple B: got %v, want %v", got, tsB)
	}
}

// TestFileState_SyncTimestamps_SurviveCreateRun is the realistic
// end-to-end flow Codex flagged on the first #255 PR commit:
// invocation #2 calls NewFileState (loading the YAML), then the
// orchestrator calls CreateRun *before* any GetLastSyncTimestamp.
// CreateRun rebuilds fileStateData; without explicit carry-over
// of SyncTimestamps the loaded map gets dropped and incremental
// sync silently degrades to full again on every run.
func TestFileState_SyncTimestamps_SurviveCreateRun(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.yaml")

	// Run #1: update timestamp, then complete.
	fs1, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #1: %v", err)
	}
	if err := fs1.CreateRun("run-1", "dbo", "public", map[string]any{"k": "v"}, "", ""); err != nil {
		t.Fatalf("CreateRun #1: %v", err)
	}
	want := time.Date(2026, 5, 12, 8, 0, 0, 987654321, time.UTC)
	if err := fs1.UpdateSyncTimestamp("dbo", "Orders", "public", want); err != nil {
		t.Fatalf("UpdateSyncTimestamp: %v", err)
	}

	// Run #2: simulate orchestrator flow — reopen, CreateRun
	// (this is where the bug manifested), then GetLastSyncTimestamp.
	fs2, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #2: %v", err)
	}
	if err := fs2.CreateRun("run-2", "dbo", "public", map[string]any{"k": "v"}, "", ""); err != nil {
		t.Fatalf("CreateRun #2: %v", err)
	}

	got, err := fs2.GetLastSyncTimestamp("dbo", "Orders", "public")
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp: %v", err)
	}
	if got == nil {
		t.Fatal("sync timestamp was wiped by CreateRun — #255 regression Codex flagged")
	}
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", *got, want)
	}
}

// TestFileState_SyncTimestamps_PersistAcrossReopen is the second
// half of the #255 contract: stored timestamps must round-trip
// through the YAML file. This is what makes the recommended
// Airflow/k8s file backend actually deliver delta-sync runs on
// the second invocation instead of silently doing a full copy.
func TestFileState_SyncTimestamps_PersistAcrossReopen(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.yaml")

	fs1, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #1: %v", err)
	}
	want := time.Date(2026, 5, 12, 8, 0, 0, 987654321, time.UTC)
	if err := fs1.UpdateSyncTimestamp("dbo", "Users", "public", want); err != nil {
		t.Fatalf("UpdateSyncTimestamp: %v", err)
	}

	// Reopen — simulates a second run picking up where the first
	// left off (the Airflow/k8s invocation pattern).
	fs2, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #2: %v", err)
	}
	got, err := fs2.GetLastSyncTimestamp("dbo", "Users", "public")
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp after reopen: %v", err)
	}
	if got == nil {
		t.Fatal("sync timestamp was lost across reopen — #255 regression")
	}
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", *got, want)
	}
}
