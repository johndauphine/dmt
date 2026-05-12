package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
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
	err = fs.SaveTransferProgress(taskID, "Users", nil, 12345, 50000, 100000)
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
	err = fs.SaveTransferProgress(taskID, "Users", nil, 12345, 50000, 100000)
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
// must MkdirAll them with safe permissions.
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

// TestAtomicWriteFile_NoTornWritesOnFailure simulates the "crash
// mid-write" failure mode #254 cares about: instead of actually
// SIGKILLing a goroutine, we inject the failure by pre-creating an
// undeletable file at the target's temp-name pattern. The point is
// to verify the safety property the atomic pattern delivers: if the
// write fails partway, the existing file is left untouched, not
// truncated.
func TestAtomicWriteFile_FailedWriteLeavesExistingFileIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.yaml")

	if err := os.WriteFile(path, []byte("ORIGINAL"), 0600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Make the directory read-only so CreateTemp fails. This
	// simulates a midway permission/disk failure that would have
	// torn os.WriteFile but must leave atomicWriteFile a no-op.
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

// TestAtomicWriteFile_NoLeftoverTempFiles guards that the deferred
// cleanup actually runs. A successful write must not leave .tmp
// siblings; a failed write must not either.
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
