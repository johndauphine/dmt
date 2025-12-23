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
