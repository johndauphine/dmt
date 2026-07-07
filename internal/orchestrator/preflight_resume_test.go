package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
)

// TestRunReachedTransfer pins the #623 ownership signal: a resume may skip the
// backup-acknowledgment gate only when the run it resumes reached the transfer
// phase (created the target tables). A run killed before transfer must report
// false so its resume still faces the gate — otherwise an unconfirmed
// drop_recreate could destroy pre-existing, unacknowledged target data. A run
// that reached transfer reports true even before its first checkpoint, because
// the transfer task row exists from job-build time (the early-interruption case
// of #623).
func TestRunReachedTransfer(t *testing.T) {
	newRun := func(t *testing.T, id string) *checkpoint.State {
		t.Helper()
		state, err := checkpoint.New(t.TempDir())
		if err != nil {
			t.Fatalf("checkpoint.New: %v", err)
		}
		t.Cleanup(func() { _ = state.Close() })
		if err := state.CreateRun(id, "src", "dst", nil, "", ""); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		return state
	}

	t.Run("no transfer task is gated", func(t *testing.T) {
		state := newRun(t, "run-a")
		// Killed during preflight/schema: only a non-transfer task exists.
		if _, err := state.CreateTask("run-a", "extract_schema", "extract_schema"); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		o := &Orchestrator{state: state}
		if o.runReachedTransfer("run-a") {
			t.Fatal("run that never reached transfer must report false (gate stays active)")
		}
	})

	t.Run("transfer task without progress owns target", func(t *testing.T) {
		state := newRun(t, "run-b")
		// Reached transfer (task created at job-build) but no checkpoint saved
		// yet — the early-interruption case must still count as owned.
		if _, err := state.CreateTask("run-b", "transfer", "transfer:src.events"); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		o := &Orchestrator{state: state}
		if !o.runReachedTransfer("run-b") {
			t.Fatal("run that reached transfer must report true even before its first checkpoint")
		}
	})

	t.Run("partial progress owns target", func(t *testing.T) {
		state := newRun(t, "run-c")
		taskID, err := state.CreateTask("run-c", "transfer", "transfer:src.events")
		if err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		if err := state.SaveTransferProgress(taskID, "src.events", nil, int64(500), 500, 2000, ""); err != nil {
			t.Fatalf("SaveTransferProgress: %v", err)
		}
		o := &Orchestrator{state: state}
		if !o.runReachedTransfer("run-c") {
			t.Fatal("run with partial transfer progress must report true")
		}
	})

	t.Run("completed table owns target", func(t *testing.T) {
		state := newRun(t, "run-d")
		if err := state.MarkTaskComplete("run-d", "transfer:src.events"); err != nil {
			t.Fatalf("MarkTaskComplete: %v", err)
		}
		o := &Orchestrator{state: state}
		if !o.runReachedTransfer("run-d") {
			t.Fatal("run with a completed transfer table must report true")
		}
	})
}
