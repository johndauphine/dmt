package checkpoint

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStateDeleteReconciliationState(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	missing, err := state.GetDeleteReconciliationState("dbo", "public")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationState() error: %v", err)
	}
	if missing != nil {
		t.Fatalf("missing state = %#v, want nil", missing)
	}

	first := time.Date(2026, 5, 19, 10, 0, 0, 123, time.FixedZone("test", -5*3600))
	if err := state.RecordDeleteReconciliationSuccess("run-1", "dbo", "public", first); err != nil {
		t.Fatalf("RecordDeleteReconciliationSuccess(first) error: %v", err)
	}
	assertDeleteReconciliationState(t, state, "run-1", first.UTC())

	second := first.Add(2 * time.Hour)
	if err := state.RecordDeleteReconciliationSuccess("run-2", "dbo", "public", second); err != nil {
		t.Fatalf("RecordDeleteReconciliationSuccess(second) error: %v", err)
	}
	assertDeleteReconciliationState(t, state, "run-2", second.UTC())
}

func TestFileStateDeleteReconciliationState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.yaml")
	state, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState() error: %v", err)
	}

	completedAt := time.Date(2026, 5, 19, 15, 30, 0, 0, time.UTC)
	if err := state.RecordDeleteReconciliationSuccess("run-file", "dbo", "public", completedAt); err != nil {
		t.Fatalf("RecordDeleteReconciliationSuccess() error: %v", err)
	}

	reopened, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState(reopen) error: %v", err)
	}
	got, err := reopened.GetDeleteReconciliationState("dbo", "public")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationState() error: %v", err)
	}
	if got == nil {
		t.Fatal("GetDeleteReconciliationState() = nil, want state")
	}
	if got.LastRunID != "run-file" {
		t.Fatalf("LastRunID = %q, want run-file", got.LastRunID)
	}
	if !got.LastSuccessAt.Equal(completedAt) {
		t.Fatalf("LastSuccessAt = %s, want %s", got.LastSuccessAt, completedAt)
	}
}

func assertDeleteReconciliationState(
	t *testing.T,
	state *State,
	wantRunID string,
	wantLastSuccessAt time.Time,
) {
	t.Helper()

	got, err := state.GetDeleteReconciliationState("dbo", "public")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationState() error: %v", err)
	}
	if got == nil {
		t.Fatal("GetDeleteReconciliationState() = nil, want state")
	}
	if got.SourceSchema != "dbo" || got.TargetSchema != "public" {
		t.Fatalf("schema pair = %s/%s, want dbo/public", got.SourceSchema, got.TargetSchema)
	}
	if got.LastRunID != wantRunID {
		t.Fatalf("LastRunID = %q, want %q", got.LastRunID, wantRunID)
	}
	if !got.LastSuccessAt.Equal(wantLastSuccessAt) {
		t.Fatalf("LastSuccessAt = %s, want %s", got.LastSuccessAt, wantLastSuccessAt)
	}
	if !got.UpdatedAt.Equal(wantLastSuccessAt) {
		t.Fatalf("UpdatedAt = %s, want %s", got.UpdatedAt, wantLastSuccessAt)
	}
}
