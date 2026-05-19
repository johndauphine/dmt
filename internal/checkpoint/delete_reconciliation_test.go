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

func TestStateDeleteReconciliationTables(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	for _, record := range []DeleteReconciliationTableRecord{
		{TableName: "dbo.logs", Skipped: true, SkipReason: "no primary key"},
		{TableName: "dbo.users", CandidateRows: 4, DeletedRows: 3},
		{TableName: "dbo.users", CandidateRows: 5, DeletedRows: 4},
	} {
		if err := state.SaveDeleteReconciliationTable("run-delete", record); err != nil {
			t.Fatalf("SaveDeleteReconciliationTable(%s) error: %v", record.TableName, err)
		}
	}

	records, err := state.GetDeleteReconciliationTables("run-delete")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationTables() error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].TableName != "dbo.logs" || !records[0].Skipped ||
		records[0].SkipReason != "no primary key" {
		t.Fatalf("logs record = %#v, want skipped no-primary-key record", records[0])
	}
	if records[1].TableName != "dbo.users" ||
		records[1].CandidateRows != 5 ||
		records[1].DeletedRows != 4 {
		t.Fatalf("users record = %#v, want overwritten 5/4 counts", records[1])
	}
	for _, record := range records {
		if record.RunID != "run-delete" {
			t.Fatalf("RunID = %q, want run-delete", record.RunID)
		}
		if record.UpdatedAt.IsZero() {
			t.Fatalf("UpdatedAt for %s is zero", record.TableName)
		}
	}
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

func TestFileStateDeleteReconciliationTables(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.yaml")
	state, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState() error: %v", err)
	}

	completedAt := time.Date(2026, 5, 19, 15, 30, 0, 0, time.UTC)
	if err := state.RecordDeleteReconciliationSuccess("run-prior", "dbo", "public", completedAt); err != nil {
		t.Fatalf("RecordDeleteReconciliationSuccess() error: %v", err)
	}
	if err := state.CreateRun("run-file", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(run-file) error: %v", err)
	}
	if err := state.SaveDeleteReconciliationTable("run-file", DeleteReconciliationTableRecord{
		TableName:     "dbo.users",
		CandidateRows: 2,
		DeletedRows:   1,
	}); err != nil {
		t.Fatalf("SaveDeleteReconciliationTable(users) error: %v", err)
	}
	if err := state.SaveDeleteReconciliationTable("run-file", DeleteReconciliationTableRecord{
		TableName:  "dbo.logs",
		Skipped:    true,
		SkipReason: "no primary key",
	}); err != nil {
		t.Fatalf("SaveDeleteReconciliationTable(logs) error: %v", err)
	}

	reopened, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState(reopen) error: %v", err)
	}
	records, err := reopened.GetDeleteReconciliationTables("run-file")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationTables() error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].TableName != "dbo.logs" || !records[0].Skipped ||
		records[0].SkipReason != "no primary key" {
		t.Fatalf("logs record = %#v, want skipped no-primary-key record", records[0])
	}
	if records[1].TableName != "dbo.users" ||
		records[1].CandidateRows != 2 ||
		records[1].DeletedRows != 1 {
		t.Fatalf("users record = %#v, want 2/1 counts", records[1])
	}

	if err := reopened.CreateRun("run-next", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(run-next) error: %v", err)
	}
	if err := reopened.SaveDeleteReconciliationTable("run-file", DeleteReconciliationTableRecord{
		TableName:     "dbo.stale",
		CandidateRows: 1,
		DeletedRows:   1,
	}); err == nil {
		t.Fatal("SaveDeleteReconciliationTable(stale run) error = nil, want mismatch error")
	}
	stillScheduled, err := reopened.GetDeleteReconciliationState("dbo", "public")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationState(after CreateRun) error: %v", err)
	}
	if stillScheduled == nil || stillScheduled.LastRunID != "run-prior" {
		t.Fatalf("carried scheduling state = %#v, want run-prior", stillScheduled)
	}
	oldRecords, err := reopened.GetDeleteReconciliationTables("run-file")
	if err != nil {
		t.Fatalf("GetDeleteReconciliationTables(old run) error: %v", err)
	}
	if len(oldRecords) != 0 {
		t.Fatalf("old run records after CreateRun = %d, want 0", len(oldRecords))
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
