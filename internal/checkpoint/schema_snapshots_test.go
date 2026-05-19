package checkpoint

import (
	"path/filepath"
	"testing"
)

func TestStateSchemaSnapshotsReturnLatestPerTable(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	if err := state.SaveSchemaSnapshot("run-1", "dbo", "Orders", `{"name":"Orders","version":1}`); err != nil {
		t.Fatalf("SaveSchemaSnapshot Orders: %v", err)
	}
	if err := state.SaveSchemaSnapshot("run-1", "dbo", "Users", `{"name":"Users","version":1}`); err != nil {
		t.Fatalf("SaveSchemaSnapshot Users v1: %v", err)
	}
	if err := state.SaveSchemaSnapshot("run-2", "dbo", "Users", `{"name":"Users","version":2}`); err != nil {
		t.Fatalf("SaveSchemaSnapshot Users v2: %v", err)
	}

	got, err := state.GetLatestSchemaSnapshots("dbo")
	if err != nil {
		t.Fatalf("GetLatestSchemaSnapshots: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("snapshots = %d, want 2: %+v", len(got), got)
	}
	if got[0].TableName != "Orders" || got[0].RunID != "run-1" {
		t.Fatalf("first snapshot = %+v, want Orders from run-1", got[0])
	}
	if got[1].TableName != "Users" || got[1].RunID != "run-2" || got[1].SchemaJSON != `{"name":"Users","version":2}` {
		t.Fatalf("second snapshot = %+v, want latest Users from run-2", got[1])
	}
}

func TestFileStateSchemaSnapshotsSurviveCreateRunAndReopen(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.yaml")

	fs1, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #1: %v", err)
	}
	if err := fs1.CreateRun("run-1", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun #1: %v", err)
	}
	if err := fs1.SaveSchemaSnapshot("run-1", "dbo", "Users", `{"name":"Users"}`); err != nil {
		t.Fatalf("SaveSchemaSnapshot: %v", err)
	}

	fs2, err := NewFileState(statePath)
	if err != nil {
		t.Fatalf("NewFileState #2: %v", err)
	}
	if err := fs2.CreateRun("run-2", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun #2: %v", err)
	}

	got, err := fs2.GetLatestSchemaSnapshots("dbo")
	if err != nil {
		t.Fatalf("GetLatestSchemaSnapshots: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("snapshots = %d, want 1: %+v", len(got), got)
	}
	if got[0].RunID != "run-1" || got[0].TableName != "Users" || got[0].SchemaJSON != `{"name":"Users"}` {
		t.Fatalf("snapshot = %+v, want run-1 Users", got[0])
	}
}
