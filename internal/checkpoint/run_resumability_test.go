package checkpoint

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteRunResumabilityUpgradeRecoversLegacyPartial(t *testing.T) {
	dataDir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE runs (
			id TEXT PRIMARY KEY,
			started_at TEXT NOT NULL,
			completed_at TEXT,
			status TEXT NOT NULL,
			source_schema TEXT NOT NULL,
			target_schema TEXT NOT NULL,
			config TEXT
		);
		INSERT INTO runs (id, started_at, completed_at, status, source_schema, target_schema)
		VALUES
			('legacy-running', '2026-01-01 00:00:00', NULL, 'running', 'dbo', 'public'),
			('legacy-success', '2026-01-02 00:00:00', '2026-01-02 00:01:00', 'success', 'dbo', 'public'),
			('legacy-partial', '2026-01-03 00:00:00', '2026-01-03 00:01:00', 'partial', 'dbo', 'public');
	`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := New(dataDir)
	if err != nil {
		t.Fatalf("New after legacy schema: %v", err)
	}
	defer state.Close()

	selected, err := state.GetLastIncompleteRun()
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected.ID != "legacy-partial" || !selected.Resumable {
		t.Fatalf("selected legacy run = %#v, want newest partial as resumable", selected)
	}
	for _, tc := range []struct {
		id        string
		resumable bool
	}{
		{id: "legacy-running", resumable: true},
		{id: "legacy-success", resumable: false},
		{id: "legacy-partial", resumable: true},
	} {
		run, err := state.GetRunByID(tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil || run.Resumable != tc.resumable || run.ResumabilityReason == "" {
			t.Errorf("GetRunByID(%s) = %#v, want resumable=%v with reason", tc.id, run, tc.resumable)
		}
	}
}

func TestFileStateRunResumabilityUpgradePersistsLegacyPartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	legacy := []byte(`run_id: legacy-partial
started_at: 2026-01-03T00:00:00Z
completed_at: 2026-01-03T00:01:00Z
status: partial
phase: validating
error: one table failed
source_schema: dbo
target_schema: public
tables: {}
`)
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}

	state, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := state.GetLastIncompleteRun()
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected.Status != "partial" || !selected.Resumable || selected.ResumabilityReason == "" {
		t.Fatalf("legacy YAML selection = %#v, want resumable partial", selected)
	}
	if err := state.CompleteRunResumable(selected.ID, "partial", selected.Error, "retry after source repair"); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := reopened.GetRunByID("legacy-partial")
	if err != nil {
		t.Fatal(err)
	}
	if persisted == nil || !persisted.Resumable || persisted.ResumabilityReason != "retry after source repair" {
		t.Fatalf("reopened YAML run = %#v, want persisted resumability", persisted)
	}
}
