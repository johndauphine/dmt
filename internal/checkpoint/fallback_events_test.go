package checkpoint

import (
	"path/filepath"
	"testing"
)

// TestSaveFallbackEvent_UpsertsPerSurfaceFingerprint pins the core
// UPSERT contract from #176: repeated calls for the same
// (run, surface, fingerprint) accumulate into a single row. This is
// what keeps a pathological migration (10K Raw columns) from
// exploding the table; one row per distinct fingerprint, not per
// occurrence.
func TestSaveFallbackEvent_UpsertsPerSurfaceFingerprint(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	const runID = "run-1"
	if err := state.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	// Same surface+fingerprint twice → one row, count=2
	mustSaveFallback(t, state, runID, "typemap", "postgres:inet")
	mustSaveFallback(t, state, runID, "typemap", "postgres:inet")
	// Same surface, different fingerprint → separate row
	mustSaveFallback(t, state, runID, "typemap", "mssql:hierarchyid")
	// Different surface → separate row
	mustSaveFallback(t, state, runID, "ddl", "raw_columns")
	// Empty fingerprint allowed; still one row per surface
	mustSaveFallback(t, state, runID, "errordiag", "")

	events, err := state.GetFallbackEventsByRun(runID)
	if err != nil {
		t.Fatalf("GetFallbackEventsByRun error: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("event rows = %d, want 4 (3 typemap fps collapsed to 2 rows)", len(events))
	}

	// Index by (surface, fingerprint) for assertions.
	byKey := map[string]FallbackEventRecord{}
	for _, e := range events {
		byKey[e.Surface+"|"+e.Fingerprint] = e
	}

	if got := byKey["typemap|postgres:inet"].Count; got != 2 {
		t.Errorf("typemap/postgres:inet count = %d, want 2 (UPSERT accumulator)", got)
	}
	if got := byKey["typemap|mssql:hierarchyid"].Count; got != 1 {
		t.Errorf("typemap/mssql:hierarchyid count = %d, want 1", got)
	}
	if got := byKey["ddl|raw_columns"].Count; got != 1 {
		t.Errorf("ddl/raw_columns count = %d, want 1", got)
	}
	if got := byKey["errordiag|"].Count; got != 1 {
		t.Errorf("errordiag/<empty> count = %d, want 1 (empty fingerprint is valid)", got)
	}

	// first_seen <= last_seen for the dup'd row
	dup := byKey["typemap|postgres:inet"]
	if dup.LastSeen.Before(dup.FirstSeen) {
		t.Errorf("typemap/postgres:inet last_seen %v < first_seen %v", dup.LastSeen, dup.FirstSeen)
	}
}

// TestGetFallbackEventsByRun_ScopedToRun proves the run_id scoping is
// real — two runs in the same state file must not leak counts to each
// other.
func TestGetFallbackEventsByRun_ScopedToRun(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("run-a", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(a) error: %v", err)
	}
	if err := state.CreateRun("run-b", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(b) error: %v", err)
	}

	mustSaveFallback(t, state, "run-a", "typemap", "postgres:inet")
	mustSaveFallback(t, state, "run-a", "typemap", "postgres:inet")
	mustSaveFallback(t, state, "run-b", "ddl", "raw_columns")

	a, err := state.GetFallbackEventsByRun("run-a")
	if err != nil {
		t.Fatalf("Get(a) error: %v", err)
	}
	b, err := state.GetFallbackEventsByRun("run-b")
	if err != nil {
		t.Fatalf("Get(b) error: %v", err)
	}
	if len(a) != 1 || a[0].Surface != "typemap" || a[0].Count != 2 {
		t.Errorf("run-a events = %+v, want one typemap row with count=2", a)
	}
	if len(b) != 1 || b[0].Surface != "ddl" || b[0].Count != 1 {
		t.Errorf("run-b events = %+v, want one ddl row with count=1", b)
	}
}

// TestSaveFallbackEvent_FileStateRoundTrip proves the Airflow-friendly
// file backend persists the same data as SQLite. This is the path
// codex flagged in review — separate-process `dmt status` polling
// must see the running migration's counts.
func TestSaveFallbackEvent_FileStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.yaml")
	fs, err := NewFileState(path)
	if err != nil {
		t.Fatalf("NewFileState error: %v", err)
	}
	defer fs.Close()

	const runID = "run-1"
	if err := fs.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	if err := fs.SaveFallbackEvent(runID, "typemap", "postgres:inet"); err != nil {
		t.Fatalf("SaveFallbackEvent(1) error: %v", err)
	}
	if err := fs.SaveFallbackEvent(runID, "typemap", "postgres:inet"); err != nil {
		t.Fatalf("SaveFallbackEvent(2) error: %v", err)
	}
	if err := fs.SaveFallbackEvent(runID, "ddl", "raw_columns"); err != nil {
		t.Fatalf("SaveFallbackEvent(3) error: %v", err)
	}

	// Reopen the file in a fresh FileState to simulate the cross-process
	// status read — this is the Airflow case the codex review flagged.
	fs2, err := NewFileState(path)
	if err != nil {
		t.Fatalf("NewFileState reopen error: %v", err)
	}
	defer fs2.Close()

	events, err := fs2.GetFallbackEventsByRun(runID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("event rows after reopen = %d, want 2", len(events))
	}
	byKey := map[string]int64{}
	for _, e := range events {
		byKey[e.Surface+"|"+e.Fingerprint] = e.Count
	}
	if byKey["typemap|postgres:inet"] != 2 {
		t.Errorf("typemap/postgres:inet count = %d, want 2 (UPSERT across writes)", byKey["typemap|postgres:inet"])
	}
	if byKey["ddl|raw_columns"] != 1 {
		t.Errorf("ddl/raw_columns count = %d, want 1", byKey["ddl|raw_columns"])
	}
}

// TestSaveFallbackEvent_IgnoresEmptyKeys covers the defensive paths
// so a buggy call site can't write nonsense rows.
func TestSaveFallbackEvent_IgnoresEmptyKeys(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("run-1", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	// Empty surface → dropped silently
	if err := state.SaveFallbackEvent("run-1", "", "fp"); err != nil {
		t.Errorf("SaveFallbackEvent(empty surface) = %v, want nil", err)
	}
	// Empty runID → dropped silently
	if err := state.SaveFallbackEvent("", "typemap", "fp"); err != nil {
		t.Errorf("SaveFallbackEvent(empty runID) = %v, want nil", err)
	}
	events, _ := state.GetFallbackEventsByRun("run-1")
	if len(events) != 0 {
		t.Errorf("event rows = %d, want 0 (empty-key writes must be dropped)", len(events))
	}
}

func mustSaveFallback(t *testing.T, state *State, runID, surface, fingerprint string) {
	t.Helper()
	if err := state.SaveFallbackEvent(runID, surface, fingerprint); err != nil {
		t.Fatalf("SaveFallbackEvent(%s,%s,%s) error: %v", runID, surface, fingerprint, err)
	}
}
