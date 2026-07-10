package checkpoint

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStructuredTransferTaskIdentityBackendParity(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) StateBackend
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) StateBackend {
				state, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = state.Close() })
				return state
			},
		},
		{
			name: "file",
			open: func(t *testing.T) StateBackend {
				state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				return state
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := tt.open(t)
			if err := backend.CreateRun("run-646", "source", "target", nil, "", ""); err != nil {
				t.Fatalf("CreateRun: %v", err)
			}
			structured, ok := backend.(StructuredTaskBackend)
			if !ok {
				t.Fatalf("%T does not implement StructuredTaskBackend", backend)
			}

			partitionID := 1
			partition := TransferTaskIdentity{Schema: "dbo", Table: "orders", PartitionID: &partitionID}
			collidingLegacyTable := TransferTaskIdentity{Schema: "dbo", Table: "orders:p1"}
			partitionTaskID, err := structured.CreateTransferTask("run-646", partition)
			if err != nil {
				t.Fatalf("CreateTransferTask(partition): %v", err)
			}
			tableTaskID, err := structured.CreateTransferTask("run-646", collidingLegacyTable)
			if err != nil {
				t.Fatalf("CreateTransferTask(table): %v", err)
			}
			if partitionTaskID == tableTaskID {
				t.Fatalf("partition and quoted table collided at task ID %d", partitionTaskID)
			}
			duplicateID, err := structured.CreateTransferTask("run-646", partition)
			if err != nil || duplicateID != partitionTaskID {
				t.Fatalf("duplicate identity = (%d, %v), want (%d, nil)", duplicateID, err, partitionTaskID)
			}

			seenTaskIDs := map[int64]string{
				partitionTaskID: "dbo.orders partition 1",
				tableTaskID:     "dbo.orders:p1",
			}
			for _, table := range []string{"a.b", "a:b", "a%b", "a_b", `a\b`} {
				id, err := structured.CreateTransferTask("run-646", TransferTaskIdentity{Schema: "dbo", Table: table})
				if err != nil {
					t.Fatalf("special table %q identity = (%d, %v)", table, id, err)
				}
				if prior, exists := seenTaskIDs[id]; exists {
					t.Fatalf("special table %q collided with %s at task ID %d", table, prior, id)
				}
				seenTaskIDs[id] = "dbo." + table
			}

			if err := backend.SaveTransferProgress(partitionTaskID, "orders", &partitionID, int64(5), 5, 10, ""); err != nil {
				t.Fatalf("SaveTransferProgress(partition): %v", err)
			}
			if err := backend.SaveTransferProgress(tableTaskID, "orders:p1", nil, int64(9), 9, 10, ""); err != nil {
				t.Fatalf("SaveTransferProgress(table): %v", err)
			}
			count, err := structured.CountTransferPartitionTasks("run-646", "dbo", "orders")
			if err != nil || count != 1 {
				t.Fatalf("CountTransferPartitionTasks = (%d, %v), want (1, nil)", count, err)
			}
			summary, err := structured.GetTransferPartitionProgressSummary("run-646", "dbo", "orders")
			if err != nil || summary.RowsDone != 5 || summary.PartitionsWithProgress != 1 {
				t.Fatalf("GetTransferPartitionProgressSummary = (%+v, %v), want rows=5 partitions=1", summary, err)
			}
			if err := structured.ClearTransferPartitionProgress("run-646", "dbo", "orders"); err != nil {
				t.Fatalf("ClearTransferPartitionProgress: %v", err)
			}
			partitionProgress, _ := backend.GetTransferProgress(partitionTaskID)
			tableProgress, _ := backend.GetTransferProgress(tableTaskID)
			if partitionProgress != nil && partitionProgress.LastPK != "" && partitionProgress.LastPK != "null" {
				t.Fatalf("partition progress not cleared: %+v", partitionProgress)
			}
			if tableProgress == nil || tableProgress.RowsDone != 9 {
				t.Fatalf("quoted table progress was incorrectly cleared: %+v", tableProgress)
			}
			summary, err = structured.GetTransferPartitionProgressSummary("run-646", "dbo", "orders")
			if err != nil || summary.RowsDone != 0 || summary.PartitionsWithProgress != 0 {
				t.Fatalf("summary after exact cleanup = (%+v, %v), want zero", summary, err)
			}
		})
	}
}

func TestTransferTaskKeyRoundTrip(t *testing.T) {
	partitionID := 17
	want := TransferTaskIdentity{Schema: `odd.schema:%_\`, Table: `orders:p1.%_\`, PartitionID: &partitionID}
	key := want.TaskKey()
	got, ok := ParseTransferTaskKey(key)
	if !ok || got.Schema != want.Schema || got.Table != want.Table || !samePartitionID(got.PartitionID, want.PartitionID) {
		t.Fatalf("ParseTransferTaskKey(%q) = (%+v, %v), want %+v", key, got, ok, want)
	}
	if _, ok := ParseTransferTaskKey(want.LegacyTaskKey()); ok {
		t.Fatal("ambiguous legacy key was accepted as canonical")
	}
	if got := TransferTaskDisplayLabel(key); got != `odd.schema:%_\.orders:p1.%_\ (partition 17)` {
		t.Fatalf("TransferTaskDisplayLabel = %q", got)
	}
	nonCanonical := transferTaskKeyPrefix + base64.RawURLEncoding.EncodeToString([]byte(`{"table":"orders","schema":"dbo","partition_id":null}`))
	if _, ok := ParseTransferTaskKey(nonCanonical); ok {
		t.Fatal("alternate JSON encoding was accepted as canonical")
	}
}

func TestStructuredTransferTaskIdentityRejectsAmbiguousLegacyRun(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) StateBackend
	}{
		{name: "sqlite", open: func(t *testing.T) StateBackend {
			state, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = state.Close() })
			return state
		}},
		{name: "file", open: func(t *testing.T) StateBackend {
			state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			return state
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := tt.open(t)
			if err := backend.CreateRun("legacy-run", "source", "target", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			if _, err := backend.CreateTask("legacy-run", "transfer", "transfer:dbo.orders:p1"); err != nil {
				t.Fatal(err)
			}
			structured := backend.(StructuredTaskBackend)
			_, err := structured.CreateTransferTask("legacy-run", TransferTaskIdentity{Schema: "dbo", Table: "orders:p1"})
			if !errors.Is(err, ErrLegacyTaskIdentity) {
				t.Fatalf("CreateTransferTask error = %v, want ErrLegacyTaskIdentity", err)
			}
		})
	}
}

func TestStructuredTransferCompletionRequiresPlannedTask(t *testing.T) {
	for _, tt := range []struct {
		name string
		open func(*testing.T) StateBackend
	}{
		{name: "sqlite", open: func(t *testing.T) StateBackend {
			state, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = state.Close() })
			return state
		}},
		{name: "file", open: func(t *testing.T) StateBackend {
			state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			return state
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backend := tt.open(t)
			if err := backend.CreateRun("completion-run", "dbo", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			identity := TransferTaskIdentity{Schema: "dbo", Table: "orders"}
			if err := backend.(StructuredTaskBackend).MarkTransferTaskComplete("completion-run", identity); err == nil {
				t.Fatal("MarkTransferTaskComplete created a missing aggregate task")
			}
			if err := backend.(AtomicTransferCompletionBackend).CompleteTransferTask("completion-run", identity, "public", nil); err == nil {
				t.Fatal("CompleteTransferTask created a missing aggregate task")
			}
		})
	}
}

func TestSQLiteAtomicTransferCompletionRollsBackTaskWhenWatermarkFails(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.CreateRun("atomic-run", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	identity := TransferTaskIdentity{Schema: "dbo", Table: "orders"}
	if _, err := state.CreateTransferTask("atomic-run", identity); err != nil {
		t.Fatal(err)
	}
	if _, err := state.db.Exec(`
		CREATE TRIGGER fail_sync_watermark
		BEFORE INSERT ON table_sync_timestamps
		BEGIN SELECT RAISE(ABORT, 'injected watermark failure'); END;
	`); err != nil {
		t.Fatal(err)
	}
	watermark := time.Now().UTC()
	if err := state.CompleteTransferTask("atomic-run", identity, "public", &watermark); err == nil {
		t.Fatal("CompleteTransferTask error = nil, want injected watermark failure")
	}
	completed, err := state.GetCompletedTables("atomic-run")
	if err != nil {
		t.Fatal(err)
	}
	if completed[identity.TaskKey()] {
		t.Fatal("aggregate task completion survived rolled-back watermark transaction")
	}
	got, err := state.GetLastSyncTimestamp(identity.Schema, identity.Table, "public")
	if err != nil || got != nil {
		t.Fatalf("sync watermark = (%v, %v), want nil after rollback", got, err)
	}
}

func TestFileStateAtomicTransferCompletionLeavesPersistedTaskPendingOnSaveFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.yaml")
	state, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.CreateRun("atomic-run", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	identity := TransferTaskIdentity{Schema: "dbo", Table: "orders"}
	if _, err := state.CreateTransferTask("atomic-run", identity); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	state.path = filepath.Join(blocker, "state.yaml")
	watermark := time.Now().UTC()
	if err := state.CompleteTransferTask("atomic-run", identity, "public", &watermark); err == nil {
		t.Fatal("CompleteTransferTask error = nil, want atomic file-save failure")
	}

	reloaded, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := reloaded.GetCompletedTables("atomic-run")
	if err != nil {
		t.Fatal(err)
	}
	if completed[identity.TaskKey()] {
		t.Fatal("failed atomic file save persisted aggregate success")
	}
	got, err := reloaded.GetLastSyncTimestamp(identity.Schema, identity.Table, "public")
	if err != nil || got != nil {
		t.Fatalf("sync watermark = (%v, %v), want nil after failed atomic save", got, err)
	}
}

func TestSQLiteTaskIdentitySchemaUpgradePreservesCompletedHistory(t *testing.T) {
	dataDir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE runs (
			id TEXT PRIMARY KEY, started_at TEXT NOT NULL, completed_at TEXT,
			status TEXT NOT NULL, source_schema TEXT NOT NULL,
			target_schema TEXT NOT NULL, config TEXT
		);
		CREATE TABLE tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, task_type TEXT NOT NULL,
			task_key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
			started_at TEXT, completed_at TEXT, retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3, error_message TEXT,
			UNIQUE(run_id, task_key)
		);
		INSERT INTO runs (id, started_at, completed_at, status, source_schema, target_schema)
		VALUES ('old-complete', datetime('now'), datetime('now'), 'success', 'dbo', 'public');
		INSERT INTO tasks (run_id, task_type, task_key, status, completed_at)
		VALUES ('old-complete', 'transfer', 'transfer:dbo.orders', 'success', datetime('now'));
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
		t.Fatalf("New after old schema: %v", err)
	}
	defer state.Close()
	completed, err := state.GetCompletedTables("old-complete")
	if err != nil || !completed["transfer:dbo.orders"] {
		t.Fatalf("completed legacy history = (%v, %v), want preserved key", completed, err)
	}
	if err := state.CreateRun("new-run", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateTransferTask("new-run", TransferTaskIdentity{Schema: "dbo", Table: "orders:p1"}); err != nil {
		t.Fatalf("CreateTransferTask after schema upgrade: %v", err)
	}
}

func TestFileStateRepairsCanonicalIdentityFieldsOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	state, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.CreateRun("repair-run", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	partitionID := 3
	identity := TransferTaskIdentity{Schema: "dbo", Table: "a:b", PartitionID: &partitionID}
	state.state.Tables[identity.TaskKey()] = tableState{Status: "pending", TaskID: 1001}
	if err := state.save(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := reloaded.CreateTransferTask("repair-run", identity)
	if err != nil || id != 1001 {
		t.Fatalf("CreateTransferTask repaired canonical entry = (%d, %v), want (1001, nil)", id, err)
	}
	repaired := reloaded.state.Tables[identity.TaskKey()]
	if repaired.TaskSchema != identity.Schema || repaired.TaskTable != identity.Table || !samePartitionID(repaired.TaskPartitionID, identity.PartitionID) {
		t.Fatalf("repaired identity = %+v, want %+v", repaired, identity)
	}
}
