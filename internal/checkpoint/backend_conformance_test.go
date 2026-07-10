package checkpoint

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type conformanceBackend struct {
	name                string
	open                func(t *testing.T) StateBackend
	wantRunHistory      bool
	wantConfigSnapshots bool
	wantProfiles        bool
	wantAIHistory       bool
}

func conformanceBackends() []conformanceBackend {
	return []conformanceBackend{
		{
			name: "sqlite",
			open: func(t *testing.T) StateBackend {
				t.Helper()
				state, err := New(t.TempDir())
				if err != nil {
					t.Fatalf("New() error: %v", err)
				}
				t.Cleanup(func() {
					if err := state.Close(); err != nil {
						t.Errorf("Close() error: %v", err)
					}
				})
				return state
			},
			wantRunHistory:      true,
			wantConfigSnapshots: true,
			wantProfiles:        true,
			wantAIHistory:       true,
		},
		{
			name: "file",
			open: func(t *testing.T) StateBackend {
				t.Helper()
				state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
				if err != nil {
					t.Fatalf("NewFileState() error: %v", err)
				}
				t.Cleanup(func() {
					if err := state.Close(); err != nil {
						t.Errorf("Close() error: %v", err)
					}
				})
				return state
			},
			wantRunHistory:      false,
			wantConfigSnapshots: false,
			wantProfiles:        false,
			wantAIHistory:       false,
		},
	}
}

// TestStateBackendConformanceIncrementalFence pins the #647 fence contract for
// both backends: absent until set, immutable once set for a run, read back
// exactly, and scoped per run.
func TestStateBackendConformanceIncrementalFence(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)

			const src, tbl, tgt = "dbo", "orders", "public"
			h1 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
			h2 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

			got, err := backend.GetIncrementalFence("run-1", src, tbl, tgt)
			if err != nil {
				t.Fatalf("GetIncrementalFence() before set: %v", err)
			}
			if got != nil {
				t.Fatalf("fence before set = %v, want nil", got)
			}

			if err := backend.SetIncrementalFence("run-1", src, tbl, tgt, h1); err != nil {
				t.Fatalf("SetIncrementalFence(): %v", err)
			}
			// Immutable once set for a run: a resume's re-set must not overwrite.
			if err := backend.SetIncrementalFence("run-1", src, tbl, tgt, h2); err != nil {
				t.Fatalf("SetIncrementalFence() second: %v", err)
			}
			got, err = backend.GetIncrementalFence("run-1", src, tbl, tgt)
			if err != nil {
				t.Fatalf("GetIncrementalFence() after set: %v", err)
			}
			if got == nil || !got.Equal(h1) {
				t.Fatalf("fence = %v, want immutable H1 %v", got, h1)
			}

			// Scoped per run: a different run has no fence yet.
			other, err := backend.GetIncrementalFence("run-2", src, tbl, tgt)
			if err != nil {
				t.Fatalf("GetIncrementalFence(run-2): %v", err)
			}
			if other != nil {
				t.Fatalf("run-2 fence = %v, want nil (per-run scope)", other)
			}
		})
	}
}

func TestStateBackendCapabilities(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			caps := backend.Capabilities()

			if !caps.HasRequiredRestartability() {
				t.Fatalf("required restartability capabilities incomplete: %+v", caps)
			}
			if caps.RunHistory != tc.wantRunHistory {
				t.Fatalf("RunHistory = %v, want %v", caps.RunHistory, tc.wantRunHistory)
			}
			if caps.RunConfigSnapshots != tc.wantConfigSnapshots {
				t.Fatalf("RunConfigSnapshots = %v, want %v", caps.RunConfigSnapshots, tc.wantConfigSnapshots)
			}
			if caps.Profiles != tc.wantProfiles {
				t.Fatalf("Profiles = %v, want %v", caps.Profiles, tc.wantProfiles)
			}
			if _, ok := backend.(HistoryBackend); ok != caps.Profiles {
				t.Fatalf("HistoryBackend assertion = %v, Profiles capability = %v", ok, caps.Profiles)
			}
			if caps.RuntimeAdjustmentHistory != tc.wantAIHistory {
				t.Fatalf("RuntimeAdjustmentHistory = %v, want %v", caps.RuntimeAdjustmentHistory, tc.wantAIHistory)
			}
			if caps.TuningHistory != tc.wantAIHistory {
				t.Fatalf("TuningHistory = %v, want %v", caps.TuningHistory, tc.wantAIHistory)
			}
		})
	}
}

func TestStateBackendRejectsUnknownRequiredWriteTargets(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			if err := backend.CreateRun("known-run", "dbo", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			const unknownTaskID = int64(987654321)
			for operation, err := range map[string]error{
				"save progress":          backend.SaveTransferProgress(unknownTaskID, "orders", nil, int64(1), 1, 1, ""),
				"update status":          backend.UpdateTaskStatus(unknownTaskID, "success", ""),
				"clear progress":         backend.ClearTransferProgress(unknownTaskID),
				"complete run":           backend.CompleteRun("missing-run", "success", ""),
				"complete resumable run": backend.CompleteRunResumable("missing-run", "partial", "boom", "retry"),
				"abandon resumable run":  backend.AbandonRun("missing-run", "operator chose restart"),
			} {
				if err == nil {
					t.Errorf("%s accepted an unknown write target", operation)
				}
			}
		})
	}
}

func TestStateBackendConformanceRunLifecycle(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			const runID = "run-lifecycle"

			if err := backend.CreateRun(runID, "dbo", "public", map[string]any{"workers": 2}, "profile-a", "config.yaml"); err != nil {
				t.Fatalf("CreateRun() error: %v", err)
			}

			run, err := backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatalf("GetLastIncompleteRun() error: %v", err)
			}
			if run == nil {
				t.Fatal("GetLastIncompleteRun() = nil, want run")
			}
			if run.ID != runID || run.Status != "running" {
				t.Fatalf("incomplete run = (%q, %q), want (%q, running)", run.ID, run.Status, runID)
			}
			if !run.Resumable || run.ResumabilityReason == "" {
				t.Fatalf("new run resumability = (%v, %q), want true with reason", run.Resumable, run.ResumabilityReason)
			}
			if run.Phase != "initializing" {
				t.Fatalf("initial phase = %q, want initializing", run.Phase)
			}
			if run.SourceSchema != "dbo" || run.TargetSchema != "public" {
				t.Fatalf("schemas = %q -> %q, want dbo -> public", run.SourceSchema, run.TargetSchema)
			}
			if run.ConfigHash == "" {
				t.Fatal("ConfigHash is empty")
			}
			if run.LastHeartbeat.IsZero() {
				t.Fatal("LastHeartbeat is empty")
			}
			if run.ProfileName != "profile-a" || run.ConfigPath != "config.yaml" {
				t.Fatalf("profile/config path = %q/%q, want profile-a/config.yaml", run.ProfileName, run.ConfigPath)
			}

			heartbeat := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
			if err := backend.UpdateRunHeartbeat(runID, heartbeat); err != nil {
				t.Fatalf("UpdateRunHeartbeat() error: %v", err)
			}
			run, err = backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatalf("GetLastIncompleteRun() after heartbeat error: %v", err)
			}
			if run == nil || run.LastHeartbeat.IsZero() || run.LastHeartbeat.Sub(heartbeat).Abs() > time.Second {
				t.Fatalf("LastHeartbeat after update = %v, want near %v", run.LastHeartbeat, heartbeat)
			}

			if err := backend.UpdatePhase(runID, "transferring"); err != nil {
				t.Fatalf("UpdatePhase() error: %v", err)
			}
			run, err = backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatalf("GetLastIncompleteRun() after phase error: %v", err)
			}
			if run == nil || run.Phase != "transferring" {
				t.Fatalf("phase after UpdatePhase = %#v, want transferring", run)
			}

			if err := backend.UpdateRunConfig(runID, map[string]any{"workers": 4}); err != nil {
				t.Fatalf("UpdateRunConfig() error: %v", err)
			}
			gotRun, err := backend.GetRunByID(runID)
			if err != nil {
				t.Fatalf("GetRunByID() error: %v", err)
			}
			if gotRun == nil || gotRun.ID != runID || gotRun.Status != "running" {
				t.Fatalf("GetRunByID() = %#v, want running run %q", gotRun, runID)
			}
			if backend.Capabilities().RunConfigSnapshots && !strings.Contains(gotRun.Config, `"workers":4`) {
				t.Fatalf("persisted config = %q, want updated workers snapshot", gotRun.Config)
			}

			if err := backend.CompleteRun(runID, "failed", "boom"); err != nil {
				t.Fatalf("CompleteRun() error: %v", err)
			}
			run, err = backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatalf("GetLastIncompleteRun() after complete error: %v", err)
			}
			if run != nil {
				t.Fatalf("GetLastIncompleteRun() after complete = %#v, want nil", run)
			}
			gotRun, err = backend.GetRunByID(runID)
			if err != nil {
				t.Fatalf("GetRunByID() after complete error: %v", err)
			}
			if gotRun == nil || gotRun.Status != "failed" || gotRun.Error != "boom" || gotRun.CompletedAt == nil {
				t.Fatalf("completed run = %#v, want failed run with error and completed_at", gotRun)
			}
			if gotRun.Resumable || gotRun.ResumabilityReason == "" {
				t.Fatalf("completed run resumability = (%v, %q), want false with reason", gotRun.Resumable, gotRun.ResumabilityReason)
			}
		})
	}
}

func TestStateBackendConformancePartialRunResumability(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)

			if err := backend.CreateRun("retry-partial", "dbo", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			if err := backend.CompleteRunResumable(
				"retry-partial",
				"partial",
				"one table failed",
				RunResumabilityPartialFailure,
			); err != nil {
				t.Fatalf("CompleteRunResumable: %v", err)
			}
			run, err := backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatal(err)
			}
			if run == nil || run.ID != "retry-partial" || run.Status != "partial" || !run.Resumable || run.CompletedAt == nil {
				t.Fatalf("resumable partial = %#v, want selectable completed partial", run)
			}

			if err := backend.MarkRunAsResumed(run.ID); err != nil {
				t.Fatalf("MarkRunAsResumed: %v", err)
			}
			run, err = backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatal(err)
			}
			if run == nil || run.Status != "running" || run.CompletedAt != nil || !run.Resumable {
				t.Fatalf("resumed transition = %#v, want active resumable run", run)
			}

			if err := backend.CompleteRunResumable(run.ID, "partial", "still failing", RunResumabilityPartialFailure); err != nil {
				t.Fatal(err)
			}
			if err := backend.AbandonRun(run.ID, "starting over from backup"); err != nil {
				t.Fatalf("AbandonRun: %v", err)
			}
			selected, err := backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatal(err)
			}
			if selected != nil {
				t.Fatalf("abandoned partial selected automatically: %#v", selected)
			}
			abandoned, err := backend.GetRunByID(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if abandoned == nil || abandoned.Status != "partial" || abandoned.Resumable || !strings.Contains(abandoned.ResumabilityReason, "starting over") {
				t.Fatalf("abandoned partial = %#v, want truthful partial/non-resumable outcome", abandoned)
			}

			if err := backend.CreateRun("accepted-partial", "dbo", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			if err := backend.CompleteRun("accepted-partial", "partial", "allowed table failure"); err != nil {
				t.Fatal(err)
			}
			selected, err = backend.GetLastIncompleteRun()
			if err != nil {
				t.Fatal(err)
			}
			if selected != nil {
				t.Fatalf("accepted partial selected automatically: %#v", selected)
			}
			accepted, err := backend.GetRunByID("accepted-partial")
			if err != nil {
				t.Fatal(err)
			}
			if accepted == nil || accepted.Resumable || accepted.ResumabilityReason != RunResumabilityAllowedPartial {
				t.Fatalf("accepted partial = %#v, want explicit non-resumable reason", accepted)
			}
		})
	}
}

func TestStateBackendConformanceTaskLifecycle(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			const runID = "run-tasks"
			const transferKey = "transfer:dbo.Users"

			if err := backend.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
				t.Fatalf("CreateRun() error: %v", err)
			}
			taskID, err := backend.CreateTask(runID, "transfer", transferKey)
			if err != nil {
				t.Fatalf("CreateTask() error: %v", err)
			}
			duplicateID, err := backend.CreateTask(runID, "transfer", transferKey)
			if err != nil {
				t.Fatalf("CreateTask() duplicate error: %v", err)
			}
			if duplicateID != taskID {
				t.Fatalf("duplicate task id = %d, want %d", duplicateID, taskID)
			}

			assertRunStats(t, backend, runID, runStats{total: 1, pending: 1})

			if err := backend.UpdateTaskStatus(taskID, "running", ""); err != nil {
				t.Fatalf("UpdateTaskStatus(running) error: %v", err)
			}
			assertRunStats(t, backend, runID, runStats{total: 1, running: 1})

			if err := backend.MarkRunAsResumed(runID); err != nil {
				t.Fatalf("MarkRunAsResumed() error: %v", err)
			}
			assertRunStats(t, backend, runID, runStats{total: 1, pending: 1})

			if err := backend.UpdateTaskStatus(taskID, "failed", "bad rows"); err != nil {
				t.Fatalf("UpdateTaskStatus(failed) error: %v", err)
			}
			assertRunStats(t, backend, runID, runStats{total: 1, failed: 1})
			task := findTaskWithProgress(t, backend, runID, transferKey)
			if task.TaskType != "transfer" || task.Status != "failed" || task.ErrorMessage != "bad rows" {
				t.Fatalf("failed task = %+v, want transfer failed with error", task)
			}

			if err := backend.MarkTaskComplete(runID, transferKey); err != nil {
				t.Fatalf("MarkTaskComplete() error: %v", err)
			}
			completed, err := backend.GetCompletedTables(runID)
			if err != nil {
				t.Fatalf("GetCompletedTables() error: %v", err)
			}
			if !completed[transferKey] {
				t.Fatalf("completed tables = %#v, want %q", completed, transferKey)
			}
			if err := backend.UpdateTaskStatus(taskID, "pending", ""); err != nil {
				t.Fatalf("UpdateTaskStatus(pending) after success error: %v", err)
			}
			assertRunStats(t, backend, runID, runStats{total: 1, pending: 1})
			completed, err = backend.GetCompletedTables(runID)
			if err != nil {
				t.Fatalf("GetCompletedTables() after pending reset error: %v", err)
			}
			if completed[transferKey] {
				t.Fatalf("completed tables retained reset task: %#v", completed)
			}

			ddlID, err := backend.CreateTask(runID, "ddl", "ddl:indexes")
			if err != nil {
				t.Fatalf("CreateTask(ddl) error: %v", err)
			}
			if err := backend.UpdateTaskStatus(ddlID, "success", ""); err != nil {
				t.Fatalf("UpdateTaskStatus(ddl success) error: %v", err)
			}
			completed, err = backend.GetCompletedTables(runID)
			if err != nil {
				t.Fatalf("GetCompletedTables() after ddl error: %v", err)
			}
			if completed["ddl:indexes"] {
				t.Fatalf("GetCompletedTables() included non-transfer task: %#v", completed)
			}
		})
	}
}

func TestStateBackendConformanceTransferProgress(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			const runID = "run-progress"
			const taskKey = "transfer:dbo.Users"

			if err := backend.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
				t.Fatalf("CreateRun() error: %v", err)
			}
			taskID, err := backend.CreateTask(runID, "transfer", taskKey)
			if err != nil {
				t.Fatalf("CreateTask() error: %v", err)
			}

			if err := backend.SaveTransferProgress(taskID, "Users", nil, "pk-10", 10, 100, `[{"last":"pk-10"}]`); err != nil {
				t.Fatalf("SaveTransferProgress() error: %v", err)
			}
			progress, err := backend.GetTransferProgress(taskID)
			if err != nil {
				t.Fatalf("GetTransferProgress() error: %v", err)
			}
			if progress == nil {
				t.Fatal("GetTransferProgress() = nil, want progress")
			}
			if progress.TableName != "Users" || progress.LastPK != `"pk-10"` || progress.RowsDone != 10 || progress.RowsTotal != 100 {
				t.Fatalf("progress = %+v, want Users/\"pk-10\"/10/100", progress)
			}

			lastPK, rowsDone, rangeState, err := NewProgressSaver(backend).GetProgress(taskID)
			if err != nil {
				t.Fatalf("ProgressSaver.GetProgress() error: %v", err)
			}
			if lastPK != "pk-10" || rowsDone != 10 {
				t.Fatalf("ProgressSaver.GetProgress() = (%#v, %d), want (pk-10, 10)", lastPK, rowsDone)
			}
			// #464: the per-range watermark JSON must round-trip on both backends.
			if rangeState != `[{"last":"pk-10"}]` {
				t.Fatalf("ProgressSaver.GetProgress() rangeState = %q, want the saved JSON", rangeState)
			}

			task := findTaskWithProgress(t, backend, runID, taskKey)
			if task.RowsDone != 10 || task.RowsTotal != 100 {
				t.Fatalf("task progress = %+v, want rows 10/100", task)
			}

			if err := backend.ClearTransferProgress(taskID); err != nil {
				t.Fatalf("ClearTransferProgress() error: %v", err)
			}
			progress, err = backend.GetTransferProgress(taskID)
			if err != nil {
				t.Fatalf("GetTransferProgress() after clear error: %v", err)
			}
			if progress != nil {
				t.Fatalf("progress after clear = %+v, want nil", progress)
			}
		})
	}
}

func TestStateBackendConformancePartitionProgress(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			const runID = "run-partitions"
			const tableKey = "transfer:dbo.Users"

			if err := backend.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
				t.Fatalf("CreateRun() error: %v", err)
			}

			tableID, err := backend.CreateTask(runID, "transfer", tableKey)
			if err != nil {
				t.Fatalf("CreateTask(table) error: %v", err)
			}
			if err := backend.SaveTransferProgress(tableID, "Users", nil, 15, 15, 300, ""); err != nil {
				t.Fatalf("SaveTransferProgress(table) error: %v", err)
			}

			partitionIDs := make([]int64, 0, 3)
			for i, rowsDone := range []int64{100, 200, 0} {
				partition := i + 1
				taskID, err := backend.CreateTask(runID, "transfer", fmt.Sprintf("%s:p%d", tableKey, partition))
				if err != nil {
					t.Fatalf("CreateTask(partition %d) error: %v", partition, err)
				}
				partitionIDs = append(partitionIDs, taskID)

				var lastPK any = partition * 1000
				if rowsDone == 0 {
					lastPK = nil
				}
				if err := backend.SaveTransferProgress(taskID, "Users", &partition, lastPK, rowsDone, 300, ""); err != nil {
					t.Fatalf("SaveTransferProgress(partition %d) error: %v", partition, err)
				}
			}

			count, err := backend.CountPartitionTasks(runID, tableKey)
			if err != nil {
				t.Fatalf("CountPartitionTasks() error: %v", err)
			}
			if count != 3 {
				t.Fatalf("partition task count = %d, want 3", count)
			}
			summary, err := backend.GetPartitionTransferProgressSummary(runID, tableKey)
			if err != nil {
				t.Fatalf("GetPartitionTransferProgressSummary() error: %v", err)
			}
			if summary.RowsDone != 300 || summary.PartitionsWithProgress != 2 {
				t.Fatalf("partition summary = %+v, want rows=300 partitions=2", summary)
			}

			progress, err := backend.GetTransferProgress(partitionIDs[0])
			if err != nil {
				t.Fatalf("GetTransferProgress(partition) error: %v", err)
			}
			if progress == nil || progress.PartitionID == nil || *progress.PartitionID != 1 {
				t.Fatalf("partition progress = %+v, want partition_id=1", progress)
			}

			if err := backend.ClearPartitionTransferProgress(runID, tableKey); err != nil {
				t.Fatalf("ClearPartitionTransferProgress() error: %v", err)
			}
			summary, err = backend.GetPartitionTransferProgressSummary(runID, tableKey)
			if err != nil {
				t.Fatalf("GetPartitionTransferProgressSummary() after clear error: %v", err)
			}
			if summary.HasProgress() || summary.RowsDone != 0 {
				t.Fatalf("partition summary after clear = %+v, want empty", summary)
			}
			progress, err = backend.GetTransferProgress(tableID)
			if err != nil {
				t.Fatalf("GetTransferProgress(table) after partition clear error: %v", err)
			}
			if progress == nil || progress.RowsDone != 15 {
				t.Fatalf("table-level progress after partition clear = %+v, want rows_done=15", progress)
			}
		})
	}
}

func TestStateBackendConformanceSyncTimestamps(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			got, err := backend.GetLastSyncTimestamp("dbo", "Users", "public")
			if err != nil {
				t.Fatalf("GetLastSyncTimestamp() initial error: %v", err)
			}
			if got != nil {
				t.Fatalf("initial timestamp = %v, want nil", got)
			}

			want := time.Date(2026, 5, 19, 12, 34, 56, 789, time.UTC)
			if err := backend.UpdateSyncTimestamp("dbo", "Users", "public", want); err != nil {
				t.Fatalf("UpdateSyncTimestamp() error: %v", err)
			}
			got, err = backend.GetLastSyncTimestamp("dbo", "Users", "public")
			if err != nil {
				t.Fatalf("GetLastSyncTimestamp() error: %v", err)
			}
			if got == nil || !got.Equal(want) {
				t.Fatalf("timestamp = %v, want %v", got, want)
			}
		})
	}
}

func TestStateBackendConformancePersistentSafetyState(t *testing.T) {
	for _, tc := range conformanceBackends() {
		t.Run(tc.name, func(t *testing.T) {
			backend := tc.open(t)
			const runID = "run-safety-state"
			if err := backend.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
				t.Fatalf("CreateRun() error: %v", err)
			}

			reconciledAt := time.Date(2026, 5, 19, 13, 0, 0, 0, time.UTC)
			state, err := backend.GetDeleteReconciliationState("dbo", "public")
			if err != nil {
				t.Fatalf("GetDeleteReconciliationState() initial error: %v", err)
			}
			if state != nil {
				t.Fatalf("initial delete reconciliation state = %+v, want nil", state)
			}
			if err := backend.RecordDeleteReconciliationSuccess(runID, "dbo", "public", reconciledAt); err != nil {
				t.Fatalf("RecordDeleteReconciliationSuccess() error: %v", err)
			}
			state, err = backend.GetDeleteReconciliationState("dbo", "public")
			if err != nil {
				t.Fatalf("GetDeleteReconciliationState() error: %v", err)
			}
			if state == nil || state.LastRunID != runID || !state.LastSuccessAt.Equal(reconciledAt) {
				t.Fatalf("delete reconciliation state = %+v, want run %q at %v", state, runID, reconciledAt)
			}

			if err := backend.SaveDeleteReconciliationTable(runID, DeleteReconciliationTableRecord{
				TableName:     "Users",
				CandidateRows: 12,
				DeletedRows:   3,
			}); err != nil {
				t.Fatalf("SaveDeleteReconciliationTable(Users) error: %v", err)
			}
			if err := backend.SaveDeleteReconciliationTable(runID, DeleteReconciliationTableRecord{
				TableName:  "Logs",
				Skipped:    true,
				SkipReason: "no primary key",
			}); err != nil {
				t.Fatalf("SaveDeleteReconciliationTable(Logs) error: %v", err)
			}
			deleteTables, err := backend.GetDeleteReconciliationTables(runID)
			if err != nil {
				t.Fatalf("GetDeleteReconciliationTables() error: %v", err)
			}
			if len(deleteTables) != 2 || deleteTables[0].TableName != "Logs" || deleteTables[1].TableName != "Users" {
				t.Fatalf("delete reconciliation tables = %+v, want Logs then Users", deleteTables)
			}
			if !deleteTables[0].Skipped || deleteTables[0].SkipReason != "no primary key" {
				t.Fatalf("skipped delete table = %+v, want skip reason", deleteTables[0])
			}
			if deleteTables[1].CandidateRows != 12 || deleteTables[1].DeletedRows != 3 {
				t.Fatalf("Users delete table = %+v, want 12 candidates and 3 deleted", deleteTables[1])
			}

			if err := backend.SaveSchemaSnapshot(runID, "dbo", "Users", `{"columns":["id"]}`); err != nil {
				t.Fatalf("SaveSchemaSnapshot(Users) error: %v", err)
			}
			if err := backend.SaveSchemaSnapshot(runID, "dbo", "Logs", `{"columns":["id","msg"]}`); err != nil {
				t.Fatalf("SaveSchemaSnapshot(Logs) error: %v", err)
			}
			snapshots, err := backend.GetLatestSchemaSnapshots("dbo")
			if err != nil {
				t.Fatalf("GetLatestSchemaSnapshots() error: %v", err)
			}
			if len(snapshots) != 2 || snapshots[0].TableName != "Logs" || snapshots[1].TableName != "Users" {
				t.Fatalf("schema snapshots = %+v, want Logs then Users", snapshots)
			}

			if err := backend.SaveFallbackEvent(runID, "typemap", "postgres:inet"); err != nil {
				t.Fatalf("SaveFallbackEvent() first error: %v", err)
			}
			if err := backend.SaveFallbackEvent(runID, "typemap", "postgres:inet"); err != nil {
				t.Fatalf("SaveFallbackEvent() second error: %v", err)
			}
			events, err := backend.GetFallbackEventsByRun(runID)
			if err != nil {
				t.Fatalf("GetFallbackEventsByRun() error: %v", err)
			}
			if len(events) != 1 || events[0].Surface != "typemap" || events[0].Fingerprint != "postgres:inet" || events[0].Count != 2 {
				t.Fatalf("fallback events = %+v, want typemap/postgres:inet count 2", events)
			}
		})
	}
}

type runStats struct {
	total   int
	pending int
	running int
	success int
	failed  int
}

func assertRunStats(t *testing.T, backend StateBackend, runID string, want runStats) {
	t.Helper()
	total, pending, running, success, failed, err := backend.GetRunStats(runID)
	if err != nil {
		t.Fatalf("GetRunStats() error: %v", err)
	}
	got := runStats{
		total:   total,
		pending: pending,
		running: running,
		success: success,
		failed:  failed,
	}
	if got != want {
		t.Fatalf("run stats = %+v, want %+v", got, want)
	}
}

func findTaskWithProgress(t *testing.T, backend StateBackend, runID, taskKey string) TaskWithProgress {
	t.Helper()
	tasks, err := backend.GetTasksWithProgress(runID)
	if err != nil {
		t.Fatalf("GetTasksWithProgress() error: %v", err)
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].TaskKey < tasks[j].TaskKey
	})
	for _, task := range tasks {
		if task.TaskKey == taskKey {
			return task
		}
	}
	t.Fatalf("task %q not found in %+v", taskKey, tasks)
	return TaskWithProgress{}
}
