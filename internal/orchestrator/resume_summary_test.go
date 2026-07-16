package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/source"
)

// TestSummaryRowsTransferredCountsOnlyThisResume pins #565 (incl. the review's
// crux): the resume summary/tuning must count only rows moved THIS resume — the
// checkpointed cumulative minus the baseline captured before this resume — so a
// table completed in the original run is excluded and a partially-transferred
// table contributes only its remaining fraction. Counting cumulative rows over
// a resume-only duration would inflate the throughput fed to ai_tuning_history.
func TestSummaryRowsTransferredCountsOnlyThisResume(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()
	o := &Orchestrator{state: state}

	if err := state.CreateRun("run-565", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	// Table A finished in the original run (rows_done 1000).
	taskA, err := state.CreateTask("run-565", "transfer", "transfer:dbo.a")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SaveTransferProgress(taskA, "a", nil, int64(1000), 1000, 1000, ""); err != nil {
		t.Fatal(err)
	}
	// Table B had moved 500 of 1000 before the crash.
	taskB, err := state.CreateTask("run-565", "transfer", "transfer:dbo.b")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SaveTransferProgress(taskB, "b", nil, int64(500), 500, 1000, ""); err != nil {
		t.Fatal(err)
	}

	// Baseline captured before this resume starts moving anything: 1000 + 500.
	baseline, err := o.transferredRowsFromState("run-565")
	if err != nil {
		t.Fatal(err)
	}
	if baseline != 1500 {
		t.Fatalf("baseline = %d, want 1500", baseline)
	}

	// This resume moves B's remaining 500 → B's cumulative rows_done reaches 1000.
	if err := state.SaveTransferProgress(taskB, "b", nil, int64(1000), 1000, 1000, ""); err != nil {
		t.Fatal(err)
	}

	// Rows THIS resume = (1000+1000) - 1500 = 500 — only B's fraction; A excluded.
	estimate := func() int64 {
		t.Error("estimate must not be used when checkpoint state has progress")
		return 999999
	}
	if got := o.summaryRowsTransferred("run-565", baseline, estimate); got != 500 {
		t.Fatalf("summaryRowsTransferred = %d, want 500 (this-resume fraction only; completed table A excluded)", got)
	}

	// No checkpointed rows for a run → fall back to the estimate.
	if got := o.summaryRowsTransferred("no-such-run", 0, func() int64 { return 42 }); got != 42 {
		t.Fatalf("fallback = %d, want 42", got)
	}
}

// A resume segment measures only unfinished tables, while applyTuning analyzes
// and saves the complete workload before completed tables are skipped. Even a
// clean segment must therefore stay out of tuning cohorts; transferAll's clean
// runtime metrics must not clear this run-scope exclusion.
func TestResumeSegmentTuningResultAlwaysExcludedFromLearning(t *testing.T) {
	base := &tuningResultState{}
	state := &atomicTuningResultState{tuningResultState: base}
	o := &Orchestrator{
		state:                           state,
		lastTuningRowID:                 42,
		excludeTuningResultFromLearning: true,
		lastRunAdjusted:                 false,
		lastSafetyProjected:             false,
	}

	// Mirror a clean TransferRunner result being stashed by transferAll. The
	// independent resume exclusion must survive these assignments.
	o.lastRunAdjusted = false
	o.lastSafetyProjected = false
	o.recordSuccessfulTuningResult(500, time.Second)

	if state.calls != 1 {
		t.Fatalf("atomic completion calls = %d, want 1", state.calls)
	}
	if !state.completion.AdjustedAtRuntime {
		t.Fatal("clean resume segment remained eligible for full-workload learning")
	}
}

func TestApplyTuningClearsPriorResumeSegmentExclusion(t *testing.T) {
	o := &Orchestrator{
		config:                          &config.Config{Migration: config.MigrationConfig{Tuning: "manual"}},
		excludeTuningResultFromLearning: true,
	}
	o.applyTuning(context.Background())
	if o.excludeTuningResultFromLearning {
		t.Fatal("fresh tuning attempt inherited prior resume-segment exclusion")
	}
}

// TestAbandonResumeAttemptKeepsRunResumable pins #566: a pre-transfer
// environmental failure (preflight, schema extraction) must NOT mark the run
// 'failed' — it must stay 'running' so GetLastIncompleteRun finds it on the
// next `dmt resume`. Marking it failed orphans all checkpointed progress.
func TestAbandonResumeAttemptKeepsRunResumable(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("run-566", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}

	noNotify := false
	o := &Orchestrator{
		state: state,
		config: &config.Config{
			Migration: config.MigrationConfig{
				Notify: config.NotifyConfig{OnFailure: &noNotify},
			},
		},
	}

	// Simulate the preflight/schema-extract failure handler.
	o.abandonResumeAttempt("run-566", errors.New("preflight: target unreachable"), time.Now())

	got, err := state.GetLastIncompleteRun()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "run-566" {
		t.Fatalf("run must stay resumable after a pre-transfer failure; GetLastIncompleteRun = %v", got)
	}
	if got.Status != "running" {
		t.Fatalf("run status = %q, want running", got.Status)
	}
}

func TestResumablePartialSchedulesOnlyFailedTable(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	const runID = "partial-two-tables"
	if err := state.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	succeeded := checkpoint.TransferTaskIdentity{Schema: "dbo", Table: "accounts"}
	failed := checkpoint.TransferTaskIdentity{Schema: "dbo", Table: "orders"}
	if _, err := state.CreateTransferTask(runID, succeeded); err != nil {
		t.Fatal(err)
	}
	failedTaskID, err := state.CreateTransferTask(runID, failed)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkTransferTaskComplete(runID, succeeded); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateTaskStatus(failedTaskID, "failed", "target rejected row"); err != nil {
		t.Fatal(err)
	}
	if err := state.CompleteRunResumable(
		runID,
		"partial",
		"one table failed",
		checkpoint.RunResumabilityPartialFailure,
	); err != nil {
		t.Fatal(err)
	}

	selected, err := state.GetLastIncompleteRun()
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected.ID != runID || selected.Status != "partial" || !selected.Resumable {
		t.Fatalf("selected run = %#v, want resumable partial", selected)
	}
	completed, err := state.GetCompletedTables(runID)
	if err != nil {
		t.Fatal(err)
	}
	tables := []source.Table{
		{Schema: "dbo", Name: "accounts", RowCount: 10},
		{Schema: "dbo", Name: "orders", RowCount: 20},
	}
	counted := make([]string, 0, 1)
	scheduled, skipped := selectResumeTables(
		tables,
		completed,
		func(table source.Table) string {
			return checkpoint.TransferTaskKeyForBackend(state, checkpoint.TransferTaskIdentity{
				Schema: table.Schema,
				Table:  table.Name,
			})
		},
		func(table source.Table) (int64, error) {
			counted = append(counted, table.Name)
			return table.RowCount, nil
		},
	)
	if len(scheduled) != 1 || scheduled[0].Name != "orders" {
		t.Fatalf("scheduled = %+v, want only failed orders table", scheduled)
	}
	if len(skipped) != 1 || skipped[0] != "accounts" {
		t.Fatalf("skipped = %v, want successful accounts table", skipped)
	}
	if len(counted) != 1 || counted[0] != "accounts" {
		t.Fatalf("target row-count checks = %v, want successful table only", counted)
	}
}

func TestPartialAllowPolicyControlsResumability(t *testing.T) {
	for _, tc := range []struct {
		name         string
		allowPartial bool
		resumable    bool
	}{
		{name: "default partial is resumable", resumable: true},
		{name: "allowed partial is accepted", allowPartial: true, resumable: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, err := checkpoint.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			if err := state.CreateRun("partial-policy", "dbo", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			o := &Orchestrator{
				state: state,
				config: &config.Config{Migration: config.MigrationConfig{
					AllowPartial: tc.allowPartial,
				}},
			}
			if err := o.completePartialRunRequired("partial-policy", "one table failed"); err != nil {
				t.Fatal(err)
			}
			run, err := state.GetRunByID("partial-policy")
			if err != nil {
				t.Fatal(err)
			}
			if run == nil || run.Status != "partial" || run.Resumable != tc.resumable {
				t.Fatalf("partial policy run = %#v, want resumable=%v", run, tc.resumable)
			}
		})
	}
}

func TestStatusAndHistoryExposeOutcomeAndResumability(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.CreateRun("status-partial", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := state.CompleteRunResumable(
		"status-partial",
		"partial",
		"one table failed",
		checkpoint.RunResumabilityPartialFailure,
	); err != nil {
		t.Fatal(err)
	}
	o := &Orchestrator{state: state}
	status, err := o.GetStatusResult()
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "partial" || !status.Resumable || status.ResumabilityReason != checkpoint.RunResumabilityPartialFailure {
		t.Fatalf("status result = %#v, want partial + resumable reason", status)
	}
	history, err := o.GetAllRuns()
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Status != "partial" || !history[0].Resumable || history[0].ResumabilityReason == "" {
		t.Fatalf("history = %#v, want partial + resumability", history)
	}
}

func TestPartialOutcomeDoesNotUseRunningHeartbeatGuard(t *testing.T) {
	o := &Orchestrator{}
	run := &checkpoint.Run{
		ID:              "legacy-partial",
		Status:          "partial",
		Resumable:       true,
		LastHeartbeat:   time.Now().UTC(),
		LeaseGeneration: 0,
	}
	if err := o.validateResumeHeartbeat(run, time.Now().UTC()); err != nil {
		t.Fatalf("validateResumeHeartbeat(partial) = %v, want lease-only ownership check", err)
	}
}

func TestAbandonResumeIsFencedAndPreservesPartialOutcome(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.CreateRun("abandon-partial", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := state.CompleteRunResumable(
		"abandon-partial",
		"partial",
		"one table failed",
		checkpoint.RunResumabilityPartialFailure,
	); err != nil {
		t.Fatal(err)
	}
	target := checkpoint.MigrationTarget{
		Driver:   "postgres",
		Host:     "db.example",
		Port:     5432,
		Database: "warehouse",
		Schema:   "public",
	}.Canonical()
	live, err := state.AcquireMigrationLease(target, "live-owner", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.BindRunLease("abandon-partial", live); err != nil {
		t.Fatal(err)
	}
	o := &Orchestrator{
		state: state,
		config: &config.Config{Target: config.TargetConfig{
			Type: "postgres", Host: "db.example", Port: 5432, Database: "warehouse", Schema: "public",
		}},
	}
	if _, err := o.AbandonResume("operator chose restart"); err == nil {
		t.Fatal("AbandonResume while live owner holds lease = nil, want rejection")
	}
	if err := state.ReleaseMigrationLease(live); err != nil {
		t.Fatal(err)
	}
	abandoned, err := o.AbandonResume("operator chose restart")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned == nil || abandoned.Status != "partial" || abandoned.Resumable || abandoned.CompletedAt == nil {
		t.Fatalf("abandoned = %#v, want preserved partial terminal outcome", abandoned)
	}
	selected, err := state.GetLastIncompleteRunForTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	if selected != nil {
		t.Fatalf("abandoned run remained selectable: %#v", selected)
	}
}
