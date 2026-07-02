package orchestrator

import (
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
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
