package orchestrator

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/orchestrator/validation"
)

func TestReviewValidationWithAIUnavailableFallback(t *testing.T) {
	orch := &Orchestrator{
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient { return nil }},
		config: &config.Config{
			Migration: config.MigrationConfig{
				TargetMode: "drop_recreate",
			},
		},
	}
	result := &ValidationRunResult{
		Mode:   "count_only",
		Failed: true,
		Error:  "validation failed password=secret",
		Rows: []ValidationRowCountResult{{
			TableName:   "public.orders",
			SourceCount: 10,
			TargetCount: 9,
			Difference:  1,
			Failed:      true,
		}},
	}

	review := orch.ReviewValidationWithAI(context.Background(), result, resultError("validation failed password=secret"))
	if review == nil {
		t.Fatal("ReviewValidationWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Kind != aicopilot.TriageKindValidationMismatch || review.Impact != aicopilot.TriageImpactAttention {
		t.Fatalf("kind/impact = %q/%q", review.Kind, review.Impact)
	}
	var hasWatermarkFact bool
	for _, fact := range review.DeterministicFacts {
		if fact.Category == "validation."+aicopilot.ValidationCategoryWatermarkIssue {
			hasWatermarkFact = true
		}
		if strings.Contains(fact.Detail, "secret") {
			t.Fatalf("fallback leaked secret in deterministic fact: %+v", fact)
		}
	}
	if !hasWatermarkFact {
		t.Fatalf("deterministic facts did not include grouped validation mismatch: %+v", review.DeterministicFacts)
	}
}

func TestReviewValidationWithAIDeepValidationOnlyDoesNotInventCountFacts(t *testing.T) {
	orch := &Orchestrator{
		opts:   Options{AIReviewClientFactory: func() aicopilot.TextClient { return nil }},
		config: &config.Config{},
	}
	result := &ValidationRunResult{
		Mode:   "sample",
		Failed: true,
		Error:  "deep validation failed",
		Deep: validation.Result{Tables: []validation.TableResult{{
			TableName: "public.orders",
			Passes: []validation.PassResult{{
				Pass:   "sample_row",
				Status: "fail",
				Detail: "timestamp value mismatch",
				Findings: []validation.Finding{{
					Pass:     "sample_row",
					Table:    "public.orders",
					Column:   "created_at",
					Severity: "error",
					Detail:   "timestamp value mismatch",
				}},
			}},
		}}},
	}

	review := orch.ReviewValidationWithAI(context.Background(), result, resultError("deep validation failed"))
	if review == nil {
		t.Fatal("ReviewValidationWithAI() returned nil")
	}
	for _, fact := range review.DeterministicFacts {
		if fact.Category == "validation.counts" {
			t.Fatalf("deep-only validation should not invent row-count facts: %+v", review.DeterministicFacts)
		}
	}
	if review.DeterministicFacts == nil {
		t.Fatal("expected deterministic validation facts")
	}
}

func TestReviewValidationWithAIRowCountRuntimeFailureDoesNotInventCountFacts(t *testing.T) {
	orch := &Orchestrator{opts: Options{AIReviewClientFactory: func() aicopilot.TextClient { return nil }}, config: &config.Config{}}
	result := &ValidationRunResult{
		Mode:   "count_only",
		Failed: true,
		Error:  "source count query timed out",
		Rows: []ValidationRowCountResult{{
			TableName: "public.orders",
			Error:     "source count query timed out",
			Failed:    true,
			TimedOut:  true,
		}},
	}

	review := orch.ReviewValidationWithAI(context.Background(), result, resultError("source count query timed out"))
	if review == nil {
		t.Fatal("ReviewValidationWithAI() returned nil")
	}
	for _, fact := range review.DeterministicFacts {
		if fact.Category == "validation.counts" {
			t.Fatalf("runtime count failure should not invent count facts: %+v", review.DeterministicFacts)
		}
		if fact.Category == "validation."+aicopilot.ValidationCategoryValidationRuntime && strings.Contains(fact.Detail, "source_count=0") {
			t.Fatalf("runtime count failure should not emit zero-count detail: %+v", fact)
		}
	}
}

func TestNewDiagnosticsWithOptionsDoesNotOpenDatabasePools(t *testing.T) {
	orch, err := NewDiagnosticsWithOptions(&config.Config{
		Source: config.SourceConfig{Type: "unsupported-source"},
		Target: config.TargetConfig{Type: "unsupported-target"},
		Migration: config.MigrationConfig{
			DataDir: t.TempDir(),
		},
	}, Options{})
	if err != nil {
		t.Fatalf("NewDiagnosticsWithOptions() error = %v", err)
	}
	defer orch.Close()
	if orch.sourcePool != nil || orch.targetPool != nil {
		t.Fatalf("diagnostics constructor opened database pools: source=%v target=%v", orch.sourcePool != nil, orch.targetPool != nil)
	}
	if orch.state == nil {
		t.Fatal("diagnostics constructor did not initialize checkpoint state")
	}
}

func TestTriageRunSkipsSuccessfulLatestRun(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New() error = %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("failed-run", "source", "target", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(failed-run) error = %v", err)
	}
	if err := state.CompleteRun("failed-run", "failed", "copy failed"); err != nil {
		t.Fatalf("CompleteRun(failed-run) error = %v", err)
	}
	if err := state.CreateRun("success-run", "source", "target", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(success-run) error = %v", err)
	}
	if err := state.CompleteRun("success-run", "success", ""); err != nil {
		t.Fatalf("CompleteRun(success-run) error = %v", err)
	}

	orch := &Orchestrator{config: &config.Config{}, state: state}
	run, err := orch.triageRun("")
	if err != nil {
		t.Fatalf("triageRun() error = %v", err)
	}
	if run.ID != "failed-run" {
		t.Fatalf("triageRun() chose %q, want failed-run", run.ID)
	}
}

func TestTriageRunReturnsErrorWhenOnlySuccessfulRunsExist(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New() error = %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("success-run", "source", "target", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(success-run) error = %v", err)
	}
	if err := state.CompleteRun("success-run", "success", ""); err != nil {
		t.Fatalf("CompleteRun(success-run) error = %v", err)
	}

	orch := &Orchestrator{config: &config.Config{}, state: state}
	if _, err := orch.triageRun(""); err == nil || !strings.Contains(err.Error(), "no failed or incomplete runs") {
		t.Fatalf("triageRun() error = %v, want no failed/incomplete runs", err)
	}
}

func TestTriageRunRejectsExplicitSuccessfulRun(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New() error = %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("success-run", "source", "target", nil, "", ""); err != nil {
		t.Fatalf("CreateRun(success-run) error = %v", err)
	}
	if err := state.CompleteRun("success-run", "success", ""); err != nil {
		t.Fatalf("CompleteRun(success-run) error = %v", err)
	}

	orch := &Orchestrator{config: &config.Config{}, state: state}
	if _, err := orch.triageRun("success-run"); err == nil || !strings.Contains(err.Error(), "no failure to diagnose") {
		t.Fatalf("triageRun(success-run) error = %v, want no failure to diagnose", err)
	}
}

func TestDeepValidationTriageOmitsSampleRowDigestValues(t *testing.T) {
	orch := &Orchestrator{opts: Options{AIReviewClientFactory: func() aicopilot.TextClient { return nil }}, config: &config.Config{}}
	result := &ValidationRunResult{
		Mode:   "sample",
		Failed: true,
		Deep: validation.Result{Tables: []validation.TableResult{{
			TableName: "public.orders",
			Passes: []validation.PassResult{{
				Pass:   "sample_row",
				Status: "fail",
				Detail: "PK 42 row digests differ: src=abcdef123456... tgt=deadbeef456789...",
				Findings: []validation.Finding{{
					Pass:     "sample_row",
					Table:    "public.orders",
					Severity: "error",
					Detail:   "PK 42 row digests differ: src=abcdef123456... tgt=deadbeef456789...",
				}},
			}},
		}}},
	}

	facts := orch.validationMismatchFacts(result, nil)
	if len(facts.Differences) != 1 {
		t.Fatalf("differences len = %d, want 1", len(facts.Differences))
	}
	if len(facts.Passes) != 1 {
		t.Fatalf("passes len = %d, want 1", len(facts.Passes))
	}
	detail := facts.Differences[0].Detail
	if strings.Contains(detail, "src=") || strings.Contains(detail, "tgt=") || strings.Contains(detail, "abcdef") {
		t.Fatalf("sample-row digest detail leaked into triage: %q", detail)
	}
	if strings.Contains(facts.Passes[0].Detail, "src=") || strings.Contains(facts.Passes[0].Detail, "tgt=") || strings.Contains(facts.Passes[0].Detail, "abcdef") {
		t.Fatalf("sample-row digest pass detail leaked into triage: %q", facts.Passes[0].Detail)
	}
	if !strings.Contains(detail, "omitted") {
		t.Fatalf("sample-row digest detail should explain omission: %q", detail)
	}
}

func TestValidationMismatchFactsUsesCurrentRunForSchemaDrift(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New() error = %v", err)
	}
	defer state.Close()
	if err := state.CreateRun("run-1", "source", "target", nil, "", ""); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	orch := &Orchestrator{
		config: &config.Config{},
		state:  state,
	}
	orch.schemaEvolution().RestoreDecisions("run-1", []SchemaContractDecision{{
		Entity: "tables",
		Table:  "public.orders",
		Drift:  "added_table",
		Action: "report",
		Reason: "schema contract reported added table",
	}})
	facts := orch.validationMismatchFacts(&ValidationRunResult{
		Mode:   "count_only",
		Failed: true,
		Rows: []ValidationRowCountResult{{
			TableName:   "public.orders",
			SourceCount: 10,
			TargetCount: 9,
			Difference:  1,
			Failed:      true,
		}},
	}, nil)

	if facts.Checkpoint == nil || facts.Checkpoint.RunID != "run-1" {
		t.Fatalf("checkpoint fact = %+v, want current run", facts.Checkpoint)
	}
	if len(facts.SchemaDrift) != 1 || facts.SchemaDrift[0].Table != "public.orders" {
		t.Fatalf("schema drift facts = %+v, want current run decisions", facts.SchemaDrift)
	}
	for _, checkpoint := range facts.RecentCheckpoints {
		if checkpoint.RunID == "run-1" {
			t.Fatalf("recent checkpoints should exclude current run: %+v", facts.RecentCheckpoints)
		}
	}
}

func TestCategorizeDeepValidationDifferenceNullParity(t *testing.T) {
	got := categorizeDeepValidationDifference("null_parity", "deleted_at", "source NULL target non-NULL")
	if got != aicopilot.ValidationCategoryNullMismatch {
		t.Fatalf("category = %q, want %q", got, aicopilot.ValidationCategoryNullMismatch)
	}
}

type resultError string

func (e resultError) Error() string { return string(e) }
