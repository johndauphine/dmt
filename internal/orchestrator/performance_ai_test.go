package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/transfer"
)

func TestBuildPerformanceExplanationPayloadOmitsEndpointIdentity(t *testing.T) {
	orch := &Orchestrator{config: &config.Config{
		Source: config.SourceConfig{
			Type:     "postgres",
			Host:     "source.internal",
			Port:     5432,
			Database: "source_prod",
			Schema:   "private_schema",
		},
		Target: config.TargetConfig{
			Type:     "mssql",
			Host:     "target.internal",
			Port:     1433,
			Database: "target_prod",
			Schema:   "dbo",
		},
		Migration: config.MigrationConfig{TargetMode: "upsert"},
	}}

	payload := orch.BuildPerformanceExplanationPayload(&driver.SmartConfigSuggestions{
		TotalTables:             4,
		TotalRows:               2000,
		AvgRowSizeBytes:         512,
		Workers:                 4,
		ChunkSizeRecommendation: 10000,
		Tier:                    "baseline",
		Reasoning:               "baseline selected workers=4",
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, leaked := range []string{"source.internal", "target.internal", "source_prod", "target_prod", "private_schema"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("payload leaked endpoint identity %q: %s", leaked, text)
		}
	}
	if !payload.Workload.SourceIdentityOmitted || !payload.Workload.TargetIdentityOmitted {
		t.Fatalf("identity omission flags = %+v", payload.Workload)
	}
	if payload.DeterministicKnobs.Workers != 4 || payload.Workload.TargetMode != "upsert" {
		t.Fatalf("payload lost deterministic facts: %+v", payload)
	}
}

func TestExplainPerformanceWithAIFallsBackWhenProviderUnavailable(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{
			AIReviewClientFactory: func() aicopilot.TextClient { return nil },
		},
	}
	explanation := orch.ExplainPerformanceWithAI(context.Background(), &driver.SmartConfigSuggestions{
		Tier:      "baseline",
		Reasoning: "baseline selected chunk_size=50000",
	})
	if explanation.Status != aicopilot.ReviewStatusUnavailable || explanation.Enabled {
		t.Fatalf("status/enabled = %q/%v", explanation.Status, explanation.Enabled)
	}
	if !strings.Contains(explanation.Summary, "unavailable") {
		t.Fatalf("summary = %q", explanation.Summary)
	}
}

func TestExplainPerformanceWithAIScrubsProviderErrors(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{
			AIReviewClientFactory: func() aicopilot.TextClient {
				return errorPerformanceClient{}
			},
		},
	}
	explanation := orch.ExplainPerformanceWithAI(context.Background(), &driver.SmartConfigSuggestions{
		Tier:      "baseline",
		Reasoning: "baseline selected workers=4",
	})
	if explanation.Status != aicopilot.ReviewStatusError || !explanation.Enabled {
		t.Fatalf("status/enabled = %q/%v", explanation.Status, explanation.Enabled)
	}
	if strings.Contains(explanation.Error, "sk-testaaaaaaaaaaaaaaaaaaaaaaaa") || !strings.Contains(explanation.Error, "[REDACTED]") {
		t.Fatalf("provider error was not scrubbed: %q", explanation.Error)
	}
}

func TestExplainPerformanceWithAISuccessPath(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{
			AIReviewClientFactory: func() aicopilot.TextClient {
				return successPerformanceClient{response: `{
  "summary": "The deterministic performance settings are explained.",
  "findings": [{
    "knob": "workers",
    "category": "baseline",
    "rationale": "Workers follows the deterministic payload.",
    "evidence": ["workers appears in deterministic_knobs"],
    "next_action": "Keep the deterministic value unless operator constraints change."
  }],
  "notes": ["No endpoint identity was included."]
}`}
			},
		},
	}

	explanation := orch.ExplainPerformanceWithAI(context.Background(), &driver.SmartConfigSuggestions{
		Workers:   4,
		Tier:      "baseline",
		Reasoning: "baseline selected workers=4",
	})
	if explanation.Status != aicopilot.ReviewStatusOK || !explanation.Enabled {
		t.Fatalf("status/enabled = %q/%v", explanation.Status, explanation.Enabled)
	}
	if explanation.Provider != "fake" || explanation.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", explanation.Provider, explanation.Model)
	}
	if len(explanation.Findings) != 1 || explanation.Findings[0].Knob != "workers" {
		t.Fatalf("findings = %+v", explanation.Findings)
	}
}

type errorPerformanceClient struct{}

func (errorPerformanceClient) CallAI(context.Context, string) (string, error) {
	return "", errors.New("request failed with api_key=sk-testaaaaaaaaaaaaaaaaaaaaaaaa")
}

func (errorPerformanceClient) ProviderName() string { return "fake" }

func (errorPerformanceClient) Model() string { return "fake-model" }

type successPerformanceClient struct {
	response string
}

func (c successPerformanceClient) CallAI(context.Context, string) (string, error) {
	return c.response, nil
}

func (successPerformanceClient) ProviderName() string { return "fake" }

func (successPerformanceClient) Model() string { return "fake-model" }

func TestSamePerformanceWorkloadRequiresExactIdentity(t *testing.T) {
	input := driver.AutoTuneInput{
		SourceHost:     "source.internal",
		SourcePort:     5432,
		SourceDatabase: "source_prod",
		SourceSchema:   "public",
		TargetHost:     "target.internal",
		TargetPort:     1433,
		TargetDatabase: "target_prod",
		TargetSchema:   "dbo",
	}
	row := checkpoint.TuningRecord{
		Timestamp:      time.Now(),
		SourceHost:     input.SourceHost,
		SourcePort:     input.SourcePort,
		SourceDatabase: input.SourceDatabase,
		SourceSchema:   input.SourceSchema,
		TargetHost:     input.TargetHost,
		TargetPort:     input.TargetPort,
		TargetDatabase: input.TargetDatabase,
		TargetSchema:   input.TargetSchema,
	}
	if !samePerformanceWorkload(input, row) {
		t.Fatal("expected exact workload identity to match")
	}
	row.TargetDatabase = "other_target"
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected different target database to be rejected")
	}
	row.TargetDatabase = input.TargetDatabase
	row.SourceHost = ""
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected incomplete persisted identity to be rejected")
	}
}

func TestScopedPerformanceAdjustmentsRequiresScopedRunConfig(t *testing.T) {
	input := driver.AutoTuneInput{
		SourceHost:     "source.internal",
		SourcePort:     5432,
		SourceDatabase: "source_prod",
		SourceSchema:   "public",
		TargetHost:     "target.internal",
		TargetPort:     1433,
		TargetDatabase: "target_prod",
		TargetSchema:   "dbo",
	}
	raw := `{
  "Source": {"Host":"source.internal","Port":5432,"Database":"source_prod","Schema":"public"},
  "Target": {"Host":"target.internal","Port":1433,"Database":"target_prod","Schema":"dbo"}
}`
	otherRaw := strings.Replace(raw, "target_prod", "other_target", 1)
	adjustments := []checkpoint.RuntimeAdjustmentRecord{
		{RunID: "other", Action: "other"},
		{RunID: "", Action: "legacy"},
		{RunID: "matching-ai", Action: "legacy", Confidence: "high"},
		{RunID: "matching", Action: "matching", Confidence: "deterministic"},
	}

	got := scopedPerformanceAdjustments(input, adjustments, map[string]string{
		"other":       otherRaw,
		"matching-ai": raw,
		"matching":    raw,
	}, 5)
	if len(got) != 1 || got[0].Action != "matching" {
		t.Fatalf("expected scoped adjustment: %+v", got)
	}

	got = scopedPerformanceAdjustments(input, adjustments[:2], map[string]string{"other": otherRaw}, 5)
	if len(got) != 0 {
		t.Fatalf("unscoped legacy adjustments should be rejected: %+v", got)
	}
}

func TestRecordingWriteErrorAdjusterPersistsStructuralAdjustment(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.CreateRun("run-1", "public", "dbo", &config.Config{}, "", ""); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	recorder := newRuntimeAdjustmentRecorder(state, "run-1")
	adjuster := recordingWriteErrorAdjuster{
		base:     monitor.NewRuleWriteErrorAdjuster(),
		recorder: recorder,
	}

	next := adjuster.EvaluateWriteError(context.Background(), transfer.WriteErrorContext{
		TableName:    "orders",
		ChunkSize:    1000,
		RowCount:     1000,
		ErrorMessage: "prepared statement contains too many placeholders",
	})
	if next != 500 {
		t.Fatalf("next chunk size = %d, want 500", next)
	}
	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatalf("GetRuntimeAdjustments() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records len = %d, want 1", len(records))
	}
	record := records[0]
	if record.RunID != "run-1" || record.AdjustmentNumber != 1 || record.Action != "chunk_size" {
		t.Fatalf("record metadata = %+v", record)
	}
	if record.Adjustments["chunk_size"] != 500 {
		t.Fatalf("record adjustments = %+v", record.Adjustments)
	}
	if strings.Contains(record.Reasoning, "orders") || strings.Contains(record.Reasoning, "too many placeholders") {
		t.Fatalf("record reasoning should omit table name and raw error text: %q", record.Reasoning)
	}
	if !strings.Contains(record.Reasoning, "chunk_size") {
		t.Fatalf("record reasoning should identify adjustment type: %q", record.Reasoning)
	}
}

func TestSamePerformanceRunConfigRequiresExactIdentity(t *testing.T) {
	input := driver.AutoTuneInput{
		SourceHost:     "source.internal",
		SourcePort:     5432,
		SourceDatabase: "source_prod",
		SourceSchema:   "public",
		TargetHost:     "target.internal",
		TargetPort:     1433,
		TargetDatabase: "target_prod",
		TargetSchema:   "dbo",
	}
	raw := `{
  "Source": {"Host":"source.internal","Port":5432,"Database":"source_prod","Schema":"public"},
  "Target": {"Host":"target.internal","Port":1433,"Database":"target_prod","Schema":"dbo"}
}`
	if !samePerformanceRunConfig(input, raw) {
		t.Fatal("expected run config identity to match")
	}
	otherTarget := strings.Replace(raw, "target_prod", "other_target", 1)
	if samePerformanceRunConfig(input, otherTarget) {
		t.Fatal("expected different target database to be rejected")
	}
	if samePerformanceRunConfig(input, `{bad json`) {
		t.Fatal("expected invalid config JSON to be rejected")
	}
}
