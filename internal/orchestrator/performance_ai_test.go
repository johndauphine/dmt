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

func TestPerformanceAutoTuneInputMapsResolvedEnvelope(t *testing.T) {
	cfg := loadSmallEnvelopeConfig(t)
	orch := &Orchestrator{config: cfg}
	input := orch.performanceAutoTuneInput(&driver.SmartConfigSuggestions{
		AvgRowSizeBytes:        2_000,
		RepresentativeRowBytes: 320,
		SafetyRowBytes:         8_192,
		SafetyRowBytesKnown:    true,
	})
	envelope := cfg.AutoConfig().MemoryEnvelope

	if input.AvailableMemoryMB != envelope.AvailableMB {
		t.Errorf("AvailableMemoryMB=%d, want envelope value %d", input.AvailableMemoryMB, envelope.AvailableMB)
	}
	if input.MemoryBudgetMB != envelope.BudgetMB || input.MaxMemoryMB != envelope.BudgetMB {
		t.Errorf("budget mappings = %d/%d, want %d", input.MemoryBudgetMB, input.MaxMemoryMB, envelope.BudgetMB)
	}
	wantMemoryGB := int((envelope.CapacityMB + 1023) / 1024)
	if input.MemoryGB != wantMemoryGB {
		t.Errorf("MemoryGB=%d, want %d from capacity (not availability)", input.MemoryGB, wantMemoryGB)
	}
	if input.AvgRowBytes != 2_000 || input.RepresentativeRowBytes != 320 || input.SafetyRowBytes != 8_192 || !input.SafetyRowBytesKnown {
		t.Errorf("row-width mappings = legacy %d representative %d safety %d known %v",
			input.AvgRowBytes, input.RepresentativeRowBytes, input.SafetyRowBytes, input.SafetyRowBytesKnown)
	}
}

func TestPerformanceAutoTuneInputCarriesCatalogPortlessIdentity(t *testing.T) {
	tests := []struct {
		name                   string
		sourceType, targetType string
		wantSource, wantTarget bool
	}{
		{name: "portless source", sourceType: "sqlite", targetType: "postgres", wantSource: true},
		{name: "portless target", sourceType: "postgres", targetType: "sqlite", wantTarget: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orch := &Orchestrator{config: &config.Config{
				Source: config.SourceConfig{
					Type: tc.sourceType, Host: "source.internal", Port: 5432,
					Database: "source_db", Schema: "public",
				},
				Target: config.TargetConfig{
					Type: tc.targetType, Host: "target.internal", Port: 5432,
					Database: "target_db", Schema: "public",
				},
			}}

			input := orch.performanceAutoTuneInput(nil)
			if input.SourcePortless != tc.wantSource || input.TargetPortless != tc.wantTarget {
				t.Fatalf("portless identity = source:%v target:%v, want %v/%v",
					input.SourcePortless, input.TargetPortless, tc.wantSource, tc.wantTarget)
			}
		})
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

func TestSamePerformanceWorkloadSupportsPortlessEndpoint(t *testing.T) {
	input := driver.AutoTuneInput{
		SourcePortless: true,
		SourceDatabase: "/tmp/source.db",
		TargetHost:     "target.internal",
		TargetPort:     5432,
		TargetDatabase: "target_prod",
		TargetSchema:   "public",
	}
	row := checkpoint.TuningRecord{
		SourceHost:     "legacy-meaningless-host",
		SourcePort:     9999,
		SourceDatabase: input.SourceDatabase,
		TargetHost:     input.TargetHost,
		TargetPort:     input.TargetPort,
		TargetDatabase: input.TargetDatabase,
		TargetSchema:   input.TargetSchema,
	}
	if !samePerformanceWorkload(input, row) {
		t.Fatal("expected portless source path plus exact portful target to match")
	}

	row.SourceDatabase = "/tmp/other.db"
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected different portless source path to be rejected")
	}
	row.SourceDatabase = ""
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected empty persisted portless source path to be rejected")
	}
	row.SourceDatabase = input.SourceDatabase
	row.SourceSchema = "main"
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected different portless source schema to be rejected")
	}
	row.SourceSchema = input.SourceSchema
	row.TargetPort++
	if samePerformanceWorkload(input, row) {
		t.Fatal("expected different portful target port to be rejected")
	}
}

func TestSamePerformanceWorkloadAllowsMatchingEmptySchemas(t *testing.T) {
	input := driver.AutoTuneInput{
		SourceHost: "source.internal", SourcePort: 5432, SourceDatabase: "source_prod",
		TargetHost: "target.internal", TargetPort: 3306, TargetDatabase: "target_prod",
	}
	row := checkpoint.TuningRecord{
		SourceHost: input.SourceHost, SourcePort: input.SourcePort, SourceDatabase: input.SourceDatabase,
		TargetHost: input.TargetHost, TargetPort: input.TargetPort, TargetDatabase: input.TargetDatabase,
	}
	if !samePerformanceWorkload(input, row) {
		t.Fatal("matching empty schemas should not disqualify engines without a schema concept")
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
	began, completed := 0, 0
	var completedAt time.Time
	adjuster := recordingWriteErrorAdjuster{
		base:                          monitor.NewRuleWriteErrorAdjuster(),
		recorder:                      recorder,
		beginObservationContamination: func() { began++ },
		completeObservationContamination: func(at time.Time) {
			completed++
			completedAt = at
		},
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
	if began != 0 || completed != 0 {
		t.Fatalf("recommendation invoked application lifecycle: begin=%d complete=%d", began, completed)
	}
	adjuster.WriteErrorAdjustmentApplying()
	adjuster.WriteErrorAdjustmentApplied()
	if began != 1 || completed != 1 || completedAt.IsZero() {
		t.Fatalf("structural lifecycle callbacks = begin:%d complete:%d at:%v", began, completed, completedAt)
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
	if record.EffectMeasured {
		t.Fatal("initial structural write-error record must remain unmeasured")
	}
	if strings.Contains(record.Reasoning, "orders") || strings.Contains(record.Reasoning, "too many placeholders") {
		t.Fatalf("record reasoning should omit table name and raw error text: %q", record.Reasoning)
	}
	if !strings.Contains(record.Reasoning, "chunk_size") {
		t.Fatalf("record reasoning should identify adjustment type: %q", record.Reasoning)
	}
	if !recorder.applied() {
		t.Fatal("recorded structural write adjustment must mark the run adjusted")
	}

	if next := adjuster.EvaluateWriteError(context.Background(), transfer.WriteErrorContext{
		ChunkSize: 500, ErrorMessage: "transient network timeout",
	}); next != 0 {
		t.Fatalf("non-structural error adjustment = %d, want 0", next)
	}
	if began != 1 || completed != 1 {
		t.Fatalf("non-adjustment changed lifecycle counts: begin=%d complete=%d", began, completed)
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

func TestSamePerformanceRunConfigSupportsPortlessEndpoint(t *testing.T) {
	input := driver.AutoTuneInput{
		SourcePortless: true,
		SourceDatabase: "/tmp/source.db",
		TargetHost:     "target.internal",
		TargetPort:     5432,
		TargetDatabase: "target_prod",
		TargetSchema:   "public",
	}
	raw := `{
  "Source": {"Host":"legacy-meaningless-host","Port":9999,"Database":"/tmp/source.db","Schema":""},
  "Target": {"Host":"target.internal","Port":5432,"Database":"target_prod","Schema":"public"}
}`
	if !samePerformanceRunConfig(input, raw) {
		t.Fatal("expected portless source path plus exact portful target run config to match")
	}
	otherSource := strings.Replace(raw, "/tmp/source.db", "/tmp/other.db", 1)
	if samePerformanceRunConfig(input, otherSource) {
		t.Fatal("expected different portless source path to be rejected")
	}
	otherTarget := strings.Replace(raw, "target.internal", "other-target.internal", 1)
	if samePerformanceRunConfig(input, otherTarget) {
		t.Fatal("expected different portful target host to be rejected")
	}
}
