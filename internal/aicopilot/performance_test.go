package aicopilot

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/driver/dbtuning"
)

func TestBuildPerformancePayloadRedactsIdentityAndFiltersRuntimeKnobs(t *testing.T) {
	input := driver.AutoTuneInput{
		CPUCores:               8,
		MemoryGB:               32,
		AvailableMemoryMB:      24000,
		DatabaseType:           "postgres",
		TargetType:             "mssql",
		TargetMode:             "upsert",
		TotalTables:            12,
		TotalRows:              100000,
		AvgRowBytes:            500,
		RepresentativeRowBytes: 420,
		SafetyRowBytes:         8_192,
		SafetyRowBytesKnown:    true,
		SourceHost:             "source.internal",
		SourcePort:             5432,
		SourceDatabase:         "source_prod",
		SourceSchema:           "private_schema",
		TargetHost:             "target.internal",
		TargetPort:             1433,
		TargetDatabase:         "target_prod",
		TargetSchema:           "dbo",
	}
	suggestions := driver.SmartConfigSuggestions{
		Workers:                  6,
		ChunkSizeRecommendation:  50000,
		ReadAheadBuffers:         4,
		WriteAheadWriters:        2,
		ParallelReaders:          2,
		MaxPartitions:            6,
		MaxSourceConnections:     10,
		MaxTargetConnections:     16,
		EstimatedMemMB:           512,
		MemoryEstimateOverBudget: true,
		Tier:                     "regression",
		Reasoning:                "regression-selected WAW=2 from token=abc123",
	}
	payload := BuildPerformancePayload(input, suggestions, []checkpoint.TuningRecord{{
		Timestamp:       time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		SourceDBType:    "postgres",
		TargetDBType:    "mssql",
		TotalTables:     12,
		TotalRows:       100000,
		AvgRowSizeBytes: 500,
		Workers:         6,
		ChunkSize:       50000,
		Reasoning:       "history row used password=secret",
		FinalThroughput: 2500,
		ChunkRetryCount: 2,
	}}, []checkpoint.RuntimeAdjustmentRecord{{
		Action:         "increase",
		Adjustments:    map[string]int{"workers": 6, "invented_knob": 99},
		CPUBefore:      70,
		CPUAfter:       82,
		MemoryBefore:   512,
		MemoryAfter:    640,
		EffectMeasured: true,
		Reasoning:      "runtime kept known knob",
		Confidence:     "deterministic",
	}})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, secret := range []string{
		"source.internal",
		"target.internal",
		"source_prod",
		"target_prod",
		"private_schema",
		"abc123",
		"secret",
		"invented_knob",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("payload leaked %q: %s", secret, text)
		}
	}
	if !payload.Workload.SourceIdentityOmitted || !payload.Workload.TargetIdentityOmitted {
		t.Fatalf("identity omission flags = %+v", payload.Workload)
	}
	if payload.Workload.RepresentativeRowBytes != 420 || payload.Workload.SafetyRowBytes != 8_192 || !payload.Workload.SafetyRowBytesKnown {
		t.Fatalf("payload lost row-width provenance: %+v", payload.Workload)
	}
	if !payload.Workload.MemoryEstimateOverBudget {
		t.Fatalf("payload lost structured over-budget state: %+v", payload.Workload)
	}
	if got := payload.RuntimeAdjustments[0].Adjustments["workers"]; got != 6 {
		t.Fatalf("runtime workers adjustment = %d, want 6", got)
	}
	if payload.RuntimeAdjustments[0].CPUBefore != 70 || payload.RuntimeAdjustments[0].CPUAfter == nil ||
		*payload.RuntimeAdjustments[0].CPUAfter != 82 || payload.RuntimeAdjustments[0].Confidence != "deterministic" {
		t.Fatalf("runtime metrics were not preserved: %+v", payload.RuntimeAdjustments[0])
	}
	if payload.RuntimeAdjustments[0].Reasoning != "" {
		t.Fatalf("runtime adjustment reasoning should be omitted from AI payload: %+v", payload.RuntimeAdjustments[0])
	}
	if payload.RecentRuns[0].Reasoning != "" {
		t.Fatalf("recent run reasoning should be omitted from AI payload: %+v", payload.RecentRuns[0])
	}
	if payload.ObservedMetrics.RunsWithRetries != 1 || payload.ObservedMetrics.TotalChunkRetries != 2 || payload.ObservedMetrics.RuntimeAdjustmentCount != 1 {
		t.Fatalf("observed metrics = %+v", payload.ObservedMetrics)
	}
}

func TestBuildPerformancePayloadOmitsUnsafeRuntimeAdjustmentAction(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{}, nil, []checkpoint.RuntimeAdjustmentRecord{{
		Action:      "public.orders",
		Adjustments: map[string]int{"workers": 4},
		Reasoning:   "table-specific legacy action",
		Confidence:  "deterministic",
	}})
	if len(payload.RuntimeAdjustments) != 1 {
		t.Fatalf("runtime adjustments len = %d, want 1", len(payload.RuntimeAdjustments))
	}
	if payload.RuntimeAdjustments[0].Action != "workers" {
		t.Fatalf("unsafe runtime adjustment action was not normalized: %+v", payload.RuntimeAdjustments[0])
	}
	if payload.RuntimeAdjustments[0].Confidence != "deterministic" {
		t.Fatalf("runtime adjustment confidence should be deterministic: %+v", payload.RuntimeAdjustments[0])
	}
}

func TestBuildPerformancePayloadOmitsNonDeterministicRuntimeAdjustments(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{}, nil, []checkpoint.RuntimeAdjustmentRecord{{
		Action:      "workers",
		Adjustments: map[string]int{"workers": 99},
		Confidence:  "high",
	}, {
		Action:      "workers",
		Adjustments: map[string]int{"workers": 4},
		Confidence:  "deterministic",
	}})
	if len(payload.RuntimeAdjustments) != 1 {
		t.Fatalf("runtime adjustments len = %d, want 1: %+v", len(payload.RuntimeAdjustments), payload.RuntimeAdjustments)
	}
	if got := payload.RuntimeAdjustments[0].Adjustments["workers"]; got != 4 {
		t.Fatalf("runtime workers adjustment = %d, want 4", got)
	}
	if payload.ObservedMetrics.RuntimeAdjustmentCount != 1 {
		t.Fatalf("observed metrics should count only deterministic runtime adjustments: %+v", payload.ObservedMetrics)
	}
}

func TestBuildPerformancePayloadPreservesRuntimeAdjustmentMeasurementState(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{}, nil, []checkpoint.RuntimeAdjustmentRecord{
		{
			Action: "workers", Adjustments: map[string]int{"workers": 2}, Confidence: "deterministic",
			ThroughputAfter: 101, EffectPercent: 102, CPUAfter: 103, MemoryAfter: 104,
			// Legacy rows can contain phantom numeric after-values while the new
			// explicit state remains false.
			EffectMeasured: false,
		},
		{
			Action: "workers", Adjustments: map[string]int{"workers": 3}, Confidence: "deterministic",
			// A newly recorded adjustment is also unmeasured until observation.
			EffectMeasured: false,
		},
		{
			Action: "workers", Adjustments: map[string]int{"workers": 4}, Confidence: "deterministic",
			EffectMeasured: true,
		},
		{
			Action: "workers", Adjustments: map[string]int{"workers": 5}, Confidence: "deterministic",
			ThroughputAfter: 201, EffectPercent: 202, CPUAfter: 203, MemoryAfter: 204,
			EffectMeasured: true,
		},
	})

	if len(payload.RuntimeAdjustments) != 4 {
		t.Fatalf("runtime adjustments len = %d, want 4", len(payload.RuntimeAdjustments))
	}
	for i := 0; i < 2; i++ {
		summary := payload.RuntimeAdjustments[i]
		if summary.ThroughputAfter != nil || summary.EffectPercent != nil ||
			summary.CPUAfter != nil || summary.MemoryAfter != nil {
			t.Fatalf("unmeasured adjustment %d exposed after metrics: %+v", i, summary)
		}
	}
	measuredZero := payload.RuntimeAdjustments[2]
	if measuredZero.ThroughputAfter == nil || *measuredZero.ThroughputAfter != 0 ||
		measuredZero.EffectPercent == nil || *measuredZero.EffectPercent != 0 ||
		measuredZero.CPUAfter == nil || *measuredZero.CPUAfter != 0 ||
		measuredZero.MemoryAfter == nil || *measuredZero.MemoryAfter != 0 {
		t.Fatalf("measured zeros were not retained: %+v", measuredZero)
	}
	measuredNonZero := payload.RuntimeAdjustments[3]
	if measuredNonZero.ThroughputAfter == nil || *measuredNonZero.ThroughputAfter != 201 ||
		measuredNonZero.EffectPercent == nil || *measuredNonZero.EffectPercent != 202 ||
		measuredNonZero.CPUAfter == nil || *measuredNonZero.CPUAfter != 203 ||
		measuredNonZero.MemoryAfter == nil || *measuredNonZero.MemoryAfter != 204 {
		t.Fatalf("measured values were not retained independently: %+v", measuredNonZero)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var encoded struct {
		RuntimeAdjustments []map[string]any `json:"runtime_adjustments"`
	}
	if err := json.Unmarshal(data, &encoded); err != nil {
		t.Fatal(err)
	}
	afterFields := []string{"throughput_after", "effect_percent", "cpu_after", "memory_after"}
	for i := 0; i < 2; i++ {
		for _, field := range afterFields {
			if _, exists := encoded.RuntimeAdjustments[i][field]; exists {
				t.Errorf("unmeasured adjustment %d serialized %s: %s", i, field, data)
			}
		}
	}
	for _, field := range afterFields {
		value, exists := encoded.RuntimeAdjustments[2][field]
		if !exists || value != float64(0) {
			t.Errorf("measured-zero %s = %#v, exists=%v; payload=%s", field, value, exists, data)
		}
	}
}

func TestRuntimeAdjustmentNumberValidationHonorsOptionalAfterMetrics(t *testing.T) {
	payload := PerformancePayload{
		RuntimeAdjustments: []RuntimeAdjustmentSummary{{
			Action:      "workers",
			Adjustments: map[string]int{"workers": 4},
		}},
	}
	assertZeroAllowed := func(t *testing.T, want bool) {
		t.Helper()
		for name, numbers := range map[string]map[string]bool{
			"general": numbersForPerformanceGeneralText(payload),
			"target":  numbersForPerformanceTarget(payload, "workers", true),
		} {
			if got := numbers["0"]; got != want {
				t.Errorf("%s validation allows zero = %v, want %v; numbers=%v", name, got, want, numbers)
			}
		}
	}

	assertZeroAllowed(t, false)
	measuredZero := 0.0
	payload.RuntimeAdjustments[0].CPUAfter = &measuredZero
	assertZeroAllowed(t, true)
}

func TestBuildPerformancePayloadIncludesRedactedDBTuningEvidence(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType:    "postgres",
			Role:            "source",
			TuningPotential: "medium",
			EstimatedImpact: "1 recommendation(s); password=secret",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "shared_buffers",
				CurrentValue:     "128MB",
				RecommendedValue: "4GB",
				Impact:           "high",
				Priority:         1,
				CanApplyRuntime:  false,
				RequiresRestart:  true,
				Reason:           "Increase memory; api_key=secret",
			}, {
				Parameter:        "work_mem",
				CurrentValue:     nil,
				RecommendedValue: map[string]string{"password": "secret"},
				Impact:           "low",
				Priority:         3,
				Reason:           "Composite values are not emitted.",
			}},
		},
	}, nil, nil)

	if len(payload.DatabaseTuning) != 1 {
		t.Fatalf("database tuning len = %d, want 1", len(payload.DatabaseTuning))
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, secret := range []string{"password=secret", "api_key=secret"} {
		if strings.Contains(text, secret) {
			t.Fatalf("payload leaked %q: %s", secret, text)
		}
	}
	if got := payload.DatabaseTuning[0].Recommendations[0].RecommendedValue; got != "4GB" {
		t.Fatalf("recommended value = %q, want 4GB", got)
	}
	if got := payload.DatabaseTuning[0].Recommendations[1].CurrentValue; got != "" {
		t.Fatalf("nil current value = %q, want empty", got)
	}
	if got := payload.DatabaseTuning[0].Recommendations[1].RecommendedValue; got != "[REDACTED]" {
		t.Fatalf("composite recommended value = %q, want redacted", got)
	}
	if !strings.Contains(text, `"can_apply_runtime":false`) {
		t.Fatalf("payload should preserve explicit false runtime-apply flag: %s", text)
	}
	if !containsPerformanceTarget(payload.AllowedFindingTargets, "db_tuning.shared_buffers") {
		t.Fatalf("allowed finding targets should include DB tuning parameter: %+v", payload.AllowedFindingTargets)
	}
}

func TestGeneratePerformanceExplanationParsesStructuredResponse(t *testing.T) {
	payload := PerformancePayload{
		PromptVersion:          PerformancePromptVersion,
		DeterministicTier:      "baseline",
		DeterministicReasoning: "baseline selected workers=6 and chunk_size=50000",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6, ChunkSize: 50000},
		AllowedKnobs:           allowedPerformanceKnobs(),
		Workload:               PerformanceWorkloadSummary{CPUCores: 8, TotalRows: 100000},
	}
	client := &fakeClient{response: "```json\n" + `{
  "summary": "The deterministic baseline used 6 workers from 8 CPU cores.",
  "findings": [
    {
      "knob": "workers",
      "category": "baseline",
      "rationale": "The worker count follows the deterministic baseline formula.",
      "evidence": ["payload has cpu_cores=8 and workers=6"],
      "next_action": "Keep this value unless operator overrides already require a different known value."
    }
  ],
  "notes": ["No endpoint identity was included."]
}` + "\n```"}

	explanation, err := GeneratePerformanceExplanation(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GeneratePerformanceExplanation() error = %v", err)
	}
	if explanation.Status != ReviewStatusOK || !explanation.Enabled {
		t.Fatalf("status/enabled = %q/%v", explanation.Status, explanation.Enabled)
	}
	if explanation.Provider != "fake" || explanation.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", explanation.Provider, explanation.Model)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("findings len = %d, want 1: %+v", len(explanation.Findings), explanation.Findings)
	}
	if explanation.Findings[0].Knob != "workers" || explanation.Findings[0].Source != "ai_advisory" {
		t.Fatalf("finding = %+v", explanation.Findings[0])
	}
	if !strings.Contains(client.prompt, `"deterministic_knobs"`) {
		t.Fatalf("prompt did not include deterministic facts: %s", client.prompt)
	}
}

func TestBuildPerformancePromptRequiresEvidenceFindings(t *testing.T) {
	prompt, err := BuildPerformancePrompt(PerformancePayload{
		DeterministicReasoning: "Memory estimate supports medium concurrency.",
		DeterministicKnobs:     PerformanceKnobs{Workers: 8},
		AllowedFindingTargets:  []string{"workers"},
	})
	if err != nil {
		t.Fatalf("BuildPerformancePrompt() error = %v", err)
	}
	for _, want := range []string{
		"Include at least one finding",
		"Every finding must include a non-empty evidence array",
		"Use only allowed_finding_targets values",
		"unobserved fallback estimate",
		"widest observed table-average model",
		"never a hard bound on every serialized row",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestParsePerformanceExplanationDropsInventedKnobsAndNumbers(t *testing.T) {
	payload := PerformancePayload{
		PromptVersion:          PerformancePromptVersion,
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Use 999 workers for better throughput.",
  "findings": [
    {"knob":"workers","rationale":"The payload supports 6 workers.","evidence":["workers=6"]},
    {"knob":"magic_parallelism","rationale":"Set it to 7."},
    {"knob":"workers","rationale":"Set workers to 999 for a 4x boost."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "999") {
		t.Fatalf("summary kept invented number: %q", explanation.Summary)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("findings len = %d, want 1: %+v", len(explanation.Findings), explanation.Findings)
	}
	if explanation.Findings[0].Knob != "workers" || !strings.Contains(explanation.Findings[0].Rationale, "6") {
		t.Fatalf("kept finding = %+v", explanation.Findings[0])
	}
}

func TestParsePerformanceExplanationDropsUnsupportedTargetNumbers(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		Workload:               PerformanceWorkloadSummary{TotalRows: 100000},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "The workload has 100000 rows.",
  "findings": [
    {"knob":"workers","rationale":"Set workers to 100000.","evidence":["total_rows=100000"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if explanation.Summary == "" {
		t.Fatal("summary should still allow workload numbers")
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with unsupported target number should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Zero value should not be treated as emitted.",
  "findings": [
    {"knob":"max_retries","rationale":"Set max_retries to 0.","evidence":["operator should not retry"]}
  ]
}`, PerformancePayload{
		DeterministicKnobs: PerformanceKnobs{},
		AllowedKnobs:       []string{"max_retries"},
	})
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with omitted zero knob should be dropped: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationAllowsRuntimeAdjustmentNumbers(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected chunk_size=50000",
		DeterministicKnobs:     PerformanceKnobs{ChunkSize: 50000},
		AllowedKnobs:           []string{"chunk_size"},
		RuntimeAdjustments: []RuntimeAdjustmentSummary{{
			Action:           "increase",
			Adjustments:      map[string]int{"chunk_size": 37500},
			ThroughputBefore: 1200,
			MemoryBefore:     95,
			Reasoning:        "memory pressure lowered chunk size",
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Runtime adjustment evidence is present.",
  "findings": [
    {"knob":"chunk_size","rationale":"chunk_size was lowered to 37500 because memory hit 95.","evidence":["runtime_adjustments chunk_size=37500 memory_before=95"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("runtime adjustment finding should be kept: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationAllowsWidthEvidenceForChunkSize(t *testing.T) {
	payload := PerformancePayload{
		Workload: PerformanceWorkloadSummary{
			RepresentativeRowBytes: 200,
			SafetyRowBytes:         8_192,
			EstimatedMemoryMB:      512,
			MaxMemoryMB:            1_024,
		},
		DeterministicKnobs: PerformanceKnobs{ChunkSize: 50_000},
		AllowedKnobs:       []string{"chunk_size"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Width evidence is present.",
  "findings": [
    {"knob":"chunk_size","rationale":"50000 rows uses representative width 200 while safety width 8192 yields 512 MB within 1024 MB.","evidence":["workload representative_row_bytes=200 safety_row_bytes=8192 estimated_memory_mb=512 max_memory_mb=1024"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("row-width evidence should keep chunk-size finding: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationAllowsRecentRunTargetEvidence(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		RecentRuns: []PerformanceHistoryRun{{
			Knobs:           PerformanceKnobs{Workers: 4},
			FinalThroughput: 2000,
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Recent same-workload history is present.",
  "findings": [
    {"knob":"workers","rationale":"A recent same-workload run used workers=4.","evidence":["recent_runs workers=4 final_throughput=2000"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("recent-run evidence should be kept: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationDoesNotUseRecentRunValuesForNextAction(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		RecentRuns: []PerformanceHistoryRun{{
			Knobs:           PerformanceKnobs{Workers: 4},
			FinalThroughput: 2000,
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Recent same-workload history is present.",
  "findings": [
    {"knob":"workers","rationale":"A recent same-workload run used workers=4.","evidence":["recent_runs workers=4 final_throughput=2000"],"next_action":"try workers=4"}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("recent-run evidence should keep finding: %+v", explanation.Findings)
	}
	if explanation.Findings[0].NextAction != "" {
		t.Fatalf("recent-run value should not be kept as next action: %+v", explanation.Findings[0])
	}
}

func TestParsePerformanceExplanationDropsMetricNumbersFromNextAction(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected chunk_size=50000",
		DeterministicKnobs:     PerformanceKnobs{ChunkSize: 50000},
		AllowedKnobs:           []string{"chunk_size"},
		RuntimeAdjustments: []RuntimeAdjustmentSummary{{
			Action:       "increase",
			Adjustments:  map[string]int{"chunk_size": 37500},
			MemoryBefore: 95,
			Reasoning:    "memory pressure lowered chunk size",
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Runtime adjustment evidence is present.",
  "findings": [
    {"knob":"chunk_size","rationale":"memory hit 95 before the adjustment.","evidence":["runtime_adjustments memory_before=95"],"next_action":"try chunk_size=95"}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("runtime metric rationale should be kept: %+v", explanation.Findings)
	}
	if explanation.Findings[0].NextAction != "" {
		t.Fatalf("next_action with metric-only chunk size should be dropped: %+v", explanation.Findings[0])
	}
}

func TestParsePerformanceExplanationDropsCrossKnobNextAction(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=4 max_retries=2",
		DeterministicKnobs:     PerformanceKnobs{Workers: 4, MaxRetries: 2},
		AllowedKnobs:           []string{"workers", "max_retries"},
	}
	explanation, err := ParsePerformanceExplanation(`{
 "summary": "Use 4 workers from deterministic knobs.",
  "findings": [
    {"knob":"workers","rationale":"workers is deterministic at 4.","evidence":["workers=4"],"next_action":"Set retry limit to 4."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("safe workers finding should remain: %+v", explanation.Findings)
	}
	if explanation.Findings[0].NextAction != "" {
		t.Fatalf("cross-knob next_action should be dropped: %+v", explanation.Findings[0])
	}

	explanation, err = ParsePerformanceExplanation(`{
 "summary": "Use 4 workers from deterministic knobs.",
  "findings": [
    {"knob":"workers","rationale":"workers is deterministic at 4.","evidence":["workers=4"],"next_action":"Use a million workers next."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 || explanation.Findings[0].NextAction != "" {
		t.Fatalf("word-number cross-knob next_action should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
 "summary": "Use 4 workers from deterministic knobs.",
  "findings": [
    {"knob":"workers","rationale":"workers is deterministic at 4.","evidence":["workers=4"],"next_action":"Use dozens of workers next."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 || explanation.Findings[0].NextAction != "" {
		t.Fatalf("colloquial-number next_action should be dropped: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationDropsInvalidConfigAdvice(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=4",
		DeterministicKnobs:     PerformanceKnobs{Workers: 4},
		AllowedKnobs:           []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
 "summary": "Set migration.validation.mode=row_hash before retrying.",
  "findings": [
    {"knob":"workers","rationale":"workers is deterministic at 4.","evidence":["workers=4"],"next_action":"Set schema_evolution.added_column=add."}
  ],
  "notes": ["Use migration.validation.mode=sample if you want sample validation."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "row_hash") || strings.Contains(explanation.Summary, "migration.validation.mode") {
		t.Fatalf("invalid config summary survived: %q", explanation.Summary)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("safe finding should remain: %+v", explanation.Findings)
	}
	if explanation.Findings[0].NextAction != "" {
		t.Fatalf("invalid config next_action should be dropped: %+v", explanation.Findings[0])
	}
	if len(explanation.Notes) != 1 || !strings.Contains(explanation.Notes[0], "migration.validation.mode=sample") {
		t.Fatalf("valid config note should remain: %+v", explanation.Notes)
	}
}

func TestParsePerformanceExplanationAllowsDBTuningTargets(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "shared_buffers",
				CurrentValue:     "128MB",
				RecommendedValue: "4GB",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)

	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.shared_buffers","category":"memory","rationale":"Payload recommends 4GB from current 128MB.","evidence":["database_tuning source shared_buffers 128MB to 4GB"]},
    {"knob":"db_tuning.invented","rationale":"Not present."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("findings len = %d, want 1: %+v", len(explanation.Findings), explanation.Findings)
	}
	if explanation.Findings[0].Knob != "db_tuning.shared_buffers" {
		t.Fatalf("finding knob = %q", explanation.Findings[0].Knob)
	}
}

func TestParsePerformanceExplanationRequiresExactDBTuningValues(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "shared_buffers",
				CurrentValue:     "128MB",
				RecommendedValue: "4GB",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)

	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.shared_buffers","category":"memory","rationale":"Payload recommends 4TB from current 128MB.","evidence":["database_tuning source shared_buffers 128MB to 4GB"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with changed DB tuning units should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.shared_buffers","category":"memory","rationale":"Payload recommends 4GB from current 128MB.","evidence":["database_tuning source shared_buffers 128MB to 4TB"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with changed DB tuning evidence units should be dropped: %+v", explanation.Findings)
	}

	payload = BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "synchronous_commit",
				CurrentValue:     "on",
				RecommendedValue: "off",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)
	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"Payload recommends remote_apply.","evidence":["database_tuning source synchronous_commit on to off"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with invented string DB tuning value should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"Payload uses off; set local commit mode.","evidence":["database_tuning source synchronous_commit on to off"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with exact plus invented nonnumeric DB tuning value should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"Payload recommends changing from on to remote_apply.","evidence":["database_tuning source synchronous_commit on to off"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with exact plus invented string DB tuning value should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"Payload recommends off.","evidence":["based on load during migration"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("short DB tuning value matched unrelated prose: %+v", explanation.Findings)
	}

	payload = BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "synchronous_commit",
				CurrentValue:     "off",
				RecommendedValue: "remote_apply",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)
	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"Payload includes remote_apply; set synchronous_commit to on.","evidence":["database_tuning source synchronous_commit off to remote_apply"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with invented short DB tuning value should be dropped: %+v", explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.synchronous_commit","category":"durability","rationale":"synchronous_commit: on is safer than off.","evidence":["database_tuning source synchronous_commit off to remote_apply"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding with invented residual DB tuning word should be dropped: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationDropsDBTuningInSummaryAndNotes(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "synchronous_commit",
				CurrentValue:     "on",
				RecommendedValue: "off",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)

	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Set synchronous_commit to remote_apply.",
  "notes": ["Set synchronous_commit to local before migration."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "remote_apply") {
		t.Fatalf("summary kept invented DB tuning value: %q", explanation.Summary)
	}
	if len(explanation.Notes) != 0 {
		t.Fatalf("notes with invented DB tuning values should be dropped: %+v", explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "synchronous_commit should be local.",
  "notes": ["synchronous_commit should be local."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "local") || len(explanation.Notes) != 0 {
		t.Fatalf("general text with invented DB tuning value should be dropped: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "logical WAL mode is safer.",
  "notes": ["logical WAL mode is safer.", "minimal."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(strings.ToLower(explanation.Summary), "logical") || len(explanation.Notes) != 0 {
		t.Fatalf("general text with value-only DB tuning synonym should be dropped: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	payload = BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "synchronous_commit",
				CurrentValue:     "off",
				RecommendedValue: "remote_apply",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)
	explanation, err = ParsePerformanceExplanation(`{
  "summary": "off improves throughput.",
  "notes": ["remote_apply improves durability.", "remote-write improves durability.", "remote apply improves durability."],
  "findings": [
    {"knob":"workers","rationale":"Set synchronous commit to remote_apply.","evidence":["workers is emitted"]},
    {"knob":"chunk_size","rationale":"remote_write improves durability.","evidence":["chunk_size is emitted"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "remote") || len(explanation.Notes) != 0 || len(explanation.Findings) != 0 {
		t.Fatalf("known DB tuning values outside DB tuning targets should be dropped: summary=%q notes=%+v findings=%+v", explanation.Summary, explanation.Notes, explanation.Findings)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"workers","rationale":"synchronous commit should be on.","evidence":["workers is emitted"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("spaced DB tuning parameter guidance outside DB tuning target should be dropped: %+v", explanation.Findings)
	}

	payload = BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "shared_buffers",
				CurrentValue:     "128MB",
				RecommendedValue: "4GB",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)
	payload.Workload.TotalTables = 4
	explanation, err = ParsePerformanceExplanation(`{
  "summary": "4 GB improves memory.",
  "notes": ["4 GB improves memory."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "4 GB") || len(explanation.Notes) != 0 {
		t.Fatalf("spaced DB tuning unit value outside DB tuning target should be dropped: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	payload = BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "mssql",
			Role:         "target",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "recovery_model",
				CurrentValue:     "FULL",
				RecommendedValue: "SIMPLE",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)
	explanation, err = ParsePerformanceExplanation(`{
  "summary": "read committed snapshot improves concurrency.",
  "notes": ["single-user mode helps.", "multi user mode helps.", "bulk-logged improves bulk loads."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(strings.ToLower(explanation.Summary), "read committed snapshot") || len(explanation.Notes) != 0 {
		t.Fatalf("enum-like DB tuning value outside DB tuning target should be dropped: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}
}

func TestParsePerformanceExplanationDropsRecentRunNumbersInSummaryAndNotes(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		Workload:               PerformanceWorkloadSummary{TotalTables: 4},
		RecentRuns: []PerformanceHistoryRun{{
			Knobs:           PerformanceKnobs{Workers: 4},
			FinalThroughput: 2000,
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Try workers=4 next.",
  "notes": ["Try workers=4 next."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "workers=4") || len(explanation.Notes) != 0 {
		t.Fatalf("general text should not use recent-run knob values as next actions: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Use worker count 4 next.",
  "notes": ["Parallel readers 4 may help."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "4") || len(explanation.Notes) != 0 {
		t.Fatalf("general text should not use knob synonyms with recent-run values: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Try concurrency 4 next.",
  "notes": ["Try concurrency 4 next."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "4") || len(explanation.Notes) != 0 {
		t.Fatalf("general text should not use concurrency synonym with recent-run values: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Try worker-count 4 next.",
  "notes": ["Try parallel-readers 4 next."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "4") || len(explanation.Notes) != 0 {
		t.Fatalf("general text should not use hyphenated knob synonyms with recent-run values: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}

	explanation, err = ParsePerformanceExplanation(`{
  "summary": "Try max-retries 4 next.",
  "notes": ["Try checkpoint-frequency 4 next.", "Try source-connections 4 next.", "Try large-table-threshold 4 next."]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if strings.Contains(explanation.Summary, "4") || len(explanation.Notes) != 0 {
		t.Fatalf("general text should not use hyphenated knob aliases with workload numbers: summary=%q notes=%+v", explanation.Summary, explanation.Notes)
	}
}

func TestParsePerformanceExplanationDropsUnsafeErrorText(t *testing.T) {
	payload := BuildPerformancePayload(driver.AutoTuneInput{}, driver.SmartConfigSuggestions{
		Workers: 6,
		SourceTuning: &dbtuning.DatabaseTuning{
			DatabaseType: "postgres",
			Role:         "source",
			Recommendations: []dbtuning.TuningRecommendation{{
				Parameter:        "synchronous_commit",
				CurrentValue:     "on",
				RecommendedValue: "off",
				Reason:           "Payload includes deterministic DB tuning.",
			}},
		},
	}, nil, nil)

	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Use 6 workers from deterministic knobs.",
  "error": "Set synchronous_commit to remote_apply and max retries to forty."
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if explanation.Error != "" {
		t.Fatalf("unsafe AI error text should be dropped: %q", explanation.Error)
	}
}

func TestParsePerformanceExplanationStripsControlText(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Use 6 workers.\nStatus: forged \u001b[31mred\u001b[0m and \u009b31mblue",
  "notes": ["line one\r\nline two \u001b[2J"],
  "findings": [
    {
      "knob": "workers",
      "rationale": "workers=6\nnext forged",
      "evidence": ["workers=6 \u001b[32mgreen\u001b[0m"],
      "next_action": "Keep workers=6\nunless constraints change."
    }
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	values := []string{explanation.Summary, explanation.Notes[0], explanation.Findings[0].Rationale, explanation.Findings[0].Evidence[0], explanation.Findings[0].NextAction}
	for _, value := range values {
		if containsControlRune(value) {
			t.Fatalf("sanitized text kept control characters: %q", value)
		}
	}
	if !strings.Contains(explanation.Summary, "Status: forged red") {
		t.Fatalf("summary was not normalized as expected: %q", explanation.Summary)
	}
}

func containsControlRune(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return true
		}
	}
	return false
}

func TestParsePerformanceExplanationAllowsBenignGeneralGuidanceWords(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Use 6 workers from deterministic knobs."
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if explanation.Summary != "Use 6 workers from deterministic knobs." {
		t.Fatalf("benign general guidance words should remain, summary=%q", explanation.Summary)
	}
}

func TestParsePerformanceExplanationCombinesAllowedTargetsWithKnobs(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		AllowedFindingTargets:  []string{"db_tuning.shared_buffers"},
		DatabaseTuning: []PerformanceDBTuningSummary{{
			Recommendations: []PerformanceDBTuningRecommendation{{
				Parameter:        "shared_buffers",
				CurrentValue:     "128MB",
				RecommendedValue: "4GB",
			}},
		}},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Workers and database tuning are explained.",
  "findings": [
    {"knob":"workers","rationale":"The payload supports workers.","evidence":["workers is in allowed knobs"]},
    {"knob":"db_tuning.shared_buffers","rationale":"Payload recommends 4GB from current 128MB.","evidence":["database_tuning source shared_buffers 128MB to 4GB"]},
    {"knob":"chunk_size","rationale":"Not allowed by this payload.","evidence":["not allowed"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 2 {
		t.Fatalf("findings len = %d, want 2: %+v", len(explanation.Findings), explanation.Findings)
	}
	if explanation.Findings[0].Knob != "workers" || explanation.Findings[1].Knob != "db_tuning.shared_buffers" {
		t.Fatalf("findings = %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationRejectsDBTargetWithoutRecommendation(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		AllowedFindingTargets:  []string{"db_tuning.shared_buffers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Database tuning evidence is advisory.",
  "findings": [
    {"knob":"db_tuning.shared_buffers","rationale":"Payload recommends 4GB from current 128MB.","evidence":["database_tuning source shared_buffers 128MB to 4GB"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("db tuning target without recommendation should be dropped: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationRequiresEvidence(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		AllowedFindingTargets:  []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Workers are explained.",
  "findings": [
    {"knob":"workers","rationale":"The payload supports 6 workers."}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 0 {
		t.Fatalf("finding without evidence should be dropped: %+v", explanation.Findings)
	}
}

func TestParsePerformanceExplanationKeepsScrubbedEvidence(t *testing.T) {
	payload := PerformancePayload{
		DeterministicReasoning: "baseline selected workers=6",
		DeterministicKnobs:     PerformanceKnobs{Workers: 6},
		AllowedKnobs:           []string{"workers"},
		AllowedFindingTargets:  []string{"workers"},
	}
	explanation, err := ParsePerformanceExplanation(`{
  "summary": "Workers are explained.",
  "findings": [
    {"knob":"workers","rationale":"The payload supports 6 workers.","evidence":["workers=6 with password=secret"]}
  ]
}`, payload)
	if err != nil {
		t.Fatalf("ParsePerformanceExplanation() error = %v", err)
	}
	if len(explanation.Findings) != 1 {
		t.Fatalf("finding with scrubbed evidence should be kept: %+v", explanation.Findings)
	}
	if strings.Contains(explanation.Findings[0].Evidence[0], "secret") {
		t.Fatalf("evidence was not scrubbed: %+v", explanation.Findings[0].Evidence)
	}
}

func TestPerformanceFallbacksUseDeterministicSummaryAndScrubErrors(t *testing.T) {
	payload := PerformancePayload{
		DeterministicTier:      "smoothed-bins",
		DeterministicReasoning: "smoothed-bins selected chunk_size=50000",
	}
	unavailable := UnavailablePerformanceExplanation("no provider token=abc123", payload)
	if unavailable.Status != ReviewStatusUnavailable || unavailable.Enabled {
		t.Fatalf("unavailable status/enabled = %q/%v", unavailable.Status, unavailable.Enabled)
	}
	if strings.Contains(unavailable.Summary, "abc123") {
		t.Fatalf("unavailable summary leaked secret: %q", unavailable.Summary)
	}
	failed := ErrorPerformanceExplanation("fake", "fake-model", errors.New("bad key token=abc123"), payload)
	if failed.Status != ReviewStatusError || !failed.Enabled {
		t.Fatalf("error status/enabled = %q/%v", failed.Status, failed.Enabled)
	}
	if strings.Contains(failed.Error, "abc123") {
		t.Fatalf("error leaked secret: %q", failed.Error)
	}
	if len(failed.Notes) == 0 || !strings.Contains(failed.Notes[0], "smoothed-bins") {
		t.Fatalf("fallback notes = %+v", failed.Notes)
	}
}

func containsPerformanceTarget(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
