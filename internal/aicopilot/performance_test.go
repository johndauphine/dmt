package aicopilot

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestBuildPerformancePayloadRedactsIdentityAndFiltersRuntimeKnobs(t *testing.T) {
	input := driver.AutoTuneInput{
		CPUCores:          8,
		MemoryGB:          32,
		AvailableMemoryMB: 24000,
		DatabaseType:      "postgres",
		TargetType:        "mssql",
		TargetMode:        "upsert",
		TotalTables:       12,
		TotalRows:         100000,
		AvgRowBytes:       500,
		SourceHost:        "source.internal",
		SourcePort:        5432,
		SourceDatabase:    "source_prod",
		SourceSchema:      "private_schema",
		TargetHost:        "target.internal",
		TargetPort:        1433,
		TargetDatabase:    "target_prod",
		TargetSchema:      "dbo",
	}
	suggestions := driver.SmartConfigSuggestions{
		Workers:                 6,
		ChunkSizeRecommendation: 50000,
		ReadAheadBuffers:        4,
		WriteAheadWriters:       2,
		ParallelReaders:         2,
		MaxPartitions:           6,
		MaxSourceConnections:    10,
		MaxTargetConnections:    16,
		EstimatedMemMB:          512,
		Tier:                    "regression",
		Reasoning:               "regression-selected WAW=2 from token=abc123",
	}
	payload := BuildPerformancePayload(input, suggestions, []driver.AITuningRecord{{
		Timestamp:       time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		SourceDBType:    "postgres",
		TargetDBType:    "mssql",
		TotalTables:     12,
		TotalRows:       100000,
		AvgRowSizeBytes: 500,
		Workers:         6,
		ChunkSize:       50000,
		AIReasoning:     "history row used password=secret",
	}}, []driver.AIAdjustmentRecord{{
		Action:      "increase",
		Adjustments: map[string]int{"workers": 6, "invented_knob": 99},
		Reasoning:   "runtime kept known knob",
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
	if got := payload.RuntimeAdjustments[0].Adjustments["workers"]; got != 6 {
		t.Fatalf("runtime workers adjustment = %d, want 6", got)
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
