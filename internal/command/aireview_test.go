package command

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/aicopilot"
)

// #442: the review must always carry the operator-review disclaimer,
// and unavailable-provider states must render explicitly.
func TestFormatConfigReview(t *testing.T) {
	val := any(16)
	out := FormatConfigReview(&aicopilot.ConfigReview{
		Status:   "ok",
		Provider: "claude",
		Model:    "claude-fable-5",
		Summary:  "two safe tuning changes",
		PatchRecommendations: []aicopilot.ConfigPatchRecommendation{
			{
				Operation:            "set",
				Path:                 "migration.workers",
				Value:                val,
				Rationale:            "source supports more parallel readers",
				Risk:                 "low",
				RequiresConfirmation: true,
			},
		},
		Runbook: aicopilot.ConfigRunbook{
			Title:      "Nightly mssql -> postgres",
			BeforeRun:  []string{"dmt preflight"},
			Run:        []string{"dmt run --dry-run", "dmt run"},
			Validation: []string{"dmt validate"},
			Rollback:   []string{"restore target from snapshot"},
		},
		Notes: []string{"advisory only"},
	})

	for _, want := range []string{
		"Patch recommendations (operator review required; no files were changed):",
		"- SET migration.workers = 16",
		"rationale: source supports more parallel readers",
		"confirmation: required",
		"Runbook: Nightly mssql -> postgres",
		"Prerequisites:",
		"- dmt preflight",
		"Rollback:",
		"- advisory only",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}

	unavailable := FormatConfigReview(aicopilot.UnavailableConfigReview(
		"no AI provider configured in secrets",
		aicopilot.ConfigReviewPayload{}))
	if !strings.Contains(unavailable, "no AI provider configured") {
		t.Errorf("unavailable state not explicit:\n%s", unavailable)
	}
}

// #442: deterministic suggestions ship regardless; the explanation
// block renders provider errors without hiding anything.
func TestFormatPerformanceExplanation(t *testing.T) {
	out := FormatPerformanceExplanation(&aicopilot.PerformanceExplanation{
		Status:  "ok",
		Summary: "chunk size is the lever here",
		Findings: []aicopilot.PerformanceFinding{
			{
				Category:   "throughput",
				Knob:       "migration.chunk_size",
				Rationale:  "rows are wide; smaller chunks reduce memory stalls",
				Evidence:   []string{"avg row 4KB", "writer p99 350ms"},
				NextAction: "try 25000",
			},
		},
	})
	for _, want := range []string{
		"AI performance explanation:",
		"[THROUGHPUT] migration.chunk_size:",
		"- avg row 4KB",
		"next: try 25000",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if FormatPerformanceExplanation(nil) != "" {
		t.Error("nil explanation must render empty")
	}
}
