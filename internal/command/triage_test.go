package command

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/aicopilot"
)

// #441: deterministic facts must render even when the AI errored, and
// AI findings stay labeled advisory.
func TestFormatTriageReview(t *testing.T) {
	out := FormatTriageReview(&aicopilot.TriageReview{
		Status:   "ok",
		Impact:   "data_mismatch",
		Provider: "claude",
		Model:    "claude-fable-5",
		Summary:  "row counts diverge on two tables",
		DeterministicFacts: []aicopilot.TriageFact{
			{Category: "rowcount", Affected: "dbo.orders", Detail: "source 100, target 98"},
		},
		Findings: []aicopilot.TriageFinding{
			{
				Severity:       "high",
				Category:       "data-loss",
				Affected:       "dbo.orders",
				AffectedTables: []string{"dbo.orders"},
				LikelyCause:    "writer retry dropped a chunk",
				NextAction:     "re-run transfer for dbo.orders",
			},
		},
		Notes: []string{"advisory only"},
	})

	for _, want := range []string{
		"AI triage: ok (impact: data_mismatch) via claude/claude-fable-5",
		"Summary: row counts diverge on two tables",
		"Deterministic facts:",
		"- rowcount [dbo.orders]: source 100, target 98",
		"AI advisory findings:",
		"- high data-loss [dbo.orders]",
		"likely cause: writer retry dropped a chunk",
		"next action: re-run transfer for dbo.orders",
		"- advisory only",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}

	if FormatTriageReview(nil) != "" {
		t.Error("nil review must render empty")
	}

	errOut := FormatTriageReview(&aicopilot.TriageReview{
		Status: "error",
		Error:  "provider unavailable",
		DeterministicFacts: []aicopilot.TriageFact{
			{Category: "rowcount", Detail: "source 100, target 98"},
		},
	})
	for _, want := range []string{"AI error: provider unavailable", "Deterministic facts:"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("error output missing %q\n%s", want, errOut)
		}
	}
}
