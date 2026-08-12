// AI advisory rendering shared by the CLI and the TUI (#442): the
// config-review/runbook output of `dmt ai config-review` and the
// performance explanation of `analyze --ai-explain`. Lives here so the
// TUI shows the exact same advisory blocks (epic #437 non-goal). JSON
// output stays in cmd/dmt.
package command

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

// FormatConfigReview renders patch recommendations and the migration
// runbook. The header states that recommendations require operator
// review and that no files were changed — that promise is part of the
// command contract on both surfaces.
func FormatConfigReview(review *aicopilot.ConfigReview) string {
	if review == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nAI config review and migration runbook:\n")
	fmt.Fprintf(&b, "  Status: %s\n", review.Status)
	if review.Provider != "" {
		fmt.Fprintf(&b, "  Provider: %s", review.Provider)
		if review.Model != "" {
			fmt.Fprintf(&b, " / %s", review.Model)
		}
		b.WriteString("\n")
	}
	if review.Summary != "" {
		fmt.Fprintf(&b, "  Summary: %s\n", review.Summary)
	}
	if review.RefusalReason != "" {
		fmt.Fprintf(&b, "  Refusal: %s\n", review.RefusalReason)
	}
	if review.Error != "" {
		fmt.Fprintf(&b, "  Error: %s\n", review.Error)
	}

	b.WriteString("\n  Patch recommendations (operator review required; no files were changed):\n")
	if len(review.PatchRecommendations) == 0 {
		b.WriteString("    - No config patch recommendations.\n")
	}
	for _, p := range review.PatchRecommendations {
		fmt.Fprintf(&b, "    - %s %s", strings.ToUpper(p.Operation), p.Path)
		if p.Value != nil {
			fmt.Fprintf(&b, " = %v", p.Value)
		}
		b.WriteString("\n")
		if p.Rationale != "" {
			fmt.Fprintf(&b, "      rationale: %s\n", p.Rationale)
		}
		if p.Risk != "" {
			fmt.Fprintf(&b, "      risk: %s\n", p.Risk)
		}
		if p.WhenToApply != "" {
			fmt.Fprintf(&b, "      apply when: %s\n", p.WhenToApply)
		}
		if p.RequiresConfirmation {
			b.WriteString("      confirmation: required\n")
		}
		for _, validationErr := range p.ValidationErrors {
			fmt.Fprintf(&b, "      validation: %s\n", validationErr)
		}
	}

	formatConfigRunbook(&b, review.Runbook)

	if len(review.Notes) > 0 {
		b.WriteString("\n  Notes:\n")
		for _, note := range review.Notes {
			fmt.Fprintf(&b, "    - %s\n", note)
		}
	}
	return b.String()
}

func formatConfigRunbook(b *strings.Builder, runbook aicopilot.ConfigRunbook) {
	if runbook.Title != "" {
		fmt.Fprintf(b, "\n  Runbook: %s\n", runbook.Title)
	} else {
		b.WriteString("\n  Runbook:\n")
	}
	if runbook.Summary != "" {
		fmt.Fprintf(b, "    %s\n", runbook.Summary)
	}
	formatRunbookSteps(b, "Prerequisites", runbook.BeforeRun)
	formatRunbookSteps(b, "Run", runbook.Run)
	formatRunbookSteps(b, "Validation", runbook.Validation)
	formatRunbookSteps(b, "Rollback", runbook.Rollback)
}

func formatRunbookSteps(b *strings.Builder, label string, steps []string) {
	if len(steps) == 0 {
		return
	}
	fmt.Fprintf(b, "    %s:\n", label)
	for _, step := range steps {
		fmt.Fprintf(b, "      - %s\n", step)
	}
}

// FormatPerformanceExplanation renders the advisory AI explanation of
// deterministic smartconfig suggestions (`analyze --ai-explain`).
func FormatPerformanceExplanation(explanation *aicopilot.PerformanceExplanation) string {
	if explanation == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nAI performance explanation:\n")
	fmt.Fprintf(&b, "  Status: %s\n", explanation.Status)
	if explanation.Provider != "" {
		fmt.Fprintf(&b, "  Provider: %s", explanation.Provider)
		if explanation.Model != "" {
			fmt.Fprintf(&b, " / %s", explanation.Model)
		}
		b.WriteString("\n")
	}
	if explanation.Summary != "" {
		fmt.Fprintf(&b, "  Summary: %s\n", explanation.Summary)
	}
	if explanation.Error != "" {
		fmt.Fprintf(&b, "  Error: %s\n", logging.Scrub(explanation.Error))
	}
	if len(explanation.Findings) > 0 {
		b.WriteString("  AI advisory findings:\n")
		for _, finding := range explanation.Findings {
			category := finding.Category
			if category == "" {
				category = "other"
			}
			fmt.Fprintf(&b, "    - [%s] %s: %s\n", strings.ToUpper(category), finding.Knob, finding.Rationale)
			if len(finding.Evidence) > 0 {
				b.WriteString("      evidence:\n")
				for _, evidence := range finding.Evidence {
					fmt.Fprintf(&b, "        - %s\n", evidence)
				}
			}
			if finding.NextAction != "" {
				fmt.Fprintf(&b, "      next: %s\n", finding.NextAction)
			}
		}
	}
	if len(explanation.Notes) > 0 {
		b.WriteString("  Notes:\n")
		for _, note := range explanation.Notes {
			fmt.Fprintf(&b, "    - %s\n", note)
		}
	}
	return b.String()
}
