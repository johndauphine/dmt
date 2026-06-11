// Triage-review rendering shared by the CLI and the TUI (#441). Lives
// here rather than cmd/migrate so /validate --ai-triage and /diagnose
// show the exact same review as the CLI commands (epic #437 non-goal).
// JSON output stays in cmd/migrate — the TUI renders blocks instead.
package command

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/aicopilot"
)

// FormatTriageReview renders an AI triage review for human reading.
// Deterministic facts always render before the AI advisory findings so
// they stay visible and authoritative even when the AI errored.
func FormatTriageReview(review *aicopilot.TriageReview) string {
	if review == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\nAI triage: %s", review.Status)
	if review.Impact != "" {
		fmt.Fprintf(&b, " (impact: %s)", review.Impact)
	}
	switch {
	case review.Provider != "" && review.Model != "":
		fmt.Fprintf(&b, " via %s/%s", review.Provider, review.Model)
	case review.Provider != "":
		fmt.Fprintf(&b, " via %s", review.Provider)
	case review.Model != "":
		fmt.Fprintf(&b, " via %s", review.Model)
	}
	b.WriteString("\n")
	if review.Summary != "" {
		fmt.Fprintf(&b, "Summary: %s\n", review.Summary)
	}
	if review.Error != "" {
		fmt.Fprintf(&b, "AI error: %s\n", review.Error)
	}
	if len(review.DeterministicFacts) > 0 {
		b.WriteString("Deterministic facts:\n")
		for _, fact := range review.DeterministicFacts {
			affected := ""
			if fact.Affected != "" {
				affected = " [" + fact.Affected + "]"
			}
			fmt.Fprintf(&b, "- %s%s: %s\n", fact.Category, affected, fact.Detail)
		}
	}
	if len(review.Findings) > 0 {
		b.WriteString("AI advisory findings:\n")
		for _, finding := range review.Findings {
			formatTriageFinding(&b, finding)
		}
	}
	if len(review.Notes) > 0 {
		b.WriteString("Notes:\n")
		for _, note := range review.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
	}
	return b.String()
}

func formatTriageFinding(b *strings.Builder, f aicopilot.TriageFinding) {
	head := strings.TrimSpace(strings.Join([]string{f.Severity, f.Category}, " "))
	if f.Affected != "" {
		head += " [" + f.Affected + "]"
	}
	fmt.Fprintf(b, "- %s\n", strings.TrimSpace(head))
	if len(f.AffectedTables) > 0 {
		fmt.Fprintf(b, "  affected tables: %s\n", strings.Join(f.AffectedTables, ", "))
	}
	if f.LikelyCause != "" {
		fmt.Fprintf(b, "  likely cause: %s\n", f.LikelyCause)
	}
	for _, h := range f.Hypotheses {
		fmt.Fprintf(b, "  hypothesis (%s): %s\n", h.Confidence, h.Rationale)
	}
	if len(f.SuggestedCommands) > 0 {
		fmt.Fprintf(b, "  suggested commands: %s\n", strings.Join(f.SuggestedCommands, "; "))
	}
	if len(f.SuggestedConfigChanges) > 0 {
		fmt.Fprintf(b, "  config changes: %s\n", strings.Join(f.SuggestedConfigChanges, "; "))
	}
	if f.ManualInspection != "" {
		fmt.Fprintf(b, "  inspect manually: %s\n", f.ManualInspection)
	}
	if f.NextAction != "" {
		fmt.Fprintf(b, "  next action: %s\n", f.NextAction)
	}
}
