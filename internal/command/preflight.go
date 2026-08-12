// Health-check / preflight rendering shared by the CLI and the TUI
// (#440). Lives here rather than cmd/dmt so /preflight shows the
// exact same readiness facts as `dmt preflight` without the TUI
// duplicating the formatting (epic #437 non-goal). Exit-code
// classification stays in cmd/dmt — it is CLI process semantics.
package command

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/orchestrator"
)

// FormatHealthCheckResult renders connectivity, preflight findings, the
// optional AI readiness review, and the overall verdict for human
// reading.
func FormatHealthCheckResult(r *orchestrator.HealthCheckResult) string {
	var b strings.Builder
	b.WriteString("\nHealth Check Results:\n")
	fmt.Fprintf(&b, "  Source (%s): %s (%dms)\n",
		r.SourceDBType, connectedStatus(r.SourceConnected), r.SourceLatencyMs)
	if r.SourceError != "" {
		fmt.Fprintf(&b, "    Error: %s\n", r.SourceError)
	}
	if r.SourceConnected && r.SourceTableCount > 0 {
		fmt.Fprintf(&b, "    Tables: %d\n", r.SourceTableCount)
	}

	fmt.Fprintf(&b, "  Target (%s): %s (%dms)\n",
		r.TargetDBType, connectedStatus(r.TargetConnected), r.TargetLatencyMs)
	if r.TargetError != "" {
		fmt.Fprintf(&b, "    Error: %s\n", r.TargetError)
	}

	if len(r.PreFlightFindings) > 0 {
		b.WriteString("\n  Preflight findings:\n")
		for _, f := range r.PreFlightFindings {
			fmt.Fprintf(&b, "    [%s] %s/%s: %s\n", f.Severity, f.Side, f.Check, f.Message)
			if f.Remedy != "" {
				fmt.Fprintf(&b, "      remedy: %s\n", f.Remedy)
			}
		}
	}

	formatAIPreflightReview(&b, r.AIPreflightReview)

	verdict := "HEALTHY"
	if !r.Healthy {
		verdict = "UNHEALTHY"
	}
	fmt.Fprintf(&b, "\n  Overall: %s\n", verdict)
	return b.String()
}

func connectedStatus(connected bool) string {
	if connected {
		return "OK"
	}
	return "FAILED"
}

func formatAIPreflightReview(b *strings.Builder, review *aicopilot.PreflightReview) {
	if review == nil {
		return
	}
	b.WriteString("\n  AI readiness review:\n")
	fmt.Fprintf(b, "    Status: %s", review.Status)
	if review.Readiness != "" {
		fmt.Fprintf(b, " (readiness: %s)", strings.ToUpper(review.Readiness))
	}
	b.WriteString("\n")
	if review.Provider != "" {
		fmt.Fprintf(b, "    Provider: %s", review.Provider)
		if review.Model != "" {
			fmt.Fprintf(b, " / %s", review.Model)
		}
		b.WriteString("\n")
	}
	if review.Summary != "" {
		fmt.Fprintf(b, "    Summary: %s\n", review.Summary)
	}
	if review.Error != "" {
		fmt.Fprintf(b, "    Error: %s\n", review.Error)
	}
	if len(review.DeterministicBlockers) > 0 {
		b.WriteString("    Deterministic blockers:\n")
		for _, blocker := range review.DeterministicBlockers {
			fmt.Fprintf(b, "      - %s\n", blocker)
		}
	}
	if len(review.Findings) > 0 {
		b.WriteString("    AI advisory findings:\n")
		for _, f := range review.Findings {
			affected := f.Affected
			if affected == "" {
				affected = f.Category
			}
			fmt.Fprintf(b, "      - [%s] %s: %s\n", f.Severity, affected, f.Rationale)
			if f.NextAction != "" {
				fmt.Fprintf(b, "        next: %s\n", f.NextAction)
			}
		}
	}
	if len(review.Notes) > 0 {
		b.WriteString("    Notes:\n")
		for _, note := range review.Notes {
			fmt.Fprintf(b, "      - %s\n", note)
		}
	}
}
