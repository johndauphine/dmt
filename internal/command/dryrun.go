// Dry-run preview rendering shared by the CLI and the TUI (#439).
// Lives here rather than cmd/dmt so /run --dry-run shows the exact
// same human-readable preview as `dmt run --dry-run` without the TUI
// duplicating the formatting (epic #437 non-goal).
package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/orchestrator"
)

// FormatDryRunResult renders the migration preview for human reading.
func FormatDryRunResult(r *orchestrator.DryRunResult) string {
	var b strings.Builder
	b.WriteString("\n=== Migration Preview (Dry Run) ===\n")
	fmt.Fprintf(&b, "Source: %s (%s)\n", r.SourceType, r.SourceSchema)
	fmt.Fprintf(&b, "Target: %s (%s)\n", r.TargetType, r.TargetSchema)
	fmt.Fprintf(&b, "Mode: %s\n\n", r.TargetMode)

	fmt.Fprintf(&b, "%-30s %12s %12s %15s\n", "Table", "Rows", "Partitions", "Pagination")
	b.WriteString(strings.Repeat("-", 75) + "\n")
	for _, t := range r.Tables {
		fmt.Fprintf(&b, "%-30s %12s %12d %15s\n",
			t.Name, orchestrator.FormatCount(t.RowCount), t.Partitions, t.PaginationMethod)
	}
	b.WriteString(strings.Repeat("-", 75) + "\n")
	fmt.Fprintf(&b, "%-30s %12s\n\n", "TOTAL", orchestrator.FormatCount(r.TotalRows))

	fmt.Fprintf(&b, "Workers: %d\n", r.Workers)
	fmt.Fprintf(&b, "Chunk Size: %d\n", r.ChunkSize)
	rowBytes := r.SafetyRowBytes
	if rowBytes <= 0 {
		rowBytes = 500
	}
	widthSource := "unobserved row-width fallback"
	if r.SafetyRowBytesKnown {
		widthSource = "widest observed table-average model; not a per-row bound"
	}
	fmt.Fprintf(&b, "Estimated Memory: ~%d MB (%s, %d bytes)\n", r.EstimatedMemMB, widthSource, rowBytes)
	if r.EstimatedRowsPerSecond > 0 && r.EstimatedDurationSeconds > 0 {
		eta := time.Duration(r.EstimatedDurationSeconds * float64(time.Second)).Round(time.Second)
		fmt.Fprintf(&b, "Estimated Duration: ~%s at %s rows/sec from recent history\n",
			eta, orchestrator.FormatCount(r.EstimatedRowsPerSecond))
	}
	formatDeleteReconciliationPreview(&b, r.DeleteReconciliation)
	formatAISchemaAdvisor(&b, r.AISchemaAdvisor)
	return b.String()
}

func formatDeleteReconciliationPreview(b *strings.Builder, p *orchestrator.DeleteReconciliationPreview) {
	if p == nil {
		return
	}

	b.WriteString("\nDelete Reconciliation:\n")
	fmt.Fprintf(b, "  Due: %t", p.Due)
	if p.Reason != "" {
		fmt.Fprintf(b, " (%s)", p.Reason)
	}
	b.WriteString("\n")
	fmt.Fprintf(b, "  Interval: %s\n", p.Interval)
	if p.LastSuccessAt != nil {
		fmt.Fprintf(b, "  Last Success: %s\n", p.LastSuccessAt.Format(time.RFC3339))
	}
	if p.NextDueAt != nil {
		fmt.Fprintf(b, "  Next Due: %s\n", p.NextDueAt.Format(time.RFC3339))
	}
	fmt.Fprintf(b, "  Eligible Tables: %d\n", p.EligibleTables)
	if p.SkippedNoPKTables > 0 {
		fmt.Fprintf(b, "  Skipped Tables Without PK: %d\n", p.SkippedNoPKTables)
	}
	if p.CandidateRows != nil {
		fmt.Fprintf(b, "  Candidate Deletes: %s\n", orchestrator.FormatCount(*p.CandidateRows))
	}
	if len(p.Tables) > 0 {
		b.WriteString("  Tables:\n")
		for _, table := range p.Tables {
			line := fmt.Sprintf("    %-30s %12s candidate",
				table.Table,
				orchestrator.FormatCount(table.CandidateRows))
			if table.Skipped {
				line += "  skipped"
				if table.SkipReason != "" {
					line += ": " + table.SkipReason
				}
			}
			if table.Error != "" {
				line += "  count unavailable: " + table.Error
			}
			b.WriteString(line + "\n")
		}
	}
}

func formatAISchemaAdvisor(b *strings.Builder, review *aicopilot.SchemaAdvisorReview) {
	if review == nil {
		return
	}
	b.WriteString("\nAI Schema Advisor:\n")
	fmt.Fprintf(b, "  Status: %s\n", review.Status)
	if review.Provider != "" {
		fmt.Fprintf(b, "  Provider: %s", review.Provider)
		if review.Model != "" {
			fmt.Fprintf(b, " / %s", review.Model)
		}
		b.WriteString("\n")
	}
	if review.Summary != "" {
		fmt.Fprintf(b, "  Summary: %s\n", review.Summary)
	}
	if len(review.DeterministicBlockers) > 0 {
		b.WriteString("  Deterministic blockers:\n")
		for _, blocker := range review.DeterministicBlockers {
			fmt.Fprintf(b, "    - %s\n", blocker)
		}
	}
	if len(review.Recommendations) > 0 {
		b.WriteString("  Recommendations:\n")
		for _, rec := range review.Recommendations {
			affected := rec.Table
			if rec.Schema != "" {
				affected = rec.Schema + "." + affected
			}
			if rec.Column != "" {
				affected += "." + rec.Column
			}
			fmt.Fprintf(b, "    [%s] %s: %s\n", strings.ToUpper(rec.Risk), affected, rec.SuggestedAction)
			if rec.Reason != "" {
				fmt.Fprintf(b, "      reason: %s\n", rec.Reason)
			}
			if rec.DeterministicGate.Action != "" {
				fmt.Fprintf(b, "      gate: %s (%s)\n", rec.DeterministicGate.Action, rec.DeterministicGate.Reason)
			}
		}
	}
	if review.Error != "" {
		fmt.Fprintf(b, "  Error: %s\n", review.Error)
	}
}
