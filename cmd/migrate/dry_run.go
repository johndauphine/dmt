package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

func printDryRunResult(r *orchestrator.DryRunResult) {
	fmt.Println("\n=== Migration Preview (Dry Run) ===")
	fmt.Printf("Source: %s (%s)\n", r.SourceType, r.SourceSchema)
	fmt.Printf("Target: %s (%s)\n", r.TargetType, r.TargetSchema)
	fmt.Printf("Mode: %s\n", r.TargetMode)
	fmt.Println()

	fmt.Printf("%-30s %12s %12s %15s\n", "Table", "Rows", "Partitions", "Pagination")
	fmt.Println(strings.Repeat("-", 75))
	for _, t := range r.Tables {
		fmt.Printf("%-30s %12s %12d %15s\n",
			t.Name, orchestrator.FormatCount(t.RowCount), t.Partitions, t.PaginationMethod)
	}
	fmt.Println(strings.Repeat("-", 75))
	fmt.Printf("%-30s %12s\n", "TOTAL", orchestrator.FormatCount(r.TotalRows))
	fmt.Println()

	fmt.Printf("Workers: %d\n", r.Workers)
	fmt.Printf("Chunk Size: %d\n", r.ChunkSize)
	fmt.Printf("Estimated Memory: ~%d MB\n", r.EstimatedMemMB)
	if r.EstimatedRowsPerSecond > 0 && r.EstimatedDurationSeconds > 0 {
		eta := time.Duration(r.EstimatedDurationSeconds * float64(time.Second)).Round(time.Second)
		fmt.Printf("Estimated Duration: ~%s at %s rows/sec from recent history\n",
			eta, orchestrator.FormatCount(r.EstimatedRowsPerSecond))
	}
	printDeleteReconciliationPreview(r.DeleteReconciliation)
	printAISchemaAdvisor(r.AISchemaAdvisor)
}

func printDeleteReconciliationPreview(p *orchestrator.DeleteReconciliationPreview) {
	if p == nil {
		return
	}

	fmt.Println()
	fmt.Println("Delete Reconciliation:")
	fmt.Printf("  Due: %t", p.Due)
	if p.Reason != "" {
		fmt.Printf(" (%s)", p.Reason)
	}
	fmt.Println()
	fmt.Printf("  Interval: %s\n", p.Interval)
	if p.LastSuccessAt != nil {
		fmt.Printf("  Last Success: %s\n", p.LastSuccessAt.Format(time.RFC3339))
	}
	if p.NextDueAt != nil {
		fmt.Printf("  Next Due: %s\n", p.NextDueAt.Format(time.RFC3339))
	}
	fmt.Printf("  Eligible Tables: %d\n", p.EligibleTables)
	if p.SkippedNoPKTables > 0 {
		fmt.Printf("  Skipped Tables Without PK: %d\n", p.SkippedNoPKTables)
	}
	if p.CandidateRows != nil {
		fmt.Printf("  Candidate Deletes: %s\n", orchestrator.FormatCount(*p.CandidateRows))
	}
	if len(p.Tables) > 0 {
		fmt.Println("  Tables:")
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
			fmt.Println(line)
		}
	}
}

func printAISchemaAdvisor(review *aicopilot.SchemaAdvisorReview) {
	if review == nil {
		return
	}
	fmt.Println()
	fmt.Println("AI Schema Advisor:")
	fmt.Printf("  Status: %s\n", review.Status)
	if review.Provider != "" {
		fmt.Printf("  Provider: %s", review.Provider)
		if review.Model != "" {
			fmt.Printf(" / %s", review.Model)
		}
		fmt.Println()
	}
	if review.Summary != "" {
		fmt.Printf("  Summary: %s\n", review.Summary)
	}
	if len(review.DeterministicBlockers) > 0 {
		fmt.Println("  Deterministic blockers:")
		for _, blocker := range review.DeterministicBlockers {
			fmt.Printf("    - %s\n", blocker)
		}
	}
	if len(review.Recommendations) > 0 {
		fmt.Println("  Recommendations:")
		for _, rec := range review.Recommendations {
			affected := rec.Table
			if rec.Schema != "" {
				affected = rec.Schema + "." + affected
			}
			if rec.Column != "" {
				affected += "." + rec.Column
			}
			fmt.Printf("    [%s] %s: %s\n", strings.ToUpper(rec.Risk), affected, rec.SuggestedAction)
			if rec.Reason != "" {
				fmt.Printf("      reason: %s\n", rec.Reason)
			}
			if rec.DeterministicGate.Action != "" {
				fmt.Printf("      gate: %s (%s)\n", rec.DeterministicGate.Action, rec.DeterministicGate.Reason)
			}
		}
	}
	if review.Error != "" {
		fmt.Printf("  Error: %s\n", review.Error)
	}
}
