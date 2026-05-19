package main

import (
	"fmt"
	"strings"
	"time"

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
