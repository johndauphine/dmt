package main

import (
	"fmt"
	"strings"

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
		fmt.Printf("%-30s %12d %12d %15s\n",
			t.Name, t.RowCount, t.Partitions, t.PaginationMethod)
	}
	fmt.Println(strings.Repeat("-", 75))
	fmt.Printf("%-30s %12d\n", "TOTAL", r.TotalRows)
	fmt.Println()

	fmt.Printf("Workers: %d\n", r.Workers)
	fmt.Printf("Chunk Size: %d\n", r.ChunkSize)
	fmt.Printf("Estimated Memory: ~%d MB\n", r.EstimatedMemMB)
}

// cacheClear implements `dmt cache clear [--ai-only]` (#177). With no
// flag it removes the entire ~/.dmt/type-cache.json file; with --ai-only
// it preserves any non-AI entries (today: none — but the format is
