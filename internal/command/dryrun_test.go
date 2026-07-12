package command

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// #439: the shared renderer must carry the preview's load-bearing facts —
// both the CLI and /run --dry-run print exactly this string.
func TestFormatDryRunResult(t *testing.T) {
	rows := int64(42)
	out := FormatDryRunResult(&orchestrator.DryRunResult{
		SourceType:   "mssql",
		SourceSchema: "dbo",
		TargetType:   "postgres",
		TargetSchema: "public",
		TargetMode:   "drop_recreate",
		Tables: []orchestrator.DryRunTable{
			{Name: "orders", RowCount: 1500000, Partitions: 4, PaginationMethod: "keyset"},
		},
		TotalRows:      1500000,
		Workers:        8,
		ChunkSize:      50000,
		EstimatedMemMB: 256,
		DeleteReconciliation: &orchestrator.DeleteReconciliationPreview{
			Due: true, Reason: "interval elapsed", Interval: "24h",
			EligibleTables: 1, CandidateRows: &rows,
		},
		AISchemaAdvisor: &aicopilot.SchemaAdvisorReview{
			Status:  "ok",
			Summary: "no drift detected",
		},
	})

	for _, want := range []string{
		"=== Migration Preview (Dry Run) ===",
		"Source: mssql (dbo)",
		"Target: postgres (public)",
		"Mode: drop_recreate",
		"orders",
		"keyset",
		"Workers: 8",
		"Chunk Size: 50000",
		"Estimated Memory: ~256 MB",
		"unobserved row-width fallback, 500 bytes",
		"Delete Reconciliation:",
		"Due: true (interval elapsed)",
		"Candidate Deletes: 42",
		"AI Schema Advisor:",
		"Summary: no drift detected",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
