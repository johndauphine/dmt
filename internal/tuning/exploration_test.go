package tuning

import (
	"fmt"
	"strings"
	"testing"
)

// TestShouldExplore covers the three entry conditions:
//   - --explore forced (regardless of bucket count)
//   - bucket cold start (rows < explorationGridRuns)
//   - steady state (rows ≥ explorationGridRuns) — neither
func TestShouldExplore(t *testing.T) {
	cases := []struct {
		name        string
		in          Input
		bucketCount int
		want        bool
	}{
		{"forced regardless of count", Input{ForceExplore: true}, 100, true},
		{"cold start", Input{}, 0, true},
		{"cold start near boundary", Input{}, explorationGridRuns - 1, true},
		{"steady state at boundary", Input{}, explorationGridRuns, false},
		{"steady state past boundary", Input{}, 100, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldExplore(tc.in, tc.bucketCount)
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func explorationRowsForTest(profile DriverProfile, avgRowBytes int64, throughput func(explorationCell) float64) []HistoryRecord {
	rows := make([]HistoryRecord, len(explorationCells))
	for i, cell := range explorationCells {
		csBytes := int64(cell.CSFraction * float64(profile.OptimumBulkChunkBytes))
		finalThroughput := throughput(cell)
		rows[i] = HistoryRecord{
			SourceDBType:         "mssql",
			TargetDBType:         "postgres",
			WriteAheadWriters:    cell.WAW,
			ChunkSize:            int(csBytes / avgRowBytes),
			AvgRowBytes:          avgRowBytes,
			ParallelReaders:      cell.ParallelReaders,
			ReadAheadBuffers:     cell.ReadAheadBuffers,
			FinalThroughput:      finalThroughput,
			FinalThroughputBytes: int64(finalThroughput * float64(avgRowBytes)),
			CPUCores:             16,
			MemoryGB:             48,
			Platform:             "linux",
		}
	}
	return rows
}

// TestExplorationCells_PreEpicMenu pins both the six-run automatic window and
// the complete eight-cell rotation used by forced/replacement probes.
func TestExplorationCells_PreEpicMenu(t *testing.T) {
	if explorationGridRuns != 6 {
		t.Fatalf("explorationGridRuns = %d, want restored six-run window", explorationGridRuns)
	}
	if explorationGridRuns >= minRowsToAttemptRegression {
		t.Fatalf("planned window %d must end before regression floor %d", explorationGridRuns, minRowsToAttemptRegression)
	}
	want := [...]explorationCell{
		{WAW: 1, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
		{WAW: 2, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},
		{WAW: 3, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
		{WAW: maxLearnableWAW, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
		{WAW: 1, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
		{WAW: 2, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},
		{WAW: 3, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
		{WAW: maxLearnableWAW, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	}
	if explorationCells != want {
		t.Fatalf("explorationCells = %#v, want pre-epic menu %#v", explorationCells, want)
	}

	seenReaders := map[readerCandidate]bool{}
	for _, cell := range explorationCells[:explorationGridRuns] {
		seenReaders[readerCandidate{ParallelReaders: cell.ParallelReaders, ReadAheadBuffers: cell.ReadAheadBuffers}] = true
	}
	if len(seenReaders) != len(readerGrid) {
		t.Errorf("six planned probes cover %d/%d reader cells", len(seenReaders), len(readerGrid))
	}
}

// TestApplyGridExploration_UsesOrderedCells verifies that every output knob
// comes from the same ordered cell. Only the first six positions use a
// cold-start run label; later forced/replacement positions use a probe label.
func TestApplyGridExploration_UsesOrderedCells(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	for i, want := range explorationCells {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, i)
		wantChunkRows := int(int64(want.CSFraction*float64(profile.OptimumBulkChunkBytes)) / in.AvgRowBytes)
		if out.WriteAheadWriters != want.WAW || out.ChunkSize != wantChunkRows ||
			out.ParallelReaders != want.ParallelReaders || out.ReadAheadBuffers != want.ReadAheadBuffers {
			t.Errorf("step %d output = (WAW=%d CS=%d PR=%d RAB=%d), want cell %+v with CS=%d",
				i, out.WriteAheadWriters, out.ChunkSize, out.ParallelReaders, out.ReadAheadBuffers, want, wantChunkRows)
		}
		if out.Tier != TierExploration {
			t.Errorf("step %d Tier = %q, want %q", i, out.Tier, TierExploration)
		}
		wantLabel := fmt.Sprintf("probe idx %d/%d", i+1, len(explorationCells))
		if i < explorationGridRuns {
			wantLabel = fmt.Sprintf("run %d/%d", i+1, explorationGridRuns)
		}
		if !strings.Contains(out.Reasoning, wantLabel) {
			t.Errorf("step %d reasoning = %q, want label %q", i, out.Reasoning, wantLabel)
		}
	}
}

func TestApplyGridExploration_UsesRepresentativeWidth(t *testing.T) {
	in := Input{
		AvgRowBytes:            2_000,
		RepresentativeRowBytes: 8_000,
		Platform:               "linux",
		CPUCores:               8,
	}
	profile := DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 16_000_000}
	out := baseline(in, profile)

	applyGridExploration(&out, in, profile, 0, 0)
	// Cell zero is the 0.5x candidate: 8 MB / 8 KB = 1000 rows.
	if out.ChunkSize != 1_000 {
		t.Fatalf("exploration ChunkSize = %d, want 1000 from representative width", out.ChunkSize)
	}
}

// TestApplyGridExploration_SetsReaderFields (#219) — every grid pick
// must produce non-zero PR and RAB. Pre-fix the exploration left both
// at whatever the baseline produced (PR=2, RAB=4); after #219 they must
// be the values carried by the selected complete cell.
func TestApplyGridExploration_SetsReaderFields(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, i)
		if out.ParallelReaders < 1 || out.ReadAheadBuffers < 1 {
			t.Errorf("bucketCount=%d: grid pick produced invalid (PR=%d, RAB=%d); expected positives",
				i, out.ParallelReaders, out.ReadAheadBuffers)
		}
		// Reasoning string must surface the picked PR/RAB so users can
		// see why this run differs from baseline.
		if !strings.Contains(out.Reasoning, "PR=") || !strings.Contains(out.Reasoning, "RAB=") {
			t.Errorf("bucketCount=%d: Reasoning missing PR/RAB tags; got %q", i, out.Reasoning)
		}
	}
}

// TestApplyEpsilonPerturbation_CanNudgeReaderAxes (#219) — verifies the
// perturbation set actually includes PR/RAB directions. With WAW and CS
// at the grid's edges the only available nudges should be on the reader
// axes; the perturbed output must show a change there.
func TestApplyEpsilonPerturbation_CanNudgeReaderAxes(t *testing.T) {
	profile := DriverProfile{HardChunkLimit: 50_000}

	// Seed many trials; with 8 directions and 4 reader-side ones, ~50%
	// of trials nudge PR or RAB. Probability all 100 are non-reader is
	// 0.5^100 ≈ 8e-31 — effectively zero. If we never see a reader
	// nudge, the directions aren't wired.
	sawReaderNudge := false
	for trial := 0; trial < 100; trial++ {
		out := Output{
			WriteAheadWriters: 2,
			ChunkSize:         50_000,
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
		}
		applyEpsilonPerturbation(&out, profile, 0)
		if out.ParallelReaders != 2 || out.ReadAheadBuffers != 4 {
			sawReaderNudge = true
			break
		}
	}
	if !sawReaderNudge {
		t.Errorf("100 perturbation trials never nudged PR or RAB; reader-side directions appear absent")
	}
}

// TestApplyGridExploration_IgnoresHistoricalRetries — issue #186
// regression guard. Exploration probes every WAW in the planned grid
// regardless of historical retries, since exploration's purpose is
// gathering data so historical verdicts can be re-examined. The
// retry-rate filter applies to SELECTION paths only, not exploration.
func TestApplyGridExploration_IgnoresHistoricalRetries(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	// The restored six-run plan still revisits WAW=1 and WAW=2 while probing
	// the two higher values once, regardless of historical retry outcomes.
	wawCounts := map[int]int{}
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, i)
		wawCounts[out.WriteAheadWriters]++
	}
	wantCounts := map[int]int{1: 2, 2: 2, 3: 1, maxLearnableWAW: 1}
	for waw, want := range wantCounts {
		if wawCounts[waw] != want {
			t.Errorf("planned grid WAW=%d count = %d, want %d (counts %v)", waw, wawCounts[waw], want, wawCounts)
		}
	}
}

// TestApplyGridExploration_HardLimitUsesEligibleRing verifies filtering is
// applied before raw indexing. With only half-CS entries eligible,
// consecutive attempts traverse all four entries before repeating.
func TestApplyGridExploration_HardLimitUsesEligibleRing(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        30_000,
	}
	eligibleIndexes := []int{0, 1, 2, 3}
	for rawIndex, originalIndex := range eligibleIndexes {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, explorationGridRuns, rawIndex)
		want := explorationCells[originalIndex]
		if out.WriteAheadWriters != want.WAW || out.ChunkSize != 25_000 ||
			out.ParallelReaders != want.ParallelReaders || out.ReadAheadBuffers != want.ReadAheadBuffers {
			t.Errorf("raw index %d output = (WAW=%d CS=%d PR=%d RAB=%d), want eligible table cell %d (%+v)",
				rawIndex, out.WriteAheadWriters, out.ChunkSize, out.ParallelReaders, out.ReadAheadBuffers, originalIndex, want)
		}
		wantLabel := fmt.Sprintf("probe idx %d/%d", originalIndex+1, len(explorationCells))
		if !strings.Contains(out.Reasoning, wantLabel) {
			t.Errorf("raw index %d reasoning = %q, want original-cell label %q", rawIndex, out.Reasoning, wantLabel)
		}
	}

	out := baseline(in, profile)
	applyGridExploration(&out, in, profile, explorationGridRuns, len(eligibleIndexes))
	if !strings.Contains(out.Reasoning, "probe idx 1/8") {
		t.Errorf("eligible ring did not repeat from first cell after one traversal: %q", out.Reasoning)
	}
}

// TestApplyGridExploration_NoEligibleCellKeepsBaseline verifies the existing
// safe fallback when both CS levels exceed the protocol cap.
func TestApplyGridExploration_NoEligibleCellKeepsBaseline(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        10_000,
	}
	out := baseline(in, profile)
	wantWAW, wantChunk, wantPR, wantRAB := out.WriteAheadWriters, out.ChunkSize, out.ParallelReaders, out.ReadAheadBuffers
	applyGridExploration(&out, in, profile, 0, 0)
	if out.WriteAheadWriters != wantWAW || out.ChunkSize != wantChunk ||
		out.ParallelReaders != wantPR || out.ReadAheadBuffers != wantRAB {
		t.Errorf("fully filtered grid changed baseline: got (WAW=%d CS=%d PR=%d RAB=%d), want (WAW=%d CS=%d PR=%d RAB=%d)",
			out.WriteAheadWriters, out.ChunkSize, out.ParallelReaders, out.ReadAheadBuffers,
			wantWAW, wantChunk, wantPR, wantRAB)
	}
	if out.Tier == TierExploration || !strings.Contains(out.Reasoning, "every planned-grid candidate filtered") {
		t.Errorf("fully filtered grid Tier/Reasoning = %q/%q, want baseline fallback explanation", out.Tier, out.Reasoning)
	}
}

func TestTune_ColdStartUsesSixUsableRuns(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	rows := explorationRowsForTest(profile, in.AvgRowBytes, func(explorationCell) float64 { return 1_000 })

	for completed := 0; completed < explorationGridRuns; completed++ {
		got := Tune(in, profile, &stubHistory{rows: rows[:completed]}, DBTuning{})
		want := baseline(in, profile)
		applyGridExploration(&want, in, profile, completed, completed)
		applyMemoryClamp(&want, in)
		if got.Tier != TierExploration {
			t.Fatalf("after %d usable rows Tier = %q, want %q; reasoning: %s", completed, got.Tier, TierExploration, got.Reasoning)
		}
		if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
			got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
			t.Errorf("after %d usable rows output = (WAW=%d CS=%d PR=%d RAB=%d), want (WAW=%d CS=%d PR=%d RAB=%d)",
				completed, got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
				want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
		}
		wantLabel := fmt.Sprintf("run %d/%d", completed+1, explorationGridRuns)
		if !strings.Contains(got.Reasoning, wantLabel) {
			t.Errorf("after %d usable rows reasoning = %q, want %q", completed, got.Reasoning, wantLabel)
		}
	}

	got := Tune(in, profile, &stubHistory{rows: rows[:explorationGridRuns]}, DBTuning{})
	if got.Tier == TierExploration || strings.Contains(got.Reasoning, "exploration: planned grid") {
		t.Fatalf("after %d usable rows planned exploration should be complete: tier=%q reasoning=%s", explorationGridRuns, got.Tier, got.Reasoning)
	}
}

func TestTune_AdjustedAttemptAdvancesRawIndexNotUsableCount(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	rows := explorationRowsForTest(profile, in.AvgRowBytes, func(explorationCell) float64 { return 1_000 })[:explorationGridRuns]
	rows[0].AdjustedAtRuntime = true

	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	want := baseline(in, profile)
	applyGridExploration(&want, in, profile, explorationGridRuns-1, explorationGridRuns)
	applyMemoryClamp(&want, in)
	if got.Tier != TierExploration {
		t.Fatalf("one adjusted attempt should leave five usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
	if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
		got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
		t.Errorf("adjusted-attempt output = (WAW=%d CS=%d PR=%d RAB=%d), want raw-index-6 cell (WAW=%d CS=%d PR=%d RAB=%d)",
			got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
			want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
	}
	if !strings.Contains(got.Reasoning, "run 6/6") || !strings.Contains(got.Reasoning, "runtime-adjusted") {
		t.Errorf("adjusted-attempt reasoning = %q, want usable label and hygiene note", got.Reasoning)
	}

	replacement := rows[0]
	replacement.AdjustedAtRuntime = false
	rows = append(rows, replacement)
	got = Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	if got.Tier == TierExploration {
		t.Fatalf("replacement clean probe should complete six usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
}

func TestTune_LowThroughputOutlierDoesNotAdvanceUsableCount(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	rows := explorationRowsForTest(profile, in.AvgRowBytes, func(explorationCell) float64 { return 1_000 })[:explorationGridRuns]
	rows[0].FinalThroughput = 1
	rows[0].FinalThroughputBytes = in.AvgRowBytes

	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	want := baseline(in, profile)
	applyGridExploration(&want, in, profile, explorationGridRuns-1, explorationGridRuns)
	applyMemoryClamp(&want, in)
	if got.Tier != TierExploration {
		t.Fatalf("one post-filter outlier should leave five usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
	if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
		got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
		t.Errorf("outlier-filtered output = (WAW=%d CS=%d PR=%d RAB=%d), want usable-index-5/raw-index-6 cell (WAW=%d CS=%d PR=%d RAB=%d)",
			got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
			want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
	}
	if !strings.Contains(got.Reasoning, "run 6/6") {
		t.Errorf("outlier-filtered attempt reasoning = %q, want usable-count label run 6/6", got.Reasoning)
	}
}

func TestTune_ExplorationSmallBudgetPreservesSelectorProvenance(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	out := Tune(in, profile, &stubHistory{}, DBTuning{})

	if out.Tier != TierExploration {
		t.Fatalf("Tier = %q, want %q", out.Tier, TierExploration)
	}
	if !strings.Contains(out.Reasoning, "exploration: planned grid") || !strings.Contains(out.Reasoning, "memory clamp") {
		t.Fatalf("Reasoning = %q, want exploration selection followed by memory clamp", out.Reasoning)
	}
	if out.ChunkSize >= 25_000 {
		t.Errorf("small budget left exploration ChunkSize=%d, want clamp below 25000", out.ChunkSize)
	}
	if out.EstimatedMemMB > in.MemoryBudgetMB {
		t.Errorf("EstimatedMemMB=%d exceeds budget=%d", out.EstimatedMemMB, in.MemoryBudgetMB)
	}
}

// TestShouldEpsilonPerturb_BoundaryProbabilities verifies the two
// degenerate ε values are handled deterministically (ε=0 always false,
// ε=1 always true) so callers can rely on those semantics in tests.
func TestShouldEpsilonPerturb_BoundaryProbabilities(t *testing.T) {
	// ε=0 → never perturb (100 trials should all be false)
	for i := 0; i < 100; i++ {
		if shouldEpsilonPerturb(0) {
			t.Fatalf("ε=0 should never perturb; trial %d returned true", i)
		}
	}
	// ε=1 → always perturb
	for i := 0; i < 100; i++ {
		if !shouldEpsilonPerturb(1) {
			t.Fatalf("ε=1 should always perturb; trial %d returned false", i)
		}
	}
}

// TestApplyEpsilonPerturbation_NudgesOneStep verifies the perturbation
// changes exactly one knob (WAW or ChunkSize) by one step, and never
// pushes outside the WAW grid bounds. Doesn't pin which direction
// (random) — just that the change happened and stayed in range.
func TestApplyEpsilonPerturbation_NudgesOneStep(t *testing.T) {
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	for trial := 0; trial < 50; trial++ {
		out := Output{WriteAheadWriters: 3, ChunkSize: 30000}
		original := out
		applyEpsilonPerturbation(&out, profile, 0)
		changed := out.WriteAheadWriters != original.WriteAheadWriters || out.ChunkSize != original.ChunkSize
		if !changed {
			t.Errorf("trial %d: perturbation didn't change anything (WAW=%d, CS=%d)",
				trial, out.WriteAheadWriters, out.ChunkSize)
		}
		if out.WriteAheadWriters < 1 || out.WriteAheadWriters > maxLearnableWAW {
			t.Errorf("trial %d: WAW=%d outside valid range [1..%d]",
				trial, out.WriteAheadWriters, maxLearnableWAW)
		}
		if out.ChunkSize < 1 {
			t.Errorf("trial %d: ChunkSize=%d below 1", trial, out.ChunkSize)
		}
	}
}

// TestApplyEpsilonPerturbation_CanComposeTwoDirections (#295) verifies
// a perturbed run can move along two axes in one probe. With ε=1 the
// second-step conditional probability is forced on, so the reasoning
// must record a composite direction.
func TestApplyEpsilonPerturbation_CanComposeTwoDirections(t *testing.T) {
	out := Output{
		WriteAheadWriters: 3,
		ChunkSize:         30_000,
		ParallelReaders:   2,
		ReadAheadBuffers:  8,
	}
	applyEpsilonPerturbation(&out, DriverProfile{Name: "postgres", HardChunkLimit: 100_000}, 1)

	if !strings.Contains(out.Reasoning, ", ") {
		t.Fatalf("expected composite ε-perturbation reasoning, got %q", out.Reasoning)
	}
	if out.Tier != TierExploration {
		t.Fatalf("composite perturbation Tier = %q, want %q", out.Tier, TierExploration)
	}
}

// TestTune_ForceExploreOverridesHistory verifies in.ForceExplore=true
// drops the run into planned-grid even when there's plenty of history
// the regression would otherwise use.
func TestTune_ForceExploreOverridesHistory(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
		ForceExplore:   true,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000, ScaleWritersWithCores: true}
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: 2, // would dominate without --explore
			ChunkSize:         50_000,
			AvgRowBytes:       500,
			FinalThroughput:   600_000,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	history := &stubHistory{rows: rows}

	out := Tune(in, profile, history, DBTuning{})

	if !strings.Contains(out.Reasoning, "exploration: planned grid") {
		t.Errorf("Reasoning should record planned-grid pick when ForceExplore=true; got %q", out.Reasoning)
	}
}
