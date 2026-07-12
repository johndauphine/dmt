package tuning

import (
	"fmt"
	"math"
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

// TestExplorationCells_BalancedDesign pins the twelve-run design itself.
// The final four cells are marginally balanced replicates; this test does not
// claim the two-level CS quadratic is independently identifiable.
func TestExplorationCells_BalancedDesign(t *testing.T) {
	if len(explorationCells) != explorationGridRuns {
		t.Fatalf("len(explorationCells) = %d, want explorationGridRuns=%d", len(explorationCells), explorationGridRuns)
	}
	if explorationGridRuns != minRowsToAttemptRegression {
		t.Fatalf("explorationGridRuns = %d, want regression floor %d", explorationGridRuns, minRowsToAttemptRegression)
	}

	firstEight := explorationCells[:8]
	unique := map[explorationCell]bool{}
	wawCounts := map[int]int{}
	wawFractions := map[int]map[float64]bool{}
	wawReaders := map[int]map[readerCandidate]bool{}
	readerCounts := map[readerCandidate]int{}
	for _, cell := range firstEight {
		unique[cell] = true
		wawCounts[cell.WAW]++
		if wawFractions[cell.WAW] == nil {
			wawFractions[cell.WAW] = map[float64]bool{}
		}
		wawFractions[cell.WAW][cell.CSFraction] = true
		if wawReaders[cell.WAW] == nil {
			wawReaders[cell.WAW] = map[readerCandidate]bool{}
		}
		reader := readerCandidate{ParallelReaders: cell.ParallelReaders, ReadAheadBuffers: cell.ReadAheadBuffers}
		wawReaders[cell.WAW][reader] = true
		readerCounts[reader]++
	}
	if len(unique) != 8 {
		t.Errorf("first eight cells contain %d unique cells, want 8", len(unique))
	}
	for _, waw := range []int{1, 2, 3, maxWAWForGrid} {
		if wawCounts[waw] != 2 {
			t.Errorf("first-eight WAW=%d count = %d, want 2", waw, wawCounts[waw])
		}
		if len(wawFractions[waw]) != 2 || !wawFractions[waw][halfOptimumFraction] || !wawFractions[waw][fullOptimumFraction] {
			t.Errorf("first-eight WAW=%d fractions = %v, want both half and full", waw, wawFractions[waw])
		}
		if len(wawReaders[waw]) != 2 {
			t.Errorf("first-eight WAW=%d reader combinations = %v, want 2 distinct", waw, wawReaders[waw])
		}
	}
	for _, reader := range readerGrid {
		if readerCounts[reader] != 2 {
			t.Errorf("first-eight reader=%+v count = %d, want 2", reader, readerCounts[reader])
		}
	}

	allWAWCounts := map[int]int{}
	allReaderCounts := map[readerCandidate]int{}
	allFractionCounts := map[float64]int{}
	for _, cell := range explorationCells {
		allWAWCounts[cell.WAW]++
		allReaderCounts[readerCandidate{ParallelReaders: cell.ParallelReaders, ReadAheadBuffers: cell.ReadAheadBuffers}]++
		allFractionCounts[cell.CSFraction]++
	}
	for _, waw := range []int{1, 2, 3, maxWAWForGrid} {
		if allWAWCounts[waw] != 3 {
			t.Errorf("twelve-run WAW=%d count = %d, want 3", waw, allWAWCounts[waw])
		}
	}
	for _, reader := range readerGrid {
		if allReaderCounts[reader] != 3 {
			t.Errorf("twelve-run reader=%+v count = %d, want 3", reader, allReaderCounts[reader])
		}
	}
	for _, fraction := range []float64{halfOptimumFraction, fullOptimumFraction} {
		if allFractionCounts[fraction] != 6 {
			t.Errorf("twelve-run CS fraction %.1f count = %d, want 6", fraction, allFractionCounts[fraction])
		}
	}

	for _, replicate := range []struct{ got, want int }{{8, 0}, {9, 1}, {10, 6}, {11, 7}} {
		if explorationCells[replicate.got] != explorationCells[replicate.want] {
			t.Errorf("cell %d = %+v, want replicate of cell %d (%+v)", replicate.got, explorationCells[replicate.got], replicate.want, explorationCells[replicate.want])
		}
	}

	profile := DriverProfile{OptimumBulkChunkBytes: 25_000_000}
	rows := explorationRowsForTest(profile, 500, func(explorationCell) float64 { return 1_000 })
	if covered := len(cellsWithCoverage(rows[:8])); covered != 8 {
		t.Errorf("first eight cells produce %d covered (WAW, PR, RAB) cells, want 8", covered)
	}
}

// TestApplyGridExploration_UsesOrderedCells verifies that every output knob
// comes from the same ordered cell and that usable-count labels reach 12/12.
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
		wantLabel := fmt.Sprintf("run %d/%d", i+1, explorationGridRuns)
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

	// Walk all 12 cold-start positions. The balanced design probes every
	// WAW exactly three times, regardless of historical retry outcomes.
	wawCounts := map[int]int{}
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, i)
		wawCounts[out.WriteAheadWriters]++
	}
	for _, waw := range []int{1, 2, 3, maxWAWForGrid} {
		if wawCounts[waw] != 3 {
			t.Errorf("planned grid WAW=%d count = %d, want 3 (counts %v)", waw, wawCounts[waw], wawCounts)
		}
	}
}

// TestApplyGridExploration_HardLimitUsesEligibleRing verifies filtering is
// applied before raw indexing. With only half-CS table entries eligible,
// consecutive attempts traverse all six eligible entries before repeating.
func TestApplyGridExploration_HardLimitUsesEligibleRing(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        30_000,
	}
	eligibleIndexes := []int{0, 1, 2, 3, 8, 9}
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
	if !strings.Contains(out.Reasoning, "probe idx 1/12") {
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

// TestExplorationCells_RegressionReady verifies the production ridge fitter
// clears its twelve-row gate on the planned design and preserves separately
// generated WAW and PR directions. It deliberately avoids coefficient-sign
// assertions because fixed row width and two CS levels leave aliased columns.
func TestExplorationCells_RegressionReady(t *testing.T) {
	profile := DriverProfile{OptimumBulkChunkBytes: 25_000_000}
	const avgRowBytes int64 = 500

	t.Run("twelve-row production gate", func(t *testing.T) {
		rows := explorationRowsForTest(profile, avgRowBytes, func(cell explorationCell) float64 {
			return 1_000 + 100*float64(cell.WAW) + 50*float64(cell.ParallelReaders)
		})
		model, err := fitRegression(rows)
		if err != nil {
			t.Fatalf("fitRegression on twelve exploration rows: %v", err)
		}
		if model.nObs != explorationGridRuns || model.nFeat != 8 {
			t.Errorf("model shape = %d observations/%d features, want %d/8", model.nObs, model.nFeat, explorationGridRuns)
		}
		pred := model.Predict(3, 25_000_000, 2, 4, "mssql", "postgres", "", avgRowBytes)
		if math.IsNaN(pred) || math.IsInf(pred, 0) {
			t.Errorf("twelve-row fit prediction = %v, want finite", pred)
		}
	})

	t.Run("WAW direction", func(t *testing.T) {
		rows := explorationRowsForTest(profile, avgRowBytes, func(cell explorationCell) float64 {
			return 1_000 + 200*float64(cell.WAW)
		})
		model, err := fitRegression(rows)
		if err != nil {
			t.Fatalf("fitRegression: %v", err)
		}
		low := model.Predict(1, 25_000_000, 2, 4, "mssql", "postgres", "", avgRowBytes)
		high := model.Predict(maxWAWForGrid, 25_000_000, 2, 4, "mssql", "postgres", "", avgRowBytes)
		if math.IsNaN(low) || math.IsInf(low, 0) || math.IsNaN(high) || math.IsInf(high, 0) {
			t.Fatalf("WAW predictions must be finite: low=%v high=%v", low, high)
		}
		if high <= low {
			t.Errorf("WAW-generated direction lost with other inputs fixed: predict(WAW=%d)=%.2f <= predict(WAW=1)=%.2f", maxWAWForGrid, high, low)
		}
	})

	t.Run("parallel-reader direction", func(t *testing.T) {
		rows := explorationRowsForTest(profile, avgRowBytes, func(cell explorationCell) float64 {
			return 1_000 + 400*float64(cell.ParallelReaders)
		})
		model, err := fitRegression(rows)
		if err != nil {
			t.Fatalf("fitRegression: %v", err)
		}
		low := model.Predict(3, 25_000_000, 2, 4, "mssql", "postgres", "", avgRowBytes)
		high := model.Predict(3, 25_000_000, 4, 4, "mssql", "postgres", "", avgRowBytes)
		if math.IsNaN(low) || math.IsInf(low, 0) || math.IsNaN(high) || math.IsInf(high, 0) {
			t.Fatalf("PR predictions must be finite: low=%v high=%v", low, high)
		}
		if high <= low {
			t.Errorf("PR-generated direction lost with other inputs fixed: predict(PR=4)=%.2f <= predict(PR=2)=%.2f", high, low)
		}
	})
}

func TestTune_ColdStartUsesTwelveUsableRuns(t *testing.T) {
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

	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	if got.Tier != TierRegression {
		t.Fatalf("after %d usable rows Tier = %q, want regression eligibility; reasoning: %s", explorationGridRuns, got.Tier, got.Reasoning)
	}
	if !strings.Contains(got.Reasoning, "regression-selected") {
		t.Errorf("post-cold-start reasoning = %q, want regression selection", got.Reasoning)
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
	rows := explorationRowsForTest(profile, in.AvgRowBytes, func(explorationCell) float64 { return 1_000 })
	rows[0].AdjustedAtRuntime = true

	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	want := baseline(in, profile)
	applyGridExploration(&want, in, profile, explorationGridRuns-1, explorationGridRuns)
	applyMemoryClamp(&want, in)
	if got.Tier != TierExploration {
		t.Fatalf("one adjusted attempt should leave 11 usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
	if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
		got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
		t.Errorf("adjusted-attempt output = (WAW=%d CS=%d PR=%d RAB=%d), want raw-index-12 cell (WAW=%d CS=%d PR=%d RAB=%d)",
			got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
			want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
	}
	if !strings.Contains(got.Reasoning, "run 12/12") || !strings.Contains(got.Reasoning, "runtime-adjusted") {
		t.Errorf("adjusted-attempt reasoning = %q, want usable label and hygiene note", got.Reasoning)
	}

	replacement := rows[0]
	replacement.AdjustedAtRuntime = false
	rows = append(rows, replacement)
	got = Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	if got.Tier != TierRegression {
		t.Fatalf("replacement clean probe should restore 12 usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
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
	rows := explorationRowsForTest(profile, in.AvgRowBytes, func(explorationCell) float64 { return 1_000 })
	rows[0].FinalThroughput = 1
	rows[0].FinalThroughputBytes = in.AvgRowBytes

	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	want := baseline(in, profile)
	applyGridExploration(&want, in, profile, explorationGridRuns-1, explorationGridRuns)
	applyMemoryClamp(&want, in)
	if got.Tier != TierExploration {
		t.Fatalf("one post-filter outlier should leave 11 usable rows: Tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
	if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
		got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
		t.Errorf("outlier-filtered output = (WAW=%d CS=%d PR=%d RAB=%d), want usable-index-11/raw-index-12 cell (WAW=%d CS=%d PR=%d RAB=%d)",
			got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
			want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
	}
	if !strings.Contains(got.Reasoning, "run 12/12") {
		t.Errorf("outlier-filtered attempt reasoning = %q, want usable-count label run 12/12", got.Reasoning)
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
		if out.WriteAheadWriters < 1 || out.WriteAheadWriters > maxWAWForGrid {
			t.Errorf("trial %d: WAW=%d outside valid range [1..%d]",
				trial, out.WriteAheadWriters, maxWAWForGrid)
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
