package tuning

import (
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

// TestApplyGridExploration_RotatesByBucketCount verifies the planned
// grid picks deterministically based on the bucket's run count, so
// across the first K=6 runs the (WAW, CS) selections cover varied
// candidates instead of repeating the same point.
func TestApplyGridExploration_RotatesByBucketCount(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	picks := make(map[gridCandidate]int)
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i)
		// Reverse the CSFraction from out.ChunkSize for set-membership check.
		// At avg=500, full optimum 25MB → 50000 rows; half → 25000 rows.
		var frac float64
		switch out.ChunkSize {
		case 50000:
			frac = fullOptimumFraction
		case 25000:
			frac = halfOptimumFraction
		default:
			t.Fatalf("unexpected ChunkSize %d (expected one of 25000, 50000)", out.ChunkSize)
		}
		picks[gridCandidate{WAW: out.WriteAheadWriters, CSFraction: frac}]++
	}
	if len(picks) < 4 {
		t.Errorf("over %d cold-start runs the grid covered only %d distinct (WAW, CS) cells; expected ≥4 for variance",
			explorationGridRuns, len(picks))
	}
}

// TestApplyGridExploration_CoversReaderGrid (#219) verifies the inner
// (PR, RAB) grid is exercised during cold-start. With explorationGridRuns=6
// and len(readerGrid)=4, the first 4 cold-start runs must cycle through
// every reader combo; after 6 runs we've seen all 4 inner combos at
// least once.
func TestApplyGridExploration_CoversReaderGrid(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	seen := map[readerCandidate]bool{}
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i)
		seen[readerCandidate{ParallelReaders: out.ParallelReaders, ReadAheadBuffers: out.ReadAheadBuffers}] = true
	}
	if len(seen) != len(readerGrid) {
		t.Errorf("over %d cold-start runs the inner grid covered only %d of %d (PR, RAB) cells; expected full coverage",
			explorationGridRuns, len(seen), len(readerGrid))
	}
}

// TestApplyGridExploration_SetsReaderFields (#219) — every grid pick
// must produce non-zero PR and RAB. Pre-fix the exploration left both
// at whatever the baseline produced (PR=2, RAB=4); after #219 it must
// be ones the inner grid actually picked.
func TestApplyGridExploration_SetsReaderFields(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i)
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

	// Walk all 6 cold-start positions; the planned grid lists WAW=2 at
	// idx 1 and idx 5, so we must see WAW=2 picked at least twice.
	wawCounts := map[int]int{}
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i)
		wawCounts[out.WriteAheadWriters]++
	}
	if wawCounts[2] < 2 {
		t.Errorf("planned grid should probe WAW=2 at least twice across cold-start runs; got %d (counts %v)",
			wawCounts[2], wawCounts)
	}
}

// TestApplyGridExploration_RespectsHardChunkLimit verifies CS candidates
// translating to row counts above HardChunkLimit are skipped.
func TestApplyGridExploration_RespectsHardChunkLimit(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	// HardChunkLimit=10000 — full-optimum (25MB/500B=50000) is too big;
	// half-optimum (12.5MB/500B=25000) is also too big. All grid CS
	// candidates filtered out → baseline holds.
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        10000,
	}
	out := baseline(in, profile)
	originalChunk := out.ChunkSize
	applyGridExploration(&out, in, profile, 0)
	if out.ChunkSize > 10000 {
		t.Errorf("ChunkSize=%d exceeds HardChunkLimit=10000; filter should have skipped or baseline should hold", out.ChunkSize)
	}
	_ = originalChunk
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

	if !strings.Contains(out.Reasoning, "+") {
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
		Platform:     "linux",
		AvgRowBytes:  500,
		ForceExplore: true,
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
