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
	skip := map[int]bool{}

	picks := make(map[gridCandidate]int)
	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, skip)
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

// TestApplyGridExploration_RespectsRule1 verifies the planned grid skips
// candidates whose WAW had historical retries — same RULE 1 as the
// regression argmax.
func TestApplyGridExploration_RespectsRule1(t *testing.T) {
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	// Skip WAW=1 and WAW=2 — entire CSFraction=0.5 sub-grid is excluded
	// (WAW=3 is allowed, WAW=maxWAWForGrid is allowed).
	skip := map[int]bool{1: true, 2: true}

	for i := 0; i < explorationGridRuns; i++ {
		out := baseline(in, profile)
		applyGridExploration(&out, in, profile, i, skip)
		if out.WriteAheadWriters == 1 || out.WriteAheadWriters == 2 {
			t.Errorf("bucketCount=%d: picked WAW=%d, but RULE 1 excluded {1, 2}", i, out.WriteAheadWriters)
		}
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
	applyGridExploration(&out, in, profile, 0, map[int]bool{})
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
	in := Input{AvgRowBytes: 500, Platform: "linux", CPUCores: 8}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	for trial := 0; trial < 50; trial++ {
		out := Output{WriteAheadWriters: 3, ChunkSize: 30000}
		original := out
		applyEpsilonPerturbation(&out, in, profile)
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

// TestTune_ForceExploreOverridesHistory verifies in.ForceExplore=true
// drops the run into planned-grid even when there's plenty of history
// the regression would otherwise use.
func TestTune_ForceExploreOverridesHistory(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
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
