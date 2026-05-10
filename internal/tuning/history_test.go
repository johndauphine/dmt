package tuning

import (
	"strings"
	"testing"
)

// stubHistory is a minimal HistoryProvider for tests.
type stubHistory struct {
	rows []HistoryRecord
	err  error
}

func (s *stubHistory) Records(_, _ string) ([]HistoryRecord, error) {
	return s.rows, s.err
}

// TestSelectWAW_PicksHighestShrunkMean — happy path. Three clean bins,
// the one with the highest measured mean wins after shrinkage.
func TestSelectWAW_PicksHighestShrunkMean(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 10, MeanThroughput: 500_000},
		{WAW: 2, TotalRuns: 10, MeanThroughput: 700_000}, // best
		{WAW: 4, TotalRuns: 10, MeanThroughput: 600_000},
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 2 {
		t.Errorf("got (%d, %v), want (2, true)", got, ok)
	}
}

// TestSelectWAW_HighRetryRateExcluded verifies a high-mean bin loses
// when its retry rate clears retryRateExclusionThreshold — the rule
// is "high retry rate excluded," not "any retry excluded" (#186).
func TestSelectWAW_HighRetryRateExcluded(t *testing.T) {
	// WAW=2 has 30% retry rate AND below-median throughput (400K is
	// below the median of [400K, 500K, 600K] = 500K) — both gates of
	// the throughput-aware filter (#204) fire → excluded → WAW=4 wins.
	bins := []wawBin{
		{WAW: 1, TotalRuns: 10, MeanThroughput: 500_000},
		{WAW: 2, TotalRuns: 10, MeanThroughput: 400_000, RunsWithRetries: 3}, // excluded
		{WAW: 4, TotalRuns: 10, MeanThroughput: 600_000},                     // wins
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 4 {
		t.Errorf("got (%d, %v), want (4, true)", got, ok)
	}
}

// TestSelectWAW_LowRetryRateNotExcluded — issue #186 regression guard.
// A single transient retry over many clean runs (10% retry rate, below
// the 15% threshold) must NOT exclude the WAW. Under the original
// "any retry → permanent exclusion" rule, this scenario locked the
// tuner out of historically-best WAWs forever.
func TestSelectWAW_LowRetryRateNotExcluded(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 10, MeanThroughput: 500_000},
		// 1/10 = 10% retry rate, < 15% threshold → eligible. Highest
		// mean → should win.
		{WAW: 2, TotalRuns: 10, MeanThroughput: 800_000, RunsWithRetries: 1},
		{WAW: 4, TotalRuns: 10, MeanThroughput: 600_000},
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 2 {
		t.Errorf("got (%d, %v), want (2, true) — low-rate retries must not exclude (#186)", got, ok)
	}
}

// TestSelectWAW_HighRetryAboveMedianKept (#204 — replaces the former
// "AllBinsRateExcluded" test, whose contract is now mathematically
// unreachable: with the throughput gate, at least one eligible bin is
// always at-or-above its own median, so retry-rate alone can never
// exclude every bin). Two bins both clear the retry-rate threshold;
// the higher-throughput one is at-or-above median and stays eligible.
func TestSelectWAW_HighRetryAboveMedianKept(t *testing.T) {
	bins := []wawBin{
		// 40% retry rate AND below median (500K < 700K) → excluded
		{WAW: 1, TotalRuns: 5, MeanThroughput: 500_000, RunsWithRetries: 2},
		// 60% retry rate but at median (700K NOT < 700K) → kept → wins
		{WAW: 2, TotalRuns: 5, MeanThroughput: 700_000, RunsWithRetries: 3},
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 2 {
		t.Errorf("got (%d, %v), want (2, true) — high-retry above median must stay eligible (#204)", got, ok)
	}
}

// TestSelectWAW_BelowMinRunsRetryFloor — a bin with very few samples
// can't trigger the retry-rate filter even at 100% rate (insufficient
// evidence). Selection then falls to the minRunsPerBin floor anyway,
// so the bin still gets excluded — but for the right reason.
func TestSelectWAW_BelowMinRunsRetryFloor(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 10, MeanThroughput: 500_000},
		// 2/2 = 100% rate, but TotalRuns < minRunsForRetryExclusion=3
		// → retry-rate filter doesn't fire. Falls out via minRunsPerBin
		// (2 < 3) anyway, so WAW=1 wins.
		{WAW: 2, TotalRuns: 2, MeanThroughput: 900_000, RunsWithRetries: 2},
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 1 {
		t.Errorf("got (%d, %v), want (1, true)", got, ok)
	}
}

// TestSelectWAW_EmptyBins guards the no-data path.
func TestSelectWAW_EmptyBins(t *testing.T) {
	_, _, ok := selectWAW(nil)
	if ok {
		t.Error("ok should be false for empty bins")
	}
}

// TestSelectWAW_ThresholdExcludesSparseBins verifies the minRunsPerBin
// floor: a 1-run high peak is treated as no-evidence, so a stable bin
// with sufficient runs wins. (Smoothed mean alone can't do this — the
// 1-run peak pulls the data-derived global up too. The threshold is
// what makes the selection robust against sparse outliers.)
func TestSelectWAW_ThresholdExcludesSparseBins(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 100, MeanThroughput: 600_000}, // stable
		{WAW: 2, TotalRuns: 1, MeanThroughput: 900_000},   // sparse peak — excluded
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 1 {
		t.Errorf("got (%d, %v), want (1, true) — sparse bin should be threshold-excluded", got, ok)
	}
}

// TestSelectWAW_BelowThresholdReturnsFalse: when every clean bin has
// fewer than minRunsPerBin runs, selection returns false so the caller
// stays at baseline.
func TestSelectWAW_BelowThresholdReturnsFalse(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 1, MeanThroughput: 600_000},
		{WAW: 2, TotalRuns: 2, MeanThroughput: 700_000},
	}
	_, _, ok := selectWAW(bins)
	if ok {
		t.Error("ok should be false when no bin clears minRunsPerBin")
	}
}

// --- Issue #204: throughput-aware retry filter tests ---------------

// TestSelectWAW_HighRetryHighThroughputKept_SO2013Fixture is the direct
// repro from the SO2013 sweep that motivated #204: a single eligible
// WAW with high retry rate (55%) but the only bin past the floor —
// median equals its own mean, so the strict-less-than throughput gate
// keeps it eligible. Pre-#204 this WAW would have been excluded on
// retry rate alone, dropping the tuner to baseline.
func TestSelectWAW_HighRetryHighThroughputKept_SO2013Fixture(t *testing.T) {
	bins := []wawBin{
		// Single eligible bin: 9 runs, 5 retries → 55%. Other "bins"
		// below floor and not even computed into median.
		{WAW: 1, TotalRuns: 9, MeanThroughput: 950_000, RunsWithRetries: 5},
		{WAW: 2, TotalRuns: 1, MeanThroughput: 800_000},
		{WAW: 8, TotalRuns: 1, MeanThroughput: 750_000},
	}
	got, _, ok := selectWAW(bins)
	if !ok || got != 1 {
		t.Errorf("got (%d, %v), want (1, true) — single eligible WAW must stay eligible "+
			"even at 55%% retry rate (#204; SO2013 sweep regression guard)", got, ok)
	}
}

// TestIsHighRetryRateBin_RequiresBothGates verifies the gate logic
// table directly: rate-only OR throughput-only triggers must NOT
// exclude; only rate AND below-median together do.
func TestIsHighRetryRateBin_RequiresBothGates(t *testing.T) {
	median := 700_000.0
	cases := []struct {
		name     string
		bin      wawBin
		excluded bool
	}{
		{
			"high_rate_above_median_kept",
			wawBin{TotalRuns: 10, RunsWithRetries: 8, MeanThroughput: 800_000},
			false,
		},
		{
			"high_rate_below_median_excluded",
			wawBin{TotalRuns: 10, RunsWithRetries: 8, MeanThroughput: 600_000},
			true,
		},
		{
			"low_rate_below_median_kept",
			wawBin{TotalRuns: 10, RunsWithRetries: 1, MeanThroughput: 600_000},
			false,
		},
		{
			"low_rate_above_median_kept",
			wawBin{TotalRuns: 10, RunsWithRetries: 1, MeanThroughput: 800_000},
			false,
		},
		{
			"at_median_high_rate_kept_preservation_bias",
			wawBin{TotalRuns: 10, RunsWithRetries: 8, MeanThroughput: 700_000},
			false,
		},
		{
			"sparse_high_rate_floored_out",
			wawBin{TotalRuns: 2, RunsWithRetries: 2, MeanThroughput: 100_000},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isHighRetryRateBin(tc.bin, median)
			if got != tc.excluded {
				t.Errorf("got %v, want %v (rate=%.2f, mean=%.0f, median=%.0f)",
					got, tc.excluded,
					float64(tc.bin.RunsWithRetries)/float64(tc.bin.TotalRuns),
					tc.bin.MeanThroughput, median)
			}
		})
	}
}

// TestBinMedianThroughput_FloorEligibility verifies sparse bins (below
// minRunsForRetryExclusion) don't drag the median around. A 1-run
// outlier shouldn't shift the median computed from the eligible cohort.
func TestBinMedianThroughput_FloorEligibility(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 5, MeanThroughput: 500_000},
		{WAW: 2, TotalRuns: 5, MeanThroughput: 700_000},
		{WAW: 4, TotalRuns: 5, MeanThroughput: 900_000},
		// Below floor — must not contribute to median.
		{WAW: 8, TotalRuns: 1, MeanThroughput: 5_000_000},
	}
	got := binMedianThroughput(bins)
	// Sorted eligible means [500K, 700K, 900K] → upper-middle = 700K.
	want := 700_000.0
	if got != want {
		t.Errorf("got %.0f, want %.0f (sparse 5M-rows/s outlier must not contribute)", got, want)
	}
}

// TestBinMedianThroughput_NoEligibleBins returns 0 when every bin is
// below the eligibility floor. Caller-side: with median=0, the
// throughput-< gate never fires, but neither does the rate gate
// (also floored), so isHighRetryRateBin uniformly returns false.
func TestBinMedianThroughput_NoEligibleBins(t *testing.T) {
	bins := []wawBin{
		{WAW: 1, TotalRuns: 1, MeanThroughput: 500_000},
		{WAW: 2, TotalRuns: 2, MeanThroughput: 700_000},
	}
	got := binMedianThroughput(bins)
	if got != 0 {
		t.Errorf("got %.0f, want 0 (no eligible bins)", got)
	}
}

// TestWawsWithHighRetryRate_ThroughputAware exercises the row-level
// path used by argmaxRegression. Mirrors TestIsHighRetryRateBin_*
// but operates on raw rows so the regression code path gets coverage.
func TestWawsWithHighRetryRate_ThroughputAware(t *testing.T) {
	// 4 WAWs, each with 5 runs:
	//   WAW=1: clean, mean 800K
	//   WAW=2: 80% retries, mean 900K (above median → KEPT)
	//   WAW=4: 80% retries, mean 400K (below median → EXCLUDED)
	//   WAW=8: clean, mean 600K
	// Eligible means [400K, 600K, 800K, 900K] → median upper-middle = 800K.
	mk := func(waw int, retries int, thr float64) HistoryRecord {
		return HistoryRecord{WriteAheadWriters: waw, ChunkRetryCount: retries, FinalThroughput: thr}
	}
	rows := []HistoryRecord{
		mk(1, 0, 800_000), mk(1, 0, 800_000), mk(1, 0, 800_000), mk(1, 0, 800_000), mk(1, 0, 800_000),
		mk(2, 1, 900_000), mk(2, 1, 900_000), mk(2, 1, 900_000), mk(2, 1, 900_000), mk(2, 0, 900_000),
		mk(4, 1, 400_000), mk(4, 1, 400_000), mk(4, 1, 400_000), mk(4, 1, 400_000), mk(4, 0, 400_000),
		mk(8, 0, 600_000), mk(8, 0, 600_000), mk(8, 0, 600_000), mk(8, 0, 600_000), mk(8, 0, 600_000),
	}
	got := wawsWithHighRetryRate(rows)
	if got[2] {
		t.Error("WAW=2 must NOT be excluded (high retry rate but above-median throughput; #204)")
	}
	if !got[4] {
		t.Error("WAW=4 must be excluded (high retry rate AND below-median throughput)")
	}
	if got[1] || got[8] {
		t.Errorf("clean WAWs (1, 8) must not be excluded; got map: %v", got)
	}
}

// TestAggregateByWAW groups records and ignores zero-throughput rows
// (incomplete runs).
func TestAggregateByWAW(t *testing.T) {
	rows := []HistoryRecord{
		{WriteAheadWriters: 1, FinalThroughput: 500_000, ChunkRetryCount: 0},
		{WriteAheadWriters: 1, FinalThroughput: 600_000, ChunkRetryCount: 0},
		{WriteAheadWriters: 2, FinalThroughput: 700_000, ChunkRetryCount: 1},
		{WriteAheadWriters: 2, FinalThroughput: 0}, // incomplete; should be ignored
		{WriteAheadWriters: 4, FinalThroughput: 800_000},
	}
	bins := aggregateByWAW(rows)
	if len(bins) != 3 {
		t.Fatalf("expected 3 bins, got %d", len(bins))
	}
	if bins[0].WAW != 1 || bins[0].TotalRuns != 2 || bins[0].MeanThroughput != 550_000 {
		t.Errorf("WAW=1 bin wrong: %+v", bins[0])
	}
	if bins[1].WAW != 2 || bins[1].TotalRuns != 1 || bins[1].RunsWithRetries != 1 {
		t.Errorf("WAW=2 bin wrong: %+v", bins[1])
	}
	if bins[2].WAW != 4 || bins[2].TotalRuns != 1 || bins[2].RunsWithRetries != 0 {
		t.Errorf("WAW=4 bin wrong: %+v", bins[2])
	}
}

// TestFilterByRegime keeps SameRegime + DifferentTuning, drops
// DifferentHW + Unknown.
func TestFilterByRegime(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	currentTuning := DBTuning{}
	rows := []HistoryRecord{
		{CPUCores: 16, MemoryGB: 48, FinalThroughput: 500_000}, // same
		{CPUCores: 4, MemoryGB: 48, FinalThroughput: 400_000},  // different_hw — drop
		{CPUCores: 0, MemoryGB: 0, FinalThroughput: 600_000},   // unknown — drop
	}
	kept := filterByRegime(rows, current, currentTuning)
	if len(kept) != 1 {
		t.Errorf("expected 1 row to survive, got %d: %+v", len(kept), kept)
	}
	if len(kept) > 0 && kept[0].CPUCores != 16 {
		t.Errorf("expected the same-regime row; got CPUCores=%d", kept[0].CPUCores)
	}
}

// TestFilterOutliers drops clean noise rows but keeps low-throughput-
// with-retries (real load contention).
func TestFilterOutliers(t *testing.T) {
	rows := []HistoryRecord{
		{FinalThroughput: 600_000, ChunkRetryCount: 0},
		{FinalThroughput: 700_000, ChunkRetryCount: 0}, // median
		{FinalThroughput: 800_000, ChunkRetryCount: 0},
		{FinalThroughput: 100_000, ChunkRetryCount: 0}, // <0.5×median, clean → drop (noise)
		{FinalThroughput: 100_000, ChunkRetryCount: 1}, // <0.5×median, retried → keep (signal)
	}
	kept := filterOutliers(rows)
	if len(kept) != 4 {
		t.Errorf("expected 4 rows to survive, got %d", len(kept))
	}
	for _, r := range kept {
		if r.FinalThroughput == 100_000 && r.ChunkRetryCount == 0 {
			t.Errorf("clean noise row wrongly kept: %+v", r)
		}
	}
}

// TestFilterOutliers_SmallSample is a no-op below 3 rows (no median to
// trust).
func TestFilterOutliers_SmallSample(t *testing.T) {
	rows := []HistoryRecord{
		{FinalThroughput: 500_000},
		{FinalThroughput: 100},
	}
	kept := filterOutliers(rows)
	if len(kept) != 2 {
		t.Errorf("small samples should pass through; got %d kept", len(kept))
	}
}

// TestApplyHistory_EndToEnd verifies the full chain: regime filter +
// outlier filter + RULE 1 + smoothed-mean → updates Output.WAW and
// records reasoning.
func TestApplyHistory_EndToEnd(t *testing.T) {
	in := Input{CPUCores: 16, MemoryGB: 48, SourceDBType: "mssql", TargetDBType: "postgres"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2}
	out := baseline(in, profile) // baseline puts WAW=2 on linux
	if out.WriteAheadWriters != 2 {
		t.Fatalf("baseline WAW should be 2, got %d (test fixture broken)", out.WriteAheadWriters)
	}

	// History: WAW=1 has clean runs at 700K (highest), WAW=2 has 3/3 =
	// 100% retry rate AND below-median throughput at 500K (well below
	// the median of [500K, 610K, 700K] = 610K) → both gates of the
	// throughput-aware filter (#204) fire → excluded. WAW=4 has clean
	// runs at 610K. WAW=1 (700K, clean) should win.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 700_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 720_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 680_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 510_000, ChunkRetryCount: 2},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 490_000, ChunkRetryCount: 1},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 500_000, ChunkRetryCount: 3},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 600_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 620_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 610_000},
	}}

	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != 1 {
		t.Errorf("WAW: got %d, want 1 (retry-rate filter should exclude WAW=2; WAW=1 has higher mean than WAW=4)",
			out.WriteAheadWriters)
	}
	if !strings.Contains(out.Reasoning, "smoothed-bins selected WAW=1") {
		t.Errorf("Reasoning should record the smoothed-bins selection; got %q", out.Reasoning)
	}
}

// TestTune_ExplorationProbesAIPollutedWAW — issue #186 integration
// regression guard. Reproduces the exact scenario the user observed:
// the SQLite ai_tuning_history table contains AI-era runs at WAW=2
// with retries (the AI tuner was running at WAW=2 with ~17% retries
// pre-cutover). Under the original "any retry → permanent exclusion"
// rule, the deterministic tuner could never probe WAW=2 again because
// every selection AND exploration path filtered it out.
//
// New contract: exploration paths ignore historical retries and probe
// regardless. With a small history (cold-start window), the planned
// grid still picks WAW=2 at probe indexes 1 and 5 even though every
// historical WAW=2 run had retries. Selection paths (separate test
// above) still respect the retry-rate threshold.
func TestTune_ExplorationProbesAIPollutedWAW(t *testing.T) {
	in := Input{
		CPUCores: 8, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	// BaselineWAW=1 (NOT 2) so out.WriteAheadWriters starts at 1.
	// Then "did exploration actually probe WAW=2" is testable by
	// asserting out.WriteAheadWriters==2 — if exploration silently
	// filtered WAW=2, the value would stay at 1 (Copilot review on
	// PR #187: with BaselineWAW=2 the test passed even when the grid
	// never picked WAW=2).
	profile := DriverProfile{Name: "postgres", BaselineWAW: 1, OptimumBulkChunkBytes: 25_000_000}

	// 5 AI-era rows at WAW=2 each with retries — places us in cold-
	// start (rows < explorationGridRuns=6) so the planned grid fires.
	// With bucketCount=5 the grid lands on idx 5 = {WAW=2, CS=1.0×opt}.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 8, MemoryGB: 48, WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500, FinalThroughput: 600_000, ChunkRetryCount: 3},
		{CPUCores: 8, MemoryGB: 48, WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500, FinalThroughput: 580_000, ChunkRetryCount: 2},
		{CPUCores: 8, MemoryGB: 48, WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500, FinalThroughput: 620_000, ChunkRetryCount: 4},
		{CPUCores: 8, MemoryGB: 48, WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500, FinalThroughput: 590_000, ChunkRetryCount: 1},
		{CPUCores: 8, MemoryGB: 48, WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500, FinalThroughput: 605_000, ChunkRetryCount: 2},
	}}

	out := Tune(in, profile, history, DBTuning{})

	// Issue #186 fix: exploration must actually pick WAW=2 despite the
	// historical retry rate, and the Reasoning string must record it.
	// Both checks are necessary — Reasoning alone (any "exploration:
	// planned grid" entry) doesn't prove the grid landed on WAW=2;
	// the WAW value alone (against a baseline that already starts at
	// 2) doesn't prove exploration fired vs. just inheriting baseline.
	if out.WriteAheadWriters != 2 {
		t.Errorf("exploration should pick WAW=2 from the planned grid (bucketCount=5 → grid idx 5); got WAW=%d, Reasoning=%q",
			out.WriteAheadWriters, out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "exploration: planned grid") {
		t.Errorf("exploration should fire on cold-start history (5 < %d runs); got Reasoning=%q",
			explorationGridRuns, out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "WAW=2") {
		t.Errorf("planned-grid Reasoning should record WAW=2; got %q", out.Reasoning)
	}
}

// TestTune_NilHistory verifies the public entry point treats a nil
// history provider as no-history (baseline stands; no panic).
func TestTune_NilHistory(t *testing.T) {
	in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: "linux"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(in, profile, nil, DBTuning{})
	if out.WriteAheadWriters != 2 {
		t.Errorf("nil provider should yield baseline WAW=2; got %d", out.WriteAheadWriters)
	}
	if out.ChunkSize != 50_000 {
		t.Errorf("nil provider should yield baseline chunk_size=50000 at 500B avg; got %d", out.ChunkSize)
	}
}

// TestApplyHistory_HighRetryAboveMedianStillSelected (#204 — replaces
// the former AllBinsRateExcluded test, whose contract is now
// mathematically unreachable). When two bins both clear the retry-rate
// threshold but neither is below the eligible-bin median, the
// throughput-aware filter excludes neither — the higher-throughput
// bin wins normally instead of falling to baseline.
func TestApplyHistory_HighRetryAboveMedianStillSelected(t *testing.T) {
	in := Input{CPUCores: 16, MemoryGB: 48, SourceDBType: "mssql", TargetDBType: "postgres"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2}
	out := baseline(in, profile)

	// 4 runs each at WAW=1 and WAW=2; 3 of 4 retried in each (75%).
	// Both clear the rate threshold, but with only 2 bins the median is
	// the upper-middle (WAW=2's 615K). WAW=2 is at-median (NOT excluded);
	// WAW=1 is below median AND high retry → excluded. WAW=2 wins.
	makeRow := func(waw, retries int, thru float64) HistoryRecord {
		return HistoryRecord{
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: waw,
			FinalThroughput:   thru,
			ChunkRetryCount:   retries,
		}
	}
	history := &stubHistory{rows: []HistoryRecord{
		makeRow(1, 1, 500_000), makeRow(1, 1, 490_000), makeRow(1, 1, 510_000), makeRow(1, 0, 500_000),
		makeRow(2, 2, 600_000), makeRow(2, 1, 610_000), makeRow(2, 1, 620_000), makeRow(2, 0, 605_000),
	}}
	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != 2 {
		t.Errorf("WAW should be 2 (above-median high-retry kept; below-median high-retry excluded); got %d",
			out.WriteAheadWriters)
	}
}

// TestApplyHistory_RegressionTier verifies the tiered dispatch fires the
// regression once row count clears minRowsForRegression. With a synthetic
// quadratic-peak fixture (throughput peaks at WAW=4), the regression
// should pick WAW=4 — something the smoothed-bins tier with its
// linear-mean comparison can't reliably do for non-monotone surfaces.
func TestApplyHistory_RegressionTier(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)
	originalChunk := out.ChunkSize

	// 60 rows, throughput = 1000 - 50·(WAW-4)² + 100 (positive bias).
	// WAW range 1..6 spread evenly; all clean (no retries) so RULE 1
	// doesn't fire. Should clear minRowsForRegression and pick WAW=4.
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		dev := waw - 4
		thru := 1000.0 - 50.0*float64(dev*dev) + 100.0
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50_000,
			AvgRowBytes:       500,
			FinalThroughput:   thru,
			CPUCores:          16,
			MemoryGB:          48,
		}
	}
	history := &stubHistory{rows: rows}

	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != 4 {
		t.Errorf("WAW: got %d, want 4 (regression should detect quadratic peak)", out.WriteAheadWriters)
	}
	if out.ChunkSize == originalChunk {
		t.Errorf("ChunkSize should have been overridden by regression; still at baseline %d", out.ChunkSize)
	}
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Reasoning should record regression tier; got %q", out.Reasoning)
	}
}

// TestApplyHistory_RegressionRespectsRetryRateExclusion verifies the
// retry-rate filter (#186) applies to the regression argmax, not just
// the smoothed-bins selectWAW. WAW=4 has the best raw throughput but
// every run retried (100% rate, well above threshold) — argmax must
// skip it and pick the next-best clean WAW.
func TestApplyHistory_RegressionRespectsRetryRateExclusion(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	// 60 rows. WAW=4 has 100% retry rate AND below-median throughput;
	// both gates of the throughput-aware filter (#204) fire → excluded.
	// Per-WAW means are 600K + 100·waw → [700, 800, 900, 200, 1100,
	// 1200] for WAW=1..6. Median across 6 means = upper-middle = 1000.
	// WAW=4's 200K < 1000 → excluded. Pre-#204 test had WAW=4 at 1.2M
	// (above median) and the retry-rate-only filter still excluded it;
	// post-#204 we need WAW=4 to actually be a bad config to be filtered.
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		thru := 600_000.0 + 100_000.0*float64(waw)
		retries := 0
		if waw == 4 {
			thru = 200_000 // worst mean AND...
			retries = 1    // ...100% retry rate (10/10) → both gates fire
		}
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50_000,
			AvgRowBytes:       500,
			FinalThroughput:   thru,
			ChunkRetryCount:   retries,
			CPUCores:          16,
			MemoryGB:          48,
		}
	}
	history := &stubHistory{rows: rows}

	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters == 4 {
		t.Error("retry-rate + below-median filter should have excluded WAW=4 (100% retry rate, lowest throughput)")
	}
	if out.WriteAheadWriters < 1 || out.WriteAheadWriters > maxWAWForGrid {
		t.Errorf("picked WAW=%d outside valid grid [1..%d]", out.WriteAheadWriters, maxWAWForGrid)
	}
}

// TestApplyHistory_RegressionRespectsHardChunkLimit verifies the argmax
// skips CS_BYTES candidates that translate to row counts above the
// HardChunkLimit. Critical for MySQL once the @@max_allowed_packet probe
// lands; PR1's #166 has all drivers returning 0 today so this is a
// future-proofing test against synthetic data.
func TestApplyHistory_RegressionRespectsHardChunkLimit(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mysql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	// HardChunkLimit=10 (artificially tight). All grid CS values
	// translate to row counts > 10, so HardChunkLimit filter rejects all
	// candidates → argmax returns ok=false → falls through to smoothed
	// bins.
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        10,
	}
	out := baseline(in, profile)
	originalWAW := out.WriteAheadWriters

	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		rows[i] = HistoryRecord{
			SourceDBType:      "mysql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         5,
			AvgRowBytes:       500,
			FinalThroughput:   500_000 + float64(waw*1000),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	history := &stubHistory{rows: rows}

	applyHistory(&out, in, profile, history, DBTuning{})

	// Either smoothed bins picked or baseline stood; regression's grid was
	// fully filtered. WAW shouldn't be a wild value.
	if out.WriteAheadWriters > maxWAWForGrid {
		t.Errorf("WAW=%d outside valid range; HardChunkLimit filter should have constrained selection", out.WriteAheadWriters)
	}
	// Reasoning should not mention regression-selected (every grid point
	// was filtered out).
	if strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Reasoning should not say regression-selected when grid was fully filtered; got %q", out.Reasoning)
	}
	_ = originalWAW
}

// TestApplyHistory_OutOfRegimeRowsDropped verifies regime filter excludes
// rows from materially different hardware before they pollute selection.
func TestApplyHistory_OutOfRegimeRowsDropped(t *testing.T) {
	in := Input{CPUCores: 16, MemoryGB: 48, SourceDBType: "mssql", TargetDBType: "postgres"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2}
	out := baseline(in, profile)
	originalWAW := out.WriteAheadWriters

	// All rows are out-of-regime (different CPU). After filter, no rows
	// survive → baseline stands.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 4, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 1_000_000},
		{CPUCores: 4, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 900_000},
	}}
	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != originalWAW {
		t.Errorf("WAW should remain at baseline %d when no in-regime history exists; got %d",
			originalWAW, out.WriteAheadWriters)
	}
}

// TestApplyHistory_DifferentWorkloadRowsDropped is the #198 integration
// test: rows with TotalRows beyond tolerance must be excluded by
// filterByRegime before they reach selection. Mirrors the hw-different
// case above but exercises the workload axis.
func TestApplyHistory_DifferentWorkloadRowsDropped(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TotalRows:   100_000_000,
		AvgRowBytes: 1000,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2}
	out := baseline(in, profile)
	originalWAW := out.WriteAheadWriters

	// Same hw + tuning, but rows from a 10× smaller dataset (the #198 repro
	// shape: yesterday's 19M-row sweep mixed with today's 106M-row sweep).
	// After filter, no rows survive → baseline stands.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 16, MemoryGB: 48, TotalRows: 10_000_000, AvgRowBytes: 1000, WriteAheadWriters: 1, FinalThroughput: 1_000_000},
		{CPUCores: 16, MemoryGB: 48, TotalRows: 10_000_000, AvgRowBytes: 1000, WriteAheadWriters: 4, FinalThroughput: 900_000},
	}}
	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != originalWAW {
		t.Errorf("WAW should remain at baseline %d when all history rows are workload-different; got %d",
			originalWAW, out.WriteAheadWriters)
	}
}

// --- Issue #202: tier + always-emit-reasoning tests ----------------

// TestApplyHistory_SmoothedBinsReasoningEmittedWhenPickMatchesBaseline
// is the direct repro of the silence bug: pre-#202, smoothed-bins only
// appended Reasoning when it changed WriteAheadWriters. When the picked
// WAW matched what baseline already set, the user got "Tuning applied
// N parameter(s)" with no reasoning line at all (orchestrator gated on
// non-empty Reasoning). Post-fix the reasoning must appear with the
// "kept" verb, and Tier must be TierSmoothedBins.
func TestApplyHistory_SmoothedBinsReasoningEmittedWhenPickMatchesBaseline(t *testing.T) {
	in := Input{CPUCores: 16, MemoryGB: 48, SourceDBType: "mssql", TargetDBType: "postgres"}
	// Baseline pins WAW=1 for this profile (postgres scales-with-cores
	// off in the synthetic profile), and history's best bin also picks
	// WAW=1. The pick equals out.WriteAheadWriters → was the silence case.
	profile := DriverProfile{Name: "postgres", BaselineWAW: 1}
	out := baseline(in, profile)
	if out.WriteAheadWriters != 1 {
		t.Fatalf("baseline WAW should be 1 for this fixture, got %d", out.WriteAheadWriters)
	}

	rows := make([]HistoryRecord, 0, 6)
	for i := 0; i < 6; i++ {
		rows = append(rows, HistoryRecord{
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: 1,
			FinalThroughput:   700_000 + float64(i*1_000),
		})
	}
	applyHistory(&out, in, profile, &stubHistory{rows: rows}, DBTuning{})

	if out.WriteAheadWriters != 1 {
		t.Errorf("WAW should remain 1 (smoothed-bins agrees with baseline); got %d", out.WriteAheadWriters)
	}
	if out.Tier != TierSmoothedBins {
		t.Errorf("Tier should be %q when smoothed-bins picks; got %q", TierSmoothedBins, out.Tier)
	}
	if !strings.Contains(out.Reasoning, "smoothed-bins kept WAW=1") {
		t.Errorf("Reasoning should record smoothed-bins kept the baseline; got %q", out.Reasoning)
	}
}

// TestTune_NilHistory_FillsBaselineReasoning verifies the pure-baseline
// case carries an explanatory Reasoning + Tier=TierBaseline. Pre-#202
// this path returned an empty Reasoning, which the orchestrator's
// non-empty guard then dropped from the log entirely.
func TestTune_NilHistory_FillsBaselineReasoning(t *testing.T) {
	in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: "linux"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(in, profile, nil, DBTuning{})

	if out.Tier != TierBaseline {
		t.Errorf("nil-history Tier should be %q; got %q", TierBaseline, out.Tier)
	}
	if !strings.Contains(out.Reasoning, "no history backend") {
		t.Errorf("nil-history Reasoning should mention the missing backend; got %q", out.Reasoning)
	}
}

// TestTune_HistoryFetchFailed_FillsBaselineReasoning verifies the
// fetch-error path also carries an explanatory Reasoning. Distinguishing
// this from "no backend configured" matters: nil backend is intentional,
// fetch error means the SQLite/file backend is broken and the user
// should investigate.
func TestTune_HistoryFetchFailed_FillsBaselineReasoning(t *testing.T) {
	in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: "linux", SourceDBType: "mssql", TargetDBType: "postgres"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	failing := &stubHistory{err: errStubFetchFailed}

	out := Tune(in, profile, failing, DBTuning{})

	if out.Tier != TierBaseline {
		t.Errorf("fetch-failed Tier should be %q; got %q", TierBaseline, out.Tier)
	}
	if !strings.Contains(out.Reasoning, "history fetch failed") {
		t.Errorf("fetch-failed Reasoning should mention the failure; got %q", out.Reasoning)
	}
}

// TestApplyEpsilonPerturbation_OverridesTierToExploration is the Codex
// P2 fix from #202 review: a perturbed run's final (WAW, ChunkSize)
// values came from exploration, not from the upstream selector — so
// Tier must be TierExploration regardless of which selector ran first.
// The Reasoning chain still preserves the upstream tier's note so the
// provenance is visible.
func TestApplyEpsilonPerturbation_OverridesTierToExploration(t *testing.T) {
	out := &Output{
		WriteAheadWriters: 4,
		ChunkSize:         50_000,
		Tier:              TierRegression,
		Reasoning:         "regression-selected WAW=4, chunk_size=50000",
	}
	profile := DriverProfile{Name: "postgres"}
	applyEpsilonPerturbation(out, profile)

	if out.Tier != TierExploration {
		t.Errorf("ε-perturbation should override Tier to %q (upstream regression's pick was nudged); got %q",
			TierExploration, out.Tier)
	}
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Reasoning chain should still preserve the upstream regression note; got %q", out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "ε-perturbation") {
		t.Errorf("Reasoning should record the ε-perturbation; got %q", out.Reasoning)
	}
}

// errStubFetchFailed is a sentinel error for the fetch-failed test —
// keeps the test free of the errors package import.
var errStubFetchFailed = stubError("simulated fetch failure")

type stubError string

func (e stubError) Error() string { return string(e) }
