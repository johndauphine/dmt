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

// TestCellsWithHighRetryRate_PerCellNotPerWAW (#219, codex review)
// pins the bug Codex flagged on the original #219 commit: WAW-level
// retry exclusion would ban an entire WAW value based on retries seen
// at a single bad reader combo, even if a different reader combo at
// the same WAW had a clean record. The fix is per-cell aggregation
// keyed by (WAW, PR, RAB).
//
// Setup: WAW=2 has 10 runs at PR=4/RAB=8 with 80% retries and below-
// median throughput (bad reader combo) AND 10 runs at PR=2/RAB=4 with
// 0 retries and above-median throughput (clean combo). Expectation:
// only the bad-combo cell appears in the skip map; the clean cell
// remains selectable.
func TestCellsWithHighRetryRate_PerCellNotPerWAW(t *testing.T) {
	mk := func(waw, pr, rab int, retries int, thr float64) HistoryRecord {
		return HistoryRecord{
			WriteAheadWriters: waw,
			ParallelReaders:   pr,
			ReadAheadBuffers:  rab,
			ChunkRetryCount:   retries,
			FinalThroughput:   thr,
		}
	}
	// WAW=1 clean cell at 800K → eligible, median anchor.
	// WAW=2/PR=4/RAB=8 bad cell at 400K with retries → excluded.
	// WAW=2/PR=2/RAB=4 clean cell at 900K → kept.
	// WAW=4 clean cell at 600K → kept.
	rows := []HistoryRecord{}
	for i := 0; i < 10; i++ {
		rows = append(rows, mk(1, 2, 4, 0, 800_000))
		rows = append(rows, mk(2, 4, 8, 1, 400_000)) // bad combo, retries
		rows = append(rows, mk(2, 2, 4, 0, 900_000)) // clean combo at same WAW
		rows = append(rows, mk(4, 2, 4, 0, 600_000))
	}

	got := cellsWithHighRetryRate(rows)

	badCell := retryCellKey{WAW: 2, ParallelReaders: 4, ReadAheadBuffers: 8}
	cleanCell := retryCellKey{WAW: 2, ParallelReaders: 2, ReadAheadBuffers: 4}

	if !got[badCell] {
		t.Errorf("bad combo (WAW=2/PR=4/RAB=8) with 100%% retries should be excluded; got %v", got)
	}
	if got[cleanCell] {
		t.Errorf("clean combo (WAW=2/PR=2/RAB=4) at same WAW must NOT be excluded — that was Codex's bug (#219 review)")
	}
	if got[retryCellKey{WAW: 1, ParallelReaders: 2, ReadAheadBuffers: 4}] {
		t.Errorf("clean WAW=1 cell must not be excluded; got %v", got)
	}
	if got[retryCellKey{WAW: 4, ParallelReaders: 2, ReadAheadBuffers: 4}] {
		t.Errorf("clean WAW=4 cell must not be excluded; got %v", got)
	}
}

// TestCellSkipsInGrid_FiltersOutOfGridEntries (Copilot review on #221)
// pins that the skip reasoning logs only retry-rate exclusions whose
// (PR, RAB) pair is in readerGrid and whose WAW is in the argmax grid
// range. Without this filter, historical rows at PR/RAB values outside
// the grid get quoted as "the reason argmax emptied" even though they
// never participated in argmax filtering.
func TestCellSkipsInGrid_FiltersOutOfGridEntries(t *testing.T) {
	// 4 retry-rate exclusions: 2 in-grid, 2 out-of-grid.
	cellSkip := map[retryCellKey]bool{
		{WAW: 2, ParallelReaders: 2, ReadAheadBuffers: 4}: true, // in grid
		{WAW: 4, ParallelReaders: 4, ReadAheadBuffers: 8}: true, // in grid
		{WAW: 2, ParallelReaders: 3, ReadAheadBuffers: 5}: true, // PR=3 not in {2,4}
		{WAW: 2, ParallelReaders: 2, ReadAheadBuffers: 7}: true, // RAB=7 not in {4,8}
	}
	got := cellSkipsInGrid(cellSkip)

	if len(got) != 2 {
		t.Errorf("filtered set has %d entries, want 2 (only the two in-grid cells)", len(got))
	}
	if !got[retryCellKey{WAW: 2, ParallelReaders: 2, ReadAheadBuffers: 4}] {
		t.Error("in-grid cell (WAW=2, PR=2, RAB=4) should be retained")
	}
	if !got[retryCellKey{WAW: 4, ParallelReaders: 4, ReadAheadBuffers: 8}] {
		t.Error("in-grid cell (WAW=4, PR=4, RAB=8) should be retained")
	}
	if got[retryCellKey{WAW: 2, ParallelReaders: 3, ReadAheadBuffers: 5}] {
		t.Error("out-of-grid cell (PR=3) leaked through filter")
	}
}

// TestCellSkipsInGrid_EmptyInput is the empty-map fast path. Returns
// the same map to avoid a needless allocation.
func TestCellSkipsInGrid_EmptyInput(t *testing.T) {
	if got := cellSkipsInGrid(nil); len(got) != 0 {
		t.Errorf("nil input should return empty; got %d entries", len(got))
	}
	if got := cellSkipsInGrid(map[retryCellKey]bool{}); len(got) != 0 {
		t.Errorf("empty input should return empty; got %d entries", len(got))
	}
}

// TestCellSkipsInGrid_FiltersOutOfRangeWAW pins that cells with WAW
// outside [1, maxWAWForGrid] are filtered. Defensive — current
// production code only writes WAW values within range, but a future
// schema change or corrupted history row shouldn't poison the log.
func TestCellSkipsInGrid_FiltersOutOfRangeWAW(t *testing.T) {
	cellSkip := map[retryCellKey]bool{
		{WAW: 0, ParallelReaders: 2, ReadAheadBuffers: 4}:                 true, // below floor
		{WAW: maxWAWForGrid + 1, ParallelReaders: 2, ReadAheadBuffers: 4}: true, // above cap
		{WAW: 1, ParallelReaders: 2, ReadAheadBuffers: 4}:                 true, // in range
	}
	got := cellSkipsInGrid(cellSkip)
	if len(got) != 1 {
		t.Errorf("filtered set has %d entries, want 1", len(got))
	}
	if !got[retryCellKey{WAW: 1, ParallelReaders: 2, ReadAheadBuffers: 4}] {
		t.Error("in-range WAW=1 should be retained")
	}
}

// TestFormatRetryCellSkips_DeterministicOrder pins that the reasoning
// log format sorts cell keys so log diffs aren't churned by map
// iteration order.
func TestFormatRetryCellSkips_DeterministicOrder(t *testing.T) {
	m := map[retryCellKey]bool{
		{WAW: 4, ParallelReaders: 2, ReadAheadBuffers: 4}: true,
		{WAW: 1, ParallelReaders: 4, ReadAheadBuffers: 8}: true,
		{WAW: 1, ParallelReaders: 2, ReadAheadBuffers: 4}: true,
		{WAW: 2, ParallelReaders: 4, ReadAheadBuffers: 4}: true,
	}
	want := "[WAW=1/PR=2/RAB=4, WAW=1/PR=4/RAB=8, WAW=2/PR=4/RAB=4, WAW=4/PR=2/RAB=4]"
	got := formatRetryCellSkips(m)
	if got != want {
		t.Errorf("formatRetryCellSkips ordering wrong:\n got=%s\nwant=%s", got, want)
	}
}

// TestFormatRetryCellSkips_EmptyMapEmitsNone documents the empty-map
// rendering — readers of the reasoning string should see "none" rather
// than "[]" so the log line is unambiguous.
func TestFormatRetryCellSkips_EmptyMapEmitsNone(t *testing.T) {
	if got := formatRetryCellSkips(nil); got != "none" {
		t.Errorf("formatRetryCellSkips(nil)=%q, want %q", got, "none")
	}
	if got := formatRetryCellSkips(map[retryCellKey]bool{}); got != "none" {
		t.Errorf("formatRetryCellSkips({})=%q, want %q", got, "none")
	}
}

// TestApplyHistorySelection_SmoothedBinsFiltersByReaderSettings
// (#219, codex review pass 3) pins the third instance of the
// PR/RAB-coupling bug Codex flagged. Before this fix, the smoothed-bins
// fallback aggregated by WAW only, so a bad reader combo's retries
// could ban a WAW from selection even though the current run would use
// a clean reader combo.
//
// Setup: history has 10 rows, fewer than minRowsToAttemptRegression=12
// (#452), so the regression tier is skipped and applyHistorySelection
// falls to smoothed bins.
//   - 5 rows at WAW=2 / PR=4/RAB=8 with retries and 200K rows/s
//     (the bad combo).
//   - 5 rows at WAW=2 / PR=2/RAB=4 clean at 800K rows/s
//     (the current run's reader settings).
//
// Expected: smoothed bins picks WAW=2 — it should ignore the PR=4/RAB=8
// rows because the current run uses PR=2/RAB=4. Pre-fix the contaminated
// bin's retry rate would have excluded WAW=2 entirely.
func TestApplyHistorySelection_SmoothedBinsFiltersByReaderSettings(t *testing.T) {
	rows := []HistoryRecord{}
	for i := 0; i < 5; i++ {
		rows = append(rows, HistoryRecord{
			WriteAheadWriters: 2,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			ParallelReaders:   4,
			ReadAheadBuffers:  8,
			ChunkRetryCount:   1,
			FinalThroughput:   200_000,
		})
		rows = append(rows, HistoryRecord{
			WriteAheadWriters: 2,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
			ChunkRetryCount:   0,
			FinalThroughput:   800_000,
		})
	}

	out := Output{
		WriteAheadWriters: 1, // baseline
		ChunkSize:         50000,
		ParallelReaders:   2, // current run's reader settings
		ReadAheadBuffers:  4,
	}
	applyHistorySelection(&out, Input{AvgRowBytes: 500}, DriverProfile{}, rows)

	if out.Tier != TierSmoothedBins {
		t.Fatalf("Tier = %q, want %q (regression should be skipped, smoothed-bins should pick)", out.Tier, TierSmoothedBins)
	}
	if out.WriteAheadWriters != 2 {
		t.Errorf("smoothed-bins picked WAW=%d, want 2 (clean combo at WAW=2 has highest mean; bad combo at same WAW should not contaminate the bin)", out.WriteAheadWriters)
	}
	// Reasoning must surface the reader filter so a reader of the log
	// can tell which slice of history smoothed-bins actually used.
	if !strings.Contains(out.Reasoning, "reader=PR2/RAB4") {
		t.Errorf("Reasoning missing reader-filter tag; got %q", out.Reasoning)
	}
}

// TestApplyHistorySelection_SmoothedBinsEmptyWhenNoMatchingReaderRows
// covers the "no rows at current reader settings" branch. When the
// current run's PR/RAB combo isn't in history, smoothed-bins should
// gracefully report it via reasoning rather than pick from contaminated
// bins.
func TestApplyHistorySelection_SmoothedBinsEmptyWhenNoMatchingReaderRows(t *testing.T) {
	// 11 rows entirely at PR=4/RAB=8 — below minRowsToAttemptRegression
	// (#452) so the regression tier is skipped and the smoothed-bins
	// branch under test is the one that runs.
	rows := []HistoryRecord{}
	for i := 0; i < 11; i++ {
		rows = append(rows, HistoryRecord{
			WriteAheadWriters: 2,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			ParallelReaders:   4,
			ReadAheadBuffers:  8,
			FinalThroughput:   500_000,
		})
	}

	out := Output{
		WriteAheadWriters: 1,
		ChunkSize:         50000,
		ParallelReaders:   2, // doesn't match any row
		ReadAheadBuffers:  4,
	}
	applyHistorySelection(&out, Input{AvgRowBytes: 500}, DriverProfile{}, rows)

	if out.Tier == TierSmoothedBins {
		t.Errorf("smoothed-bins should NOT pick when no rows match current reader settings; got tier=%q WAW=%d", out.Tier, out.WriteAheadWriters)
	}
	if !strings.Contains(out.Reasoning, "no rows match current reader settings") {
		t.Errorf("Reasoning missing 'no rows match current reader settings' explanation; got %q", out.Reasoning)
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
	// Rows pinned at the baseline reader settings (PR=2, RAB=4) so the
	// #219 reader-filter on smoothed-bins keeps them. Real persisted
	// history has actual PR/RAB values; the Go zero default in test
	// fixtures had to be made explicit after the filter landed.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 1, FinalThroughput: 700_000},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 1, FinalThroughput: 720_000},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 1, FinalThroughput: 680_000},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 2, FinalThroughput: 510_000, ChunkRetryCount: 2},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 2, FinalThroughput: 490_000, ChunkRetryCount: 1},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 2, FinalThroughput: 500_000, ChunkRetryCount: 3},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 4, FinalThroughput: 600_000},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 4, FinalThroughput: 620_000},
		{CPUCores: 16, MemoryGB: 48, ParallelReaders: 2, ReadAheadBuffers: 4, WriteAheadWriters: 4, FinalThroughput: 610_000},
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
	// start (rows < explorationGridRuns=12) so the planned grid fires.
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
// regression once row count clears the regression attempt gate (#452). With a synthetic
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
	// doesn't fire. Should clear the regression row floor and pick WAW=4.
	// Reader settings pinned at baseline (PR=2, RAB=4) so the #221
	// coverage gate keeps every WAW cell selectable.
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
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
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
	// Negative guard: R²= was removed from the operator-facing reasoning
	// line because on noisy real workloads it reads structurally low
	// (~0.13) while regression decisions are near-optimal — operators
	// thought the tuner was broken. The 95% CI carries the relevant
	// point-level confidence in units operators understand. If R²= comes
	// back, this assertion catches the regression.
	if strings.Contains(out.Reasoning, "R²=") {
		t.Errorf("Reasoning should not include R²= (removed from operator-facing log); got %q", out.Reasoning)
	}
}

// TestApplyHistoryRegression_ArgmaxSearchesReaderGrid (#294) pins the
// regression selection side, not just the fit side: when history shows
// a faster (PR, RAB) combination at the same WAW/chunk surface, argmax
// must select that reader cell instead of carrying baseline PR/RAB
// through unchanged.
func TestApplyHistoryRegression_ArgmaxSearchesReaderGrid(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	rows := make([]HistoryRecord, 0, 32)
	for _, reader := range readerGrid {
		for i := 0; i < 8; i++ {
			throughput := 900_000.0
			if reader.ParallelReaders == 4 {
				throughput += 100_000
			}
			if reader.ReadAheadBuffers == 4 {
				throughput += 50_000
			}
			rows = append(rows, HistoryRecord{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				WriteAheadWriters: 3,
				ChunkSize:         50_000,
				AvgRowBytes:       500,
				ParallelReaders:   reader.ParallelReaders,
				ReadAheadBuffers:  reader.ReadAheadBuffers,
				FinalThroughput:   throughput + float64(i),
				CPUCores:          16,
				MemoryGB:          48,
			})
		}
	}

	if !applyHistoryRegression(&out, in, profile, rows) {
		t.Fatalf("expected regression tier to select a reader-grid argmax; reasoning: %s", out.Reasoning)
	}
	if out.WriteAheadWriters != 3 {
		t.Errorf("WAW = %d, want 3", out.WriteAheadWriters)
	}
	if out.ParallelReaders != 4 || out.ReadAheadBuffers != 4 {
		t.Errorf("reader argmax = PR=%d/RAB=%d, want PR=4/RAB=4; reasoning: %s",
			out.ParallelReaders, out.ReadAheadBuffers, out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "PR=4, RAB=4") {
		t.Errorf("reasoning should surface selected reader settings; got %q", out.Reasoning)
	}
}

// TestApplyHistory_RegressionRefusesUncoveredCubeCorner (#221) pins
// the cube-corner extrapolation gate. Reproduces the exact failure
// mode measured live on SO2010: history has WAW=1 only at PR=2 and
// PR=4 only at WAW≥2, so the corner (WAW=1, PR=4) has zero training
// samples. Pre-fix the linear regression would add the WAW and PR
// effects independently and pick that corner with a confident-looking
// prediction; the actual run came in 14× slower than predicted.
//
// Expected post-fix: argmax must NOT pick a (WAW, PR, RAB) combo
// with zero training coverage. With this fixture the only covered
// (WAW=1, PR=*) cell is (1, 2, 4) and the only covered (WAW=2, PR=*)
// cells are (2, 2, 4), (2, 4, 4), (2, 4, 8) — argmax stays within
// those.
func TestApplyHistory_RegressionRefusesUncoveredCubeCorner(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	// 36 rows total; readerGrid coverage:
	//   (WAW=1, PR=2, RAB=4): 12 rows at 800K — the only WAW=1 cell.
	//   (WAW=2, PR=2, RAB=4): 12 rows at 700K.
	//   (WAW=2, PR=4, RAB=4): 6 rows at 600K.
	//   (WAW=2, PR=4, RAB=8): 6 rows at 550K.
	// Uncovered: (1,4,4), (1,4,8), (1,2,8), (2,2,8), and every other
	// (WAW≥3, *, *). Linear extrapolation would push the model toward
	// believing (WAW=1, PR=4) inherits PR=4's effect at WAW=2 — but
	// PR=4 was actually worse at WAW=2 than PR=2, so the bad pick
	// here would be (WAW=1, PR=2) again (correct) OR (WAW>=2, PR=2).
	// Either way the argmax must NOT pick an uncovered cell like
	// (WAW=1, PR=4, RAB=4) or (WAW=3, *, *).
	rows := []HistoryRecord{}
	add := func(waw, pr, rab, n int, thr float64) {
		for i := 0; i < n; i++ {
			rows = append(rows, HistoryRecord{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				WriteAheadWriters: waw,
				ChunkSize:         50_000,
				AvgRowBytes:       500,
				ParallelReaders:   pr,
				ReadAheadBuffers:  rab,
				FinalThroughput:   thr,
				CPUCores:          16,
				MemoryGB:          48,
			})
		}
	}
	add(1, 2, 4, 12, 800_000)
	add(2, 2, 4, 12, 700_000)
	add(2, 4, 4, 6, 600_000)
	add(2, 4, 8, 6, 550_000)

	history := &stubHistory{rows: rows}
	applyHistory(&out, in, profile, history, DBTuning{})

	// Define what "covered" looks like for the assertion. argmax must
	// land on one of these triples.
	type cell struct{ waw, pr, rab int }
	covered := map[cell]bool{
		{1, 2, 4}: true,
		{2, 2, 4}: true,
		{2, 4, 4}: true,
		{2, 4, 8}: true,
	}
	picked := cell{out.WriteAheadWriters, out.ParallelReaders, out.ReadAheadBuffers}
	if !covered[picked] {
		t.Errorf("argmax picked uncovered cube-corner (WAW=%d, PR=%d, RAB=%d); covered set = %v; full reasoning: %s",
			picked.waw, picked.pr, picked.rab, covered, out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "covered cells") {
		t.Errorf("Reasoning should surface covered-cells count; got %q", out.Reasoning)
	}
}

// TestApplyHistory_RegressionFallsThroughWhenNoCoverage (#221) pins
// the degradation path: when no training rows match any cell in the
// readerGrid (e.g. all history is at PR=3 / RAB=5 — values outside
// the grid), every argmax candidate is filtered. argmaxRegression
// must return ok=false and the caller falls through to smoothed bins.
func TestApplyHistory_RegressionFallsThroughWhenNoCoverage(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	// 30 rows all at PR=3, RAB=5 — values not in readerGrid ({2,4}×{4,8}).
	// Regression argmax will find every grid candidate uncovered;
	// applyHistoryRegression returns false and applyHistorySelection
	// falls to smoothed-bins. Smoothed-bins's reader-filter (#219
	// codex pass 3) will then find no rows matching baseline (PR=2,
	// RAB=4) — both tiers cleanly skip with reasoning emitted.
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: (i % 6) + 1,
			ChunkSize:         50_000,
			AvgRowBytes:       500,
			ParallelReaders:   3, // outside readerGrid
			ReadAheadBuffers:  5, // outside readerGrid
			FinalThroughput:   600_000 + float64(i*1000),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	history := &stubHistory{rows: rows}

	applyHistory(&out, in, profile, history, DBTuning{})

	if !strings.Contains(out.Reasoning, "regression skipped: every grid candidate filtered") {
		t.Errorf("Reasoning should announce regression-tier filter-empty skip; got %q", out.Reasoning)
	}
	// Coverage count is non-zero in the fixture (6 distinct WAW values
	// at PR=3/RAB=5), but every covered cell is outside readerGrid so
	// argmax filters them all. The diagnostic surfaces the count so a
	// reader of the log can tell the difference between "no history
	// at all" and "history doesn't intersect the grid".
	if !strings.Contains(out.Reasoning, "covered cells:") {
		t.Errorf("Reasoning should surface covered-cells count even when filtered out; got %q", out.Reasoning)
	}
	// And the smoothed-bins tier should also skip — none of these
	// rows match the current run's baseline reader settings.
	if !strings.Contains(out.Reasoning, "no rows match current reader settings") {
		t.Errorf("Reasoning should record smoothed-bins skip when no rows match baseline reader settings; got %q", out.Reasoning)
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
	// Per-WAW means are 600K + 100K·waw, with WAW=4 overridden to 200K:
	//   WAW=1..6 → [700K, 800K, 900K, 200K, 1.1M, 1.2M]
	//   sorted   → [200K, 700K, 800K, 900K, 1.1M, 1.2M]
	//   median   = upper-middle (len/2 = 3) = 900K
	// WAW=4's 200K < 900K → excluded. Pre-#204 test had WAW=4 at 1.2M
	// (above any median) and the retry-rate-only filter still excluded
	// it; post-#204 we need WAW=4 to actually be a bad config to be
	// filtered.
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
			ParallelReaders:   2, // pinned at baseline so the #221 coverage
			ReadAheadBuffers:  4, // gate doesn't pre-empt the retry-rate filter
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
	// Sanity: the regression tier (not smoothed-bins) should have done the
	// picking — otherwise this test would pass via the coverage-gate
	// fall-through and not actually exercise the retry-rate filter.
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Reasoning should record regression-selected (not a coverage-gate fall-through); got %q", out.Reasoning)
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
			ParallelReaders:   2, // pinned at baseline so the #221 coverage
			ReadAheadBuffers:  4, // gate doesn't pre-empt the HardChunkLimit filter
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
	// The skip reason should specifically cite HardChunkLimit (not just
	// that something filtered). Otherwise the test could pass via the
	// coverage-gate fall-through and not actually exercise HardChunkLimit.
	if !strings.Contains(out.Reasoning, "HardChunkLimit=10") {
		t.Errorf("Reasoning should attribute the regression skip to HardChunkLimit=10; got %q", out.Reasoning)
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
			// Pinned at baseline reader settings so #219's
			// smoothed-bins reader-filter keeps the rows.
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
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
	applyEpsilonPerturbation(out, profile, 0)

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

// --- #218: PI upper-bound display cap ---

// TestFormatPredictionInterval_AbsoluteCeiling_EmitsWideMarker — the
// exact failure mode from #218: an astronomical upper bound from a
// leverage explosion at a sparse-data x*. The raw number erodes
// operator trust ("did dmt overflow?"); display the "wide" marker
// instead. Post-#224 inputs are bytes/sec; pred ~ 300 MB/s and
// high ~ 10²⁴ B/s is the bytes/sec analogue of the original repro.
func TestFormatPredictionInterval_AbsoluteCeiling_EmitsWideMarker(t *testing.T) {
	// pred ≈ 300 MB/s (600K rows/s × 500B), high ≈ 9.67e23 B/s,
	// low clamped at 0.
	got := formatPredictionInterval(308_967_000, 0, 9.67e23)
	if !strings.Contains(got, "wide") {
		t.Errorf("absurd CI upper bound should emit 'wide' marker; got %q", got)
	}
	if strings.Contains(got, "MB/s") || strings.Contains(got, "GB/s") {
		t.Errorf("absurd CI display should NOT include unit-formatted numbers (defeats the trust-preserving point); got %q", got)
	}
}

// TestFormatPredictionInterval_RelativeRatio_EmitsWideMarker — the
// pred-relative cap: even when the absolute ceiling wouldn't fire,
// a CI upper bound far past the prediction tells the operator
// nothing useful. Marker, not number. Post-#224 in bytes/sec:
// pred=1 GB/s, high=25 GB/s — under the 1 TB/s absolute cap but
// 25× pred, past the 10× relative cap.
func TestFormatPredictionInterval_RelativeRatio_EmitsWideMarker(t *testing.T) {
	got := formatPredictionInterval(1_000_000_000, 0, 25_000_000_000)
	if !strings.Contains(got, "wide") {
		t.Errorf("high/pred=25× should emit 'wide' marker (relative cap); got %q", got)
	}
}

// TestFormatPredictionInterval_NormalRange_DisplaysNumbers — guard
// against over-zealous capping: a sensible CI must still render as
// numbers, otherwise the fix would silently break the common case.
// Post-#224: 350 MB/s lo, 620 MB/s hi (the so2010-shape range,
// rescaled to bytes/sec at ~500B avg row).
func TestFormatPredictionInterval_NormalRange_DisplaysNumbers(t *testing.T) {
	got := formatPredictionInterval(500_000_000, 350_000_000, 620_000_000)
	if strings.Contains(got, "wide") {
		t.Errorf("normal CI should NOT emit 'wide' marker; got %q", got)
	}
	if !strings.Contains(got, "MB/s") {
		t.Errorf("sub-1GB/s CI should render in MB/s units; got %q", got)
	}
}

// TestFormatPredictionInterval_NegativeLowClamped — the existing
// clamp-low-to-zero behavior must survive the unit switch (#224);
// users would be misled by "throughput could be negative."
func TestFormatPredictionInterval_NegativeLowClamped(t *testing.T) {
	got := formatPredictionInterval(400_000_000, -50_000_000, 800_000_000)
	if !strings.HasPrefix(got, "0 MB/s–") {
		t.Errorf("low<0 should render as '0 MB/s–...'; got %q", got)
	}
}

// TestFormatPredictionInterval_ZeroPredAllowsAnyHigh — when the
// prediction itself is zero (or nonpositive), the relative ratio
// can't fire (division-by-zero guard); only the absolute ceiling
// gates the display. Defensive test for the pred-zero edge.
// Post-#224 in bytes/sec, well under the 1 TB/s absolute ceiling.
func TestFormatPredictionInterval_ZeroPredAllowsAnyHigh(t *testing.T) {
	got := formatPredictionInterval(0, 0, 25_000_000_000)
	if strings.Contains(got, "wide") {
		t.Errorf("pred=0 should bypass relative ratio cap (high < absolute ceiling); got %q", got)
	}
}

// TestFormatPredictionInterval_EndpointsShareUnit pins the Copilot
// fix on PR #289: when the PI straddles the 1 GB/s switchover, both
// endpoints must render in the same unit. Pre-fix this rendered as
// "500 MB/s–2.00 GB/s" which makes the range hard to compare. With
// the upper-bound-driven unit selection it should render in GB/s for
// both sides.
func TestFormatPredictionInterval_EndpointsShareUnit(t *testing.T) {
	got := formatPredictionInterval(1_200_000_000, 500_000_000, 2_000_000_000)
	if strings.Contains(got, "MB/s") && strings.Contains(got, "GB/s") {
		t.Errorf("PI endpoints must share a unit; got %q (mixes MB/s and GB/s)", got)
	}
	if !strings.Contains(got, "GB/s") {
		t.Errorf("PI with high ≥ 1 GB/s should render both endpoints in GB/s; got %q", got)
	}
}

// TestApplyHistorySelection_RegressionEngagesMidWindow locks in the
// #452 behavior change: a history in the 12-29 row window — which the
// old 30-row floor consigned to WAW-only smoothed bins — now fires the
// regression tier, which can place chunk size and detect a non-monotone
// WAW peak. Fixture mirrors TestApplyHistory_RegressionTier's quadratic
// peak at WAW=4, shrunk to 18 rows (3 per WAW cell so the #221 coverage
// gate keeps every cell selectable).
func TestApplyHistorySelection_RegressionEngagesMidWindow(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	rows := make([]HistoryRecord, 18)
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
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
			FinalThroughput:   thru,
			CPUCores:          16,
			MemoryGB:          48,
		}
	}

	applyHistorySelection(&out, in, profile, rows)

	if out.Tier != TierRegression {
		t.Fatalf("Tier = %q, want %q (18 rows must clear the #452 attempt gate); reasoning: %s", out.Tier, TierRegression, out.Reasoning)
	}
	if out.WriteAheadWriters != 4 {
		t.Errorf("WAW = %d, want 4 (regression should detect the quadratic peak)", out.WriteAheadWriters)
	}
}

// TestApplyHistory_RuntimeAdjustedRowsExcluded locks in the #451 fix: rows
// flagged AdjustedAtRuntime must not reach any selection tier. The fixture
// poisons the history with adjusted runs showing spectacular throughput at
// WAW=4 — exactly the flattering blend a mid-run controller rescue
// produces. Pre-fix, the 14 total rows cleared the regression gate and the
// argmax chased the poisoned WAW=4 cells; post-fix only the 6 clean WAW=2
// rows remain, the smoothed-bins tier runs, and WAW stays 2.
func TestApplyHistory_RuntimeAdjustedRowsExcluded(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

	rows := []HistoryRecord{}
	for i := 0; i < 6; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 800_000,
			CPUCores:        16, MemoryGB: 48,
		})
	}
	for i := 0; i < 8; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 4, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 2_000_000,
			CPUCores:        16, MemoryGB: 48,
			AdjustedAtRuntime: true,
		})
	}

	applyHistory(&out, in, profile, &stubHistory{rows: rows}, DBTuning{})

	if out.WriteAheadWriters != 2 {
		t.Errorf("WAW = %d, want 2 (adjusted WAW=4 rows must not train any tier); reasoning: %s",
			out.WriteAheadWriters, out.Reasoning)
	}
	if out.Tier != TierSmoothedBins {
		t.Errorf("Tier = %q, want %q (6 clean rows -> bins, not regression over 14)", out.Tier, TierSmoothedBins)
	}
	if !strings.Contains(out.Reasoning, "runtime-adjusted") {
		t.Errorf("Reasoning missing runtime-adjusted hygiene note; got %q", out.Reasoning)
	}
}

// TestTune_AllRowsRuntimeAdjusted_TreatedAsColdStart verifies the bucket
// count also excludes adjusted rows (#451): when every history row is
// adjusted, the bucket is effectively cold and Tune should re-enter the
// planned exploration grid to gather clean measurements — not train on
// the poisoned set, and not sit at bare baseline when a working history
// backend is available to learn through.
func TestTune_AllRowsRuntimeAdjusted_TreatedAsColdStart(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	rows := make([]HistoryRecord, 8)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 4, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 2_000_000,
			CPUCores:        16, MemoryGB: 48,
			AdjustedAtRuntime: true,
		}
	}

	out := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})

	if out.Tier != TierExploration {
		t.Errorf("Tier = %q, want %q (all-adjusted history = cold-start bucket)", out.Tier, TierExploration)
	}
	if !strings.Contains(out.Reasoning, "runtime-adjusted") {
		t.Errorf("Reasoning missing runtime-adjusted hygiene note; got %q", out.Reasoning)
	}
}

func TestTune_ExplorationUsesRawPersistedCountAndUsableLabel(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	rows := make([]HistoryRecord, 10)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 800_000,
			CPUCores:        16, MemoryGB: 48,
			AdjustedAtRuntime: i >= 2,
		}
	}

	assertPick := func(t *testing.T, historyRows []HistoryRecord, rawCount int) Output {
		t.Helper()
		want := baseline(in, profile)
		applyGridExploration(&want, in, profile, 2, rawCount)
		applyMemoryClamp(&want, in)
		got := Tune(in, profile, &stubHistory{rows: historyRows}, DBTuning{})
		if got.Tier != TierExploration {
			t.Fatalf("Tier = %q, want %q; reasoning: %s", got.Tier, TierExploration, got.Reasoning)
		}
		if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
			got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
			t.Fatalf("exploration pick = (WAW=%d CS=%d PR=%d RAB=%d), want (WAW=%d CS=%d PR=%d RAB=%d)",
				got.WriteAheadWriters, got.ChunkSize, got.ParallelReaders, got.ReadAheadBuffers,
				want.WriteAheadWriters, want.ChunkSize, want.ParallelReaders, want.ReadAheadBuffers)
		}
		if !strings.Contains(got.Reasoning, "run 3/12") {
			t.Fatalf("reasoning = %q, want usable-count label run 3/12", got.Reasoning)
		}
		return got
	}

	first := assertPick(t, rows, 10)
	rows = append(rows, HistoryRecord{
		SourceDBType: "mssql", TargetDBType: "postgres",
		WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500,
		ParallelReaders: 2, ReadAheadBuffers: 4,
		FinalThroughput: 800_000,
		CPUCores:        16, MemoryGB: 48,
		AdjustedAtRuntime: true,
	})
	second := assertPick(t, rows, 11)
	if first.WriteAheadWriters == second.WriteAheadWriters && first.ChunkSize == second.ChunkSize &&
		first.ParallelReaders == second.ParallelReaders && first.ReadAheadBuffers == second.ReadAheadBuffers {
		t.Fatal("appending a filtered raw row did not advance the exploration cell")
	}
}

func TestTune_ForcedExplorationUsesRawIndexWithoutColdStartLabel(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:       "linux",
		AvgRowBytes:    500,
		MemoryBudgetMB: 64_000,
		ForceExplore:   true,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	rows := make([]HistoryRecord, 16)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 2, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 800_000,
			CPUCores:        16, MemoryGB: 48,
			AdjustedAtRuntime: i >= 12,
		}
	}

	want := baseline(in, profile)
	applyGridExploration(&want, in, profile, 12, 16)
	applyMemoryClamp(&want, in)
	got := Tune(in, profile, &stubHistory{rows: rows}, DBTuning{})
	if got.WriteAheadWriters != want.WriteAheadWriters || got.ChunkSize != want.ChunkSize ||
		got.ParallelReaders != want.ParallelReaders || got.ReadAheadBuffers != want.ReadAheadBuffers {
		t.Fatalf("forced exploration did not use raw index: got %+v, want %+v", got, want)
	}
	if !strings.Contains(got.Reasoning, "probe idx") || strings.Contains(got.Reasoning, "run ") {
		t.Fatalf("forced exploration reasoning has invalid cold-start label: %q", got.Reasoning)
	}
}
