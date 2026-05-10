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
	bins := []wawBin{
		{WAW: 1, TotalRuns: 10, MeanThroughput: 500_000},
		// 3/10 = 30% retry rate, > 15% threshold → excluded
		{WAW: 2, TotalRuns: 10, MeanThroughput: 800_000, RunsWithRetries: 3},
		{WAW: 4, TotalRuns: 10, MeanThroughput: 600_000}, // wins
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

// TestSelectWAW_AllBinsRateExcluded returns ok=false so the caller falls
// back to baseline. Both bins clear the retry-rate threshold.
func TestSelectWAW_AllBinsRateExcluded(t *testing.T) {
	bins := []wawBin{
		// 2/5 = 40% retry rate, well above threshold
		{WAW: 1, TotalRuns: 5, MeanThroughput: 500_000, RunsWithRetries: 2},
		// 3/5 = 60% retry rate
		{WAW: 2, TotalRuns: 5, MeanThroughput: 700_000, RunsWithRetries: 3},
	}
	_, _, ok := selectWAW(bins)
	if ok {
		t.Error("ok should be false when every bin clears the retry-rate threshold")
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

	// History: WAW=1 has clean runs at 700K, WAW=2 has 3/3 = 100%
	// retry rate (well above threshold, ≥ minRunsForRetryExclusion →
	// excluded), WAW=4 has clean runs at 600K. WAW=1 should win.
	history := &stubHistory{rows: []HistoryRecord{
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 700_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 720_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 1, FinalThroughput: 680_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 750_000, ChunkRetryCount: 2},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 720_000, ChunkRetryCount: 1},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 2, FinalThroughput: 760_000, ChunkRetryCount: 3},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 600_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 620_000},
		{CPUCores: 16, MemoryGB: 48, WriteAheadWriters: 4, FinalThroughput: 610_000},
	}}

	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != 1 {
		t.Errorf("WAW: got %d, want 1 (retry-rate filter should exclude WAW=2; WAW=1 has higher mean than WAW=4)",
			out.WriteAheadWriters)
	}
	if !strings.Contains(out.Reasoning, "history-selected WAW=1") {
		t.Errorf("Reasoning should record the selection; got %q", out.Reasoning)
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

// TestApplyHistory_AllBinsRateExcluded falls back to baseline when every
// bin has a retry rate above the threshold over enough samples to fire.
func TestApplyHistory_AllBinsRateExcluded(t *testing.T) {
	in := Input{CPUCores: 16, MemoryGB: 48, SourceDBType: "mssql", TargetDBType: "postgres"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2}
	out := baseline(in, profile)
	originalWAW := out.WriteAheadWriters

	// 4 runs each at WAW=1 and WAW=2; 3 of 4 retried in each → 75%
	// retry rate, above the 15% threshold, ≥ minRunsForRetryExclusion.
	makeRow := func(waw, retries int, thru float64) HistoryRecord {
		return HistoryRecord{
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: waw,
			FinalThroughput:   thru,
			ChunkRetryCount:   retries,
		}
	}
	history := &stubHistory{rows: []HistoryRecord{
		makeRow(1, 1, 700_000), makeRow(1, 1, 690_000), makeRow(1, 1, 710_000), makeRow(1, 0, 720_000),
		makeRow(2, 2, 600_000), makeRow(2, 1, 610_000), makeRow(2, 1, 620_000), makeRow(2, 0, 605_000),
	}}
	applyHistory(&out, in, profile, history, DBTuning{})

	if out.WriteAheadWriters != originalWAW {
		t.Errorf("WAW should remain at baseline %d when every bin clears the retry-rate threshold; got %d",
			originalWAW, out.WriteAheadWriters)
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

	// 60 rows. WAW=4 has high throughput AND one retry; should be skipped.
	// WAW=2 has clean runs at lower throughput but is the next best.
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		thru := 600_000.0 + 100.0*float64(waw)
		retries := 0
		if waw == 4 {
			thru = 1_200_000 // best mean, but...
			retries = 1      // ...100% retry rate (10/10) → excluded
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
		t.Error("retry-rate filter should have excluded WAW=4 (100% historical retry rate)")
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
