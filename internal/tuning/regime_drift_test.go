package tuning

import (
	"strings"
	"testing"
	"time"
)

// TestDetectRegimeDrift_NoDrift: stable throughput at the same config →
// no drift.
func TestDetectRegimeDrift_NoDrift(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		700_000, 720_000, 680_000, 710_000, 690_000, 705_000,
	})
	if detectRegimeDrift(rows) {
		t.Error("stable throughput should not trigger drift")
	}
}

// TestDetectRegimeDrift_DownwardShift: recent runs ~half older runs →
// drift fires.
func TestDetectRegimeDrift_DownwardShift(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 1_050_000, 980_000, // older — high throughput
		1_020_000, 1_010_000, 990_000,
		400_000, 420_000, 380_000, // recent 3 — about half
	})
	if !detectRegimeDrift(rows) {
		t.Error("recent ~half older should trigger drift")
	}
}

// TestDetectRegimeDrift_UpwardShift: recent runs ~50% above older runs
// → drift also fires (upper threshold).
func TestDetectRegimeDrift_UpwardShift(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		400_000, 420_000, 380_000,
		410_000, 390_000, 405_000,
		800_000, 820_000, 790_000, // recent 3 — about 2× older
	})
	if !detectRegimeDrift(rows) {
		t.Error("recent ~2× older should trigger drift (upper threshold)")
	}
}

// TestDetectRegimeDrift_BelowMinAtConfig: too few runs at the config →
// no drift call (insufficient data).
func TestDetectRegimeDrift_BelowMinAtConfig(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 100_000, 100_000, // 3 rows at this config; below
		// driftMinAtConfig=6
	})
	if detectRegimeDrift(rows) {
		t.Error("should not fire below driftMinAtConfig threshold")
	}
}

// TestDetectRegimeDrift_BelowGlobalMin: total rows below the floor →
// short-circuits without grouping.
func TestDetectRegimeDrift_BelowGlobalMin(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 100_000, // 2 rows total; below driftMinAtConfig
	})
	if detectRegimeDrift(rows) {
		t.Error("should not fire below global minimum")
	}
}

// TestTune_DriftDetectedBeforeOutlierFilter is the regression for the
// Codex-flagged ordering bug on PR #183: when recent clean runs slow
// down enough to fall below the outlier-filter floor (< 0.5×median),
// running detectRegimeDrift on the post-outlier set would silently lose
// exactly the rows that signal drift. Tune now runs drift on the
// regime-filtered, pre-outlier set so this case fires.
func TestTune_DriftDetectedBeforeOutlierFilter(t *testing.T) {
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform:    "linux",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}

	// 12 older runs around 1M rows/s + 3 recent runs around 380K. The
	// recent runs are clean (no retries), so the outlier filter would
	// drop them as "noise" (380K < 0.5 × 1M median). Without the fix,
	// drift detection on the post-outlier set wouldn't see the recent runs;
	// the 12 survivors are deliberately past the cold-start threshold, so
	// Tune would choose history instead of reaching exploration by accident.
	// With the fix, drift runs on the regime-filtered set and catches the shift.
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 1_050_000, 980_000,
		1_020_000, 1_010_000, 990_000,
		1_030_000, 970_000, 1_040_000,
		995_000, 1_005_000, 1_015_000,
		380_000, 400_000, 360_000, // recent — would be "noise" post-filter
	})
	history := &stubHistory{rows: rows}

	out := Tune(in, profile, history, DBTuning{})

	// Drift forces exploration; the planned-grid pick produces a
	// "exploration: planned grid" reasoning entry (vs. regression-
	// selected or smoothed-bins which would mean drift was missed).
	if out.Reasoning == "" {
		t.Fatalf("expected exploration reasoning when drift fires, got empty")
	}
	want := "exploration: planned grid"
	if !strings.Contains(out.Reasoning, want) {
		t.Errorf("expected reasoning to mention %q (drift → exploration); got %q", want, out.Reasoning)
	}
}

// TestDetectRegimeDrift_DriftAtRecentCellFires: when the most recent
// run's cell is the one showing drift, the detector fires.
//
// Renamed from PerConfigGrouping after #184 narrowed drift detection
// to the most-recent cell only. Both groups in this test start with
// the same base timestamp, but the WAW=2 group has 9 runs (vs 6 for
// WAW=1), so its latest timestamp wins → most-recent cell is WAW=2.
// Drift at WAW=2 fires.
func TestDetectRegimeDrift_DriftAtRecentCellFires(t *testing.T) {
	rows := append(
		// (WAW=1, CS=50000): stable, 6 runs
		makeFixedConfigRuns(1, 50000, []float64{
			500_000, 510_000, 490_000, 505_000, 495_000, 500_000,
		}),
		// (WAW=2, CS=50000): drifted (recent half of older), 9 runs —
		// latest timestamp is here, so this is the "current" cell.
		makeFixedConfigRuns(2, 50000, []float64{
			1_000_000, 1_050_000, 980_000,
			1_020_000, 1_010_000, 990_000,
			400_000, 420_000, 380_000,
		})...,
	)
	if !detectRegimeDrift(rows) {
		t.Error("drift at the most recently used cell should fire")
	}
}

// TestDetectRegimeDrift_StaleCellDriftIgnored — #184 regression guard.
// Old behavior: drift at ANY cell with enough runs would fire,
// including stale cells the system moved on from. Result: every
// subsequent run hit drift on the stale cell and was forced into
// exploration, starving the regression tier.
//
// New behavior: only the most-recent cell is checked. A stale cell
// that drifted in the past is irrelevant — the system is no longer
// using that config.
func TestDetectRegimeDrift_StaleCellDriftIgnored(t *testing.T) {
	// (WAW=2, CS=50000): drifted in the past, EARLIER timestamps.
	staleGroup := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 1_050_000, 980_000,
		1_020_000, 1_010_000, 990_000,
		400_000, 420_000, 380_000,
	})
	// (WAW=3, CS=49115): stable, but starts at the same base time as
	// staleGroup. Bump these timestamps so this becomes the most-recent
	// cell. Capture the boundary explicitly rather than hard-coding 9.
	currentGroup := makeFixedConfigRuns(3, 49115, []float64{
		800_000, 810_000, 790_000, 805_000, 795_000, 800_000,
	})
	staleEnd := len(staleGroup)
	rows := append(staleGroup, currentGroup...)
	for i := staleEnd; i < len(rows); i++ {
		rows[i].Timestamp = rows[i].Timestamp.Add(24 * time.Hour)
	}

	if detectRegimeDrift(rows) {
		t.Error("stale cell drift should NOT fire — system has moved on (#184 regression)")
	}
}

// TestDetectRegimeDrift_WiderThresholdTolerantOfNoise — #184 fix part A.
// Old [0.70, 1.30] threshold fired on every run because real-world
// within-bin variance is ~2.5×. The new [0.50, 1.50] band tolerates
// up to 50% recent-vs-older shifts without firing. This test pins
// that a 1.4× shift (well within real noise) no longer fires.
func TestDetectRegimeDrift_WiderThresholdTolerantOfNoise(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		700_000, 720_000, 680_000, // older median ≈ 700K
		710_000, 690_000, 705_000,
		980_000, 1_000_000, 990_000, // recent median ≈ 990K, ratio ≈ 1.4
	})
	if detectRegimeDrift(rows) {
		t.Error("1.4× shift is within the [0.50, 1.50] band; should NOT fire (#184 fix)")
	}
}

// TestDetectRegimeDrift_IgnoresIncompleteLatestRow — Codex review on
// PR #184. If the newest history row is incomplete (FinalThroughput
// <= 0, e.g. a crashed migration or NULL surfaced as 0 from the
// checkpoint backend), `mostRecentCell` must skip it and pick the
// most recent COMPLETED row's cell. Otherwise drift would route to a
// "phantom" cell that immediately gets filtered out by rowsAtCell.
func TestDetectRegimeDrift_IgnoresIncompleteLatestRow(t *testing.T) {
	// Drift at WAW=2/CS=50000 across 9 completed runs. Then a 10th
	// row appended at WAW=99/CS=99 (a "phantom" cell representing a
	// crashed run) with FinalThroughput=0, timestamp later than all
	// completed rows. Without the fix, mostRecentCell returns the
	// phantom cell, rowsAtCell finds 0 completed rows there, drift
	// returns false → genuine drift at WAW=2 missed.
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 1_050_000, 980_000,
		1_020_000, 1_010_000, 990_000,
		400_000, 420_000, 380_000,
	})
	rows = append(rows, HistoryRecord{
		Timestamp:         rows[len(rows)-1].Timestamp.Add(time.Hour),
		WriteAheadWriters: 99,
		ChunkSize:         99,
		FinalThroughput:   0, // crashed / in-progress
		CPUCores:          16, MemoryGB: 48,
	})

	if !detectRegimeDrift(rows) {
		t.Error("incomplete latest row should be ignored; drift at the most recent COMPLETED cell must still fire (#184 Codex regression)")
	}
}

// TestDetectRegimeDrift_WidenedThresholdStillFiresAtTwoX — sanity guard
// that widening to [0.50, 1.50] still catches genuine regime changes.
// A 2× shift in either direction must still fire.
func TestDetectRegimeDrift_WidenedThresholdStillFiresAtTwoX(t *testing.T) {
	rows := makeFixedConfigRuns(2, 50000, []float64{
		500_000, 510_000, 490_000,
		505_000, 495_000, 500_000,
		1_000_000, 1_020_000, 1_050_000, // recent ~2× older
	})
	if !detectRegimeDrift(rows) {
		t.Error("2× shift exceeds 1.50 upper bound; should still fire")
	}
}

// TestDetectRegimeDrift_ReaderAxisChangeNotDrift (#219, codex review)
// guards against the bug Codex flagged after the initial #219 commit:
// once the tuner started varying ParallelReaders / ReadAheadBuffers
// at the same (WAW, ChunkSize) outer cell, throughput shifts caused
// by reader-axis picks could be misclassified as regime drift if
// the drift cell still keyed by (WAW, ChunkSize) only.
//
// The fix extends configKey to the full (WAW, ChunkSize, PR, RAB)
// quadruple. This test pins that behavior: 9 runs at the same
// (WAW=2, CS=50000) but split across two PR/RAB combos — the "old"
// runs at PR=2/RAB=4 ~500K and the "recent" run at PR=4/RAB=8 ~1M —
// must NOT fire drift, because the recent cell only has one sample
// (below driftMinAtConfig) and the older cell is unchanged.
func TestDetectRegimeDrift_ReaderAxisChangeNotDrift(t *testing.T) {
	// 8 runs at (WAW=2, CS=50000, PR=2, RAB=4) at ~500K rows/s.
	older := makeFixedConfigRunsWithReaders(2, 50000, 2, 4, []float64{
		500_000, 510_000, 490_000,
		505_000, 495_000, 500_000,
		498_000, 502_000,
	})
	// 1 newer run at the same outer cell but different inner combo,
	// ~2× throughput (a plausible reader-axis effect). Newer
	// timestamp so it becomes mostRecentCell.
	newer := makeFixedConfigRunsWithReaders(2, 50000, 4, 8, []float64{
		1_000_000,
	})
	for i := range newer {
		newer[i].Timestamp = older[len(older)-1].Timestamp.Add(time.Hour)
	}
	rows := append(older, newer...)

	if detectRegimeDrift(rows) {
		t.Error("reader-axis change at same (WAW, CS) should NOT fire drift — the recent cell has just 1 run, below driftMinAtConfig")
	}
}

// makeFixedConfigRuns builds a synthetic slice where every row uses the
// same WAW + chunk_size and the throughputs come from the slice; each
// row gets a monotonically increasing Timestamp so the detector's
// chronological sort is well-defined.
func makeFixedConfigRuns(waw, chunkSize int, throughputs []float64) []HistoryRecord {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]HistoryRecord, len(throughputs))
	for i, th := range throughputs {
		rows[i] = HistoryRecord{
			Timestamp:         base.Add(time.Duration(i) * time.Hour),
			WriteAheadWriters: waw,
			ChunkSize:         chunkSize,
			AvgRowBytes:       500,
			FinalThroughput:   th,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	return rows
}

// makeFixedConfigRunsWithReaders is the #219 counterpart that also pins
// ParallelReaders and ReadAheadBuffers on the synthetic rows. Used by
// tests that need to distinguish runs by the full configKey quadruple.
func makeFixedConfigRunsWithReaders(waw, chunkSize, pr, rab int, throughputs []float64) []HistoryRecord {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]HistoryRecord, len(throughputs))
	for i, th := range throughputs {
		rows[i] = HistoryRecord{
			Timestamp:         base.Add(time.Duration(i) * time.Hour),
			WriteAheadWriters: waw,
			ChunkSize:         chunkSize,
			ParallelReaders:   pr,
			ReadAheadBuffers:  rab,
			AvgRowBytes:       500,
			FinalThroughput:   th,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	return rows
}
