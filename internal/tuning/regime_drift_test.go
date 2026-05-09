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

	// 6 older runs around 1M rows/s + 3 recent runs around 380K. The
	// recent runs are clean (no retries), so the outlier filter would
	// drop them as "noise" (380K < 0.5 × 1M median). Without the fix,
	// drift detection on the post-outlier set wouldn't see the recent
	// runs and would NOT fire. With the fix, drift runs on the
	// regime-filtered set and catches the shift.
	rows := makeFixedConfigRuns(2, 50000, []float64{
		1_000_000, 1_050_000, 980_000,
		1_020_000, 1_010_000, 990_000,
		380_000, 400_000, 360_000, // recent — would be "noise" post-filter
	})
	history := &stubHistory{rows: rows}

	out := Tune(in, profile, history, DBTuning{})

	// Drift forces exploration; the planned-grid pick produces a
	// "exploration: planned grid" reasoning entry (vs. regression-
	// or history-selected which would mean drift was missed).
	if out.Reasoning == "" {
		t.Fatalf("expected exploration reasoning when drift fires, got empty")
	}
	want := "exploration: planned grid"
	if !strings.Contains(out.Reasoning, want) {
		t.Errorf("expected reasoning to mention %q (drift → exploration); got %q", want, out.Reasoning)
	}
}

// TestDetectRegimeDrift_PerConfigGrouping: drift at one config doesn't
// require drift at others — single shifted config is enough.
func TestDetectRegimeDrift_PerConfigGrouping(t *testing.T) {
	rows := append(
		// (WAW=1, CS=50000): stable
		makeFixedConfigRuns(1, 50000, []float64{
			500_000, 510_000, 490_000, 505_000, 495_000, 500_000,
		}),
		// (WAW=2, CS=50000): drifted (recent half of older)
		makeFixedConfigRuns(2, 50000, []float64{
			1_000_000, 1_050_000, 980_000,
			1_020_000, 1_010_000, 990_000,
			400_000, 420_000, 380_000,
		})...,
	)
	if !detectRegimeDrift(rows) {
		t.Error("drift at one config should fire, even when others are stable")
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
