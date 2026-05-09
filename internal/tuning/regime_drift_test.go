package tuning

import (
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
