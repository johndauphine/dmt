package tuning

import (
	"strings"
	"testing"
)

// TestClassifyRegime_Same verifies the happy path: identical CPU /
// memory / DB-tuning means RegimeSame and no deltas.
func TestClassifyRegime_Same(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{CPUCores: 16, MemoryGB: 48}
	tuning := DBTuning{TargetSharedBuffersMB: 8192, TargetSyncCommit: "off"}
	histTuning := tuning // identical

	hist.TargetSharedBuffersMB = histTuning.TargetSharedBuffersMB
	hist.TargetSyncCommit = histTuning.TargetSyncCommit

	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeSame {
		t.Errorf("label: got %q, want %q", label, RegimeSame)
	}
	if len(deltas) != 0 {
		t.Errorf("deltas: got %v, want none", deltas)
	}
}

// TestClassifyRegime_UnknownOnMissingFields locks in the pre-#144
// fallback: rows without CPU + memory data classify as Unknown so they
// get filtered out (treated as weak evidence) instead of accidentally
// matching as Same.
func TestClassifyRegime_UnknownOnMissingFields(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	cases := []HistoryRecord{
		{CPUCores: 0, MemoryGB: 48},
		{CPUCores: 16, MemoryGB: 0},
		{CPUCores: 0, MemoryGB: 0},
	}
	for i, hist := range cases {
		label, _ := ClassifyRegime(hist, current, DBTuning{})
		if label != RegimeUnknown {
			t.Errorf("case %d: got %q, want %q", i, label, RegimeUnknown)
		}
	}
}

// TestClassifyRegime_HardwareTolerances locks the boundary behavior on
// CPU and memory similarity. CPU threshold is max(2, current/5); memory
// must agree within 20%.
func TestClassifyRegime_HardwareTolerances(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48} // CPU threshold = 16/5 = 3
	cases := []struct {
		name     string
		hist     HistoryRecord
		wantSame bool
	}{
		{"identical", HistoryRecord{CPUCores: 16, MemoryGB: 48}, true},
		{"cpu within threshold", HistoryRecord{CPUCores: 13, MemoryGB: 48}, true},
		{"cpu beyond threshold", HistoryRecord{CPUCores: 12, MemoryGB: 48}, false}, // 4 > 3
		{"memory within 20%", HistoryRecord{CPUCores: 16, MemoryGB: 40}, true},     // 40/48 = 83%
		{"memory beyond 20%", HistoryRecord{CPUCores: 16, MemoryGB: 38}, false},    // 38/48 = 79%
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, deltas := ClassifyRegime(tc.hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("%s: label=%q, isSame=%v, wantSame=%v, deltas=%v",
					tc.name, label, isSame, tc.wantSame, deltas)
			}
		})
	}
}

// TestClassifyRegime_DBTuningDifferences exercises each DB-tuning field
// independently to verify they all contribute to RegimeDifferentTuning.
func TestClassifyRegime_DBTuningDifferences(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	baseHist := HistoryRecord{CPUCores: 16, MemoryGB: 48}
	curr := DBTuning{
		TargetSharedBuffersMB:   8192,
		TargetSyncCommit:        "on",
		TargetFsync:             "on",
		TargetFullPageWrites:    "on",
		TargetMaxWALSizeMB:      4096,
		TargetWALLevel:          "replica",
		SourceMaxServerMemoryMB: 16384,
	}
	cases := []struct {
		name string
		mut  func(*HistoryRecord)
	}{
		{"shared_buffers differs >10%", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = 4096
			// fill the rest from current to isolate this delta
			h.TargetSyncCommit = curr.TargetSyncCommit
			h.TargetFsync = curr.TargetFsync
		}},
		{"sync_commit differs", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = curr.TargetSharedBuffersMB
			h.TargetSyncCommit = "off"
			h.TargetFsync = curr.TargetFsync
		}},
		{"max_wal differs >10%", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = curr.TargetSharedBuffersMB
			h.TargetSyncCommit = curr.TargetSyncCommit
			h.TargetMaxWALSizeMB = 1024
		}},
		{"mssql_max_server_memory differs >10%", func(h *HistoryRecord) {
			h.SourceMaxServerMemoryMB = 4096
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := baseHist
			tc.mut(&h)
			label, deltas := ClassifyRegime(h, current, curr)
			if label != RegimeDifferentTuning {
				t.Errorf("got %q (deltas=%v), want %q", label, deltas, RegimeDifferentTuning)
			}
		})
	}
}

// TestClassifyRegime_SizeTolerance locks in the 10% size tolerance —
// 8000MB vs 8192MB shouldn't count as a regime change.
func TestClassifyRegime_SizeTolerance(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:              16,
		MemoryGB:              48,
		TargetSharedBuffersMB: 8000,
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192} // 8000/8192 = 97.6% → within 10%
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeSame {
		t.Errorf("got %q (deltas=%v), want %q", label, deltas, RegimeSame)
	}
}

// TestClassifyRegime_DifferentHWAndTuning verifies the combined label
// fires when both axes differ.
func TestClassifyRegime_DifferentHWAndTuning(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:              4, // way fewer cores
		MemoryGB:              48,
		TargetSharedBuffersMB: 1024, // way smaller buffers
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192}
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeDifferentHWAndTuning {
		t.Errorf("got %q, want %q", label, RegimeDifferentHWAndTuning)
	}
	if len(deltas) < 2 {
		t.Errorf("expected at least 2 deltas (hw + tuning); got %d: %v", len(deltas), deltas)
	}
}

// TestClassifyRegime_WorkloadDiffers covers the #198 workload-population
// check: identical hardware and DB tuning, but the dataset shape differs
// materially.
func TestClassifyRegime_WorkloadDiffers(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	cases := []struct {
		name     string
		hist     HistoryRecord
		wantSame bool
	}{
		// Row-count axis. 3.0× tolerance.
		// "just inside 3×" uses 34M not the round 100M/3 = 33,333,333: that
		// integer truncates down so the float ratio comes out as
		// 3.0000000003, tripping the inclusive (≤) boundary. 34M gives ratio
		// 2.94 — comfortably within tolerance and verifies the boundary
		// without depending on float precision.
		{"rows within 1.5×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 67_000_000, AvgRowBytes: 1000}, true},
		{"rows just inside 3×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 34_000_000, AvgRowBytes: 1000}, true},
		{"rows beyond 3× (5×)", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 20_000_000, AvgRowBytes: 1000}, false},
		// Avg-row-bytes axis. 2.0× tolerance.
		{"avg_row_bytes within 1.5×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 600}, true},
		{"avg_row_bytes at 2× exactly", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 500}, true},
		{"avg_row_bytes beyond 2× (3×)", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 333}, false},
		// Missing values fall through (don't fire the rule).
		{"hist rows missing", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 0, AvgRowBytes: 1000}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, deltas := ClassifyRegime(tc.hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("got label=%q, isSame=%v, wantSame=%v, deltas=%v",
					label, isSame, tc.wantSame, deltas)
			}
			if !tc.wantSame && label != RegimeDifferentWorkload {
				t.Errorf("workload diff should yield RegimeDifferentWorkload, got %q", label)
			}
		})
	}
}

// TestClassifyRegime_WorkloadWinsOverHwAndTuning verifies that when
// workload differs alongside hw and/or tuning, the label collapses to
// DifferentWorkload — filterByRegime drops all three semantics so the
// combo distinction would only matter for diagnostics, and the deltas
// slice still carries each axis's delta.
func TestClassifyRegime_WorkloadWinsOverHwAndTuning(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{
		CPUCores:              4,             // hw differs
		MemoryGB:              48,
		TotalRows:             10_000_000,    // workload differs (10×)
		AvgRowBytes:           1000,
		TargetSharedBuffersMB: 1024,          // tuning differs (vs 8192)
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192}
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeDifferentWorkload {
		t.Errorf("got %q, want %q (workload should win)", label, RegimeDifferentWorkload)
	}
	if len(deltas) < 3 {
		t.Errorf("expected ≥3 deltas (hw + tuning + workload); got %d: %v", len(deltas), deltas)
	}
}

// TestClassifyRegime_WorkloadDeltaReadable spot-checks the human-readable
// delta string for the workload axis.
func TestClassifyRegime_WorkloadDeltaReadable(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 19_000_000, AvgRowBytes: 1000}
	_, deltas := ClassifyRegime(hist, current, DBTuning{})
	joined := strings.Join(deltas, "|")
	if !strings.Contains(joined, "total_rows: 19000000→100000000") {
		t.Errorf("workload delta missing or malformed: %q", joined)
	}
}

// TestClassifyRegime_DeltasAreReadable spot-checks the human-readable
// delta strings used in diagnostic logging.
func TestClassifyRegime_DeltasAreReadable(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:         4,
		MemoryGB:         16,
		TargetSyncCommit: "off",
	}
	tuning := DBTuning{TargetSyncCommit: "on"}
	_, deltas := ClassifyRegime(hist, current, tuning)
	joined := strings.Join(deltas, "|")
	if !strings.Contains(joined, "history=4c/16GB vs current=16c/48GB") {
		t.Errorf("hw delta missing or malformed: %q", joined)
	}
	if !strings.Contains(joined, "pg_sync_commit: off→on") {
		t.Errorf("tuning delta missing or malformed: %q", joined)
	}
}
