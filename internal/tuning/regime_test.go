package tuning

import (
	"math"
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

// TestClassifyRegime_WorkloadBands locks in the #214 bucket-based
// workload gate. Two runs are workload-comparable when they share a
// total-bytes band; ratio comparisons no longer apply. The current
// dataset is 100M × 1000 = 100 GB → Large band.
func TestClassifyRegime_WorkloadBands(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	cases := []struct {
		name     string
		hist     HistoryRecord
		wantSame bool
	}{
		// Same Large band, different points within the band.
		{"another 100 GB run", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}, true},
		{"different rows, same band", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 500_000_000, AvgRowBytes: 1000}, true},
		// Different band: Medium (50 GB).
		{"medium-band history", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 50_000_000, AvgRowBytes: 1000}, false},
		// Different band: Huge (>1 TB).
		{"huge-band history", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 2_000_000_000, AvgRowBytes: 1000}, false},
		// Missing values fall through to bandUnknown — gate skipped.
		{"hist rows missing", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 0, AvgRowBytes: 1000}, true},
		{"hist avg_row_bytes missing", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 0}, true},
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

// TestClassifyRegime_BoundaryNoLongerDiscontinuous is the #214
// motivating case: under the old 3× ratio gate, 100M and 305M rows
// flagged as different but 100M and 295M flagged as same — a sharp
// discontinuity at an arbitrary multiplier. With bucket gating both
// pairs land in the same band (each ~100–305 GB at 1000 B/row →
// Large), so neither flags as different_workload. Performance physics
// is smooth across the band.
func TestClassifyRegime_BoundaryNoLongerDiscontinuous(t *testing.T) {
	// All three runs are in the Large band (100 GB – 1 TB).
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist295 := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 295_000_000, AvgRowBytes: 1000}
	hist305 := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 305_000_000, AvgRowBytes: 1000}

	label295, _ := ClassifyRegime(hist295, current, DBTuning{})
	label305, _ := ClassifyRegime(hist305, current, DBTuning{})

	if label295 != RegimeSame {
		t.Errorf("295M and 100M should share Large band, got %q", label295)
	}
	if label305 != RegimeSame {
		t.Errorf("305M and 100M should share Large band, got %q", label305)
	}
}

// TestClassifyRegime_AsymmetryAcrossBands is the second #214 motivating
// case. Under the old ratio gate, 1M→3M was the same ratio (3×) as
// 30M→90M, and both were "same regime." The physics is entirely
// different: 1M→3M is Tiny↔Small (network-bound either way), while
// 30M→90M crosses Medium→Large (page-cache regime change).
func TestClassifyRegime_AsymmetryAcrossBands(t *testing.T) {
	t.Run("1M to 3M stays in same band", func(t *testing.T) {
		// 1M × 1000 = 1 GB → Small; 3M × 1000 = 3 GB → Small. Same band.
		current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 3_000_000, AvgRowBytes: 1000}
		hist := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 1_000_000, AvgRowBytes: 1000}
		label, deltas := ClassifyRegime(hist, current, DBTuning{})
		if label != RegimeSame {
			t.Errorf("1M and 3M share Small band, expected same; got %q deltas=%v", label, deltas)
		}
	})
	t.Run("30M to 90M crosses Medium→Large", func(t *testing.T) {
		// 30M × 1000 = 30 GB → Medium; 90M × 1000 = 90 GB → Medium.
		// To cross the cliff we need a hist on the other side of the
		// 100 GB boundary. The motivating asymmetry is "same ratio,
		// different physics" — use 120M as the larger so we land in
		// Large while 30M sits in Medium.
		current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 120_000_000, AvgRowBytes: 1000}
		hist := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 30_000_000, AvgRowBytes: 1000}
		label, _ := ClassifyRegime(hist, current, DBTuning{})
		if label != RegimeDifferentWorkload {
			t.Errorf("30M (Medium) vs 120M (Large) should be different workload; got %q", label)
		}
	})
}

// TestClassifyRegime_UncappedAvgRowBytesUsedForBand pins the Copilot
// fix on PR #288: AvgRowBytes is the legacy 2KB-capped feature, but using
// that capped value for band
// classification mis-buckets wide-row workloads. A 2M-row workload at
// 8KB/row (16 GB physical → Medium band) would otherwise look like
// 2M × 2KB (4 GB → Small) and never match a 16 GB Medium-band history.
//
// With UncappedAvgRowBytes populated, the classifier uses it; both
// runs land in Medium and the regime gate accepts.
func TestClassifyRegime_UncappedAvgRowBytesUsedForBand(t *testing.T) {
	// Both current and history are 2M × 8KB = 16 GB → Medium band.
	current := Input{
		CPUCores: 16, MemoryGB: 48,
		TotalRows:           2_000_000,
		AvgRowBytes:         2000, // capped value
		UncappedAvgRowBytes: 8000, // real value
	}
	hist := HistoryRecord{
		CPUCores: 16, MemoryGB: 48,
		TotalRows:           2_000_000,
		AvgRowBytes:         2000,
		UncappedAvgRowBytes: 8000,
	}
	label, deltas := ClassifyRegime(hist, current, DBTuning{})
	if label != RegimeSame {
		t.Errorf("wide-row workloads at same uncapped size should share band; got %q deltas=%v",
			label, deltas)
	}

	// Sanity check: dropping UncappedAvgRowBytes on the history side
	// falls back to AvgRowBytes (capped). 2M × 2KB = 4 GB lands in
	// Small, while current 2M × 8KB = 16 GB lands in Medium → different
	// workload. That's the under-band bias documented as the
	// conservative failure mode for un-backfilled history rows.
	histLegacy := hist
	histLegacy.UncappedAvgRowBytes = 0
	label2, _ := ClassifyRegime(histLegacy, current, DBTuning{})
	if label2 != RegimeDifferentWorkload {
		t.Errorf("history without uncapped value should fall back to capped (Small) and mismatch current (Medium); got %q",
			label2)
	}
}

func TestClassifyRegime_NewWidthsDoNotChangeLegacyComparison(t *testing.T) {
	history := HistoryRecord{
		CPUCores:            16,
		MemoryGB:            48,
		TotalRows:           2_000_000,
		AvgRowBytes:         2_000,
		UncappedAvgRowBytes: 8_000,
	}
	current := Input{
		CPUCores:            16,
		MemoryGB:            48,
		TotalRows:           2_000_000,
		AvgRowBytes:         2_000,
		UncappedAvgRowBytes: 8_000,
	}

	wantLabel, wantDeltas := ClassifyRegime(history, current, DBTuning{})
	current.RepresentativeRowBytes = 200
	current.SafetyRowBytes = 64 * 1024
	current.SafetyRowBytesKnown = true
	gotLabel, gotDeltas := ClassifyRegime(history, current, DBTuning{})

	if gotLabel != wantLabel || strings.Join(gotDeltas, "|") != strings.Join(wantDeltas, "|") {
		t.Fatalf("new widths changed legacy regime comparison: before=(%q,%v) after=(%q,%v)",
			wantLabel, wantDeltas, gotLabel, gotDeltas)
	}
}

func TestTotalBytes_OverflowSaturates(t *testing.T) {
	if got := totalBytes(math.MaxInt64, 2); got != math.MaxInt64 {
		t.Fatalf("overflowing totalBytes = %d, want MaxInt64 saturation", got)
	}
	if got := workloadBand(math.MaxInt64, 2); got != bandHuge {
		t.Fatalf("overflowing workload band = %q, want %q", got, bandHuge)
	}
}

// TestClassifyRegime_SkewSecondaryGate verifies the #214 secondary
// axis fires only when the primary band agrees. Two runs at the same
// band but with very different skew distributions are classified
// different_workload; runs missing largest-table data fall through.
func TestClassifyRegime_SkewSecondaryGate(t *testing.T) {
	// 100 GB → Large band. Vary the largest-table share to flip the
	// skew tier.
	totalBytes := int64(100_000_000) * 1000 // 100 GB
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000,
		LargestTableBytes: int64(float64(totalBytes) * 0.10)} // EvenlyDistributed
	cases := []struct {
		name     string
		histLT   int64
		wantSame bool
	}{
		{"both evenly distributed", int64(float64(totalBytes) * 0.15), true},
		{"history is dominant", int64(float64(totalBytes) * 0.85), false},
		{"history is skewed", int64(float64(totalBytes) * 0.50), false},
		// 0 → skewUnknown — gate skipped, falls through to count or same.
		{"history missing largest_table_bytes", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hist := HistoryRecord{
				CPUCores: 16, MemoryGB: 48,
				TotalRows: 100_000_000, AvgRowBytes: 1000,
				LargestTableBytes: tc.histLT,
			}
			label, deltas := ClassifyRegime(hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("got label=%q, isSame=%v, wantSame=%v, deltas=%v",
					label, isSame, tc.wantSame, deltas)
			}
		})
	}
}

// TestClassifyRegime_CountTertiaryGate verifies the #214 tertiary
// axis: with primary and secondary aligned, a Few-vs-Massive table-
// count difference still flips the row to different_workload. Runs
// missing table count fall through (the gate is skipped).
func TestClassifyRegime_CountTertiaryGate(t *testing.T) {
	// Identical band + skew; only TotalTables varies.
	totalBytes := int64(100_000_000) * 1000
	largest := int64(float64(totalBytes) * 0.50) // Skewed on both sides
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000,
		LargestTableBytes: largest, TotalTables: 5} // Few
	cases := []struct {
		name     string
		histCnt  int
		wantSame bool
	}{
		{"both few", 7, true},
		{"history is massive", 250, false},
		{"history is many", 50, false},
		// 0 → countUnknown — gate skipped.
		{"history missing TotalTables", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hist := HistoryRecord{
				CPUCores: 16, MemoryGB: 48,
				TotalRows: 100_000_000, AvgRowBytes: 1000,
				LargestTableBytes: largest,
				TotalTables:       tc.histCnt,
			}
			label, deltas := ClassifyRegime(hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("got label=%q, isSame=%v, wantSame=%v, deltas=%v",
					label, isSame, tc.wantSame, deltas)
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
	// 100M × 1000 = 100 GB → Large; 10M × 1000 = 10 GB → boundary
	// (exactly bandSmallMaxBytes). The "<" comparator in workloadBand
	// puts 10 GB exactly into Medium (not Small). Pick a hist that's
	// unambiguously a different band: 1M × 1000 = 1 GB → Small.
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{
		CPUCores:              4, // hw differs
		MemoryGB:              48,
		TotalRows:             1_000_000, // workload differs (Small vs Large band)
		AvgRowBytes:           1000,
		TargetSharedBuffersMB: 1024, // tuning differs (vs 8192)
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
// delta string for the workload axis under bucket gating (#214).
func TestClassifyRegime_WorkloadDeltaReadable(t *testing.T) {
	// 100M × 1000 = 100 GB → Large; 1M × 1000 = 1 GB → Small.
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 1_000_000, AvgRowBytes: 1000}
	_, deltas := ClassifyRegime(hist, current, DBTuning{})
	joined := strings.Join(deltas, "|")
	if !strings.Contains(joined, "workload_band: small→large") {
		t.Errorf("workload band delta missing or malformed: %q", joined)
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

// --- Issue #215: exact-identity workload classifier ---

// mkIdentity helper returns an Input with all 8 identity fields set.
// Saves verbosity in the test fixtures below.
func mkIdentity() Input {
	return Input{
		SourceHost:     "src.example.com",
		SourcePort:     1433,
		SourceDatabase: "MyDB",
		SourceSchema:   "dbo",
		TargetHost:     "tgt.example.com",
		TargetPort:     5432,
		TargetDatabase: "mydb",
		TargetSchema:   "public",
		CPUCores:       16, MemoryGB: 48,
	}
}

// TestHasExactIdentity_FullTuple — happy path. All required fields set.
func TestHasExactIdentity_FullTuple(t *testing.T) {
	if !hasExactIdentity(mkIdentity()) {
		t.Error("full identity tuple should be detected as complete")
	}
}

// TestHasExactIdentity_MissingHostFails — missing required field.
// Schemas may legitimately be empty (MySQL has no schema concept), so
// empty schema does NOT disqualify; empty host does.
func TestHasExactIdentity_MissingHostFails(t *testing.T) {
	in := mkIdentity()
	in.SourceHost = ""
	if hasExactIdentity(in) {
		t.Error("missing SourceHost should fail hasExactIdentity")
	}
}

// TestHasExactIdentity_MissingPortFails — port is required because
// SQLite NULL != 0 in equality, so a missing port would silently
// prevent any matches.
func TestHasExactIdentity_MissingPortFails(t *testing.T) {
	in := mkIdentity()
	in.SourcePort = 0
	if hasExactIdentity(in) {
		t.Error("missing SourcePort should fail hasExactIdentity")
	}
}

func TestHasExactIdentity_PortlessEndpointsUseDatabasePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
		want   bool
	}{
		{
			name: "portless source permits empty host and zero port",
			mutate: func(in *Input) {
				in.SourcePortless = true
				in.SourceHost = ""
				in.SourcePort = 0
				in.SourceDatabase = "/tmp/source.db"
			},
			want: true,
		},
		{
			name: "both endpoints portless",
			mutate: func(in *Input) {
				in.SourcePortless, in.TargetPortless = true, true
				in.SourceHost, in.TargetHost = "", ""
				in.SourcePort, in.TargetPort = 0, 0
				in.SourceDatabase, in.TargetDatabase = "/tmp/source.db", "/tmp/target.db"
			},
			want: true,
		},
		{
			name: "portless source still requires path",
			mutate: func(in *Input) {
				in.SourcePortless = true
				in.SourceHost, in.SourceDatabase = "", ""
				in.SourcePort = 0
			},
			want: false,
		},
		{
			name: "portless target still requires path",
			mutate: func(in *Input) {
				in.TargetPortless = true
				in.TargetHost, in.TargetDatabase = "", ""
				in.TargetPort = 0
			},
			want: false,
		},
		{
			name: "portful peer still requires host",
			mutate: func(in *Input) {
				in.SourcePortless = true
				in.SourceHost, in.SourcePort = "", 0
				in.SourceDatabase = "/tmp/source.db"
				in.TargetHost = ""
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := mkIdentity()
			tc.mutate(&in)
			if got := hasExactIdentity(in); got != tc.want {
				t.Fatalf("hasExactIdentity() = %v, want %v for %+v", got, tc.want, in)
			}
		})
	}
}

// TestHasExactIdentity_EmptySchemaAllowed — drivers without schema
// concept (MySQL) leave the schema field empty. That's legitimate;
// the check should pass.
func TestHasExactIdentity_EmptySchemaAllowed(t *testing.T) {
	in := mkIdentity()
	in.SourceSchema = ""
	in.TargetSchema = ""
	if !hasExactIdentity(in) {
		t.Error("empty schemas should not disqualify identity (drivers without schema concept)")
	}
}

// TestFilterByExactIdentity_AllMatchPass — every row matches the input.
func TestFilterByExactIdentity_AllMatchPass(t *testing.T) {
	in := mkIdentity()
	rows := []HistoryRecord{
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
			FinalThroughput: 1_000_000},
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
			FinalThroughput: 1_100_000},
	}
	got := filterByExactIdentity(rows, in)
	if len(got) != 2 {
		t.Errorf("expected 2 matching rows, got %d", len(got))
	}
}

func TestFilterByExactIdentity_PortlessIgnoresHostAndPort(t *testing.T) {
	in := mkIdentity()
	in.SourcePortless = true
	in.SourceHost = ""
	in.SourcePort = 0
	in.SourceDatabase = "/tmp/source.db"

	rows := []HistoryRecord{
		{
			SourceHost: "legacy-meaningless-host", SourcePort: 9999,
			SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort,
			TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
		},
		{
			SourceDatabase: "/tmp/other.db", SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort,
			TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
		},
	}
	got := filterByExactIdentity(rows, in)
	if len(got) != 1 || got[0].SourceDatabase != in.SourceDatabase {
		t.Fatalf("portless identity rows = %+v, want only matching path", got)
	}
}

// TestFilterByExactIdentity_OneFieldDifferentRejected — exactly one
// field differs on each test row; all must be rejected.
func TestFilterByExactIdentity_OneFieldDifferentRejected(t *testing.T) {
	in := mkIdentity()
	cases := []struct {
		name string
		row  HistoryRecord
	}{
		{"source_host_diff", HistoryRecord{SourceHost: "OTHER", SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_port_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: 9999, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_db_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: "OTHER", SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_schema_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: "OTHER", TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_host_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: "OTHER", TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_port_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: 9999, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_db_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: "OTHER", TargetSchema: in.TargetSchema}},
		{"target_schema_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: "OTHER"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterByExactIdentity([]HistoryRecord{tc.row}, in)
			if len(got) != 0 {
				t.Errorf("row with %s should be rejected; got %d matches", tc.name, len(got))
			}
		})
	}
}

// TestFilterByExactIdentity_PreIdentityRowsRejected — rows persisted
// before #215 have empty identity fields. Equality comparison rejects
// them, leaving them out of the Tier 1 cohort. That's correct: their
// identity is unrecoverable, so they shouldn't claim to match anything.
func TestFilterByExactIdentity_PreIdentityRowsRejected(t *testing.T) {
	in := mkIdentity()
	rows := []HistoryRecord{
		{}, // all-zero — represents a pre-#215 row
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase}, // partial — also rejected
	}
	got := filterByExactIdentity(rows, in)
	if len(got) != 0 {
		t.Errorf("pre-#215 rows (empty identity) should not match; got %d", len(got))
	}
}

// TestTune_ExactIdentityCohort_FiresRegression — integration test for
// the Tier 1 dispatch. When the history holds enough rows for the regression (#452 floor)
// rows from the exact same identity, the tuner should fire regression
// on those alone (not the broader regime-comparable set).
func TestTune_ExactIdentityCohort_FiresRegression(t *testing.T) {
	current := mkIdentity()
	current.SourceDBType = "mssql"
	current.TargetDBType = "postgres"
	current.AvgRowBytes = 500

	// 30 matching-identity rows + 30 different-identity rows. Tier 1
	// should ignore the different-identity rows entirely.
	rows := make([]HistoryRecord, 0, 60)
	mkRow := func(host string, throughput float64, waw int) HistoryRecord {
		return HistoryRecord{
			SourceHost: host, SourcePort: current.SourcePort, SourceDatabase: current.SourceDatabase, SourceSchema: current.SourceSchema,
			TargetHost: current.TargetHost, TargetPort: current.TargetPort, TargetDatabase: current.TargetDatabase, TargetSchema: current.TargetSchema,
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: waw, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4, // match baseline()'s defaults so argmax covers a cell (#220/#222)
			FinalThroughput: throughput,
		}
	}
	// Matching identity: 30 rows, throughput peaks at WAW=4.
	for i := 0; i < 30; i++ {
		waw := (i % 6) + 1
		dev := waw - 4
		rows = append(rows, mkRow(current.SourceHost, 1_000_000.0-50_000.0*float64(dev*dev), waw))
	}
	// Different host: 30 rows with very different shape (throughput peaks
	// at WAW=1). If these leaked into the Tier 1 cohort, the regression
	// would mispredict.
	for i := 0; i < 30; i++ {
		waw := (i % 6) + 1
		rows = append(rows, mkRow("other-host.example.com", 100_000.0+200_000.0/float64(waw), waw))
	}

	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Tier 1 should have fired regression; reasoning should reflect that.
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 1 should fire regression on exact-identity cohort; got: %q", out.Reasoning)
	}
	// The "over N filtered rows" count should be 30 (the matching
	// identity cohort), not 60 (all rows). Tier 1 isolation matters.
	if !strings.Contains(out.Reasoning, "over 30 filtered rows") {
		t.Errorf("Tier 1 should train on 30 matching rows, not the full 60; got: %q", out.Reasoning)
	}
}

func TestTune_SQLitePortlessIdentityReachesExactIdentityTier(t *testing.T) {
	current := Input{
		SourceDBType: "sqlite", TargetDBType: "sqlite",
		SourcePortless: true, TargetPortless: true,
		SourceDatabase: "/tmp/source.db", TargetDatabase: "/tmp/target.db",
		CPUCores: 16, MemoryGB: 48, AvgRowBytes: 500,
	}
	rows := make([]HistoryRecord, 0, 30)
	for i := 0; i < 30; i++ {
		waw := (i % 6) + 1
		dev := waw - 3
		rows = append(rows, HistoryRecord{
			SourceDBType: "sqlite", TargetDBType: "sqlite",
			SourceDatabase: current.SourceDatabase,
			TargetDatabase: current.TargetDatabase,
			CPUCores:       16, MemoryGB: 48,
			WriteAheadWriters: waw, ChunkSize: 20_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4,
			FinalThroughput: 500_000 - 20_000*float64(dev*dev),
		})
	}

	out := Tune(current, DriverProfile{Name: "sqlite", BaselineWAW: 1}, &stubHistory{rows: rows}, DBTuning{})
	if !strings.Contains(out.Reasoning, "regression-selected") ||
		!strings.Contains(out.Reasoning, "over 30 filtered rows") {
		t.Fatalf("SQLite portless history did not reach exact-identity regression tier: %q", out.Reasoning)
	}
}

// TestTune_ExactIdentityCohortTooThin_FallsThroughToTier2 — when the
// exact-identity cohort has too few rows for the regression floor (#452), the
// dispatch falls through to the Tier 2 regime filter. The rest of the
// rows (regime-comparable but not identity-matching) take over.
func TestTune_ExactIdentityCohortTooThin_FallsThroughToTier2(t *testing.T) {
	current := mkIdentity()
	current.SourceDBType = "mssql"
	current.TargetDBType = "postgres"
	current.AvgRowBytes = 500
	current.TotalRows = 100_000_000

	mkRow := func(host string, totalRows int64, throughput float64, waw int) HistoryRecord {
		return HistoryRecord{
			SourceHost: host, SourcePort: current.SourcePort, SourceDatabase: current.SourceDatabase, SourceSchema: current.SourceSchema,
			TargetHost: current.TargetHost, TargetPort: current.TargetPort, TargetDatabase: current.TargetDatabase, TargetSchema: current.TargetSchema,
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			TotalRows: totalRows, AvgRowBytes: 500,
			WriteAheadWriters: waw, ChunkSize: 50_000,
			ParallelReaders: 2, ReadAheadBuffers: 4, // match baseline()'s defaults (#220/#222)
			FinalThroughput: throughput,
		}
	}

	rows := make([]HistoryRecord, 0, 35)
	// 5 matching-identity rows — below regression floor.
	for i := 0; i < 5; i++ {
		rows = append(rows, mkRow(current.SourceHost, current.TotalRows, 1_000_000, (i%6)+1))
	}
	// 30 different-identity but regime-comparable rows (same workload,
	// different source host). Tier 2 will route through these.
	for i := 0; i < 30; i++ {
		rows = append(rows, mkRow("other-host.example.com", current.TotalRows, 800_000, (i%6)+1))
	}

	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Should NOT be from Tier 1 (only 5 matches); should fall through
	// to Tier 2 (regime-filtered, larger cohort). The "filtered rows"
	// count in the reasoning should be > 5 (some subset of the 35).
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 2 should fire regression on the 35-row cohort; got: %q", out.Reasoning)
	}
}

// TestTune_OutlierReasoningEmittedOnce_Tier1 guards the P3 codex-review
// finding on PR #225: when Tier 1 (identity) wins, the regime cohort
// is filtered up-front but its drops must NOT be reported — only the
// identity cohort's drops (the ones the selector actually consumed)
// belong in Output.Reasoning. Without the fix, both passes appended
// summaries, double-counting drops to the operator.
func TestTune_OutlierReasoningEmittedOnce_Tier1(t *testing.T) {
	current := mkIdentity()
	current.SourceDBType = "mssql"
	current.TargetDBType = "postgres"
	current.AvgRowBytes = 500

	mkRow := func(host string, throughput float64, waw int) HistoryRecord {
		return HistoryRecord{
			SourceHost: host, SourcePort: current.SourcePort, SourceDatabase: current.SourceDatabase, SourceSchema: current.SourceSchema,
			TargetHost: current.TargetHost, TargetPort: current.TargetPort, TargetDatabase: current.TargetDatabase, TargetSchema: current.TargetSchema,
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: waw, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4, // baseline defaults (#220/#222)
			FinalThroughput: throughput,
		}
	}

	// 60 matching-identity rows — clusters tightly around 1.0M rows/s
	// EXCEPT one host-throttled outlier at 200K rows/s. The residual
	// filter should drop the outlier; the regression-selected line
	// should fire.
	rows := make([]HistoryRecord, 0, 120)
	for i := 0; i < 59; i++ {
		waw := (i % 6) + 1
		rows = append(rows, mkRow(current.SourceHost, 1_000_000+float64(i*1000), waw))
	}
	rows = append(rows, mkRow(current.SourceHost, 200_000, 1)) // outlier in identity cohort
	// 60 different-host rows — regime-comparable but excluded by identity.
	for i := 0; i < 60; i++ {
		waw := (i % 6) + 1
		rows = append(rows, mkRow("other-host.example.com", 900_000+float64(i*1000), waw))
	}

	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Fatalf("expected Tier 1 regression to fire; got: %q", out.Reasoning)
	}
	// The outlier summary must appear AT MOST once. Pre-fix it appeared
	// twice (once for the regime cohort, once for the identity cohort).
	occurrences := strings.Count(out.Reasoning, "outlier filter dropped")
	if occurrences > 1 {
		t.Errorf("outlier reasoning emitted %d times, want at most 1 (Tier 1 should only report its own cohort's drops): %q",
			occurrences, out.Reasoning)
	}
}

// TestTune_NoIdentityInInput_FallsThroughToTier2 — when the caller
// doesn't populate the identity fields (e.g., test fixtures or
// pre-#215 callers that haven't been updated), Tier 1 is skipped
// silently and the Tier 2 path takes over. Backward compatibility.
func TestTune_NoIdentityInInput_FallsThroughToTier2(t *testing.T) {
	current := Input{
		SourceDBType: "mssql", TargetDBType: "postgres",
		CPUCores: 16, MemoryGB: 48,
		AvgRowBytes: 500, TotalRows: 100_000_000,
		// No identity fields set.
	}
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		waw := (i % 6) + 1
		rows[i] = HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			TotalRows: current.TotalRows, AvgRowBytes: 500,
			WriteAheadWriters: waw, ChunkSize: 50_000,
			ParallelReaders: 2, ReadAheadBuffers: 4, // baseline defaults (#220/#222)
			FinalThroughput: 800_000,
		}
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Tier 2 fires with the full 30-row cohort.
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 2 should fire when identity is missing; got: %q", out.Reasoning)
	}
}
