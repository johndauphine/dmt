package tuning

import (
	"strings"
	"testing"
)

func pinnedAdviceRows(waw int, throughput float64, n int) []HistoryRecord {
	rows := make([]HistoryRecord, n)
	for i := range rows {
		rows[i] = HistoryRecord{WriteAheadWriters: waw, FinalThroughput: throughput, ParallelReaders: 2, ReadAheadBuffers: 4}
	}
	return rows
}

// TestAppendPinnedWAWAdvice_FiresOnMeasuredGain is the #461 contract:
// both sides measured (pinned bin and best bin each have >= minRunsPerBin
// runs), gain above the floor, advice states the means and the delta.
func TestAppendPinnedWAWAdvice_FiresOnMeasuredGain(t *testing.T) {
	rows := append(pinnedAdviceRows(4, 500_000, 5), pinnedAdviceRows(8, 800_000, 5)...)
	var out Output
	appendPinnedWAWAdvice(&out, 4, rows)
	if len(out.PinnedAdvice) != 1 {
		t.Fatalf("advice = %v, want one finding", out.PinnedAdvice)
	}
	a := out.PinnedAdvice[0]
	for _, want := range []string{"pinned at 4", "WAW=8", "remove the override"} {
		if !strings.Contains(a, want) {
			t.Errorf("advice missing %q: %s", want, a)
		}
	}
}

func TestAppendPinnedWAWAdvice_SilentWithoutEvidence(t *testing.T) {
	cases := []struct {
		name string
		rows []HistoryRecord
	}{
		{"no history", nil},
		{"pinned bin too thin", append(pinnedAdviceRows(4, 500_000, 2), pinnedAdviceRows(8, 800_000, 5)...)},
		{"pin already best", append(pinnedAdviceRows(4, 900_000, 5), pinnedAdviceRows(8, 500_000, 5)...)},
		{"gain below floor", append(pinnedAdviceRows(4, 760_000, 5), pinnedAdviceRows(8, 800_000, 5)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out Output
			appendPinnedWAWAdvice(&out, 4, tc.rows)
			if len(out.PinnedAdvice) != 0 {
				t.Errorf("advice = %v, want none (silence means no measured evidence)", out.PinnedAdvice)
			}
		})
	}
}

// TestTune_PinnedAdviceThreadsThrough verifies the Input→Output plumbing.
func TestTune_PinnedAdviceThreadsThrough(t *testing.T) {
	// Twelve usable rows place Tune past the cold-start exploration window;
	// six per bin still exceeds the measured-advice evidence floor.
	rows := append(pinnedAdviceRows(4, 500_000, 6), pinnedAdviceRows(8, 800_000, 6)...)
	for i := range rows {
		rows[i].SourceDBType, rows[i].TargetDBType = "mssql", "postgres"
		rows[i].CPUCores, rows[i].MemoryGB = 16, 48
		rows[i].ChunkSize, rows[i].AvgRowBytes = 50_000, 500
		rows[i].ParallelReaders, rows[i].ReadAheadBuffers = 2, 4
	}
	pinned := 4
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform: "linux", AvgRowBytes: 500,
		PinnedWriteAheadWriters: &pinned,
	}
	out := Tune(in, DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}, &stubHistory{rows: rows}, DBTuning{})
	if len(out.PinnedAdvice) != 1 {
		t.Fatalf("PinnedAdvice = %v, want one finding through Tune", out.PinnedAdvice)
	}
}

// TestTune_PinnedAdviceUsesPinnedReaderSettings: when PR/RAB are also
// pinned, the advice cohort filters by the PINNED reader settings (what
// will actually run), not the tuner's output (codex review on #461).
func TestTune_PinnedAdviceUsesPinnedReaderSettings(t *testing.T) {
	rows := append(pinnedAdviceRows(4, 500_000, 6), pinnedAdviceRows(8, 800_000, 6)...)
	for i := range rows {
		rows[i].SourceDBType, rows[i].TargetDBType = "mssql", "postgres"
		rows[i].CPUCores, rows[i].MemoryGB = 16, 48
		rows[i].ChunkSize, rows[i].AvgRowBytes = 50_000, 500
		// History exists ONLY at the pinned reader settings (4/8), not
		// at the tuner's baseline (2/4).
		rows[i].ParallelReaders, rows[i].ReadAheadBuffers = 4, 8
	}
	pinnedWAW, pinnedPR, pinnedRAB := 4, 4, 8
	in := Input{
		CPUCores: 16, MemoryGB: 48,
		SourceDBType: "mssql", TargetDBType: "postgres",
		Platform: "linux", AvgRowBytes: 500,
		PinnedWriteAheadWriters: &pinnedWAW,
		PinnedParallelReaders:   &pinnedPR,
		PinnedReadAheadBuffers:  &pinnedRAB,
	}
	out := Tune(in, DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}, &stubHistory{rows: rows}, DBTuning{})
	if len(out.PinnedAdvice) != 1 {
		t.Fatalf("PinnedAdvice = %v, want one finding (cohort must use pinned PR/RAB)", out.PinnedAdvice)
	}
}
