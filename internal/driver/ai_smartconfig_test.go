package driver

import (
	"testing"

	"github.com/johndauphine/dmt/internal/tuning"
)

// mockHistoryProvider satisfies the trimmed TuningHistoryProvider interface
// (PR #175 dropped the per-WAW / per-chunk_size aggregate methods; the new
// tuner reads raw rows and aggregates in-package). Just enough to verify
// SaveTuningWithActualParams persists the post-override params.
type mockHistoryProvider struct {
	saved   *AITuningRecord
	history []AITuningRecord
}

func (m *mockHistoryProvider) GetAIAdjustments(int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

func (m *mockHistoryProvider) GetAITuningHistory(_ int, _, _ string) ([]AITuningRecord, error) {
	return m.history, nil
}

func (m *mockHistoryProvider) SaveAITuning(record AITuningRecord) error {
	m.saved = &record
	return nil
}

func (m *mockHistoryProvider) UpdateAITuningResult(float64, float64, int) error {
	return nil
}

// TestSaveTuningWithActualParams verifies the persisted record uses
// post-override params (issue #160) and the recomputed memory estimate.
func TestSaveTuningWithActualParams(t *testing.T) {
	mock := &mockHistoryProvider{}

	analyzer := &SmartConfigAnalyzer{
		dbType:          "mssql",
		targetDBType:    "postgres",
		historyProvider: mock,
		suggestions: &SmartConfigSuggestions{
			Workers:                 3,
			ChunkSizeRecommendation: 50000,
			ReadAheadBuffers:        2,
			WriteAheadWriters:       2,
			ParallelReaders:         4,
			MaxPartitions:           3,
			// Pre-set to a stale value (the smartconfig-time estimate, which
			// is wrong once user overrides land below). Issue #160: without
			// the recompute this stale value would be saved alongside the
			// post-override chunk_size=14000.
			EstimatedMemMB: 1135,
		},
	}
	analyzer.pendingSave = &pendingTuningSave{
		input:     AutoTuneInput{CPUCores: 15, MemoryGB: 24, AvgRowBytes: 500},
		reasoning: "test reasoning",
	}

	analyzer.SaveTuningWithActualParams(ActualParams{
		Workers:           12,
		ChunkSize:         14000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 3,
		ParallelReaders:   6,
		MaxPartitions:     8,
	})

	if analyzer.pendingSave != nil {
		t.Error("pendingSave should be nil after SaveTuningWithActualParams")
	}
	if mock.saved == nil {
		t.Fatal("expected record to be saved")
	}
	if mock.saved.Workers != 12 || mock.saved.ChunkSize != 14000 {
		t.Errorf("post-override params not persisted: workers=%d chunk=%d (want 12, 14000)",
			mock.saved.Workers, mock.saved.ChunkSize)
	}
	if mock.saved.SourceDBType != "mssql" || mock.saved.TargetDBType != "postgres" {
		t.Errorf("DB types wrong: source=%q target=%q", mock.saved.SourceDBType, mock.saved.TargetDBType)
	}
	// PR1: WasAIUsed always false now; AIReasoning carries the deterministic
	// tuner's reasoning string.
	if mock.saved.WasAIUsed {
		t.Error("WasAIUsed should be false post-PR #175")
	}
	if mock.saved.AIReasoning != "test reasoning" {
		t.Errorf("AIReasoning: got %q, want %q", mock.saved.AIReasoning, "test reasoning")
	}

	// Issue #160: persisted EstimatedMemoryMB must reflect post-override
	// params, not the smartconfig-time stale value (1135 above).
	want := tuning.EstimatedMemMB(12, 4, 3, 14000, 500)
	if mock.saved.EstimatedMemoryMB != want {
		t.Errorf("EstimatedMemoryMB = %d, want %d (recomputed from post-override params)",
			mock.saved.EstimatedMemoryMB, want)
	}
}

// TestSaveTuningWithActualParams_NoOverride pins #160's no-op acceptance:
// when ActualParams matches the pre-save values, the persisted memory
// estimate equals the same formula applied to those params (no surprise).
func TestSaveTuningWithActualParams_NoOverride(t *testing.T) {
	const (
		workers     = 4
		chunk       = 50000
		readAhead   = 4
		writeAhead  = 2
		parReaders  = 2
		maxParts    = 4
		avgRowBytes = 500
	)
	preSave := tuning.EstimatedMemMB(workers, readAhead, writeAhead, chunk, avgRowBytes)

	mock := &mockHistoryProvider{}
	analyzer := &SmartConfigAnalyzer{
		dbType:          "mssql",
		targetDBType:    "postgres",
		historyProvider: mock,
		suggestions: &SmartConfigSuggestions{
			Workers:                 workers,
			ChunkSizeRecommendation: chunk,
			ReadAheadBuffers:        readAhead,
			WriteAheadWriters:       writeAhead,
			ParallelReaders:         parReaders,
			MaxPartitions:           maxParts,
			EstimatedMemMB:          preSave,
		},
	}
	analyzer.pendingSave = &pendingTuningSave{
		input: AutoTuneInput{CPUCores: 8, MemoryGB: 16, AvgRowBytes: avgRowBytes},
	}

	analyzer.SaveTuningWithActualParams(ActualParams{
		Workers:           workers,
		ChunkSize:         chunk,
		ReadAheadBuffers:  readAhead,
		WriteAheadWriters: writeAhead,
		ParallelReaders:   parReaders,
		MaxPartitions:     maxParts,
	})

	if mock.saved == nil {
		t.Fatal("expected record to be saved")
	}
	if mock.saved.EstimatedMemoryMB != preSave {
		t.Errorf("EstimatedMemoryMB = %d, want %d (matching ActualParams should preserve the pre-save formula value)",
			mock.saved.EstimatedMemoryMB, preSave)
	}
}

// TestSaveTuningWithActualParams_NoPending: no-op when there's nothing
// to save (Analyze never ran or already saved).
func TestSaveTuningWithActualParams_NoPending(t *testing.T) {
	mock := &mockHistoryProvider{}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock}

	analyzer.SaveTuningWithActualParams(ActualParams{Workers: 12, ChunkSize: 14000})

	if mock.saved != nil {
		t.Error("should not save when no pending save exists")
	}
}

// TestChunkLimitFromProbe is the pure-function test for the #166
// HardChunkLimit calculation. Each row drives the exact path used in
// toTuningProfile without depending on driver registration (the
// driver-package internal tests can't blank-import the per-driver
// sub-packages — they'd form a cycle).
func TestChunkLimitFromProbe(t *testing.T) {
	tests := []struct {
		name              string
		driverStaticLimit int
		probe             TargetProbe
		avgRowBytes       int64
		want              int
	}{
		{
			name:              "MySQL default packet (4MB), 500-byte rows — packet wins",
			driverStaticLimit: 0,
			probe:             TargetProbe{MaxAllowedPacket: 4 * 1024 * 1024},
			avgRowBytes:       500,
			want:              6710, // (4MB * 0.8) / 500
		},
		{
			name:              "MySQL 64MB packet, 8KB JSON rows",
			driverStaticLimit: 0,
			probe:             TargetProbe{MaxAllowedPacket: 64 * 1024 * 1024},
			avgRowBytes:       8 * 1024,
			want:              6553, // (64MB * 0.8) / 8KB
		},
		{
			name:              "no probe — fall back to driver static",
			driverStaticLimit: 12345,
			probe:             TargetProbe{},
			avgRowBytes:       500,
			want:              12345,
		},
		{
			name:              "probe present but zero avgRowBytes — fall back to static",
			driverStaticLimit: 999,
			probe:             TargetProbe{MaxAllowedPacket: 4 * 1024 * 1024},
			avgRowBytes:       0,
			want:              999,
		},
		{
			name:              "no probe, no static — zero",
			driverStaticLimit: 0,
			probe:             TargetProbe{},
			avgRowBytes:       500,
			want:              0,
		},
		{
			// Codex review on #166: when row size exceeds 80% of the
			// packet budget, integer division rounds down to 0. Clamping
			// to 1 keeps the cap meaningful — one row per chunk is the
			// minimum unit and is still less than the packet limit.
			name:              "row larger than packet budget — clamped to 1",
			driverStaticLimit: 0,
			probe:             TargetProbe{MaxAllowedPacket: 4 * 1024 * 1024},
			avgRowBytes:       3_500_000, // > 80% of 4MB
			want:              1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkLimitFromProbe(tc.driverStaticLimit, tc.probe, tc.avgRowBytes)
			if got != tc.want {
				t.Errorf("chunkLimitFromProbe(%d, %+v, %d) = %d, want %d",
					tc.driverStaticLimit, tc.probe, tc.avgRowBytes, got, tc.want)
			}
		})
	}
}

// TestSetTargetProbe_FlowsToAnalyzer is a lightweight check that the
// setter actually persists the value. toTuningProfile's full integration
// path isn't exercised here — it depends on driver registration. The
// math is covered by TestChunkLimitFromProbe above.
func TestSetTargetProbe_FlowsToAnalyzer(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	probe := TargetProbe{MaxAllowedPacket: 4 * 1024 * 1024}
	analyzer.SetTargetProbe(probe)
	if analyzer.targetProbe.MaxAllowedPacket != probe.MaxAllowedPacket {
		t.Errorf("SetTargetProbe did not persist value; got %d, want %d",
			analyzer.targetProbe.MaxAllowedPacket, probe.MaxAllowedPacket)
	}
}

// TestCalculateAvgRowSize_StoresUncapped guards the #166 regression:
// the memory-budget cap (2000) must NOT propagate into the packet
// calculation, or wide-row workloads can still pick a chunk_size that
// exceeds @@max_allowed_packet and crash mid-transfer.
//
// Codex review on the first cut of this PR caught the bug — the packet
// math used s.suggestions.AvgRowSizeBytes (capped), so an 8KB-row
// workload with 4MB packet was capped near 1677 rows (13.4MB per
// chunk; would crash) instead of ~409 rows (3.3MB; fits).
func TestCalculateAvgRowSize_StoresUncapped(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		{Name: "wide_json_table", RowCount: 1_000_000, AvgRowSizeBytes: 8192}, // 8KB rows
	}
	capped := analyzer.calculateAvgRowSize(tables)
	if capped != 2000 {
		t.Errorf("capped value should be 2000 (the memory-budget cap); got %d", capped)
	}
	if analyzer.uncappedAvgRowBytes != 8192 {
		t.Errorf("uncapped value should be 8192 (the real row size); got %d", analyzer.uncappedAvgRowBytes)
	}
}

// TestCalculateAvgRowSize_NoCapWhenSmall verifies the uncapped slot
// still reflects reality when rows are under the 2000-byte cap (the
// cap is a no-op for those workloads).
func TestCalculateAvgRowSize_NoCapWhenSmall(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		{Name: "narrow_table", RowCount: 1_000_000, AvgRowSizeBytes: 500},
	}
	capped := analyzer.calculateAvgRowSize(tables)
	if capped != 500 || analyzer.uncappedAvgRowBytes != 500 {
		t.Errorf("narrow rows: capped=%d uncapped=%d, want both 500", capped, analyzer.uncappedAvgRowBytes)
	}
}

// TestCalculateAvgRowSize_TracksMaxForPacketCap guards the #166
// follow-up fix: when sampled tables have mixed row widths, the packet
// cap must be derived from the WIDEST row, not the average. Otherwise
// chunks sized for the average would still exceed @@max_allowed_packet
// when inserting the wide table.
func TestCalculateAvgRowSize_TracksMaxForPacketCap(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		{Name: "narrow1", RowCount: 1_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 900_000, AvgRowSizeBytes: 150},
		{Name: "wide_json", RowCount: 500_000, AvgRowSizeBytes: 8192},
		{Name: "narrow3", RowCount: 100_000, AvgRowSizeBytes: 200},
	}
	analyzer.calculateAvgRowSize(tables)

	// Average of {100, 150, 8192, 200} ≈ 2160. The capped value is 2000.
	if analyzer.maxSampledRowBytes != 8192 {
		t.Errorf("maxSampledRowBytes = %d, want 8192 (the wide_json row)", analyzer.maxSampledRowBytes)
	}
	// uncapped average should still reflect the mix
	const wantUncapped = 2160
	if analyzer.uncappedAvgRowBytes != wantUncapped {
		t.Errorf("uncappedAvgRowBytes = %d, want %d (average across the four tables)",
			analyzer.uncappedAvgRowBytes, wantUncapped)
	}
}

// TestCalculateAvgRowSize_MaxIncludesTablesOutsideTop5 guards the
// second-round #166 fix: maxSampledRowBytes must consider ALL tables,
// not just the top-5 by row count used for the average. Otherwise a
// small wide table (low row count, but each row huge) gets missed by
// the packet cap, and the runtime controller can still produce chunks
// that crash mid-migration when that table is being transferred
// (Codex review).
func TestCalculateAvgRowSize_MaxIncludesTablesOutsideTop5(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		// Top-5 by row count: five narrow tables. The average is small.
		{Name: "narrow1", RowCount: 5_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 4_000_000, AvgRowSizeBytes: 110},
		{Name: "narrow3", RowCount: 3_000_000, AvgRowSizeBytes: 120},
		{Name: "narrow4", RowCount: 2_000_000, AvgRowSizeBytes: 130},
		{Name: "narrow5", RowCount: 1_000_000, AvgRowSizeBytes: 140},
		// Sixth table: tiny row count but a 16KB-per-row JSON column.
		// Falls outside the top-5 cap on the average, but chunk_size
		// applies to it too and the packet cap MUST consider it.
		{Name: "tiny_wide", RowCount: 1000, AvgRowSizeBytes: 16384},
	}
	analyzer.calculateAvgRowSize(tables)
	if analyzer.maxSampledRowBytes != 16384 {
		t.Errorf("maxSampledRowBytes = %d, want 16384 (the small-but-wide tiny_wide table); top-5 limit must not gate the max",
			analyzer.maxSampledRowBytes)
	}
}
