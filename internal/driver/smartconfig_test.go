package driver

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"

	"github.com/johndauphine/dmt/internal/tuning"
)

// mockHistoryProvider satisfies the trimmed TuningHistoryProvider interface
// (PR #175 dropped the per-WAW / per-chunk_size aggregate methods; the new
// tuner reads raw rows and aggregates in-package). Just enough to verify
// SaveTuningWithActualParams persists the post-override params.
type mockHistoryProvider struct {
	saved     *checkpoint.TuningRecord
	history   []checkpoint.TuningRecord
	rowID     int64
	saveError error
}

func (m *mockHistoryProvider) GetRuntimeAdjustments(int) ([]checkpoint.RuntimeAdjustmentRecord, error) {
	return nil, nil
}

func (m *mockHistoryProvider) GetTuningHistory(_ int, _, _ string) ([]checkpoint.TuningRecord, error) {
	return m.history, nil
}

func (m *mockHistoryProvider) SaveTuningRecord(record checkpoint.TuningRecord) (int64, error) {
	m.saved = &record
	return m.rowID, m.saveError
}

func (m *mockHistoryProvider) UpdateTuningResult(int64, float64, float64, int, bool) error {
	return nil
}

// TestSaveTuningWithActualParams verifies the persisted record uses
// post-override params (issue #160) and the recomputed memory estimate.
func TestSaveTuningWithActualParams(t *testing.T) {
	mock := &mockHistoryProvider{rowID: 42}

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

	rowID := analyzer.SaveTuningWithActualParams(ActualParams{
		Workers:           12,
		ChunkSize:         14000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 3,
		ParallelReaders:   6,
		MaxPartitions:     8,
	})
	if rowID != 42 {
		t.Fatalf("SaveTuningWithActualParams row ID = %d, want 42", rowID)
	}

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
	// PR1: WasAIUsed always false now; Reasoning carries the deterministic
	// tuner's reasoning string.
	if mock.saved.WasAIUsed {
		t.Error("WasAIUsed should be false post-PR #175")
	}
	if mock.saved.Reasoning != "test reasoning" {
		t.Errorf("Reasoning: got %q, want %q", mock.saved.Reasoning, "test reasoning")
	}

	// Issue #160: persisted EstimatedMemoryMB must reflect post-override
	// params, not the smartconfig-time stale value (1135 above).
	want := tuning.EstimatedMemMB(12, 4, 3, 14000, 500)
	if mock.saved.EstimatedMemoryMB != want {
		t.Errorf("EstimatedMemoryMB = %d, want %d (recomputed from post-override params)",
			mock.saved.EstimatedMemoryMB, want)
	}
	if analyzer.suggestions.RepresentativeRowBytes != fallbackRowBytes ||
		analyzer.suggestions.SafetyRowBytes != fallbackRowBytes || analyzer.suggestions.SafetyRowBytesKnown {
		t.Errorf("direct pending save did not preserve unproven numeric fallbacks: %+v", analyzer.suggestions)
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

func TestSaveTuningWithActualParams_UsesSafetyWidthAndPersistsLegacyFeature(t *testing.T) {
	const (
		legacyRowBytes         = int64(500)
		representativeRowBytes = int64(220)
		safetyRowBytes         = int64(2 * 1024 * 1024)
	)
	mock := &mockHistoryProvider{}
	analyzer := &SmartConfigAnalyzer{
		dbType:          "mssql",
		targetDBType:    "postgres",
		historyProvider: mock,
		suggestions: &SmartConfigSuggestions{
			AvgRowSizeBytes: legacyRowBytes,
		},
		pendingSave: &pendingTuningSave{
			input: AutoTuneInput{
				AvgRowBytes:            legacyRowBytes,
				MemoryBudgetMB:         1,
				RepresentativeRowBytes: representativeRowBytes,
				SafetyRowBytes:         safetyRowBytes,
				SafetyRowBytesKnown:    true,
			},
			representativeRowBytes: representativeRowBytes,
			safetyRowBytes:         safetyRowBytes,
			safetyRowBytesKnown:    true,
		},
	}
	actual := ActualParams{
		Workers:           4,
		ChunkSize:         1,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ParallelReaders:   2,
	}

	analyzer.SaveTuningWithActualParams(actual)

	wantEstimate := tuning.EstimatedMemMB(actual.Workers, actual.ReadAheadBuffers, actual.WriteAheadWriters, actual.ChunkSize, safetyRowBytes)
	if analyzer.suggestions.EstimatedMemMB != wantEstimate {
		t.Errorf("post-override estimate = %d, want %d from safety width", analyzer.suggestions.EstimatedMemMB, wantEstimate)
	}
	if !analyzer.suggestions.MemoryEstimateOverBudget {
		t.Error("post-override minimum-progress surface must retain over-budget state")
	}
	if analyzer.suggestions.RepresentativeRowBytes != representativeRowBytes ||
		analyzer.suggestions.SafetyRowBytes != safetyRowBytes || !analyzer.suggestions.SafetyRowBytesKnown {
		t.Errorf("pending #703 width state was not preserved: %+v", analyzer.suggestions)
	}
	if mock.saved == nil {
		t.Fatal("expected tuning record to be saved")
	}
	if mock.saved.AvgRowSizeBytes != legacyRowBytes {
		t.Errorf("persisted AvgRowSizeBytes = %d, want legacy feature %d", mock.saved.AvgRowSizeBytes, legacyRowBytes)
	}
	if mock.saved.EstimatedMemoryMB != wantEstimate {
		t.Errorf("persisted estimate = %d, want %d from safety width", mock.saved.EstimatedMemoryMB, wantEstimate)
	}
}

func TestApplyTuningOutputCarriesMemoryEstimateOverBudget(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	analyzer.applyTuningOutput(tuning.Output{EstimatedMemMB: 2, MemoryEstimateOverBudget: true})
	if analyzer.suggestions.EstimatedMemMB != 2 || !analyzer.suggestions.MemoryEstimateOverBudget {
		t.Errorf("applyTuningOutput lost over-budget surface: %+v", analyzer.suggestions)
	}
}

func TestSaveTuningWithActualParams_OverflowRemainsOverBudget(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	analyzer := &SmartConfigAnalyzer{
		suggestions: &SmartConfigSuggestions{},
		pendingSave: &pendingTuningSave{input: AutoTuneInput{
			MemoryBudgetMB:      math.MaxInt64,
			SafetyRowBytes:      math.MaxInt64,
			SafetyRowBytesKnown: true,
		}},
	}
	analyzer.SaveTuningWithActualParams(ActualParams{
		Workers:           maxInt,
		ReadAheadBuffers:  maxInt,
		WriteAheadWriters: maxInt,
		ChunkSize:         maxInt,
	})
	if !analyzer.suggestions.MemoryEstimateOverBudget {
		t.Fatalf("overflowing post-override model appeared safe: %+v", analyzer.suggestions)
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

func TestBuildAutoTuneInput_UsesInjectedMemoryEnvelope(t *testing.T) {
	analyzer := NewSmartConfigAnalyzer(nil, "mssql")
	analyzer.SetMemoryEnvelope(24*1024, 3*1024, 2*1024)

	input := analyzer.buildAutoTuneInput(nil, 500)
	if input.MemoryGB != 24 {
		t.Errorf("MemoryGB = %d, want 24 from capacity (not transient availability)", input.MemoryGB)
	}
	if input.AvailableMemoryMB != 3*1024 {
		t.Errorf("AvailableMemoryMB = %d, want %d", input.AvailableMemoryMB, 3*1024)
	}
	if input.MemoryBudgetMB != 2*1024 || input.MaxMemoryMB != input.MemoryBudgetMB {
		t.Errorf("budget projections = MemoryBudgetMB %d / MaxMemoryMB %d, want %d", input.MemoryBudgetMB, input.MaxMemoryMB, 2*1024)
	}

	tuningInput := analyzer.toTuningInput(input)
	if tuningInput.MemoryBudgetMB != input.MemoryBudgetMB {
		t.Errorf("tuning MemoryBudgetMB = %d, want %d", tuningInput.MemoryBudgetMB, input.MemoryBudgetMB)
	}
}

func TestBuildAutoTuneInput_NoEnvelopeDoesNotInventMemory(t *testing.T) {
	analyzer := NewSmartConfigAnalyzer(nil, "mssql")
	input := analyzer.buildAutoTuneInput(nil, 500)
	if input.MemoryGB != 0 || input.AvailableMemoryMB != 0 || input.MemoryBudgetMB != 0 || input.MaxMemoryMB != 0 {
		t.Errorf("unset envelope produced memory values: %+v", input)
	}
	if input.AvgRowBytes != 500 || input.UncappedAvgRowBytes != 500 || input.RepresentativeRowBytes != 500 ||
		input.SafetyRowBytes != 500 || input.SafetyRowBytesKnown {
		t.Errorf("direct input did not carry explicit unproven width fallbacks: %+v", input)
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
			// When the modeled table-average width exceeds 80% of the
			// packet budget, integer division rounds down to 0. Clamping
			// to 1 keeps the cap meaningful as minimum progress; it does
			// not claim every individual row will fit the packet.
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

func TestCalculateAvgRowSize_SeparatesLegacyRepresentativeAndSafetyWidths(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		{Name: "dominant_narrow", RowCount: 100_000_000, AvgRowSizeBytes: 200},
		{Name: "tiny_wide", RowCount: 100, AvgRowSizeBytes: 8 * 1024},
	}

	legacyCapped := analyzer.calculateAvgRowSize(tables)
	if legacyCapped != 2000 || analyzer.uncappedAvgRowBytes != 4196 {
		t.Fatalf("legacy widths = capped %d/uncapped %d, want 2000/4196",
			legacyCapped, analyzer.uncappedAvgRowBytes)
	}
	if analyzer.representativeRowBytes != 200 {
		t.Errorf("RepresentativeRowBytes = %d, want 200 (tiny wide table must not distort byte-shaped sizing)", analyzer.representativeRowBytes)
	}
	if analyzer.safetyRowBytes != 8*1024 || !analyzer.safetyRowBytesKnown {
		t.Errorf("safety width = (%d, known=%v), want (8192, true)", analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
	}
	if analyzer.largestSampledTableBytes != 100_000_000*200 {
		t.Errorf("LargestTableBytes = %d, want %d", analyzer.largestSampledTableBytes, int64(100_000_000*200))
	}

	input := analyzer.buildAutoTuneInput(tables, legacyCapped)
	tuningInput := analyzer.toTuningInput(input)
	if input.RepresentativeRowBytes != 200 || input.SafetyRowBytes != 8192 || !input.SafetyRowBytesKnown {
		t.Errorf("AutoTuneInput lost #703 widths: %+v", input)
	}
	if tuningInput.RepresentativeRowBytes != input.RepresentativeRowBytes ||
		tuningInput.SafetyRowBytes != input.SafetyRowBytes ||
		tuningInput.SafetyRowBytesKnown != input.SafetyRowBytesKnown {
		t.Errorf("tuning.Input mapping lost #703 widths: %+v", tuningInput)
	}
}

func TestCalculateAvgRowSize_UniformTablesPreserveWidths(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	legacy := analyzer.calculateAvgRowSize([]tableInfo{
		{Name: "a", RowCount: 10_000, AvgRowSizeBytes: 640},
		{Name: "b", RowCount: 1_000, AvgRowSizeBytes: 640},
		{Name: "c", RowCount: 100, AvgRowSizeBytes: 640},
	})
	if legacy != 640 || analyzer.uncappedAvgRowBytes != 640 || analyzer.representativeRowBytes != 640 ||
		analyzer.safetyRowBytes != 640 || !analyzer.safetyRowBytesKnown {
		t.Fatalf("uniform widths diverged: legacy=%d uncapped=%d representative=%d safety=(%d,%v)",
			legacy, analyzer.uncappedAvgRowBytes, analyzer.representativeRowBytes,
			analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
	}
}

func TestCalculateAvgRowSize_UnknownAndWidthOnlySafety(t *testing.T) {
	t.Run("no observed widths uses unproven fallback", func(t *testing.T) {
		analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
		legacy := analyzer.calculateAvgRowSize([]tableInfo{{Name: "unknown", RowCount: 10}})
		if legacy != fallbackRowBytes || analyzer.uncappedAvgRowBytes != fallbackRowBytes ||
			analyzer.representativeRowBytes != fallbackRowBytes || analyzer.safetyRowBytes != fallbackRowBytes ||
			analyzer.safetyRowBytesKnown {
			t.Fatalf("unknown widths did not retain explicit fallback state: legacy=%d uncapped=%d representative=%d safety=(%d,%v)",
				legacy, analyzer.uncappedAvgRowBytes, analyzer.representativeRowBytes,
				analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
		}
	})

	t.Run("positive width with unknown count proves safety only", func(t *testing.T) {
		analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
		legacy := analyzer.calculateAvgRowSize([]tableInfo{{Name: "width_only", RowCount: 0, AvgRowSizeBytes: 8192}})
		if legacy != fallbackRowBytes || analyzer.representativeRowBytes != fallbackRowBytes {
			t.Fatalf("count-less width changed legacy/representative: legacy=%d representative=%d", legacy, analyzer.representativeRowBytes)
		}
		if analyzer.safetyRowBytes != 8192 || !analyzer.safetyRowBytesKnown {
			t.Fatalf("count-less positive width did not prove safety: (%d,%v)", analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
		}
	})
}

func TestCalculateAvgRowSize_SaturatesPathologicalProducts(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	analyzer.calculateAvgRowSize([]tableInfo{
		{Name: "huge", RowCount: math.MaxInt64, AvgRowSizeBytes: math.MaxInt64},
		{Name: "also_huge", RowCount: math.MaxInt64, AvgRowSizeBytes: 2},
	})

	if analyzer.largestSampledTableBytes != math.MaxInt64 {
		t.Errorf("LargestTableBytes = %d, want saturated MaxInt64", analyzer.largestSampledTableBytes)
	}
	if analyzer.safetyRowBytes != math.MaxInt64 || !analyzer.safetyRowBytesKnown {
		t.Errorf("safety width = (%d,%v), want (MaxInt64,true)", analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
	}
	if analyzer.representativeRowBytes != math.MaxInt64 || analyzer.uncappedAvgRowBytes != math.MaxInt64/2+1 {
		t.Errorf("overflow widths = representative %d/legacy %d, want conservative representative MaxInt64 and exact legacy mean %d",
			analyzer.representativeRowBytes, analyzer.uncappedAvgRowBytes, math.MaxInt64/2+1)
	}
}

func TestTargetHardChunkLimitUsesSafetyWidthEstimate(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{
		suggestions:         &SmartConfigSuggestions{AvgRowSizeBytes: 500},
		uncappedAvgRowBytes: 500,
		safetyRowBytes:      8192,
		targetProbe:         TargetProbe{MaxAllowedPacket: 4 * 1024 * 1024},
	}
	if got, want := analyzer.TargetHardChunkLimit(), 409; got != want {
		t.Errorf("TargetHardChunkLimit() = %d, want %d from the 8192-byte safety estimate", got, want)
	}
}

func TestChunkLimitFromProbeSaturatesPacketMath(t *testing.T) {
	got := chunkLimitFromProbe(0, TargetProbe{MaxAllowedPacket: math.MaxInt64}, 1)
	if got <= 0 {
		t.Fatalf("near-MaxInt64 packet produced non-positive chunk limit %d", got)
	}
}

// TestCalculateAvgRowSize_TracksSafetyWidthForPacketSizing verifies the
// packet estimate uses the widest observed table-average width. This is a
// modeled sizing input, not a hard bound on every serialized row.
func TestCalculateAvgRowSize_TracksSafetyWidthForPacketSizing(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		{Name: "narrow1", RowCount: 1_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 900_000, AvgRowSizeBytes: 150},
		{Name: "wide_json", RowCount: 500_000, AvgRowSizeBytes: 8192},
		{Name: "narrow3", RowCount: 100_000, AvgRowSizeBytes: 200},
	}
	analyzer.calculateAvgRowSize(tables)

	// Average of {100, 150, 8192, 200} ≈ 2160. The capped value is 2000.
	if analyzer.safetyRowBytes != 8192 || !analyzer.safetyRowBytesKnown {
		t.Errorf("safety width = (%d, known=%v), want (8192, true)", analyzer.safetyRowBytes, analyzer.safetyRowBytesKnown)
	}
	// uncapped average should still reflect the mix
	const wantUncapped = 2160
	if analyzer.uncappedAvgRowBytes != wantUncapped {
		t.Errorf("uncappedAvgRowBytes = %d, want %d (average across the four tables)",
			analyzer.uncappedAvgRowBytes, wantUncapped)
	}
}

// TestCalculateAvgRowSize_SafetyIncludesTablesOutsideTop5 verifies a small
// wide table cannot evade the all-table safety-width model.
func TestCalculateAvgRowSize_SafetyIncludesTablesOutsideTop5(t *testing.T) {
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
	if analyzer.safetyRowBytes != 16384 {
		t.Errorf("safetyRowBytes = %d, want 16384 (the small-but-wide tiny_wide table); top-5 limit must not gate safety width",
			analyzer.safetyRowBytes)
	}
}

// TestApplyTableNameFilter_ExcludedWideTableDoesNotDrivePacketCap guards
// #241: the orchestrator filters include/exclude before tuning runs, and
// the analyzer must scope to that same set. Otherwise an excluded wide
// table (e.g. an archive blob) drives @@max_allowed_packet → chunk_size
// derivation and clamps chunk_size for the narrow tables that actually
// ship. Worked example from the issue body: five 100-byte narrow tables
// + one excluded 16KB blob table → cap must derive from 140-byte max
// (the narrow set's widest), not 16384.
func TestApplyTableNameFilter_ExcludedWideTableDoesNotDrivePacketCap(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	allTables := []tableInfo{
		{Name: "narrow1", RowCount: 1_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 900_000, AvgRowSizeBytes: 110},
		{Name: "narrow3", RowCount: 800_000, AvgRowSizeBytes: 120},
		{Name: "narrow4", RowCount: 700_000, AvgRowSizeBytes: 130},
		{Name: "narrow5", RowCount: 600_000, AvgRowSizeBytes: 140},
		{Name: "blob_archive", RowCount: 50_000, AvgRowSizeBytes: 16384},
	}

	// Orchestrator excluded blob_archive — the analyzer should never see it.
	analyzer.SetTableNameFilter([]string{
		"narrow1", "narrow2", "narrow3", "narrow4", "narrow5",
	})
	kept := analyzer.applyTableNameFilter(allTables)
	if len(kept) != 5 {
		t.Fatalf("applyTableNameFilter kept %d tables, want 5", len(kept))
	}
	for _, k := range kept {
		if k.Name == "blob_archive" {
			t.Fatalf("applyTableNameFilter leaked excluded blob_archive into the in-scope set")
		}
	}

	// Run the same packet-cap derivation that calculateAutoTuneParams
	// does, but only over the in-scope tables.
	analyzer.calculateAvgRowSize(kept)
	if analyzer.safetyRowBytes != 140 {
		t.Errorf("safetyRowBytes = %d, want 140 (widest narrow table average); the excluded 16384-byte blob_archive must not drive packet sizing",
			analyzer.safetyRowBytes)
	}
}

// TestApplyTableNameFilter_NoFilterIsIdentity is the regression test
// called out in #241: existing migrations with no include/exclude
// filters must produce identical caps before and after this change.
// Same input through both code paths must yield the same safetyRowBytes
// and same uncappedAvgRowBytes.
func TestApplyTableNameFilter_NoFilterIsIdentity(t *testing.T) {
	tables := []tableInfo{
		{Name: "narrow1", RowCount: 1_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 900_000, AvgRowSizeBytes: 150},
		{Name: "wide_json", RowCount: 500_000, AvgRowSizeBytes: 8192},
		{Name: "narrow3", RowCount: 100_000, AvgRowSizeBytes: 200},
	}

	// Baseline: never set a filter.
	baseline := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	baselineTables := baseline.applyTableNameFilter(tables)
	baseline.calculateAvgRowSize(baselineTables)

	// Mirror the orchestrator behavior when the user has no filters
	// configured: filterTables returns the full set, and the
	// orchestrator passes that full set into SetTableNameFilter.
	// (tableNamesForTuning never returns nil except for empty input.)
	filtered := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	filtered.SetTableNameFilter([]string{"narrow1", "narrow2", "wide_json", "narrow3"})
	filteredTables := filtered.applyTableNameFilter(tables)
	filtered.calculateAvgRowSize(filteredTables)

	if baseline.safetyRowBytes != filtered.safetyRowBytes || baseline.safetyRowBytesKnown != filtered.safetyRowBytesKnown {
		t.Errorf("safety width differs: baseline=(%d,%v), filtered=(%d,%v) (no-filter behavior changed)",
			baseline.safetyRowBytes, baseline.safetyRowBytesKnown, filtered.safetyRowBytes, filtered.safetyRowBytesKnown)
	}
	if baseline.uncappedAvgRowBytes != filtered.uncappedAvgRowBytes {
		t.Errorf("uncappedAvgRowBytes differs: baseline=%d, filtered=%d (no-filter behavior changed)",
			baseline.uncappedAvgRowBytes, filtered.uncappedAvgRowBytes)
	}
	if len(baselineTables) != len(filteredTables) {
		t.Errorf("table count differs: baseline=%d, filtered=%d", len(baselineTables), len(filteredTables))
	}
}

// TestSetTableNameFilter_EmptyClears verifies passing an empty slice
// clears any prior filter, returning the analyzer to the unscoped
// path used by the analyze CLI subcommand.
func TestSetTableNameFilter_EmptyClears(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	analyzer.SetTableNameFilter([]string{"foo", "bar"})
	if analyzer.tableNameFilter == nil {
		t.Fatal("filter was not set")
	}
	analyzer.SetTableNameFilter(nil)
	if analyzer.tableNameFilter != nil {
		t.Errorf("nil input did not clear filter; got %v", analyzer.tableNameFilter)
	}
	analyzer.SetTableNameFilter([]string{"foo"})
	analyzer.SetTableNameFilter([]string{})
	if analyzer.tableNameFilter != nil {
		t.Errorf("empty input did not clear filter; got %v", analyzer.tableNameFilter)
	}
}

// TestApplyTableNameFilter_CaseInsensitive confirms the filter matches
// table names regardless of case — getTables results come straight from
// information_schema and casing can differ from what the user typed in
// config.Migration.IncludeTables/ExcludeTables (which filterTables also
// already lowercases for matching).
func TestApplyTableNameFilter_CaseInsensitive(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	analyzer.SetTableNameFilter([]string{"MyTable"})
	kept := analyzer.applyTableNameFilter([]tableInfo{
		{Name: "mytable", AvgRowSizeBytes: 100},
		{Name: "MYTABLE", AvgRowSizeBytes: 200},
		{Name: "other", AvgRowSizeBytes: 300},
	})
	if len(kept) != 2 {
		t.Fatalf("expected 2 case-insensitive matches, got %d: %+v", len(kept), kept)
	}
	for _, k := range kept {
		if k.Name == "other" {
			t.Errorf("non-matching table %q kept", k.Name)
		}
	}
}

// TestCalculateAvgRowSize_LargestSampledTableBytesUsesAllTables pins
// the Copilot fix on PR #288: largestSampledTableBytes must consider
// ALL tables, not just the top-5-by-row-count slice used for the
// average. A low-row-count table with very wide rows can be the true
// bytes heavyweight, and the regime classifier's skew tier needs to
// reflect that.
func TestCalculateAvgRowSize_LargestSampledTableBytesUsesAllTables(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{suggestions: &SmartConfigSuggestions{}}
	tables := []tableInfo{
		// Top-5 by row count. The widest by bytes is narrow1: 5M × 100 = 500M.
		{Name: "narrow1", RowCount: 5_000_000, AvgRowSizeBytes: 100},
		{Name: "narrow2", RowCount: 4_000_000, AvgRowSizeBytes: 110},
		{Name: "narrow3", RowCount: 3_000_000, AvgRowSizeBytes: 120},
		{Name: "narrow4", RowCount: 2_000_000, AvgRowSizeBytes: 130},
		{Name: "narrow5", RowCount: 1_000_000, AvgRowSizeBytes: 140},
		// Outside top-5: 100K rows × 16KB = 1.6 GB. Real heavyweight by
		// bytes but easy to miss if largestTable is read off LargestTables[:5].
		{Name: "tiny_but_wide_archive", RowCount: 100_000, AvgRowSizeBytes: 16384},
	}
	analyzer.calculateAvgRowSize(tables)
	const want = int64(100_000) * 16384 // 1.6 GB
	if analyzer.largestSampledTableBytes != want {
		t.Errorf("largestSampledTableBytes = %d, want %d (the tiny_but_wide_archive table; top-5-by-rows must not gate the bytes max)",
			analyzer.largestSampledTableBytes, want)
	}
}

// TestThroughputBytesForHistory_GuardsBadValues pins the Copilot fix
// on PR #289: the rows/sec → bytes/sec adapter conversion must not
// feed garbage into the regression's int64 y vector when a persisted
// row carries a non-positive, NaN, or +Inf throughput value.
func TestThroughputBytesForHistory_GuardsBadValues(t *testing.T) {
	cases := []struct {
		name         string
		rowsPerSec   float64
		avgRowBytes  int64
		wantZero     bool
		wantPositive bool
	}{
		{name: "negative throughput", rowsPerSec: -100, avgRowBytes: 500, wantZero: true},
		{name: "zero throughput", rowsPerSec: 0, avgRowBytes: 500, wantZero: true},
		{name: "NaN throughput", rowsPerSec: math.NaN(), avgRowBytes: 500, wantZero: true},
		{name: "+Inf throughput", rowsPerSec: math.Inf(1), avgRowBytes: 500, wantZero: true},
		{name: "normal value", rowsPerSec: 500_000, avgRowBytes: 1000, wantPositive: true},
		{name: "avgRowBytes=0 uses safeAvgRowBytes fallback", rowsPerSec: 1000, avgRowBytes: 0, wantPositive: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := throughputBytesForHistory(tc.rowsPerSec, tc.avgRowBytes)
			if tc.wantZero && got != 0 {
				t.Errorf("got %d, want 0 (defensive zero for bad inputs)", got)
			}
			if tc.wantPositive && got <= 0 {
				t.Errorf("got %d, want positive int64", got)
			}
		})
	}
}

// TestTuningHistoryAdapter_MapsAllSharedFields is the #455 guard against
// the adapter failure mode that hit #219: a field present on both
// checkpoint.TuningRecord and tuning.HistoryRecord silently dropped by
// the hand-written mapping in tuningHistoryAdapter.Records. The test sets
// a distinctive non-zero value on every checkpoint field via reflection,
// runs the adapter, and requires every same-name same-type field on the
// tuning side to carry that exact value. Adding a shared field without
// mapping it fails here instead of silently starving the tuner.
func TestTuningHistoryAdapter_MapsAllSharedFields(t *testing.T) {
	src := checkpoint.TuningRecord{}
	rv := reflect.ValueOf(&src).Elem()
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Field(i)
		switch {
		case f.Kind() == reflect.String:
			f.SetString(fmt.Sprintf("v%d", i))
		case f.Kind() == reflect.Int || f.Kind() == reflect.Int64:
			f.SetInt(int64(i + 1))
		case f.Kind() == reflect.Float64:
			f.SetFloat(float64(i) + 0.5)
		case f.Kind() == reflect.Bool:
			f.SetBool(true)
		case f.Type() == reflect.TypeOf(time.Time{}):
			f.Set(reflect.ValueOf(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
		default:
			t.Fatalf("checkpoint.TuningRecord field %s has unhandled kind %s — extend this test's value generator",
				rv.Type().Field(i).Name, f.Kind())
		}
	}

	adapter := &tuningHistoryAdapter{base: &mockHistoryProvider{
		history: []checkpoint.TuningRecord{src},
	}}
	rows, err := adapter.Records(src.SourceDBType, src.TargetDBType)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	out := reflect.ValueOf(rows[0])
	srcType := rv.Type()
	checked := 0
	for i := 0; i < out.NumField(); i++ {
		of := out.Type().Field(i)
		sf, shared := srcType.FieldByName(of.Name)
		if !shared || sf.Type != of.Type {
			continue // renamed/derived fields (AvgRowBytes, FinalThroughputBytes) are mapped deliberately, not verbatim
		}
		got := out.Field(i).Interface()
		want := rv.FieldByName(of.Name).Interface()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("shared field %s not copied by tuningHistoryAdapter: got %v, want %v (did a new field miss the mapping?)",
				of.Name, got, want)
		}
		checked++
	}
	// Sanity: the test is vacuous if the shared-field set ever collapses.
	if checked < 15 {
		t.Fatalf("only %d shared fields checked — expected the record shapes to overlap heavily; did a rename break the guard?", checked)
	}
}
