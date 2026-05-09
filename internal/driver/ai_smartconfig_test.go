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
