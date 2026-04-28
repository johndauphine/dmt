package driver

import (
	"strings"
	"testing"
)

func TestDetectParameterTrend(t *testing.T) {
	tests := []struct {
		name    string
		history []AITuningRecord
		wantMsg bool // whether a non-empty warning is expected
	}{
		{
			name:    "empty history",
			history: nil,
			wantMsg: false,
		},
		{
			name:    "single entry",
			history: []AITuningRecord{{Workers: 8, ChunkSize: 100000}},
			wantMsg: false,
		},
		{
			name: "stable parameters",
			// newest first: all same values
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 100000},
				{Workers: 8, ChunkSize: 100000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: false,
		},
		{
			name: "increasing parameters",
			// newest first: 12, 10, 8 → values increasing over time (oldest=8, newest=12)
			history: []AITuningRecord{
				{Workers: 12, ChunkSize: 200000},
				{Workers: 10, ChunkSize: 150000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: false,
		},
		{
			name: "chunk_size decreasing >30%",
			// newest first: 50000, 80000, 100000 → strict decrease, 50% drop
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 50000},
				{Workers: 8, ChunkSize: 80000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: true,
		},
		{
			name: "workers decreasing >30%",
			// newest first: 4, 6, 8 → strict decrease, 50% drop
			history: []AITuningRecord{
				{Workers: 4, ChunkSize: 100000},
				{Workers: 6, ChunkSize: 100000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: true,
		},
		{
			name: "both decreasing",
			history: []AITuningRecord{
				{Workers: 4, ChunkSize: 50000},
				{Workers: 6, ChunkSize: 80000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: true,
		},
		{
			name: "decrease but not monotonic",
			// newest first: 60000, 90000, 80000 → not monotonic (90000 > 80000)
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 60000},
				{Workers: 8, ChunkSize: 90000},
				{Workers: 8, ChunkSize: 80000},
			},
			wantMsg: false,
		},
		{
			name: "plateau breaks monotonic",
			// newest first: 50000, 50000, 100000 → not strictly decreasing (50000 >= 50000)
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 50000},
				{Workers: 8, ChunkSize: 50000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: false,
		},
		{
			name: "small decrease under threshold",
			// newest first: 80000, 90000, 100000 → 20% drop, under 30% threshold
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 80000},
				{Workers: 8, ChunkSize: 90000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: false,
		},
		{
			name: "exactly 30% drop no warning",
			// newest first: 70000, 80000, 100000 → 30% drop, threshold is >30
			history: []AITuningRecord{
				{Workers: 8, ChunkSize: 70000},
				{Workers: 8, ChunkSize: 80000},
				{Workers: 8, ChunkSize: 100000},
			},
			wantMsg: false,
		},
		{
			name: "zero oldest value",
			history: []AITuningRecord{
				{Workers: 4, ChunkSize: 50000},
				{Workers: 0, ChunkSize: 0},
			},
			wantMsg: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectParameterTrend(tt.history)
			if tt.wantMsg && result == "" {
				t.Error("expected warning message, got empty string")
			}
			if !tt.wantMsg && result != "" {
				t.Errorf("expected no warning, got: %s", result)
			}
		})
	}
}

// mockHistoryProvider implements TuningHistoryProvider for testing.
type mockHistoryProvider struct {
	saved   *AITuningRecord
	history []AITuningRecord
}

func (m *mockHistoryProvider) GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}
func (m *mockHistoryProvider) GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error) {
	return m.history, nil
}
func (m *mockHistoryProvider) SaveAITuning(record AITuningRecord) error {
	m.saved = &record
	return nil
}
func (m *mockHistoryProvider) UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int) error {
	return nil
}

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
		},
	}

	// Simulate deferred save (what Analyze() does)
	analyzer.pendingSave = &pendingTuningSave{
		input:       AutoTuneInput{CPUCores: 15, MemoryGB: 24},
		wasAIUsed:   true,
		aiReasoning: "test reasoning",
	}

	// Save with actual params (user overrides)
	analyzer.SaveTuningWithActualParams(ActualParams{
		Workers:           12,
		ChunkSize:         14000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 3,
		ParallelReaders:   6,
		MaxPartitions:     8,
	})

	// Verify pendingSave was cleared
	if analyzer.pendingSave != nil {
		t.Error("pendingSave should be nil after SaveTuningWithActualParams")
	}

	// Verify record was saved with actual params, not AI recommendations
	if mock.saved == nil {
		t.Fatal("expected record to be saved")
	}
	if mock.saved.Workers != 12 {
		t.Errorf("Workers = %d, want 12", mock.saved.Workers)
	}
	if mock.saved.ChunkSize != 14000 {
		t.Errorf("ChunkSize = %d, want 14000", mock.saved.ChunkSize)
	}
	if mock.saved.ReadAheadBuffers != 4 {
		t.Errorf("ReadAheadBuffers = %d, want 4", mock.saved.ReadAheadBuffers)
	}
	if mock.saved.WriteAheadWriters != 3 {
		t.Errorf("WriteAheadWriters = %d, want 3", mock.saved.WriteAheadWriters)
	}
	if mock.saved.ParallelReaders != 6 {
		t.Errorf("ParallelReaders = %d, want 6", mock.saved.ParallelReaders)
	}
	if mock.saved.MaxPartitions != 8 {
		t.Errorf("MaxPartitions = %d, want 8", mock.saved.MaxPartitions)
	}
	if mock.saved.SourceDBType != "mssql" {
		t.Errorf("SourceDBType = %q, want mssql", mock.saved.SourceDBType)
	}
	if mock.saved.TargetDBType != "postgres" {
		t.Errorf("TargetDBType = %q, want postgres", mock.saved.TargetDBType)
	}
}

func TestSaveTuningWithActualParams_NoPending(t *testing.T) {
	mock := &mockHistoryProvider{}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock}

	// Call without pending save — should be a no-op
	analyzer.SaveTuningWithActualParams(ActualParams{Workers: 12, ChunkSize: 14000})

	if mock.saved != nil {
		t.Error("should not save when no pending save exists")
	}
}

func TestTrajectoryIncludesAllTunableParams(t *testing.T) {
	mock := &mockHistoryProvider{
		history: []AITuningRecord{
			{
				SourceDBType:         "mssql",
				TotalTables:          9,
				TotalRows:            19310703,
				Workers:              6,
				ChunkSize:            50000,
				ReadAheadBuffers:     2,
				WriteAheadWriters:    1,
				ParallelReaders:      5,
				MaxPartitions:        4,
				LargeTableThreshold:  500000,
				MaxSourceConnections: 12,
				MaxTargetConnections: 12,
				FinalThroughput:      800000,
				FinalDurationSecs:    24,
			},
		},
	}

	analyzer := &SmartConfigAnalyzer{historyProvider: mock}
	ctx := analyzer.formatHistoricalContext()

	params := []string{
		"workers=6",
		"chunk_size=50000",
		"read_ahead_buffers=2",
		"write_ahead_writers=1",
		"parallel_readers=5",
		"max_partitions=4",
		"large_table_threshold=500000",
		"max_source_connections=12",
		"max_target_connections=12",
	}
	for _, p := range params {
		if !strings.Contains(ctx, p) {
			t.Errorf("trajectory missing %q", p)
		}
	}
}

func TestSummarizeWriteAheadWritersRetryRate(t *testing.T) {
	tests := []struct {
		name         string
		history      []AITuningRecord
		wantContains []string
		wantEmpty    bool
	}{
		{
			name:      "empty history",
			history:   nil,
			wantEmpty: true,
		},
		{
			name: "skips records without completed runs",
			history: []AITuningRecord{
				{WriteAheadWriters: 2, FinalThroughput: 0, ChunkRetryCount: 5},
			},
			wantEmpty: true,
		},
		{
			name: "single config with no retries",
			history: []AITuningRecord{
				{WriteAheadWriters: 2, FinalThroughput: 1000000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1100000, ChunkRetryCount: 0},
			},
			wantContains: []string{
				"write_ahead_writers=2 → 0/2 runs retried (0% retry rate, 0 total chunk retries)",
			},
		},
		{
			name: "mixed retry rates per config — the case the AI must not cherry-pick",
			history: []AITuningRecord{
				// waw=2: 3 of 11 retried (matches the empirically observed regression case)
				{WriteAheadWriters: 2, FinalThroughput: 1200000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1100000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1300000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1250000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1180000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1190000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1320000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 1370000, ChunkRetryCount: 0},
				{WriteAheadWriters: 2, FinalThroughput: 500000, ChunkRetryCount: 1},
				{WriteAheadWriters: 2, FinalThroughput: 466000, ChunkRetryCount: 1},
				{WriteAheadWriters: 2, FinalThroughput: 428000, ChunkRetryCount: 1},
				// waw=1: 0 of 4 retried
				{WriteAheadWriters: 1, FinalThroughput: 920000, ChunkRetryCount: 0},
				{WriteAheadWriters: 1, FinalThroughput: 880000, ChunkRetryCount: 0},
				{WriteAheadWriters: 1, FinalThroughput: 900000, ChunkRetryCount: 0},
				{WriteAheadWriters: 1, FinalThroughput: 870000, ChunkRetryCount: 0},
			},
			wantContains: []string{
				"write_ahead_writers=1 → 0/4 runs retried (0% retry rate, 0 total chunk retries)",
				"write_ahead_writers=2 → 3/11 runs retried (27% retry rate, 3 total chunk retries)",
				"any non-zero retry rate at a given write_ahead_writers value means the target's transport saturates",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := summarizeWriteAheadWritersRetryRate(tt.history)
			if tt.wantEmpty {
				if result != "" {
					t.Errorf("expected empty result, got: %q", result)
				}
				return
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("result missing %q\nfull result:\n%s", want, result)
				}
			}
		})
	}
}
