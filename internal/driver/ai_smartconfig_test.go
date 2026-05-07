package driver

import (
	"sort"
	"strings"
	"testing"
	"time"
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
// `history` is treated as the full ai_tuning_history; GetAITuningHistory respects
// the limit param (smartconfig uses trajectoryLimit for the bounded trajectory),
// and the aggregate methods compute over the FULL history exactly the way the
// SQL backend would.
type mockHistoryProvider struct {
	saved   *AITuningRecord
	history []AITuningRecord
}

func (m *mockHistoryProvider) GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

func (m *mockHistoryProvider) GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error) {
	// Match the SQL backend's ordering: most-recent first by Timestamp.
	// Without this, tests that depend on "last N" semantics could drift away
	// from production behavior (caught in PR #145 review). SliceStable
	// preserves test-fixture order for rows with equal (e.g. zero) Timestamps.
	sorted := make([]AITuningRecord, len(m.history))
	copy(sorted, m.history)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.After(sorted[j].Timestamp)
	})
	if limit > 0 && limit < len(sorted) {
		return sorted[:limit], nil
	}
	return sorted, nil
}

func (m *mockHistoryProvider) GetAITuningAggregatesByWaw(sourceType, targetType string) ([]WawAggregateRecord, error) {
	type stats struct{ totalRuns, retried, totalRetries int; peak, sum float64 }
	byWaw := map[int]*stats{}
	for _, h := range m.history {
		if h.FinalThroughput <= 0 {
			continue
		}
		s, ok := byWaw[h.WriteAheadWriters]
		if !ok {
			s = &stats{}
			byWaw[h.WriteAheadWriters] = s
		}
		s.totalRuns++
		s.sum += h.FinalThroughput
		if h.FinalThroughput > s.peak {
			s.peak = h.FinalThroughput
		}
		if h.ChunkRetryCount > 0 {
			s.retried++
			s.totalRetries += h.ChunkRetryCount
		}
	}
	keys := make([]int, 0, len(byWaw))
	for k := range byWaw {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	out := make([]WawAggregateRecord, 0, len(keys))
	for _, k := range keys {
		s := byWaw[k]
		out = append(out, WawAggregateRecord{
			WriteAheadWriters: k,
			TotalRuns:         s.totalRuns,
			RunsWithRetries:   s.retried,
			TotalRetries:      s.totalRetries,
			PeakThroughput:    s.peak,
			MeanThroughput:    s.sum / float64(s.totalRuns),
		})
	}
	return out, nil
}

func (m *mockHistoryProvider) GetAITuningAggregatesByChunkSize(sourceType, targetType string) ([]ChunkSizeAggregateRecord, error) {
	type stats struct{ runs int; sum float64 }
	byChunk := map[int]*stats{}
	for _, h := range m.history {
		if h.FinalThroughput <= 0 {
			continue
		}
		s, ok := byChunk[h.ChunkSize]
		if !ok {
			s = &stats{}
			byChunk[h.ChunkSize] = s
		}
		s.runs++
		s.sum += h.FinalThroughput
	}
	keys := make([]int, 0, len(byChunk))
	for k := range byChunk {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	out := make([]ChunkSizeAggregateRecord, 0, len(keys))
	for _, k := range keys {
		s := byChunk[k]
		out = append(out, ChunkSizeAggregateRecord{
			ChunkSize:     k,
			Runs:          s.runs,
			AvgThroughput: s.sum / float64(s.runs),
		})
	}
	return out, nil
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

// TestFormatHistoricalContextBoundsTrajectory pins the issue #141 behavior:
// the trajectory section in the prompt MUST be bounded to trajectoryLimit rows
// regardless of how much history exists, but the per-waw and per-chunk-size
// summaries downstream MUST reflect the FULL history (not just the bounded
// slice). Without this split, the retry-rate rule's denominators would silently
// drop older runs.
//
// The fixture uses distinct timestamps and distinct throughput values so the
// test can verify which rows are selected, not just how many — i.e. that the
// trajectory really is the *most recent* 20 (not just any 20). PR #145 review
// flagged the prior version as too weak.
func TestFormatHistoricalContextBoundsTrajectory(t *testing.T) {
	// Build 50 history rows. Throughput is a function of index so we can detect
	// which rows landed in the trajectory: row i has FinalThroughput=1000000+i.
	// Older rows (low i) have older Timestamps so production sorting puts them
	// at the back — the trajectory should contain rows i in [30..49] only.
	const totalRows = 50
	base := time.Now()
	hist := make([]AITuningRecord, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		waw := 1
		if i%2 == 0 {
			waw = 2
		}
		hist = append(hist, AITuningRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			Timestamp:         base.Add(time.Duration(i) * time.Minute),
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			Workers:           16,
			ReadAheadBuffers:  4,
			ParallelReaders:   4,
			MaxPartitions:     16,
			FinalThroughput:   float64(1000000 + i),
			FinalDurationSecs: 20,
			ChunkRetryCount:   0,
		})
	}
	mock := &mockHistoryProvider{history: hist}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock, dbType: "mssql", targetDBType: "postgres"}
	ctx := analyzer.formatHistoricalContext()

	// Trajectory section must announce the bounded count, not the full 50.
	wantHeader := "PARAMETER TRAJECTORY (most recent 20 analyses"
	if !strings.Contains(ctx, wantHeader) {
		t.Errorf("trajectory header missing %q\nfull context:\n%s", wantHeader, ctx)
	}

	// Numbered rows should stop at 20.
	if strings.Contains(ctx, "  21. ") {
		t.Error("trajectory rendered more than trajectoryLimit rows; row 21 should not appear")
	}

	// Recency assertion: the most recent run (i=49, throughput=1000049) must be
	// in the trajectory, and the oldest run (i=0, throughput=1000000) must NOT
	// be. Without the Timestamp DESC sort in the mock + production code, this
	// would slip through.
	if !strings.Contains(ctx, "1000049 rows/sec") {
		t.Errorf("trajectory missing the most-recent run (throughput=1000049); selection is not by recency\nfull context:\n%s", ctx)
	}
	if strings.Contains(ctx, "1000000 rows/sec") {
		t.Errorf("trajectory contains the oldest run (throughput=1000000); should have been bounded out by recency\nfull context:\n%s", ctx)
	}
	// And a row from the middle (i=20, throughput=1000020) is in the bounded
	// window: rows 30..49 are kept; row 20 is dropped.
	if strings.Contains(ctx, "1000020 rows/sec") {
		t.Errorf("trajectory contains row from beyond the bounded window (throughput=1000020 = i=20); only the 20 most recent (i=30..49) should appear\nfull context:\n%s", ctx)
	}
	if !strings.Contains(ctx, "1000030 rows/sec") {
		t.Errorf("trajectory missing the boundary recent row (throughput=1000030 = i=30, which IS in the most-recent 20)\nfull context:\n%s", ctx)
	}

	// But the per-waw summary must still see all 50 rows (25/waw).
	wantWaw1 := "write_ahead_writers=1 → 0/25 runs retried (0.0% retry rate, 0 total chunk retries)"
	wantWaw2 := "write_ahead_writers=2 → 0/25 runs retried (0.0% retry rate, 0 total chunk retries)"
	if !strings.Contains(ctx, wantWaw1) {
		t.Errorf("per-waw summary missing %q (denominator should reflect full history, not bounded trajectory)\nfull context:\n%s", wantWaw1, ctx)
	}
	if !strings.Contains(ctx, wantWaw2) {
		t.Errorf("per-waw summary missing %q\nfull context:\n%s", wantWaw2, ctx)
	}
}

// TestTrajectoryRendersZeroChunkRetries pins the post-#139 behavior: a clean
// run (chunk_retry_count=0) MUST render ", 0 chunk retries" in the trajectory.
// Pre-fix, the suffix was omitted for clean runs, leaving zeros invisible and
// priming the AI to confabulate retries when applying the retry-rate rule.
func TestTrajectoryRendersZeroChunkRetries(t *testing.T) {
	mock := &mockHistoryProvider{
		history: []AITuningRecord{
			{
				SourceDBType:      "mssql",
				WriteAheadWriters: 1,
				FinalThroughput:   1044709,
				FinalDurationSecs: 18,
				ChunkRetryCount:   0,
			},
			{
				SourceDBType:      "mssql",
				WriteAheadWriters: 2,
				FinalThroughput:   500000,
				FinalDurationSecs: 40,
				ChunkRetryCount:   3,
			},
		},
	}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock}
	ctx := analyzer.formatHistoricalContext()

	// Both runs must render the chunk-retries suffix explicitly.
	if !strings.Contains(ctx, "0 chunk retries") {
		t.Errorf("clean run did not render \"0 chunk retries\" suffix; full trajectory:\n%s", ctx)
	}
	if !strings.Contains(ctx, "3 chunk retries") {
		t.Errorf("retried run did not render \"3 chunk retries\" suffix; full trajectory:\n%s", ctx)
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

// TestFormatWawAggregateBlock exercises the rendering of the per-write_ahead_writers
// retry-rate block. Inputs are raw history records for ergonomic test fixtures;
// the test pipes them through mockHistoryProvider.GetAITuningAggregatesByWaw
// (which mirrors the SQL backend's GROUP BY) and then through the format helper.
//
// Renamed from TestSummarizeWriteAheadWritersRetryRate after PR #141 split the
// aggregator (now SQL-side) from the formatter (now formatWawAggregateBlock).
func TestFormatWawAggregateBlock(t *testing.T) {
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
				"write_ahead_writers=2 → 0/2 runs retried (0.0% retry rate, 0 total chunk retries)",
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
				"write_ahead_writers=1 → 0/4 runs retried (0.0% retry rate, 0 total chunk retries)",
				"write_ahead_writers=2 → 3/11 runs retried (27.3% retry rate, 3 total chunk retries)",
				// The framing must be neutral and data-grounded, not assertive about
				// causal mechanisms (issue #139): the AI was echoing "transport saturation"
				// even when retry rates were uniformly zero.
				"The retry-rate rule applies ONLY to waw values that show a non-zero retry rate",
				"do not invent or repeat causal explanations",
			},
		},
		{
			// Regression test: a small-but-nonzero retry rate (1/200 = 0.5%)
			// must NOT round to "0%" in the prompt — that would falsely trip
			// clause 4(b) "rule does not apply" and disable the retry-rate
			// rule. Caught in PR #140 review.
			name: "low-retry-rate must not round to zero",
			history: func() []AITuningRecord {
				h := make([]AITuningRecord, 0, 200)
				for i := 0; i < 199; i++ {
					h = append(h, AITuningRecord{WriteAheadWriters: 2, FinalThroughput: 1000000, ChunkRetryCount: 0})
				}
				h = append(h, AITuningRecord{WriteAheadWriters: 2, FinalThroughput: 500000, ChunkRetryCount: 1})
				return h
			}(),
			wantContains: []string{
				"write_ahead_writers=2 → 1/200 runs retried (0.5% retry rate, 1 total chunk retries)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Aggregation moved to SQL (issue #141); drive the test through the
			// mock's aggregator so the test inputs stay easy to express as raw
			// records. Pre-#141 this was a single-call path; now it's two
			// calls (aggregate, then format) but the formatted output is the
			// same load-bearing string the AI prompt depends on.
			mock := &mockHistoryProvider{history: tt.history}
			aggs, err := mock.GetAITuningAggregatesByWaw("", "")
			if err != nil {
				t.Fatalf("aggregator: %v", err)
			}
			result := formatWawAggregateBlock(aggs)
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
