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
	type stats struct {
		totalRuns, retried, totalRetries int
		peak, sum                        float64
	}
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
	type stats struct {
		runs int
		sum  float64
	}
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
			// Pre-set to a stale value (representing the smartconfig-time
			// estimate, which is wrong once user overrides land below).
			// Issue #160 — without the recompute, this stale 1135 would
			// be saved alongside the post-override chunk_size=14000.
			EstimatedMemMB: 1135,
		},
	}

	// Simulate deferred save (what Analyze() does). AvgRowBytes is what the
	// recompute uses to derive the post-override memory estimate.
	analyzer.pendingSave = &pendingTuningSave{
		input:       AutoTuneInput{CPUCores: 15, MemoryGB: 24, AvgRowBytes: 500},
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

	// Issue #160: the saved EstimatedMemoryMB must reflect the post-override
	// params, not the smartconfig-time stale value (1135 above). With
	// workers=12, ra=4, wa=3, chunk=14000, avg=500, the formula gives
	// 12 * 7 * 14000 * 500 / 1048576 = 561 MB.
	wantMem := ComputeEstimatedMemMB(12, 4, 3, 14000, 500)
	if mock.saved.EstimatedMemoryMB != wantMem {
		t.Errorf("EstimatedMemoryMB = %d, want %d (recomputed from post-override params; stale pre-save value was 1135)",
			mock.saved.EstimatedMemoryMB, wantMem)
	}
}

// TestSaveTuningWithActualParams_NoOverride pins the no-op case from issue
// #160's acceptance criteria: when ActualParams matches what smartconfig
// already had in suggestions, the saved EstimatedMemoryMB equals the same
// formula applied to those params (no surprise behavior).
func TestSaveTuningWithActualParams_NoOverride(t *testing.T) {
	mock := &mockHistoryProvider{}

	const (
		workers     = 4
		chunk       = 50000
		readAhead   = 4
		writeAhead  = 2
		parReaders  = 2
		maxParts    = 4
		avgRowBytes = 500
	)
	preSave := ComputeEstimatedMemMB(workers, readAhead, writeAhead, chunk, avgRowBytes)

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

	// Trajectory section must announce the bounding ratio (issue #146 — header
	// now says "BOUNDED — most recent 20 of 50 completed analyses" so the
	// model can SEE the bounded vs full-history distinction).
	wantHeader := "PARAMETER TRAJECTORY (BOUNDED — most recent 20 of 50 completed analyses"
	if !strings.Contains(ctx, wantHeader) {
		t.Errorf("trajectory header missing %q\nfull context:\n%s", wantHeader, ctx)
	}

	// And the prompt must explicitly forbid using trajectory rows as
	// retry-rate denominators (the citation-source disambiguation from #146).
	wantWarning := "Do NOT count waw values or chunk_size values within these rows to derive retry-rate or sample-size denominators"
	if !strings.Contains(ctx, wantWarning) {
		t.Errorf("trajectory warning missing %q\nfull context:\n%s", wantWarning, ctx)
	}

	// Numbered rows should stop at 20.
	if strings.Contains(ctx, "  21. ") {
		t.Error("trajectory rendered more than trajectoryLimit rows; row 21 should not appear")
	}

	// Aggregate block header must surface the full-history denominator with
	// the explicit "cite from HERE" instruction (issue #146).
	wantAggHeader := "WRITE_AHEAD_WRITERS vs CHUNK RETRY RATE (FULL-HISTORY aggregate over all 50 completed analyses — cite denominators from HERE, not from the bounded trajectory above)"
	if !strings.Contains(ctx, wantAggHeader) {
		t.Errorf("aggregate header missing %q\nfull context:\n%s", wantAggHeader, ctx)
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

// TestBuildOfflinePromptInvariants pins #142 / PR #151 review:
// the offline (no-history) smartconfig prompt must (a) include the
// observed_retry_rates JSON field with "no history" as the instructed
// value (so future shared post-unmarshal validation passes), (b) explicitly
// mark the retry-rate rule as non-applicable offline, (c) avoid asserting
// causal mechanisms (saturation/pressure/contention) since there's no
// chunk_retry_count history to ground them, and (d) keep the JSON schema
// in sync with the online prompt's tunable-parameter list.
func TestBuildOfflinePromptInvariants(t *testing.T) {
	input := AutoTuneInput{
		Platform:          "linux",
		CPUCores:          16,
		MemoryGB:          32,
		AvailableMemoryMB: 30000,
	}
	prompt := buildOfflinePrompt(input, `{"placeholder": "test"}`)

	musts := []string{
		// (a) schema parity
		`"observed_retry_rates": "no history"`,
		// (b) rule explicitly inapplicable
		"retry-rate rule that the with-history smartconfig uses does NOT apply",
		// (d) all tunable parameters present in the JSON schema
		`"workers": <int>`,
		`"chunk_size": <int>`,
		`"read_ahead_buffers": <int>`,
		`"write_ahead_writers": <int>`,
		`"parallel_readers": <int>`,
		`"max_partitions": <int>`,
		// reasoning instruction forbids mechanism assertion
		"do not assert mechanisms",
		// Pre-computed memory budget block must replace the old LLM-arithmetic
		// "Memory formula" / "Memory constraint" pair (follow-up to PR #154).
		"Memory budget (computed for you",
		"Memory budget:",
		"safe chunk_size ceiling",
	}
	for _, m := range musts {
		if !strings.Contains(prompt, m) {
			t.Errorf("offline prompt missing required substring %q\nfull prompt:\n%s", m, prompt)
		}
	}

	// (c) no causal-mechanism language directly asserted in the guideline
	// text. The "do not assert mechanisms (saturation, pressure, contention)"
	// instruction itself is allowed (it forbids the assertions). The pre-fix
	// affirmative claims must not appear.
	//
	// Plus: the LLM must NOT be asked to compute or output `estimated_memory_mb`
	// (follow-up to PR #154 — observed wrong by 7-10x on the same inputs;
	// removing the field entirely is the cleanest fix).
	mustsNot := []string{
		"can saturate",
		"produce transient stalls",
		"deterministically",
		"estimated_memory_mb",
		"Memory formula:",
		"Memory constraint:",
	}
	for _, m := range mustsNot {
		if strings.Contains(prompt, m) {
			t.Errorf("offline prompt contains forbidden substring %q\nfull prompt:\n%s", m, prompt)
		}
	}
}

// TestComputeSafeChunkSize pins the formula used to populate the prompt's
// "safe chunk_size ceiling" line. Inverts ComputeEstimatedMemMB so that
// substituting the result back into the formula yields a memory footprint at
// or below the budget.
func TestComputeSafeChunkSize(t *testing.T) {
	tests := []struct {
		name        string
		budgetMB    int64
		workers     int
		raw, waw    int
		avgRowBytes int64
		want        int64
	}{
		// Realistic so2010 case: budget=28000 MB, 16 workers, 6 buffers,
		// 248 bytes/row. Expected from int math: 28000 * 1MiB / (16*6*248).
		{"so2010", 28000, 16, 4, 2, 248, 1233204},
		{"capped-rows", 28000, 16, 4, 2, 2000, 152917},
		{"zero-workers", 28000, 0, 4, 2, 500, 0},
		{"zero-buffers", 28000, 16, 0, 0, 500, 0},
		{"zero-budget", 0, 16, 4, 2, 500, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := computeSafeChunkSize(tc.budgetMB, tc.workers, tc.raw, tc.waw, tc.avgRowBytes)
			if got != tc.want {
				t.Errorf("computeSafeChunkSize(%d MB, w=%d, r=%d, w=%d, avg=%d) = %d, want %d",
					tc.budgetMB, tc.workers, tc.raw, tc.waw, tc.avgRowBytes, got, tc.want)
			}
		})
	}
}

// TestBuildMemoryBudgetBlock locks in the prompt invariants for the
// pre-computed budget block so a future refactor doesn't accidentally drop
// the budget number, the avg-row-size, or the WSL2 caveat.
// pgStub mimics the postgres driver for tests in this package, which can't
// blank-import driver/postgres without an init-cycle. Only the methods
// buildMemoryBudgetBlock actually calls are populated; the embedded Driver
// is nil, so any other method call would NPE — that's intentional, it
// surfaces accidental new dependencies on the stub instead of silently
// returning zero values.
type chunkAdviceStub struct {
	Driver
	bytes     int64
	hardLimit int
}

func (s *chunkAdviceStub) Defaults() DriverDefaults {
	return DriverDefaults{OptimumBulkChunkBytes: s.bytes}
}
func (s *chunkAdviceStub) HardChunkLimit(int64) int { return s.hardLimit }

func TestBuildMemoryBudgetBlock(t *testing.T) {
	pg := &chunkAdviceStub{bytes: 25_000_000} // PG seed value (#166)
	in := AutoTuneInput{
		Platform:          "wsl2",
		CPUCores:          18,
		MemoryGB:          30,
		AvailableMemoryMB: 30000,
		AvgRowBytes:       248,
		TargetType:        "postgres",
	}
	got := buildMemoryBudgetBlock(in, 16, pg)
	musts := []string{
		"Memory budget: 27952 MB",   // 30000 - 2048 headroom
		"available_memory_mb=30000", // budget source attribution
		"Avg row size: 248 bytes",
		"baseline workers=16",
		"safe chunk_size ceiling is ~1231090 rows", // int math: 27952 * 1MiB / (16*6*248)
		"CRITICAL CONSTRAINT",                      // imperative framing (issue #162)
		"MUST NOT exceed 1231090",                  // ceiling restated as hard cap
		"Default action: set chunk_size = 100806",  // PG 25MB/248B = 100806 rows (#166 per-target anchor)
		"per-target optimum for postgres",          // names the source of the anchor
		"hard cap, not a target",                   // makes ceiling-as-cap framing explicit
		"silently auto-clamp",                      // consequence framing
		"doubling workers OR doubling",             // scaling rule still present
		"Platform=wsl2",
	}
	for _, m := range musts {
		if !strings.Contains(got, m) {
			t.Errorf("memory budget block missing %q; got:\n%s", m, got)
		}
	}

	// User cap should narrow the budget when smaller than (available - 2GB).
	inCap := in
	inCap.MaxMemoryMB = 8000
	gotCap := buildMemoryBudgetBlock(inCap, 16, pg)
	if !strings.Contains(gotCap, "Memory budget: 8000 MB") {
		t.Errorf("user-cap budget should win when smaller; got:\n%s", gotCap)
	}
	if !strings.Contains(gotCap, "user max_memory_mb=8000") {
		t.Errorf("missing user-cap source annotation; got:\n%s", gotCap)
	}

	// Linux platform should not include the WSL2 caveat.
	inLin := in
	inLin.Platform = "linux"
	gotLin := buildMemoryBudgetBlock(inLin, 16, pg)
	if strings.Contains(gotLin, "Platform=wsl2") {
		t.Errorf("Linux platform should not render the WSL2 caveat; got:\n%s", gotLin)
	}

	// Ceiling-below-50000 branch (PR #163 review): when the safe ceiling is
	// below the 50000 baseline default, "Default action" must use the ceiling
	// and explain that 50000 does not fit. With budget=512 MB, baseline=16,
	// avg=248 → ceiling = 512*1MiB/(16*6*248) ≈ 22550 (computeSafeChunkSize).
	inTight := AutoTuneInput{
		Platform:          "linux",
		CPUCores:          18,
		MemoryGB:          1,
		AvailableMemoryMB: 0,
		MaxMemoryMB:       512,
		AvgRowBytes:       248,
		TargetType:        "postgres",
	}
	gotTight := buildMemoryBudgetBlock(inTight, 16, pg)
	tightMusts := []string{
		"Memory budget: 512 MB",
		"safe chunk_size ceiling is ~22550 rows",
		"Default action: set chunk_size = 22550",
		"per-target optimum 100806 does not fit", // PG 25MB/248B = 100806 (#166)
		"Picking a larger value is forbidden",
	}
	for _, m := range tightMusts {
		if !strings.Contains(gotTight, m) {
			t.Errorf("tight-budget block missing %q; got:\n%s", m, gotTight)
		}
	}
}

// TestBuildMemoryBudgetBlockFallback locks in the issue #158 fix: when
// AvailableMemoryMB <= 0 (offline callers, failed system probe), the helper
// must NOT emit "Memory budget: 0 MB". User cap is authoritative when set;
// otherwise a sensible default kicks in.
func TestBuildMemoryBudgetBlockFallback(t *testing.T) {
	cases := []struct {
		name        string
		in          AutoTuneInput
		wantBudget  string
		wantSource  string
		notContains []string // substrings that must NOT appear
	}{
		{
			name: "available=0 + cap=2000 → cap wins",
			in: AutoTuneInput{
				AvailableMemoryMB: 0,
				MaxMemoryMB:       2000,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 2000 MB",
			wantSource: "user max_memory_mb=2000",
		},
		{
			name: "available=0 + cap=0 → fallback default, NOT 0",
			in: AutoTuneInput{
				AvailableMemoryMB: 0,
				MaxMemoryMB:       0,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 1024 MB",
			wantSource: "fallback default",
			notContains: []string{
				"Memory budget: 0 MB",
				"safe chunk_size ceiling is ~0 rows",
			},
		},
		{
			// Pre-fix this hit the bare "fallback default" branch via the
			// `available > 2048` guard. After the floor refactor it routes
			// through the headroom path and reports the floor explicitly.
			// Same budget value (1024 MB), more honest source label.
			name: "available=1000 (below 2GB headroom) + cap=0 → floored",
			in: AutoTuneInput{
				AvailableMemoryMB: 1000,
				MaxMemoryMB:       0,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 1024 MB",
			wantSource: "floored at 1024",
		},
		{
			name: "available=30000 + cap=0 → headroom budget",
			in: AutoTuneInput{
				AvailableMemoryMB: 30000,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 27952 MB",
			wantSource: "available_memory_mb=30000",
		},
		{
			name: "available=30000 + cap=2000 → cap wins (more restrictive)",
			in: AutoTuneInput{
				AvailableMemoryMB: 30000,
				MaxMemoryMB:       2000,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 2000 MB",
			wantSource: "user max_memory_mb=2000",
		},
		{
			name: "available=30000 + cap=50000 → headroom wins (cap is looser)",
			in: AutoTuneInput{
				AvailableMemoryMB: 30000,
				MaxMemoryMB:       50000,
				AvgRowBytes:       248,
			},
			wantBudget: "Memory budget: 27952 MB",
			wantSource: "available_memory_mb=30000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildMemoryBudgetBlock(tc.in, 16, nil) // nil target → 10 MB conservative fallback (#166)
			if !strings.Contains(got, tc.wantBudget) {
				t.Errorf("missing %q; got:\n%s", tc.wantBudget, got)
			}
			if !strings.Contains(got, tc.wantSource) {
				t.Errorf("missing source annotation %q; got:\n%s", tc.wantSource, got)
			}
			for _, bad := range tc.notContains {
				if strings.Contains(got, bad) {
					t.Errorf("must not contain %q; got:\n%s", bad, got)
				}
			}
		})
	}
}

// TestFormatHistoricalContextNoBoundingWhenSmall verifies that the trajectory
// header drops the "BOUNDED — X of Y" qualifier when all completed runs fit
// within trajectoryLimit. The bounded-warning text is also absent in that case
// (no need to disambiguate two views when they're the same).
func TestFormatHistoricalContextNoBoundingWhenSmall(t *testing.T) {
	hist := make([]AITuningRecord, 0, 5)
	base := time.Now()
	for i := 0; i < 5; i++ {
		hist = append(hist, AITuningRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			Timestamp:         base.Add(time.Duration(i) * time.Minute),
			WriteAheadWriters: 1,
			ChunkSize:         50000,
			Workers:           16,
			ReadAheadBuffers:  4,
			ParallelReaders:   4,
			MaxPartitions:     16,
			FinalThroughput:   1000000,
			FinalDurationSecs: 20,
			ChunkRetryCount:   0,
		})
	}
	mock := &mockHistoryProvider{history: hist}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock, dbType: "mssql", targetDBType: "postgres"}
	ctx := analyzer.formatHistoricalContext()

	// Header should NOT have the BOUNDED qualifier — 5 ≤ trajectoryLimit (20).
	if strings.Contains(ctx, "BOUNDED — most recent") {
		t.Errorf("trajectory header should not be marked BOUNDED when history fits within trajectoryLimit\nfull context:\n%s", ctx)
	}
	wantHeader := "PARAMETER TRAJECTORY (most recent 5 analyses, oldest first)"
	if !strings.Contains(ctx, wantHeader) {
		t.Errorf("trajectory header missing %q\nfull context:\n%s", wantHeader, ctx)
	}
	// And the disambiguation warning is irrelevant when there's no bounding.
	if strings.Contains(ctx, "Do NOT count waw values") {
		t.Errorf("disambiguation warning should not appear when history fits within trajectoryLimit\nfull context:\n%s", ctx)
	}
}

func TestFormatHistoricalContextIncludesRegimeApplicability(t *testing.T) {
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	mock := &mockHistoryProvider{
		history: []AITuningRecord{
			{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				Timestamp:         base.Add(time.Minute),
				TotalTables:       10,
				TotalRows:         1000000,
				CPUCores:          18,
				MemoryGB:          30,
				Workers:           16,
				ChunkSize:         50000,
				ReadAheadBuffers:  4,
				WriteAheadWriters: 2,
				ParallelReaders:   4,
				MaxPartitions:     16,
				FinalThroughput:   1100000,
				FinalDurationSecs: 20,
				ChunkRetryCount:   0,
			},
			{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				Timestamp:         base,
				TotalTables:       10,
				TotalRows:         1000000,
				CPUCores:          14,
				MemoryGB:          9,
				Workers:           12,
				ChunkSize:         50000,
				ReadAheadBuffers:  4,
				WriteAheadWriters: 1,
				ParallelReaders:   4,
				MaxPartitions:     12,
				FinalThroughput:   900000,
				FinalDurationSecs: 24,
				ChunkRetryCount:   0,
			},
		},
	}
	analyzer := &SmartConfigAnalyzer{historyProvider: mock, dbType: "mssql", targetDBType: "postgres"}
	input := AutoTuneInput{
		Platform:          "wsl2",
		CPUCores:          18,
		MemoryGB:          30,
		AvailableMemoryMB: 28000,
	}
	currentTuning := DBTuningSnapshot{
		TargetSharedBuffersMB: 8192,
		TargetSyncCommit:      "off",
		TargetFsync:           "on",
		TargetMaxWALSizeMB:    16384,
		TargetWALLevel:        "replica",
	}
	ctx := analyzer.formatHistoricalContextForInput(&input, currentTuning)

	musts := []string{
		"HISTORY APPLICABILITY:",
		"Treat historical throughput as regime-specific, not universal",
		"Current run host regime: platform=wsl2, cpu_cores=18, memory_gb=30, available_memory_mb=28000, max_memory_mb=none.",
		"Current run effective DB tuning: pg{shared_buffers=8192MB, sync_commit=off, fsync=on, max_wal=16384MB, wal_level=replica}.",
		"target shared_buffers, max_wal_size, synchronous_commit, source max server memory",
		"prefer the baseline and state that history may be out-of-regime",
		// Both fixture rows have unknown DB tuning (zero/empty), so the
		// classifier reports "different_hw" or "same_regime" based on
		// hardware alone.
		"mssql->postgres (2026-05-08, 10 tables, 1.0M rows, cpu_cores=14, memory_gb=9, tuning=unknown, regime=different_hw [hw: history=14c/9GB vs current=18c/30GB])",
		"mssql->postgres (2026-05-08, 10 tables, 1.0M rows, cpu_cores=18, memory_gb=30, tuning=unknown, regime=same_regime)",
	}
	for _, m := range musts {
		if !strings.Contains(ctx, m) {
			t.Errorf("historical context missing regime-applicability substring %q\nfull context:\n%s", m, ctx)
		}
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
			totalCompleted := 0
			for _, a := range aggs {
				totalCompleted += a.TotalRuns
			}
			result := formatWawAggregateBlock(aggs, totalCompleted)
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

// TestClassifyRegimeDeltas exercises the multi-axis regime classifier added
// alongside Codex's #144 first cut. It covers all four label paths
// (same_regime, different_hw, different_tuning, different_hw_and_tuning,
// unknown) plus the deltas list contents.
func TestClassifyRegimeDeltas(t *testing.T) {
	current := AutoTuneInput{CPUCores: 18, MemoryGB: 30}
	currentTuning := DBTuningSnapshot{
		TargetSharedBuffersMB: 8192,
		TargetSyncCommit:      "off",
		TargetFsync:           "on",
		TargetMaxWALSizeMB:    16384,
		TargetWALLevel:        "replica",
	}

	cases := []struct {
		name      string
		history   AITuningRecord
		wantLabel string
		wantInOne []string // every substring must appear in at least one delta
	}{
		{
			name: "same hardware and same tuning",
			history: AITuningRecord{
				CPUCores: 18, MemoryGB: 30,
				TargetSharedBuffersMB: 8192, TargetSyncCommit: "off",
				TargetFsync: "on", TargetMaxWALSizeMB: 16384, TargetWALLevel: "replica",
			},
			wantLabel: "same_regime",
		},
		{
			name: "same hardware, fsync flipped",
			history: AITuningRecord{
				CPUCores: 18, MemoryGB: 30,
				TargetSharedBuffersMB: 8192, TargetSyncCommit: "off",
				TargetFsync: "off", TargetMaxWALSizeMB: 16384, TargetWALLevel: "replica",
			},
			wantLabel: "different_tuning",
			wantInOne: []string{"pg_fsync: off→on"},
		},
		{
			name: "same hardware, smaller PG buffers",
			history: AITuningRecord{
				CPUCores: 18, MemoryGB: 30,
				TargetSharedBuffersMB: 1024, TargetSyncCommit: "off",
				TargetFsync: "on", TargetMaxWALSizeMB: 16384, TargetWALLevel: "replica",
			},
			wantLabel: "different_tuning",
			wantInOne: []string{"pg_shared_buffers: 1024MB→8192MB"},
		},
		{
			name: "different hardware, same tuning",
			history: AITuningRecord{
				CPUCores: 8, MemoryGB: 16,
				TargetSharedBuffersMB: 8192, TargetSyncCommit: "off",
				TargetFsync: "on", TargetMaxWALSizeMB: 16384, TargetWALLevel: "replica",
			},
			wantLabel: "different_hw",
			wantInOne: []string{"hw: history=8c/16GB vs current=18c/30GB"},
		},
		{
			name: "different hardware AND tuning",
			history: AITuningRecord{
				CPUCores: 8, MemoryGB: 16,
				TargetSharedBuffersMB: 1024, TargetFsync: "off",
			},
			wantLabel: "different_hw_and_tuning",
			wantInOne: []string{"hw: history=8c/16GB", "pg_fsync: off→on"},
		},
		{
			name:      "unknown when history hardware missing",
			history:   AITuningRecord{CPUCores: 0, MemoryGB: 0, TargetFsync: "off"},
			wantLabel: "unknown",
		},
		{
			name: "small PG buffer diff within tolerance is NOT a regime change",
			history: AITuningRecord{
				CPUCores: 18, MemoryGB: 30,
				TargetSharedBuffersMB: 8000, // 192MB diff (~2.3%) within 10% tolerance
				TargetSyncCommit:      "off",
				TargetFsync:           "on", TargetMaxWALSizeMB: 16384, TargetWALLevel: "replica",
			},
			wantLabel: "same_regime",
		},
		{
			name: "history with empty tuning fields skips them (treats as unknown, not mismatch)",
			history: AITuningRecord{
				CPUCores: 18, MemoryGB: 30,
				// No tuning fields at all (pre-#144 row)
			},
			wantLabel: "same_regime",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, deltas := classifyRegime(tc.history, current, currentTuning)
			if label != tc.wantLabel {
				t.Errorf("label = %q, want %q (deltas: %v)", label, tc.wantLabel, deltas)
			}
			for _, want := range tc.wantInOne {
				found := false
				for _, d := range deltas {
					if strings.Contains(d, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected a delta containing %q; got %v", want, deltas)
				}
			}
		})
	}
}

// TestComputeEstimatedMemMB locks in the formula advertised in the smartconfig
// prompt: workers * (read_ahead_buffers + write_ahead_writers) * chunk_size *
// avg_row_bytes / 1024 / 1024. The third case mirrors the SO2013 run #3
// reproduction in issue #153, where the LLM returned 343 MB.
func TestComputeEstimatedMemMB(t *testing.T) {
	tests := []struct {
		name              string
		workers, raw, waw int
		chunkSize         int
		avgRowBytes       int64
		want              int64
	}{
		{"so2010-typical", 6, 4, 2, 50000, 509, 873},
		{"so2013-waw1", 6, 4, 1, 50000, 573, 819},
		{"so2013-waw2-issue-153", 6, 4, 2, 50000, 573, 983},
		{"zero-workers", 0, 4, 2, 50000, 500, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeEstimatedMemMB(tc.workers, tc.raw, tc.waw, tc.chunkSize, tc.avgRowBytes)
			if got != tc.want {
				t.Errorf("ComputeEstimatedMemMB(%d, %d, %d, %d, %d) = %d, want %d",
					tc.workers, tc.raw, tc.waw, tc.chunkSize, tc.avgRowBytes, got, tc.want)
			}
		})
	}
}

// TestApplyAISuggestionsComputesMemoryFromParams verifies that
// applyAISuggestions populates EstimatedMemMB from the formula on the LLM's
// chosen params. Locks in the fix from issue #153 and the follow-up that
// removed `estimated_memory_mb` from the prompt schema entirely — the LLM no
// longer outputs a memory estimate, so the value must come from the formula.
func TestApplyAISuggestionsComputesMemoryFromParams(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{
		suggestions: &SmartConfigSuggestions{},
	}
	ai := &AutoTuneOutput{
		Workers:           6,
		ChunkSize:         50000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	}
	input := AutoTuneInput{AvgRowBytes: 573}

	analyzer.applyAISuggestions(ai, input)

	const want = int64(983)
	if got := analyzer.suggestions.EstimatedMemMB; got != want {
		t.Errorf("EstimatedMemMB = %d, want %d (must be computed from params, not from any LLM-supplied field)", got, want)
	}
}

// TestApplyDefaultSuggestionsUsesBufferSum verifies the fallback formula uses
// (read_ahead_buffers + write_ahead_writers) instead of a hard-coded 4 — keeping
// the no-AI path consistent with the AI path and the prompt's stated formula.
func TestApplyDefaultSuggestionsUsesBufferSum(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{
		suggestions: &SmartConfigSuggestions{},
	}
	input := AutoTuneInput{
		CPUCores:    8, // → workers = 6
		AvgRowBytes: 500,
	}

	analyzer.applyDefaultSuggestions(input)

	// Defaults: workers=6, read_ahead_buffers=4, write_ahead_writers=2,
	// chunk_size=50000, avg_row_bytes=500. Formula: 6 * 6 * 50000 * 500 / 1024 / 1024 = 858 MB.
	const want = int64(858)
	if got := analyzer.suggestions.EstimatedMemMB; got != want {
		t.Errorf("EstimatedMemMB = %d, want %d", got, want)
	}
}

// TestEffectiveMemoryBudgetMB locks in the priority order shared by the
// prompt's "Memory budget" block AND the post-AI clamp (issue #156): user
// MaxMemoryMB > available - 2GB headroom > 1024 MB last-resort default.
// These two callers MUST agree on the budget number, else the LLM is told
// one ceiling and the clamp enforces a different one.
func TestEffectiveMemoryBudgetMB(t *testing.T) {
	cases := []struct {
		name       string
		in         AutoTuneInput
		wantBudget int64
		wantSrcSub string // substring expected in the source description
	}{
		{
			name:       "cap < available-headroom → cap wins",
			in:         AutoTuneInput{AvailableMemoryMB: 30000, MaxMemoryMB: 2000},
			wantBudget: 2000,
			wantSrcSub: "user max_memory_mb=2000",
		},
		{
			name:       "cap > available-headroom → headroom wins",
			in:         AutoTuneInput{AvailableMemoryMB: 30000, MaxMemoryMB: 50000},
			wantBudget: 27952,
			wantSrcSub: "available_memory_mb=30000",
		},
		{
			name:       "no cap → headroom",
			in:         AutoTuneInput{AvailableMemoryMB: 30000},
			wantBudget: 27952,
			wantSrcSub: "available_memory_mb=30000",
		},
		{
			name:       "available unset, cap set → cap (issue #158 fix)",
			in:         AutoTuneInput{AvailableMemoryMB: 0, MaxMemoryMB: 2000},
			wantBudget: 2000,
			wantSrcSub: "user max_memory_mb=2000",
		},
		{
			name:       "both unset → fallback default, NOT 0",
			in:         AutoTuneInput{},
			wantBudget: 1024,
			wantSrcSub: "fallback default",
		},
		{
			name:       "available below 2GB headroom + no cap → floored at fallback",
			in:         AutoTuneInput{AvailableMemoryMB: 1000},
			wantBudget: 1024,
			wantSrcSub: "floored",
		},
		// Regression: monotonicity around the 2 GB headroom threshold.
		// Previously available=2049 fell into the headroom branch and yielded
		// budget=1 (available - 2048), while available=2048 hit the fallback
		// and yielded budget=1024 — i.e. less RAM produced a bigger budget.
		// Reproduced on WSL2 (7.7 GB host) where back-to-back SO2010 runs
		// crossed the cliff and the second run got chunk_size clamped from
		// 50K to ~19.5K despite the first run proving the larger value fit.
		{
			name:       "available just above headroom threshold → floored, not cliff",
			in:         AutoTuneInput{AvailableMemoryMB: 2049},
			wantBudget: 1024,
			wantSrcSub: "floored",
		},
		{
			// With cap=2000 and available=2049, raw headroom is 1 MB; floored
			// to 1024. Floored headroom (1024) is more restrictive than the
			// cap (2000), so floored headroom wins per the min-of-ceilings
			// rule. Pre-fix this returned 1 MB.
			name:       "available just above headroom + cap → floor wins over cap",
			in:         AutoTuneInput{AvailableMemoryMB: 2049, MaxMemoryMB: 2000},
			wantBudget: 1024,
			wantSrcSub: "floored",
		},
		{
			name:       "available exactly 3072 → headroom equals floor (1024)",
			in:         AutoTuneInput{AvailableMemoryMB: 3072},
			wantBudget: 1024,
			wantSrcSub: "floored",
		},
		{
			name:       "available 4096 → headroom 2048 wins over floor",
			in:         AutoTuneInput{AvailableMemoryMB: 4096},
			wantBudget: 2048,
			wantSrcSub: "available_memory_mb=4096",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotBudget, gotSrc := effectiveMemoryBudgetMB(tc.in)
			if gotBudget != tc.wantBudget {
				t.Errorf("budget = %d MB, want %d", gotBudget, tc.wantBudget)
			}
			if !strings.Contains(gotSrc, tc.wantSrcSub) {
				t.Errorf("source = %q, want substring %q", gotSrc, tc.wantSrcSub)
			}
		})
	}
}

// TestClampChunkSizeToBudget locks in issue #156 enforcement: when the
// LLM's chosen params exceed the memory budget, chunk_size is clamped to
// fit and EstimatedMemMB is recomputed. The clamp persists into the
// suggestions struct so AI tuning history records the post-clamp value.
func TestClampChunkSizeToBudget(t *testing.T) {
	t.Run("clamps over-budget AI choice (cap=200 vs Sonnet 35K)", func(t *testing.T) {
		// Real reproduction from the issue #156 sweep: at cap=200 the AI
		// picked chunk_size=35000 → 794 MB → 297% over cap. Safe ceiling
		// at workers=16, buffers=4+2=6, avg=248 is ~8808 rows.
		analyzer := &SmartConfigAnalyzer{
			suggestions: &SmartConfigSuggestions{
				Workers:                 16,
				ChunkSizeRecommendation: 35000,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				EstimatedMemMB:          794, // pre-clamp value matching the LLM's choice
			},
		}
		input := AutoTuneInput{
			AvailableMemoryMB: 30000,
			MaxMemoryMB:       200,
			AvgRowBytes:       248,
		}

		analyzer.clampChunkSizeToBudget(input)

		// Safe ceiling: 200 * 1MiB / (16*6*248) = 8808 rows.
		const wantChunk = 8808
		if got := analyzer.suggestions.ChunkSizeRecommendation; got != wantChunk {
			t.Errorf("ChunkSizeRecommendation = %d, want %d (the safe ceiling at this budget; clamp didn't fire if got==35000)", got, wantChunk)
		}
		// EstimatedMemMB must be recomputed from the clamped chunk_size, not
		// left at the pre-clamp 794 MB. Otherwise the AI tuning history would
		// record a memory value inconsistent with the actual params.
		const wantMem = 199 // 16*6*8808*248 / 1MiB = ~199 MB; just under cap
		if got := analyzer.suggestions.EstimatedMemMB; got != wantMem {
			t.Errorf("EstimatedMemMB = %d MB, want %d (recomputed from clamped chunk_size)", got, wantMem)
		}
	})

	t.Run("no-op when AI choice already fits", func(t *testing.T) {
		// Default chunk=50000 at workers=16, buffers=6, avg=248 → 1135 MB.
		// Cap=2000 leaves room. Clamp must not trigger.
		analyzer := &SmartConfigAnalyzer{
			suggestions: &SmartConfigSuggestions{
				Workers:                 16,
				ChunkSizeRecommendation: 50000,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				EstimatedMemMB:          1135,
			},
		}
		input := AutoTuneInput{
			AvailableMemoryMB: 30000,
			MaxMemoryMB:       2000,
			AvgRowBytes:       248,
		}

		analyzer.clampChunkSizeToBudget(input)
		if got := analyzer.suggestions.ChunkSizeRecommendation; got != 50000 {
			t.Errorf("ChunkSizeRecommendation modified to %d; should be unchanged at 50000 (clamp must be a no-op when 1135 MB <= 2000 MB budget)", got)
		}
		if got := analyzer.suggestions.EstimatedMemMB; got != 1135 {
			t.Errorf("EstimatedMemMB modified to %d; should be unchanged at 1135", got)
		}
	})

	t.Run("no-op when no cap and plenty of headroom", func(t *testing.T) {
		// Common path: user did not set max_memory_mb, system has 30 GB.
		// Default chunk=50000 → 1135 MB; budget = 30000 - 2048 = 27952. Fits.
		analyzer := &SmartConfigAnalyzer{
			suggestions: &SmartConfigSuggestions{
				Workers:                 16,
				ChunkSizeRecommendation: 50000,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				EstimatedMemMB:          1135,
			},
		}
		input := AutoTuneInput{AvailableMemoryMB: 30000, AvgRowBytes: 248}

		analyzer.clampChunkSizeToBudget(input)
		if got := analyzer.suggestions.ChunkSizeRecommendation; got != 50000 {
			t.Errorf("ChunkSizeRecommendation = %d; clamp must be a no-op when no cap is set and headroom is plenty", got)
		}
	})

	t.Run("refuses to clamp to zero", func(t *testing.T) {
		// Pathological: zero workers means computeSafeChunkSize returns 0.
		// Clamping to 0 would block the migration entirely; better to leave
		// the original chunk and let the migration fail loudly elsewhere.
		analyzer := &SmartConfigAnalyzer{
			suggestions: &SmartConfigSuggestions{
				Workers:                 0, // pathological
				ChunkSizeRecommendation: 50000,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				EstimatedMemMB:          5000, // arbitrary high value
			},
		}
		input := AutoTuneInput{
			AvailableMemoryMB: 30000,
			MaxMemoryMB:       100,
			AvgRowBytes:       248,
		}

		analyzer.clampChunkSizeToBudget(input)
		if got := analyzer.suggestions.ChunkSizeRecommendation; got != 50000 {
			t.Errorf("ChunkSizeRecommendation = %d; should be unchanged at 50000 to avoid blocking the migration", got)
		}
	})

	t.Run("clamp gate uses bytes, not floor-MB (Copilot review fix)", func(t *testing.T) {
		// Pre-fix: EstimatedMemMB used integer division to MB, so a real
		// footprint of e.g. 200.9 MiB would render as 200 MB and slide
		// under a 200 MB cap. Post-fix: gate compares bytes directly.
		//
		// Construct params where bytes / 1024 / 1024 truncates from
		// strictly-over-cap to exactly-at-cap. With workers=1, buffers=1,
		// avg=1024, chunk=205200: bytes = 1*1*205200*1024 = 210,124,800 =
		// ~200.4 MiB; floor MB = 200. At a 200 MB cap, the floor-MB
		// comparison would say "200 <= 200" and skip; the bytes comparison
		// correctly says "210124800 > 209715200" and clamps.
		analyzer := &SmartConfigAnalyzer{
			suggestions: &SmartConfigSuggestions{
				Workers:                 1,
				ChunkSizeRecommendation: 205200,
				ReadAheadBuffers:        1,
				WriteAheadWriters:       0,
				EstimatedMemMB:          200, // ComputeEstimatedMemMB floors to 200
			},
		}
		input := AutoTuneInput{
			AvailableMemoryMB: 30000,
			MaxMemoryMB:       200,
			AvgRowBytes:       1024,
		}

		analyzer.clampChunkSizeToBudget(input)
		if got := analyzer.suggestions.ChunkSizeRecommendation; got >= 205200 {
			t.Errorf("ChunkSizeRecommendation = %d; expected clamp to fire on 210MB > 200MB cap (bytes comparison, not floor-MB)", got)
		}
	})
}

// TestApplyAISuggestionsClampsOverBudget verifies the clamp is wired into
// applyAISuggestions, not just the standalone helper. Reproduces the
// cap=500 + Sonnet failure mode from issue #156: AI returns chunk=35000
// (794 MB), Go clamps to fit the 500 MB cap.
func TestApplyAISuggestionsClampsOverBudget(t *testing.T) {
	analyzer := &SmartConfigAnalyzer{
		suggestions: &SmartConfigSuggestions{},
	}
	ai := &AutoTuneOutput{
		Workers:           16,
		ChunkSize:         35000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ParallelReaders:   4,
	}
	input := AutoTuneInput{
		AvailableMemoryMB: 30000,
		MaxMemoryMB:       500,
		AvgRowBytes:       248,
	}

	analyzer.applyAISuggestions(ai, input)

	// Safe ceiling at 500 MB budget: 500 * 1MiB / (16*6*248) = 22021 rows.
	const wantChunk = 22021
	if got := analyzer.suggestions.ChunkSizeRecommendation; got != wantChunk {
		t.Errorf("ChunkSizeRecommendation = %d, want %d (clamped from AI's 35000)", got, wantChunk)
	}
	// Recomputed memory should be just under the cap.
	if got := analyzer.suggestions.EstimatedMemMB; got > 500 {
		t.Errorf("EstimatedMemMB = %d MB, want <= 500 (post-clamp must fit cap)", got)
	}
}
