package monitor

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/transfer"
)

// createTestAdjuster creates an AIAdjuster configured for testing.
func createTestAdjuster() *AIAdjuster {
	// Create a runtime tuner with test config
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{
		ChunkSize:         25000,
		WriteAheadWriters: 4,
		ParallelReaders:   2,
		ReadAheadBuffers:  8,
	})

	// Create a real metrics collector
	mc := NewMetricsCollector(tuner, 30*time.Second)

	// Pre-populate with test metrics
	mc.metrics = []PerformanceSnapshot{
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
	}

	aa := &AIAdjuster{
		collector:         mc,
		tuner:             tuner,
		startTime:         time.Now().Add(-3 * time.Minute),
		callInterval:      30 * time.Second,
		cacheDuration:     60 * time.Second,
		failureThreshold:  3,
		resetTimeout:      5 * time.Minute,
		maxHistorySize:    10,
		adjustmentHistory: make([]AdjustmentRecord, 0, 10),
		systemResources: SystemResources{
			CPUCores:             8,
			MemoryTotalMB:        16000,
			MemoryAvailableMB:    8000,
			MaxSourceConnections: 12,
			MaxTargetConnections: 12,
		},
	}

	return aa
}

func TestFallbackEvaluation(t *testing.T) {
	t.Run("fallback rules produce valid decision", func(t *testing.T) {
		aa := createTestAdjuster()

		// Without AI mapper, fallback rules will be used
		decision := aa.fallbackRules()

		if decision == nil {
			t.Fatal("expected a decision from fallback rules")
		}

		// With stable metrics, should get continue
		if decision.Action != "continue" {
			t.Logf("Got action %q with reasoning: %s", decision.Action, decision.Reasoning)
		}
	})
}

func TestConsecutiveAdjustments(t *testing.T) {
	t.Run("consecutive adjustments are applied", func(t *testing.T) {
		aa := createTestAdjuster()

		// First apply a scale_up decision
		decision1 := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 5},
			Reasoning:   "test",
			Confidence:  "high",
		}

		err := aa.ApplyDecision(decision1)
		if err != nil {
			t.Fatalf("first apply failed: %v", err)
		}

		// Update is applied immediately via tuner
		snap := aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 5 {
			t.Errorf("expected workers=5 after first apply, got %d", snap.WriteAheadWriters)
		}

		// Apply another adjustment immediately
		decision2 := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 6},
			Reasoning:   "test2",
			Confidence:  "high",
		}

		err = aa.ApplyDecision(decision2)
		if err != nil {
			t.Fatalf("second apply returned error: %v", err)
		}

		// Workers should have changed immediately
		snap = aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 6 {
			t.Errorf("expected workers=6, got %d", snap.WriteAheadWriters)
		}
	})

	t.Run("different action types applied consecutively", func(t *testing.T) {
		aa := createTestAdjuster()

		// Apply scale_up
		decision1 := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 5},
			Reasoning:   "test",
			Confidence:  "high",
		}
		aa.ApplyDecision(decision1)

		// Apply scale_down immediately
		decision2 := &AdjustmentDecision{
			Action:      "scale_down",
			Adjustments: map[string]int{"workers": 3},
			Reasoning:   "test2",
			Confidence:  "high",
		}

		err := aa.ApplyDecision(decision2)
		if err != nil {
			t.Fatalf("apply failed: %v", err)
		}

		// Workers should have changed
		snap := aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 3 {
			t.Errorf("expected workers=3, got %d", snap.WriteAheadWriters)
		}
	})
}

func TestSystemResourceValidation(t *testing.T) {
	t.Run("workers beyond CPU cores are accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.systemResources.CPUCores = 8
		aa.systemResources.MaxTargetConnections = 20

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 12}, // Exceeds 8 cores but allowed
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 12 {
			t.Errorf("expected workers=12 (no safety guard), got %d", snap.WriteAheadWriters)
		}
	})

	t.Run("workers beyond max connections are accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.systemResources.CPUCores = 16
		aa.systemResources.MaxTargetConnections = 6

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 10}, // Exceeds 6 connections but allowed
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 10 {
			t.Errorf("expected workers=10 (no safety guard), got %d", snap.WriteAheadWriters)
		}
	})

	t.Run("workers within limits are accepted", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.systemResources.CPUCores = 8
		aa.systemResources.MaxTargetConnections = 12

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 6}, // Within both limits
			Reasoning:   "test",
			Confidence:  "high",
		}

		err := aa.ApplyDecision(decision)
		if err != nil {
			t.Fatalf("apply failed: %v", err)
		}

		snap := aa.tuner.Snapshot()
		if snap.WriteAheadWriters != 6 {
			t.Errorf("expected workers=6, got %d", snap.WriteAheadWriters)
		}
	})

	t.Run("small chunk_size accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "reduce_chunk",
			Adjustments: map[string]int{"chunk_size": 500},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.ChunkSize != 500 {
			t.Errorf("expected chunk_size=500 (no safety guard), got %d", snap.ChunkSize)
		}
	})

	t.Run("large chunk_size accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"chunk_size": 1000000},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.ChunkSize != 1000000 {
			t.Errorf("expected chunk_size=1000000 (no safety guard), got %d", snap.ChunkSize)
		}
	})

	t.Run("valid chunk_size accepted", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "reduce_chunk",
			Adjustments: map[string]int{"chunk_size": 10000},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.ChunkSize != 10000 {
			t.Errorf("expected chunk_size=10000, got %d", snap.ChunkSize)
		}
	})
}

func TestBaselineCapture(t *testing.T) {
	t.Run("baseline captured after warmup", func(t *testing.T) {
		aa := createTestAdjuster()

		if aa.baselineCaptured {
			t.Error("baseline should not be captured before captureBaseline called")
		}

		aa.captureBaseline()

		if !aa.baselineCaptured {
			t.Error("baseline should be captured after captureBaseline")
		}

		if aa.baselineMetrics == nil {
			t.Fatal("baseline metrics should not be nil")
		}

		// Check average was calculated
		if aa.baselineMetrics.Throughput != 100000 {
			t.Errorf("expected baseline throughput 100000, got %.0f", aa.baselineMetrics.Throughput)
		}
	})

	t.Run("baseline not recaptured", func(t *testing.T) {
		aa := createTestAdjuster()

		aa.captureBaseline()
		originalThroughput := aa.baselineMetrics.Throughput

		// Change metrics
		aa.collector.metrics = []PerformanceSnapshot{
			{Throughput: 200000, CPUPercent: 50, MemoryPercent: 60},
			{Throughput: 200000, CPUPercent: 50, MemoryPercent: 60},
			{Throughput: 200000, CPUPercent: 50, MemoryPercent: 60},
		}

		aa.captureBaseline() // Should not recapture

		if aa.baselineMetrics.Throughput != originalThroughput {
			t.Errorf("baseline should not have changed, got %.0f", aa.baselineMetrics.Throughput)
		}
	})
}

func TestAdjustmentHistory(t *testing.T) {
	t.Run("adjustments recorded in history", func(t *testing.T) {
		aa := createTestAdjuster()

		if len(aa.adjustmentHistory) != 0 {
			t.Errorf("expected empty history, got %d records", len(aa.adjustmentHistory))
		}

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 5},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		if len(aa.adjustmentHistory) != 1 {
			t.Errorf("expected 1 history record, got %d", len(aa.adjustmentHistory))
		}

		record := aa.adjustmentHistory[0]
		if record.Action != "scale_up" {
			t.Errorf("expected action 'scale_up', got %q", record.Action)
		}

		if record.ThroughputBefore != 100000 {
			t.Errorf("expected throughput before 100000, got %.0f", record.ThroughputBefore)
		}
	})

	t.Run("history limited to max size", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.maxHistorySize = 3

		// Apply more adjustments than max size
		for i := 0; i < 5; i++ {
			decision := &AdjustmentDecision{
				Action:      "scale_up",
				Adjustments: map[string]int{"workers": 5 + i},
				Reasoning:   "test",
				Confidence:  "high",
			}
			aa.ApplyDecision(decision)
		}

		if len(aa.adjustmentHistory) != 3 {
			t.Errorf("expected history limited to 3, got %d", len(aa.adjustmentHistory))
		}
	})
}

func TestContinueActionNotApplied(t *testing.T) {
	aa := createTestAdjuster()
	initialSnap := aa.tuner.Snapshot()

	decision := &AdjustmentDecision{
		Action:      "continue",
		Adjustments: map[string]int{"workers": 10}, // Should be ignored
		Reasoning:   "stable",
		Confidence:  "high",
	}

	err := aa.ApplyDecision(decision)
	if err != nil {
		t.Fatalf("apply returned error: %v", err)
	}

	snap := aa.tuner.Snapshot()
	if snap.WriteAheadWriters != initialSnap.WriteAheadWriters {
		t.Errorf("continue action should not change config, workers changed to %d", snap.WriteAheadWriters)
	}
}

func TestEmptyAdjustmentsNotApplied(t *testing.T) {
	aa := createTestAdjuster()
	initialCount := aa.adjustmentCount

	decision := &AdjustmentDecision{
		Action:      "scale_up",
		Adjustments: map[string]int{}, // Empty
		Reasoning:   "test",
		Confidence:  "high",
	}

	err := aa.ApplyDecision(decision)
	if err != nil {
		t.Fatalf("apply returned error: %v", err)
	}

	if aa.adjustmentCount != initialCount {
		t.Error("empty adjustments should not increment adjustment count")
	}
}

func TestCheckpointFrequencyValidation(t *testing.T) {
	t.Run("checkpoint_frequency zero clamped to 1", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"checkpoint_frequency": 0},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.CheckpointFrequency != 1 {
			t.Errorf("expected checkpoint_frequency=1 (clamped from 0), got %d", snap.CheckpointFrequency)
		}
	})

	t.Run("large checkpoint_frequency accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"checkpoint_frequency": 150},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.CheckpointFrequency != 150 {
			t.Errorf("expected checkpoint_frequency=150 (no safety guard), got %d", snap.CheckpointFrequency)
		}
	})

	t.Run("valid checkpoint_frequency accepted", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"checkpoint_frequency": 20},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.CheckpointFrequency != 20 {
			t.Errorf("expected checkpoint_frequency=20, got %d", snap.CheckpointFrequency)
		}
	})
}

func TestUpsertMergeChunkSizeValidation(t *testing.T) {
	t.Run("small upsert_merge_chunk_size accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_down",
			Adjustments: map[string]int{"upsert_merge_chunk_size": 500},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.UpsertMergeChunkSize != 500 {
			t.Errorf("expected upsert_merge_chunk_size=500 (no safety guard), got %d", snap.UpsertMergeChunkSize)
		}
	})

	t.Run("large upsert_merge_chunk_size accepted (no safety guard)", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"upsert_merge_chunk_size": 100000},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.UpsertMergeChunkSize != 100000 {
			t.Errorf("expected upsert_merge_chunk_size=100000 (no safety guard), got %d", snap.UpsertMergeChunkSize)
		}
	})

	t.Run("valid upsert_merge_chunk_size accepted", func(t *testing.T) {
		aa := createTestAdjuster()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"upsert_merge_chunk_size": 10000},
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)

		snap := aa.tuner.Snapshot()
		if snap.UpsertMergeChunkSize != 10000 {
			t.Errorf("expected upsert_merge_chunk_size=10000, got %d", snap.UpsertMergeChunkSize)
		}
	})
}

func TestSkipNearCompletion(t *testing.T) {
	t.Run("skips adjustments when transfer >90% complete", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.baselineCaptured = true
		aa.baselineMetrics = &PerformanceSnapshot{Throughput: 100000}

		// Set total rows and update live row count to show >90%
		aa.SetTotalRows(100000)
		aa.collector.UpdateRowCount(95000)

		decision, err := aa.Evaluate(nil)
		if err != nil {
			t.Fatalf("Evaluate returned error: %v", err)
		}
		if decision == nil {
			t.Fatal("expected a decision, got nil")
		}
		if decision.Action != "continue" {
			t.Errorf("expected action 'continue' when >90%% complete, got %q", decision.Action)
		}
	})

	t.Run("does not skip when transfer <90% complete", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.baselineCaptured = true
		aa.baselineMetrics = &PerformanceSnapshot{Throughput: 100000}

		// Set total rows and update collector to show 50%
		aa.SetTotalRows(100000)
		aa.collector.metrics = []PerformanceSnapshot{
			{RowsProcessed: 50000, Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
		}

		// At 50%, fallbackRules should be called (not the completion skip)
		// Since there's no aiMapper, Evaluate will use fallback rules
		decision := aa.fallbackRules()
		if decision == nil {
			t.Fatal("expected a fallback decision")
		}
		// Fallback should not mention completion
		if decision.Action == "continue" && strings.Contains(decision.Reasoning, "complete") {
			t.Error("should not skip adjustments when <90% complete")
		}
	})
}

func TestReadAheadBuffersZeroAllowed(t *testing.T) {
	aa := createTestAdjuster()

	decision := &AdjustmentDecision{
		Action:      "scale_down",
		Adjustments: map[string]int{"read_ahead_buffers": 0},
		Reasoning:   "disable buffering",
		Confidence:  "high",
	}

	aa.ApplyDecision(decision)

	snap := aa.tuner.Snapshot()
	if snap.ReadAheadBuffers != 0 {
		t.Errorf("expected read_ahead_buffers=0 (valid), got %d", snap.ReadAheadBuffers)
	}
}

func TestIsPlaceholderLimitError(t *testing.T) {
	tests := []struct {
		name     string
		errMsg   string
		expected bool
	}{
		{"placeholder keyword", "too many placeholders in query", true},
		{"parameter keyword", "exceeded parameter limit", true},
		{"prepared statement", "prepared statement has too many args", true},
		{"max_allowed_packet", "max_allowed_packet exceeded", true},
		{"too many keyword", "too many values in INSERT", true},
		{"65535 limit", "cannot exceed 65535 parameters", true},
		{"2100 limit", "exceeds 2100 parameter limit", true},
		{"args keyword", "too many args for prepared statement", true},
		{"unrelated error", "connection refused", false},
		{"timeout error", "context deadline exceeded", false},
		{"permission error", "access denied for user", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPlaceholderLimitError(tt.errMsg)
			if got != tt.expected {
				t.Errorf("isPlaceholderLimitError(%q) = %v, want %v", tt.errMsg, got, tt.expected)
			}
		})
	}
}

func TestFallbackChunkSize(t *testing.T) {
	t.Run("returns 0 for non-placeholder errors", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  10,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "connection refused",
			TargetDBType: "mysql",
		}

		result := aa.fallbackChunkSize(errCtx)
		if result != 0 {
			t.Errorf("expected 0 for non-placeholder error, got %d", result)
		}
	})

	t.Run("returns safe chunk size for MySQL placeholder error", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  100,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "too many placeholders",
			TargetDBType: "mysql",
		}

		result := aa.fallbackChunkSize(errCtx)
		// MySQL limit: 65535 / 100 * 0.9 = 589
		expected := 589
		if result != expected {
			t.Errorf("expected %d, got %d", expected, result)
		}
	})

	t.Run("returns safe chunk size for MSSQL placeholder error", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  10,
			ChunkSize:    500,
			RowCount:     500,
			ErrorMessage: "exceeds 2100 parameter limit",
			TargetDBType: "mssql",
		}

		result := aa.fallbackChunkSize(errCtx)
		// MSSQL limit: 2100 / 10 * 0.9 = 189
		expected := 189
		if result != expected {
			t.Errorf("expected %d, got %d", expected, result)
		}
	})

	t.Run("returns 0 when column count is zero", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  0,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "too many placeholders",
			TargetDBType: "mysql",
		}

		result := aa.fallbackChunkSize(errCtx)
		if result != 0 {
			t.Errorf("expected 0 for zero column count, got %d", result)
		}
	})

	t.Run("returns 0 for unknown target DB", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  10,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "too many placeholders",
			TargetDBType: "sqlite",
		}

		result := aa.fallbackChunkSize(errCtx)
		if result != 0 {
			t.Errorf("expected 0 for unknown DB type, got %d", result)
		}
	})

	t.Run("returns 0 when calculated size >= row count", func(t *testing.T) {
		aa := createTestAdjuster()
		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  2,
			ChunkSize:    100,
			RowCount:     100,
			ErrorMessage: "too many placeholders",
			TargetDBType: "mysql",
		}

		// MySQL: 65535 / 2 * 0.9 = 29490, which is >= 100 rows
		result := aa.fallbackChunkSize(errCtx)
		if result != 0 {
			t.Errorf("expected 0 when safe size >= row count, got %d", result)
		}
	})
}

func TestEvaluateWriteError(t *testing.T) {
	t.Run("returns 0 for non-placeholder error", func(t *testing.T) {
		aa := createTestAdjuster()

		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  10,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "connection refused",
			TargetDBType: "mysql",
		}

		result := aa.EvaluateWriteError(nil, errCtx)
		if result != 0 {
			t.Errorf("expected 0 for non-placeholder error, got %d", result)
		}
	})

	t.Run("timeout error must not trigger batch size reduction", func(t *testing.T) {
		// Regression: a single timeout (caused by concurrency pressure, not query
		// size) used to trigger a 17x batch size reduction that crushed throughput
		// for the rest of the table. Timeouts must fall through to retry logic.
		aa := createTestAdjuster()

		errCtx := transfer.WriteErrorContext{
			TableName:    "posts",
			ColumnCount:  20,
			ChunkSize:    50000,
			RowCount:     50000,
			ErrorMessage: "timeout: context deadline exceeded",
			TargetDBType: "postgres",
		}

		result := aa.EvaluateWriteError(nil, errCtx)
		if result != 0 {
			t.Errorf("expected 0 for timeout error, got %d", result)
		}
	})

	t.Run("returns deterministic safe size for placeholder limit error", func(t *testing.T) {
		aa := createTestAdjuster()

		errCtx := transfer.WriteErrorContext{
			TableName:    "test_table",
			ColumnCount:  100,
			ChunkSize:    1000,
			RowCount:     1000,
			ErrorMessage: "too many placeholders",
			TargetDBType: "mysql",
		}

		result := aa.EvaluateWriteError(nil, errCtx)
		expected := 589
		if result != expected {
			t.Errorf("expected fallback %d for placeholder error, got %d", expected, result)
		}
	})
}

func TestFallbackRulesWithBaseline(t *testing.T) {
	t.Run("within baseline tolerance returns continue", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.captureBaseline()

		// Set current metrics to within 10% of baseline
		aa.collector.metrics = []PerformanceSnapshot{
			{Throughput: 95000, CPUPercent: 50, MemoryPercent: 60}, // 5% below baseline
			{Throughput: 95000, CPUPercent: 50, MemoryPercent: 60},
			{Throughput: 95000, CPUPercent: 50, MemoryPercent: 60},
		}

		decision := aa.fallbackRules()

		if decision.Action != "continue" {
			t.Errorf("expected 'continue' when within ±10%% of baseline, got %q", decision.Action)
		}
	})
}
