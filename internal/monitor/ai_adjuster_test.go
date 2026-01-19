package monitor

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/pipeline"
)

// createTestAdjuster creates an AIAdjuster configured for testing.
func createTestAdjuster() *AIAdjuster {
	// Create a real pipeline with test config
	p := pipeline.New(nil, nil, pipeline.Config{
		ChunkSize:         25000,
		WriteAheadWriters: 4,
		ParallelReaders:   2,
		ReadAheadBuffers:  8,
	})

	// Create a real metrics collector
	mc := NewMetricsCollector(p, 30*time.Second)

	// Pre-populate with test metrics
	mc.metrics = []PerformanceSnapshot{
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
		{Throughput: 100000, CPUPercent: 50, MemoryPercent: 60},
	}

	aa := &AIAdjuster{
		collector:         mc,
		pipeline:          p,
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

		// Simulate pipeline consuming the config update
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.WriteAheadWriters != 5 {
			t.Errorf("expected workers=5 after first apply, got %d", config.WriteAheadWriters)
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

		// Simulate pipeline consuming any pending updates
		aa.pipeline.ApplyPendingUpdates(true)

		// Workers should have changed (no cooldown)
		config = aa.pipeline.GetConfig()
		if config.WriteAheadWriters != 6 {
			t.Errorf("expected workers=6, got %d", config.WriteAheadWriters)
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
		aa.pipeline.ApplyPendingUpdates(true)

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

		// Simulate pipeline consuming the config update
		aa.pipeline.ApplyPendingUpdates(true)

		// Workers should have changed
		config := aa.pipeline.GetConfig()
		if config.WriteAheadWriters != 3 {
			t.Errorf("expected workers=3, got %d", config.WriteAheadWriters)
		}
	})
}

func TestSystemResourceValidation(t *testing.T) {
	t.Run("workers cannot exceed CPU cores", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.systemResources.CPUCores = 8
		aa.systemResources.MaxTargetConnections = 20 // High enough to not be the limit

		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 12}, // Exceeds 8 cores
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		// Workers should NOT have changed (exceeds CPU cores)
		config := aa.pipeline.GetConfig()
		if config.WriteAheadWriters != initialConfig.WriteAheadWriters {
			t.Errorf("expected workers unchanged (validation failed), got %d", config.WriteAheadWriters)
		}
	})

	t.Run("workers cannot exceed max connections", func(t *testing.T) {
		aa := createTestAdjuster()
		aa.systemResources.CPUCores = 16
		aa.systemResources.MaxTargetConnections = 6 // Lower than CPU cores

		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"workers": 10}, // Exceeds 6 connections
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		// Workers should NOT have changed (exceeds max connections)
		config := aa.pipeline.GetConfig()
		if config.WriteAheadWriters != initialConfig.WriteAheadWriters {
			t.Errorf("expected workers unchanged (validation failed), got %d", config.WriteAheadWriters)
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

		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.WriteAheadWriters != 6 {
			t.Errorf("expected workers=6, got %d", config.WriteAheadWriters)
		}
	})

	t.Run("chunk_size too small rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "reduce_chunk",
			Adjustments: map[string]int{"chunk_size": 500}, // Below 1000 minimum
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.ChunkSize != initialConfig.ChunkSize {
			t.Errorf("expected chunk_size unchanged (too small), got %d", config.ChunkSize)
		}
	})

	t.Run("chunk_size too large rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"chunk_size": 1000000}, // Above 500000 max
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.ChunkSize != initialConfig.ChunkSize {
			t.Errorf("expected chunk_size unchanged (too large), got %d", config.ChunkSize)
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
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.ChunkSize != 10000 {
			t.Errorf("expected chunk_size=10000, got %d", config.ChunkSize)
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
		aa.pipeline.ApplyPendingUpdates(true)

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
			aa.pipeline.ApplyPendingUpdates(true)
		}

		if len(aa.adjustmentHistory) != 3 {
			t.Errorf("expected history limited to 3, got %d", len(aa.adjustmentHistory))
		}
	})
}

func TestContinueActionNotApplied(t *testing.T) {
	aa := createTestAdjuster()
	initialConfig := aa.pipeline.GetConfig()

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

	config := aa.pipeline.GetConfig()
	if config.WriteAheadWriters != initialConfig.WriteAheadWriters {
		t.Errorf("continue action should not change config, workers changed to %d", config.WriteAheadWriters)
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
	t.Run("checkpoint_frequency too small rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"checkpoint_frequency": 0}, // Below 1 minimum
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.CheckpointFrequency != initialConfig.CheckpointFrequency {
			t.Errorf("expected checkpoint_frequency unchanged (too small), got %d", config.CheckpointFrequency)
		}
	})

	t.Run("checkpoint_frequency too large rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"checkpoint_frequency": 150}, // Above 100 max
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.CheckpointFrequency != initialConfig.CheckpointFrequency {
			t.Errorf("expected checkpoint_frequency unchanged (too large), got %d", config.CheckpointFrequency)
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
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.CheckpointFrequency != 20 {
			t.Errorf("expected checkpoint_frequency=20, got %d", config.CheckpointFrequency)
		}
	})
}

func TestUpsertMergeChunkSizeValidation(t *testing.T) {
	t.Run("upsert_merge_chunk_size too small rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_down",
			Adjustments: map[string]int{"upsert_merge_chunk_size": 500}, // Below 1000 minimum
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.UpsertMergeChunkSize != initialConfig.UpsertMergeChunkSize {
			t.Errorf("expected upsert_merge_chunk_size unchanged (too small), got %d", config.UpsertMergeChunkSize)
		}
	})

	t.Run("upsert_merge_chunk_size too large rejected", func(t *testing.T) {
		aa := createTestAdjuster()
		initialConfig := aa.pipeline.GetConfig()

		decision := &AdjustmentDecision{
			Action:      "scale_up",
			Adjustments: map[string]int{"upsert_merge_chunk_size": 100000}, // Above 50000 max
			Reasoning:   "test",
			Confidence:  "high",
		}

		aa.ApplyDecision(decision)
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.UpsertMergeChunkSize != initialConfig.UpsertMergeChunkSize {
			t.Errorf("expected upsert_merge_chunk_size unchanged (too large), got %d", config.UpsertMergeChunkSize)
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
		aa.pipeline.ApplyPendingUpdates(true)

		config := aa.pipeline.GetConfig()
		if config.UpsertMergeChunkSize != 10000 {
			t.Errorf("expected upsert_merge_chunk_size=10000, got %d", config.UpsertMergeChunkSize)
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
