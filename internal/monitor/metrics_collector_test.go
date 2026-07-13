package monitor

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/systemmemory"
	"github.com/johndauphine/dmt/internal/transfer"
)

type fakeMemoryReader struct {
	snapshot systemmemory.Snapshot
	err      error
	reads    int
}

func (r *fakeMemoryReader) Read() (systemmemory.Snapshot, error) {
	r.reads++
	return r.snapshot, r.err
}

func TestAnalyzeTrends(t *testing.T) {
	t.Run("insufficient data", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000},
				{Throughput: 100000},
			},
		}

		trends := mc.AnalyzeTrends()

		if !trends.Insufficient {
			t.Error("expected Insufficient=true with less than 3 samples")
		}
	})

	t.Run("throughput declining over 20%", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 85000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 70000, CPUPercent: 50, MemoryPercent: 50}, // 30% decline from first
			},
		}

		trends := mc.AnalyzeTrends()

		if !trends.ThroughputDecreasing {
			t.Error("expected ThroughputDecreasing=true with >20% decline")
		}

		if trends.ThroughputDecline < 25 || trends.ThroughputDecline > 35 {
			t.Errorf("expected ThroughputDecline around 30%%, got %.1f%%", trends.ThroughputDecline)
		}
	})

	t.Run("throughput declining under 20%", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 95000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 90000, CPUPercent: 50, MemoryPercent: 50}, // 10% decline
			},
		}

		trends := mc.AnalyzeTrends()

		if trends.ThroughputDecreasing {
			t.Error("expected ThroughputDecreasing=false with <20% decline")
		}

		if trends.ThroughputDecline < 8 || trends.ThroughputDecline > 12 {
			t.Errorf("expected ThroughputDecline around 10%%, got %.1f%%", trends.ThroughputDecline)
		}
	})

	t.Run("throughput increasing sets decline to zero", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 110000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 120000, CPUPercent: 50, MemoryPercent: 50}, // Increasing
			},
		}

		trends := mc.AnalyzeTrends()

		if trends.ThroughputDecreasing {
			t.Error("expected ThroughputDecreasing=false when throughput increasing")
		}

		if trends.ThroughputDecline != 0 {
			t.Errorf("expected ThroughputDecline=0 when increasing, got %.1f%%", trends.ThroughputDecline)
		}
	})

	t.Run("stable throughput", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50},
			},
		}

		trends := mc.AnalyzeTrends()

		if trends.ThroughputDecreasing {
			t.Error("expected ThroughputDecreasing=false when stable")
		}

		if trends.ThroughputDecline != 0 {
			t.Errorf("expected ThroughputDecline=0 when stable, got %.1f%%", trends.ThroughputDecline)
		}
	})

	t.Run("CPU saturated", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 85, MemoryPercent: 50},
				{Throughput: 100000, CPUPercent: 90, MemoryPercent: 50},
				{Throughput: 100000, CPUPercent: 95, MemoryPercent: 50}, // >90%
			},
		}

		trends := mc.AnalyzeTrends()

		if !trends.CPUSaturated {
			t.Error("expected CPUSaturated=true when CPU >90%")
		}
	})

	t.Run("memory saturated", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 70, MemoryPressureKnown: true},
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 75, MemoryPressureKnown: true},
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 80, MemoryPressureKnown: true}, // >75%
			},
		}

		trends := mc.AnalyzeTrends()

		if !trends.MemorySaturated {
			t.Error("expected MemorySaturated=true when memory >75%")
		}
	})

	t.Run("unknown memory is not saturated", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, MemoryPercent: 70, MemoryPressureKnown: true},
				{Throughput: 100000, MemoryPercent: 75, MemoryPressureKnown: true},
				{Throughput: 100000, MemoryPercent: 99, MemoryPressureKnown: false},
			},
		}

		if trends := mc.AnalyzeTrends(); trends.MemorySaturated {
			t.Error("unknown pressure must not be classified as saturated")
		}
	})

	t.Run("memory increasing", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 50, MemoryPressureKnown: true},
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 56, MemoryPressureKnown: true}, // >5% increase
				{Throughput: 100000, CPUPercent: 50, MemoryPercent: 63, MemoryPressureKnown: true}, // >5% increase
			},
		}

		trends := mc.AnalyzeTrends()

		if !trends.MemoryIncreasing {
			t.Error("expected MemoryIncreasing=true when memory increases >5% per sample")
		}
	})

	t.Run("unknown sample suppresses memory trend", func(t *testing.T) {
		mc := &MetricsCollector{
			metrics: []PerformanceSnapshot{
				{MemoryPercent: 50, MemoryPressureKnown: true, HeapAllocMB: 100},
				{MemoryPercent: 60, MemoryPressureKnown: false, HeapAllocMB: 200},
				{MemoryPercent: 70, MemoryPressureKnown: true, HeapAllocMB: 400},
			},
		}

		if trends := mc.AnalyzeTrends(); trends.MemoryIncreasing {
			t.Error("unknown pressure must suppress MemoryIncreasing even when heap allocation rises")
		}
	})
}

func TestCollectSnapshotUsesEffectiveMemoryPressureAndLogsSource(t *testing.T) {
	reader := &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		HostCapacityMB:  16_000,
		HostAvailableMB: 4_000, // host pressure 75%
		CgroupLimitMB:   1_000,
		CgroupCurrentMB: 950, // cgroup pressure 95% wins
		Source:          "cgroup-v2",
	}}
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000, Workers: 6})
	mc := NewMetricsCollector(tuner, 30*time.Second, reader)

	var logs bytes.Buffer
	previousLevel := logging.GetLevel()
	logging.SetLevel(logging.LevelDebug)
	logging.SetOutput(&logs)
	t.Cleanup(func() {
		logging.SetLevel(previousLevel)
		logging.SetOutput(os.Stdout)
	})

	mc.collectSnapshot()
	got := mc.GetRecentMetrics(1)[0]
	if !got.MemoryPressureKnown || got.MemoryPercent != 95 {
		t.Fatalf("memory pressure = (%.1f, known=%v), want (95.0, true)", got.MemoryPercent, got.MemoryPressureKnown)
	}
	if got.memoryPressureSource != "cgroup-v2" {
		t.Fatalf("pressure source = %q, want cgroup-v2", got.memoryPressureSource)
	}
	if reader.reads != 1 {
		t.Fatalf("memory reader calls = %d, want 1", reader.reads)
	}
	if got.CurrentConfig.Workers != 6 {
		t.Fatalf("current config workers = %d, want 6 from runtime snapshot", got.CurrentConfig.Workers)
	}
	logText := logs.String()
	if !strings.Contains(logText, "memory_pressure=95.0% source=cgroup-v2") || !strings.Contains(logText, "heap_alloc=") {
		t.Fatalf("metrics log does not separate effective pressure from heap diagnostic: %q", logText)
	}
}

func TestCollectSnapshotClampsEffectivePressure(t *testing.T) {
	reader := &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		CgroupLimitMB:   100,
		CgroupCurrentMB: 250,
		Source:          "cgroup-v1",
	}}
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000})
	mc := NewMetricsCollector(tuner, 30*time.Second, reader)

	mc.collectSnapshot()
	got := mc.GetRecentMetrics(1)[0]
	if !got.MemoryPressureKnown || got.MemoryPercent != 100 || got.memoryPressureSource != "cgroup-v1" {
		t.Fatalf("clamped pressure = (%.1f, %v, %q), want (100, true, cgroup-v1)",
			got.MemoryPercent, got.MemoryPressureKnown, got.memoryPressureSource)
	}
}

func TestCollectSnapshotUsesComponentPressureNotEffectiveMinima(t *testing.T) {
	reader := &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		CapacityMB:      8_000,
		AvailableMB:     4_000, // combining effective minima would report 50%
		HostCapacityMB:  16_000,
		HostAvailableMB: 4_000, // real host pressure is 75% and wins
		CgroupLimitMB:   8_000,
		CgroupCurrentMB: 2_000, // real cgroup pressure is 25%
		Source:          "cgroup-v2",
	}}
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000})
	mc := NewMetricsCollector(tuner, 30*time.Second, reader)

	mc.collectSnapshot()
	got := mc.GetRecentMetrics(1)[0]
	if !got.MemoryPressureKnown || got.MemoryPercent != 75 || got.memoryPressureSource != "host" {
		t.Fatalf("component pressure = (%.1f, %v, %q), want (75, true, host)",
			got.MemoryPercent, got.MemoryPressureKnown, got.memoryPressureSource)
	}
}

func TestCollectSnapshotUnknownOnMemoryReadOrCalculationFailure(t *testing.T) {
	tests := []struct {
		name   string
		reader *fakeMemoryReader
	}{
		{name: "read failure", reader: &fakeMemoryReader{err: errors.New("memory unavailable")}},
		{name: "calculation failure", reader: &fakeMemoryReader{snapshot: systemmemory.Snapshot{}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000})
			mc := NewMetricsCollector(tuner, 30*time.Second, tc.reader)

			mc.collectSnapshot()
			got := mc.GetRecentMetrics(1)[0]
			if got.MemoryPressureKnown || got.MemoryPercent != 0 || got.memoryPressureSource != "" {
				t.Fatalf("unknown pressure encoded as real signal: %+v", got)
			}
			if tc.reader.reads != 1 {
				t.Fatalf("memory reader calls = %d, want 1", tc.reader.reads)
			}
		})
	}
}

func TestCollectSnapshotLogsUnknownPressureSeparatelyFromHeap(t *testing.T) {
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000})
	mc := NewMetricsCollector(tuner, 30*time.Second, &fakeMemoryReader{err: errors.New("memory unavailable")})

	var logs bytes.Buffer
	previousLevel := logging.GetLevel()
	logging.SetLevel(logging.LevelDebug)
	logging.SetOutput(&logs)
	t.Cleanup(func() {
		logging.SetLevel(previousLevel)
		logging.SetOutput(os.Stdout)
	})

	mc.collectSnapshot()
	logText := logs.String()
	if !strings.Contains(logText, "memory_pressure=unknown") || !strings.Contains(logText, "heap_alloc=") {
		t.Fatalf("unknown-pressure log must preserve heap as a separate diagnostic: %q", logText)
	}
}

func TestWindowedTimePercentages(t *testing.T) {
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000, ReadAheadBuffers: 4})
	mc := NewMetricsCollector(tuner, 30*time.Second, &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		HostCapacityMB:  100,
		HostAvailableMB: 50,
	}})

	// First snapshot: report some transfer time
	tuner.ReportTransferTime(400, 100, 500, 1000) // query=400, scan=100, write=500 → source=50%, write=50%
	tuner.ReportBudgetWait(250)
	mc.collectSnapshot()

	metrics := mc.GetRecentMetrics(1)
	if len(metrics) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(metrics))
	}

	// Source (query+scan) should be 50%, write should be 50%
	if metrics[0].QueryTimePercent < 49 || metrics[0].QueryTimePercent > 51 {
		t.Errorf("QueryTimePercent = %.1f%%, want ~50%%", metrics[0].QueryTimePercent)
	}
	if metrics[0].WriteTimePercent < 49 || metrics[0].WriteTimePercent > 51 {
		t.Errorf("WriteTimePercent = %.1f%%, want ~50%%", metrics[0].WriteTimePercent)
	}
	if metrics[0].BudgetWaitNs != 250 || metrics[0].BudgetWaitCount != 1 {
		t.Errorf("budget waits = (%d ns, %d count), want (250, 1)", metrics[0].BudgetWaitNs, metrics[0].BudgetWaitCount)
	}

	// Second snapshot: report more time, heavily write-bound
	tuner.ReportTransferTime(100, 0, 900, 2000) // delta: query=100, scan=0, write=900 → source=10%, write=90%
	mc.collectSnapshot()

	metrics = mc.GetRecentMetrics(1)
	if len(metrics) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(metrics))
	}

	// Should reflect windowed (delta) percentages, not cumulative
	if metrics[0].QueryTimePercent < 8 || metrics[0].QueryTimePercent > 12 {
		t.Errorf("QueryTimePercent = %.1f%%, want ~10%% (windowed)", metrics[0].QueryTimePercent)
	}
	if metrics[0].WriteTimePercent < 88 || metrics[0].WriteTimePercent > 92 {
		t.Errorf("WriteTimePercent = %.1f%%, want ~90%% (windowed)", metrics[0].WriteTimePercent)
	}
}

func TestWindowedTimePercentagesZeroDelta(t *testing.T) {
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1000, ReadAheadBuffers: 4})
	mc := NewMetricsCollector(tuner, 30*time.Second, &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		HostCapacityMB:  100,
		HostAvailableMB: 50,
	}})

	// Report some time then collect
	tuner.ReportTransferTime(500, 500, 1000, 100)
	mc.collectSnapshot()

	// No new time reported — delta is zero
	mc.collectSnapshot()

	metrics := mc.GetRecentMetrics(1)
	if len(metrics) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(metrics))
	}

	// With zero delta, both should be 0
	if metrics[0].QueryTimePercent != 0 {
		t.Errorf("QueryTimePercent = %.1f%%, want 0%% (no delta)", metrics[0].QueryTimePercent)
	}
	if metrics[0].WriteTimePercent != 0 {
		t.Errorf("WriteTimePercent = %.1f%%, want 0%% (no delta)", metrics[0].WriteTimePercent)
	}
}

func TestGetRecentMetrics(t *testing.T) {
	mc := &MetricsCollector{
		metrics: []PerformanceSnapshot{
			{Throughput: 100000},
			{Throughput: 110000},
			{Throughput: 120000},
			{Throughput: 130000},
			{Throughput: 140000},
		},
	}

	t.Run("get last 3", func(t *testing.T) {
		recent := mc.GetRecentMetrics(3)

		if len(recent) != 3 {
			t.Errorf("expected 3 metrics, got %d", len(recent))
		}

		if recent[0].Throughput != 120000 {
			t.Errorf("expected first metric throughput 120000, got %.0f", recent[0].Throughput)
		}
	})

	t.Run("request more than available", func(t *testing.T) {
		recent := mc.GetRecentMetrics(10)

		if len(recent) != 5 {
			t.Errorf("expected 5 metrics (all available), got %d", len(recent))
		}
	})

	t.Run("empty metrics", func(t *testing.T) {
		emptyMc := &MetricsCollector{metrics: []PerformanceSnapshot{}}

		recent := emptyMc.GetRecentMetrics(3)

		if len(recent) != 0 {
			t.Errorf("expected 0 metrics, got %d", len(recent))
		}
	})
}
