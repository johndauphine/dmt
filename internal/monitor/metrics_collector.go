package monitor

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
	"github.com/shirou/gopsutil/v3/cpu"
)

// PerformanceSnapshot captures a single measurement of migration performance.
type PerformanceSnapshot struct {
	Timestamp       time.Time
	ElapsedSeconds  float64
	RowsProcessed   int64
	Throughput      float64 // rows/sec
	ThroughputTrend float64 // % change from previous sample

	// Resource usage
	MemoryUsedMB  int64
	MemoryPercent float64
	CPUPercent    float64

	// Database metrics
	QueryLatencyMs   float64
	WriteLatencyMs   float64
	QueryTimePercent float64
	WriteTimePercent float64

	// Transfer breakdown
	ActiveWorkers   int
	QueueDepth      int // Buffered chunks waiting
	ErrorCount      int // Tables that exhausted all retries
	ChunkRetryCount int // Transient chunk retries that succeeded on a later attempt

	// Current configuration
	CurrentConfig ConfigSnapshot
}

// ConfigSnapshot captures the current configuration at a moment in time.
type ConfigSnapshot struct {
	ChunkSize         int
	Workers           int
	ReadAheadBuffers  int
	ParallelReaders   int
	WriteAheadWriters int
}

// TrendAnalysis captures detected performance trends.
type TrendAnalysis struct {
	Insufficient         bool
	ThroughputDecreasing bool    // >20% decline (significant threshold)
	ThroughputDecline    float64 // actual decline percentage
	MemoryIncreasing     bool    // >5% increase per sample
	LatencyIncreasing    bool    // >20% increase
	CPUSaturated         bool
	MemorySaturated      bool
}

// MetricsCollector continuously collects performance metrics during migration.
type MetricsCollector struct {
	tuner         transfer.RuntimeTuner
	startTime     time.Time
	interval      time.Duration
	rowsProcessed atomic.Int64

	// Previous snapshot state for windowed throughput calculation
	prevRowsProcessed int64
	prevTimestamp     time.Time

	// Previous snapshot state for windowed transfer time calculation
	prevQueryNs int64
	prevScanNs  int64
	prevWriteNs int64

	metricsMu sync.RWMutex
	metrics   []PerformanceSnapshot
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(tuner transfer.RuntimeTuner, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		tuner:     tuner,
		startTime: time.Now(),
		interval:  interval,
		metrics:   make([]PerformanceSnapshot, 0, 128), // Pre-allocate for ~64 minutes of data
	}
}

// UpdateRowCount updates the number of rows processed.
func (mc *MetricsCollector) UpdateRowCount(count int64) {
	mc.rowsProcessed.Store(count)
}

// GetCurrentRowCount returns the live row count (not from a snapshot).
func (mc *MetricsCollector) GetCurrentRowCount() int64 {
	return mc.rowsProcessed.Load()
}

// Start begins collecting metrics at regular intervals.
func (mc *MetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.collectSnapshot()
		case <-ctx.Done():
			return
		}
	}
}

// collectSnapshot gathers metrics and stores them.
func (mc *MetricsCollector) collectSnapshot() {
	snapshot := PerformanceSnapshot{
		Timestamp:      time.Now(),
		ElapsedSeconds: time.Since(mc.startTime).Seconds(),
		RowsProcessed:  mc.rowsProcessed.Load(),
	}

	// Collect expensive I/O metrics outside the lock
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	snapshot.MemoryUsedMB = int64(m.Alloc / 1024 / 1024)

	totalMemBytes := int64(m.Sys)
	if totalMemBytes > 0 {
		snapshot.MemoryPercent = float64(m.Alloc) / float64(totalMemBytes) * 100
	}

	cpuPercent, err := cpu.Percent(100*time.Millisecond, false)
	if err == nil && len(cpuPercent) > 0 {
		snapshot.CPUPercent = cpuPercent[0]
	}

	snap := mc.tuner.Snapshot()
	snapshot.CurrentConfig = ConfigSnapshot{
		ChunkSize:         snap.ChunkSize,
		ReadAheadBuffers:  snap.ReadAheadBuffers,
		ParallelReaders:   snap.ParallelReaders,
		WriteAheadWriters: snap.WriteAheadWriters,
	}

	// Populate operational metrics from tuner
	metrics := mc.tuner.Metrics()
	snapshot.ActiveWorkers = metrics.ActiveJobs
	snapshot.QueueDepth = metrics.QueueDepth
	snapshot.ErrorCount = metrics.ErrorCount
	snapshot.ChunkRetryCount = metrics.ChunkRetryCount

	// Hold lock for windowed throughput/time calculation, trend, and append
	mc.metricsMu.Lock()

	// Calculate windowed transfer time percentages (delta since last snapshot)
	deltaQuery := metrics.TotalQueryNs - mc.prevQueryNs
	deltaScan := metrics.TotalScanNs - mc.prevScanNs
	deltaWrite := metrics.TotalWriteNs - mc.prevWriteNs
	deltaTotal := deltaQuery + deltaScan + deltaWrite
	if deltaTotal > 0 {
		snapshot.QueryTimePercent = float64(deltaQuery+deltaScan) / float64(deltaTotal) * 100
		snapshot.WriteTimePercent = float64(deltaWrite) / float64(deltaTotal) * 100
	}
	mc.prevQueryNs = metrics.TotalQueryNs
	mc.prevScanNs = metrics.TotalScanNs
	mc.prevWriteNs = metrics.TotalWriteNs

	// Calculate windowed throughput (rows processed since last snapshot)
	// This avoids the cumulative average problem where early fast tables
	// inflate the average and mask real-time slowdowns.
	now := time.Now()
	if !mc.prevTimestamp.IsZero() {
		intervalSec := now.Sub(mc.prevTimestamp).Seconds()
		if intervalSec > 0 {
			snapshot.Throughput = float64(snapshot.RowsProcessed-mc.prevRowsProcessed) / intervalSec
		}
	} else if snapshot.ElapsedSeconds > 0 {
		// First snapshot: use cumulative as fallback
		snapshot.Throughput = float64(snapshot.RowsProcessed) / snapshot.ElapsedSeconds
	}
	mc.prevRowsProcessed = snapshot.RowsProcessed
	mc.prevTimestamp = now

	// Calculate trend
	if len(mc.metrics) > 0 {
		prev := mc.metrics[len(mc.metrics)-1]
		if prev.Throughput > 0 {
			snapshot.ThroughputTrend = (snapshot.Throughput - prev.Throughput) / prev.Throughput * 100
		}
	}

	mc.metrics = append(mc.metrics, snapshot)
	mc.metricsMu.Unlock()

	logging.Debug("Metrics snapshot: %.0f rows/sec, memory=%dMB (%.1f%%), CPU=%.1f%%, throughput_trend=%.1f%%",
		snapshot.Throughput, snapshot.MemoryUsedMB, snapshot.MemoryPercent, snapshot.CPUPercent, snapshot.ThroughputTrend)
}

// GetRecentMetrics returns the last N snapshots.
func (mc *MetricsCollector) GetRecentMetrics(count int) []PerformanceSnapshot {
	mc.metricsMu.RLock()
	defer mc.metricsMu.RUnlock()

	if len(mc.metrics) == 0 {
		return []PerformanceSnapshot{}
	}

	start := len(mc.metrics) - count
	if start < 0 {
		start = 0
	}

	result := make([]PerformanceSnapshot, len(mc.metrics[start:]))
	copy(result, mc.metrics[start:])
	return result
}

// GetAllMetrics returns all collected snapshots.
func (mc *MetricsCollector) GetAllMetrics() []PerformanceSnapshot {
	mc.metricsMu.RLock()
	defer mc.metricsMu.RUnlock()

	result := make([]PerformanceSnapshot, len(mc.metrics))
	copy(result, mc.metrics)
	return result
}

// AnalyzeTrends examines recent metrics for performance patterns.
func (mc *MetricsCollector) AnalyzeTrends() TrendAnalysis {
	mc.metricsMu.RLock()
	defer mc.metricsMu.RUnlock()

	result := TrendAnalysis{}

	// Need at least 3 samples for trend analysis
	if len(mc.metrics) < 3 {
		result.Insufficient = true
		return result
	}

	// Analyze last 3 samples
	recent := mc.metrics[len(mc.metrics)-3:]

	// Throughput trend (>20% decline - significant threshold to avoid over-adjusting)
	if recent[0].Throughput > 0 && recent[2].Throughput > 0 {
		decline := (recent[0].Throughput - recent[2].Throughput) / recent[0].Throughput
		if decline > 0 {
			// Store positive decline as percentage; direction captured by ThroughputDecreasing
			result.ThroughputDecline = decline * 100
			if decline > 0.20 {
				result.ThroughputDecreasing = true
			}
		} else {
			// No decline (flat or increasing throughput)
			result.ThroughputDecline = 0
		}
	}

	// Memory trend (>5% increase per sample)
	if len(recent) >= 3 {
		memIncrease1 := float64(recent[1].MemoryUsedMB-recent[0].MemoryUsedMB) / float64(max(1, recent[0].MemoryUsedMB))
		memIncrease2 := float64(recent[2].MemoryUsedMB-recent[1].MemoryUsedMB) / float64(max(1, recent[1].MemoryUsedMB))
		if memIncrease1 > 0.05 && memIncrease2 > 0.05 {
			result.MemoryIncreasing = true
		}
	}

	// CPU saturation
	if recent[len(recent)-1].CPUPercent > 90 {
		result.CPUSaturated = true
	}

	// Memory saturation
	if recent[len(recent)-1].MemoryPercent > 75 {
		result.MemorySaturated = true
	}

	return result
}
