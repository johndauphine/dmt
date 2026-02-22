package monitor

import (
	"context"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
)

// AIMonitor orchestrates real-time performance monitoring and AI-driven adjustment.
type AIMonitor struct {
	collector     *MetricsCollector
	adjuster      *AIAdjuster
	tuner         transfer.RuntimeTuner
	interval      time.Duration
	rowsProcessed int64
}

// NewAIMonitor creates a new AI monitoring system.
func NewAIMonitor(
	tuner transfer.RuntimeTuner,
	aiMapper *driver.AITypeMapper,
	interval time.Duration,
) *AIMonitor {
	collector := NewMetricsCollector(tuner, interval)
	adjuster := NewAIAdjuster(aiMapper, collector, tuner)

	return &AIMonitor{
		collector: collector,
		adjuster:  adjuster,
		tuner:     tuner,
		interval:  interval,
	}
}

// SetConnectionLimits sets the max connection limits for source and target.
func (am *AIMonitor) SetConnectionLimits(maxSource, maxTarget int) {
	am.adjuster.SetConnectionLimits(maxSource, maxTarget)
}

// SetTargetMode sets the migration target mode (drop_recreate or upsert).
func (am *AIMonitor) SetTargetMode(mode string) {
	am.adjuster.SetTargetMode(mode)
}

// SetStateBackend sets the state backend for persistent history.
func (am *AIMonitor) SetStateBackend(state checkpoint.StateBackend, runID string) {
	am.adjuster.SetStateBackend(state, runID)
}

// SetTotalRows sets the total rows so the adjuster skips adjustments near completion.
func (am *AIMonitor) SetTotalRows(total int64) {
	am.adjuster.SetTotalRows(total)
}

// UpdateRowsProcessed updates the count of rows transferred.
func (am *AIMonitor) UpdateRowsProcessed(count int64) {
	am.rowsProcessed = count
	am.collector.UpdateRowCount(count)
}

// Start begins the monitoring and adjustment loop.
func (am *AIMonitor) Start(ctx context.Context) {
	logging.Debug("AI monitoring started (evaluation interval: %v)", am.interval)

	// Start metrics collection in background
	go am.collector.Start(ctx)

	// Main evaluation loop
	ticker := time.NewTicker(am.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			am.evaluateAndAdjust(ctx)
		case <-ctx.Done():
			logging.Debug("AI monitoring stopped")
			return
		}
	}
}

// evaluateAndAdjust checks metrics and applies adjustments if needed.
func (am *AIMonitor) evaluateAndAdjust(ctx context.Context) {
	// Get AI recommendation
	decision, err := am.adjuster.Evaluate(ctx)
	if err != nil {
		logging.Debug("AI evaluation skipped: %v", err)
		return
	}

	if decision == nil {
		return
	}

	// Apply if action is not "continue"
	if decision.Action != "continue" {
		if err := am.adjuster.ApplyDecision(decision); err != nil {
			logging.Warn("Failed to apply AI decision: %v", err)
		}
	}

	// Log warnings
	if len(decision.Warnings) > 0 {
		for _, w := range decision.Warnings {
			logging.Warn("AI warning: %s", w)
		}
	}
}

// GetMetrics returns all collected metrics snapshots.
func (am *AIMonitor) GetMetrics() []PerformanceSnapshot {
	return am.collector.GetAllMetrics()
}

// GetLastMetric returns the most recent metric snapshot.
func (am *AIMonitor) GetLastMetric() *PerformanceSnapshot {
	metrics := am.collector.GetRecentMetrics(1)
	if len(metrics) > 0 {
		return &metrics[0]
	}
	return nil
}
