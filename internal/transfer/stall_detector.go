package transfer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/logging"
)

// StallDetector monitors transfer progress and warns when no rows have been
// written for a configurable duration. This catches stalls from any cause
// (frozen TCP connections, driver hangs, lock contention) without needing
// driver-level timeout support.
type StallDetector struct {
	getProgress func() int64 // returns current row count
	interval    time.Duration // how often to check
	threshold   time.Duration // warn after this much inactivity
	lastRows    int64
	stalled     atomic.Bool
	state       checkpoint.StateBackend // optional: log stalls to SQLite for AI learning
	runID       string
}

// NewStallDetector creates a detector that checks progress every interval
// and warns if no rows have been written for threshold duration.
func NewStallDetector(getProgress func() int64, interval, threshold time.Duration) *StallDetector {
	return &StallDetector{
		getProgress: getProgress,
		interval:    interval,
		threshold:   threshold,
	}
}

// SetStateBackend enables logging stalls to SQLite for AI tuning history.
func (d *StallDetector) SetStateBackend(state checkpoint.StateBackend, runID string) {
	d.state = state
	d.runID = runID
}

// IsStalled returns true if the transfer appears stalled.
func (d *StallDetector) IsStalled() bool {
	return d.stalled.Load()
}

// logStall records a stall event to SQLite for AI tuning history.
func (d *StallDetector) logStall(rowsAtStall int64, stallDuration time.Duration) {
	if d.state == nil || d.runID == "" {
		return
	}
	record := checkpoint.AIAdjustmentRecord{
		RunID:            d.runID,
		Timestamp:        time.Now(),
		Action:           "stall_detected",
		ThroughputBefore: 0,
		ThroughputAfter:  0,
		Reasoning:        "No rows written for " + stallDuration.Round(time.Second).String(),
	}
	if err := d.state.SaveAIAdjustment(d.runID, record); err != nil {
		logging.Debug("Failed to log stall to state: %v", err)
	}
}

// logStallResolved records a stall resolution to SQLite.
func (d *StallDetector) logStallResolved(rowsAtResume int64, stallDuration time.Duration) {
	if d.state == nil || d.runID == "" {
		return
	}
	record := checkpoint.AIAdjustmentRecord{
		RunID:     d.runID,
		Timestamp: time.Now(),
		Action:    "stall_resolved",
		Reasoning: "Transfer resumed after " + stallDuration.String() + " stall",
	}
	if err := d.state.SaveAIAdjustment(d.runID, record); err != nil {
		logging.Debug("Failed to log stall resolution to state: %v", err)
	}
}

// Run starts monitoring in the background. Returns when ctx is cancelled.
func (d *StallDetector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	d.lastRows = d.getProgress()
	lastProgressTime := time.Now()
	warned := false
	stallStart := time.Now()
	lastWarnDuration := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := d.getProgress()

			if current > d.lastRows {
				// Progress made — reset
				d.lastRows = current
				lastProgressTime = time.Now()
				if warned {
					resolvedDuration := time.Since(stallStart).Round(time.Second)
					logging.Info("Transfer resumed — stall resolved after %s", resolvedDuration)
					d.logStallResolved(current, resolvedDuration)
					warned = false
				}
				d.stalled.Store(false)
			} else {
				// No progress
				stallDuration := time.Since(lastProgressTime)
				if stallDuration >= d.threshold {
					d.stalled.Store(true)
					if !warned {
						stallStart = time.Now()
						logging.Warn("Transfer stalled: no rows written for %s — possible connection hang or lock contention",
							stallDuration.Round(time.Second))
						warned = true

						// Log to SQLite for AI tuning history
						d.logStall(current, stallDuration)
					} else if stallDuration.Truncate(time.Minute) > lastWarnDuration {
						// Log every additional minute
						lastWarnDuration = stallDuration.Truncate(time.Minute)
						logging.Warn("Transfer still stalled (%s) — consider killing and resuming the migration",
							stallDuration.Round(time.Second))
					}
				}
			}
		}
	}
}
