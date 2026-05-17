package monitor

import (
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/transfer"
)

// apply persists the decision via the tuner and starts the per-knob
// cooldown. Cooldown is set ONLY after the tuner update succeeds —
// without that ordering, a failed Update would leave the knob in
// cooldown for 90s, suppressing retries even though no change took
// effect (Copilot review on PR #194).
func (c *Controller) apply(d *Decision) error {
	update := transfer.RuntimeUpdate{}
	switch d.Knob {
	case "chunk_size":
		update.ChunkSize = &d.NewValue
	case "write_ahead_writers":
		update.WriteAheadWriters = &d.NewValue
	default:
		return fmt.Errorf("unknown knob %q", d.Knob)
	}
	if err := c.tuner.Update(update); err != nil {
		return err
	}
	switch d.Knob {
	case "chunk_size":
		c.chunkSizeCooldownUntil = c.nowFn().Add(controllerCooldown)
		// Reset the WAW throughput-aware baseline (#199): chunk-size
		// changes alter the throughput surface, so prior WAW-add
		// throughput isn't comparable. Treating the next WAW add as
		// the first one (un-gated) is the safe default — the alternative
		// is to keep a stale baseline that suppresses legit adds.
		c.lastWAWAddThroughput = 0
		c.lastWAWAddThroughputSet = false
	case "write_ahead_writers":
		c.writeAheadCooldownUntil = c.nowFn().Add(controllerCooldown)
		// #199 throughput-aware gate (Codex review on PR #201):
		//   - Increase (Rule 2 add): snapshot the recent-window mean
		//     throughput so the next Rule 2 evaluation can verify
		//     this add actually delivered.
		//   - Decrease (Rule 3 error back-off): CLEAR the baseline.
		//     The WAW count just changed under us, so the prior-add
		//     throughput is no longer comparable; carrying it forward
		//     would gate a recovery re-add against a stale "post-add"
		//     reading and could suppress the recovery indefinitely.
		// Use the same lookback window Evaluate uses for queueGrew so
		// the comparison is apples-to-apples.
		if d.NewValue > d.PreviousValue {
			c.lastWAWAddThroughput = meanThroughput(c.collector.GetRecentMetrics(queueGrowthLookback))
			c.lastWAWAddThroughputSet = true
		} else {
			c.lastWAWAddThroughput = 0
			c.lastWAWAddThroughputSet = false
		}
	}
	return nil
}

// knobReady returns true when the named knob is past its cooldown
// (or has never been adjusted, which is the zero-time deadline).
func (c *Controller) knobReady(knob string, now time.Time) bool {
	switch knob {
	case "chunk_size":
		return now.After(c.chunkSizeCooldownUntil) || now.Equal(c.chunkSizeCooldownUntil)
	case "write_ahead_writers":
		return now.After(c.writeAheadCooldownUntil) || now.Equal(c.writeAheadCooldownUntil)
	default:
		return false
	}
}
