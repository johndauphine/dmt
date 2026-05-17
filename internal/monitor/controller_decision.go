package monitor

import (
	"fmt"
	"time"
)

// Decision describes one runtime adjustment the controller wants to
// make. nil from Evaluate means "no rule fires this tick"; a non-nil
// Decision is always actionable (NewValue != PreviousValue, knob in
// {"chunk_size","write_ahead_writers"}).
type Decision struct {
	Knob          string // "chunk_size" or "write_ahead_writers"
	PreviousValue int
	NewValue      int
	Reasoning     string
}

// Evaluate inspects the latest metrics + cooldowns and returns the
// next adjustment, or nil when no rule fires. Pure function — does
// not mutate the controller's cooldown state. apply() is what
// updates cooldowns after a successful adjustment.
//
// The now argument is the wall-clock value used for cooldown
// comparisons. Production calls pass time.Now(); tests pass a fixed
// time so cooldown windows are deterministic.
func (c *Controller) Evaluate(now time.Time) *Decision {
	recent := c.collector.GetRecentMetrics(queueGrowthLookback)
	if len(recent) == 0 {
		return nil
	}
	latest := recent[len(recent)-1]
	cur := latest.CurrentConfig

	// Rule 1: memory pressure → shrink chunk_size.
	if latest.MemoryPercent > memoryPressureThreshold && c.knobReady("chunk_size", now) {
		next := shrinkChunk(cur.ChunkSize, chunkShrinkFactor, c.minChunkSize)
		if next < cur.ChunkSize {
			return &Decision{
				Knob:          "chunk_size",
				PreviousValue: cur.ChunkSize,
				NewValue:      next,
				Reasoning: fmt.Sprintf(
					"memory %.1f%% above %.0f%% threshold — shrinking chunk_size from %d to %d",
					latest.MemoryPercent, memoryPressureThreshold, cur.ChunkSize, next,
				),
			}
		}
	}

	// Rule 2: queue depth grew for N consecutive ticks → add a writer.
	// Two suppression gates added in #199:
	//   - Memory interlock: don't add a writer when memory% is already
	//     in the "swamped" zone (≥80, below Rule 1's 90% OOM threshold).
	//   - Throughput-aware: if the prior writer-add didn't improve the
	//     recent-window mean throughput by ≥2%, the writer-side wasn't
	//     the bottleneck — don't add another.
	// Both gates are skip-only — when either fires, control falls through
	// to Rules 3 and 4 normally.
	if c.knobReady("write_ahead_writers", now) && queueGrew(recent) {
		suppressed := false
		if latest.MemoryPercent >= writerAddMemoryInterlockThreshold {
			suppressed = true
		}
		if !suppressed && c.lastWAWAddThroughputSet {
			// `recent` is guaranteed non-empty here — Evaluate returns
			// early when the metrics window is empty. So meanThroughput
			// returns the actual mean (possibly 0 if every recent
			// snapshot has zero throughput, which is the strongest
			// possible "writers are stalled" signal — the gate must
			// suppress in that case, not skip it.)
			currentMean := meanThroughput(recent)
			switch {
			case c.lastWAWAddThroughput == 0:
				// Prior add happened during a stall (baseline=0). Any
				// positive throughput counts as improvement; continued
				// zero is no improvement and must be suppressed.
				// Special case because the proportional check below
				// (0 < 0*1.02 = 0) wouldn't fire on continued zero.
				if currentMean == 0 {
					suppressed = true
				}
			case currentMean < c.lastWAWAddThroughput*writerAddMinImprovementRatio:
				suppressed = true
			}
		}
		if !suppressed {
			next := cur.WriteAheadWriters + 1
			if next <= c.maxWAW {
				return &Decision{
					Knob:          "write_ahead_writers",
					PreviousValue: cur.WriteAheadWriters,
					NewValue:      next,
					Reasoning: fmt.Sprintf(
						"queue_depth grew for %d consecutive ticks — adding writer (%d → %d)",
						queueGrowthLookback, cur.WriteAheadWriters, next,
					),
				}
			}
		}
	}

	// Rule 3: NEW errors since last tick → back off parallelism.
	//
	// ErrorCount is the cumulative-since-run-start count from the
	// runtime tuner. A naive `latest.ErrorCount > 0` check would
	// re-fire the back-off rule every cooldown window after a single
	// historical failure, eventually pinning WAW at 1 even when no
	// new errors are occurring (Codex review on PR #194). Compare
	// to the prior snapshot to detect NEW errors in the tick window
	// instead.
	//
	// On the first tick (no prior snapshot), the rule doesn't fire
	// — there's no baseline to delta against. The next tick will
	// have a baseline.
	if newErrors := newErrorsSinceLast(recent); newErrors > 0 && c.knobReady("write_ahead_writers", now) {
		next := cur.WriteAheadWriters - 1
		if next >= 1 {
			return &Decision{
				Knob:          "write_ahead_writers",
				PreviousValue: cur.WriteAheadWriters,
				NewValue:      next,
				Reasoning: fmt.Sprintf(
					"%d new error(s) since last tick — backing off writers (%d → %d)",
					newErrors, cur.WriteAheadWriters, next,
				),
			}
		}
	}

	// Rule 4: idle CPU + stable throughput → grow chunk_size.
	if latest.CPUPercent < idleCPUThreshold && c.knobReady("chunk_size", now) && throughputStable(recent) {
		next := growChunk(cur.ChunkSize, chunkGrowFactor, c.maxChunkSize)
		if next > cur.ChunkSize {
			return &Decision{
				Knob:          "chunk_size",
				PreviousValue: cur.ChunkSize,
				NewValue:      next,
				Reasoning: fmt.Sprintf(
					"CPU %.1f%% below %.0f%% idle threshold + throughput stable for %d ticks — growing chunk_size from %d to %d",
					latest.CPUPercent, idleCPUThreshold, throughputStabilityLookback, cur.ChunkSize, next,
				),
			}
		}
	}

	return nil
}
