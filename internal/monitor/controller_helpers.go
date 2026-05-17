package monitor

// queueGrew returns true when the QueueDepth values across the
// recent slice are strictly monotonically increasing AND the slice
// has at least queueGrowthLookback entries. Strict monotonicity
// avoids firing on flat queues (the consumer is keeping up) or
// jittery queues (depth bouncing around but not trending).
func queueGrew(recent []PerformanceSnapshot) bool {
	if len(recent) < queueGrowthLookback {
		return false
	}
	tail := recent[len(recent)-queueGrowthLookback:]
	for i := 1; i < len(tail); i++ {
		if tail[i].QueueDepth <= tail[i-1].QueueDepth {
			return false
		}
	}
	return true
}

// throughputStable returns true when the last throughputStabilityLookback
// samples have a (max-min)/average within throughputStabilityRange.
// Loose enough to tolerate tick-to-tick noise; tight enough that
// real trends (a sustained drop or climb) are NOT classified as
// stable.
func throughputStable(recent []PerformanceSnapshot) bool {
	if len(recent) < throughputStabilityLookback {
		return false
	}
	tail := recent[len(recent)-throughputStabilityLookback:]
	min, max := tail[0].Throughput, tail[0].Throughput
	var sum float64
	for _, s := range tail {
		if s.Throughput < min {
			min = s.Throughput
		}
		if s.Throughput > max {
			max = s.Throughput
		}
		sum += s.Throughput
	}
	avg := sum / float64(len(tail))
	if avg <= 0 {
		// Zero-throughput window — caller decides; "stable" doesn't
		// apply meaningfully. Returning false here means the grow
		// rule won't fire when nothing's flowing.
		return false
	}
	return (max-min)/avg <= throughputStabilityRange
}

// meanThroughput returns the arithmetic mean of Throughput across the
// recent slice. Returns 0 on empty input. Note: 0 is also a legitimate
// mean for non-empty input (every snapshot has zero throughput, the
// "writers stalled" case), so callers that need to distinguish
// "no baseline yet" from "baseline of zero" must track presence
// separately — the #199 throughput-aware gate uses
// lastWAWAddThroughputSet for that, with a special case for the
// zero-baseline branch where the proportional check would otherwise
// no-op.
func meanThroughput(recent []PerformanceSnapshot) float64 {
	if len(recent) == 0 {
		return 0
	}
	var sum float64
	for _, s := range recent {
		sum += s.Throughput
	}
	return sum / float64(len(recent))
}

// newErrorsSinceLast returns the count of errors that occurred between
// the second-most-recent and most-recent snapshots (i.e., new errors
// in the latest tick window). Returns 0 when there are fewer than 2
// snapshots — no baseline to compare against, so we don't fire the
// back-off rule on the first tick.
//
// Codex review on PR #194: ErrorCount is cumulative, so the original
// `latest.ErrorCount > 0` check would re-fire on every tick after
// any historical failure. This delta-based check fires only on NEW
// errors, which is the actual control signal we want.
func newErrorsSinceLast(recent []PerformanceSnapshot) int {
	if len(recent) < 2 {
		return 0
	}
	latest := recent[len(recent)-1]
	prev := recent[len(recent)-2]
	delta := latest.ErrorCount - prev.ErrorCount
	if delta < 0 {
		// Defensive: shouldn't happen since ErrorCount is monotonic
		// per the tuner's atomic counter. Treat negative as zero.
		return 0
	}
	return delta
}

// shrinkChunk multiplies cur by factor (0 < factor < 1) and floors
// the result at minChunkSize. Truncation toward zero on the multiply
// is fine here — for shrink we'd rather over-shrink slightly than
// undershrink. minChunkSize=0 falls back to a defensive 1.
func shrinkChunk(cur int, factor float64, minChunkSize int) int {
	if minChunkSize <= 0 {
		minChunkSize = 1
	}
	scaled := int(float64(cur) * factor)
	if scaled < minChunkSize {
		scaled = minChunkSize
	}
	return scaled
}

// growChunk multiplies cur by factor (factor > 1) and caps the
// result at maxChunkSize. Rounds UP rather than truncating so a
// small `cur × 1.10` doesn't stay at the original value (Copilot
// review on PR #194 — int truncation could pin chunk_size at 1).
// Guarantees at least +1 when growth would otherwise round to a
// no-op. maxChunkSize=0 means uncapped (the driver-layer
// HardChunkLimit isn't always known to the controller).
func growChunk(cur int, factor float64, maxChunkSize int) int {
	scaled := int(float64(cur)*factor + 0.5) // round half up
	if scaled <= cur {
		scaled = cur + 1
	}
	if maxChunkSize > 0 && scaled > maxChunkSize {
		scaled = maxChunkSize
	}
	return scaled
}
