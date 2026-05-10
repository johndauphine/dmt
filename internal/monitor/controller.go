// Rule-based runtime parameter controller — the deterministic
// replacement for AIAdjuster (#172 of the AI-optional epic #167).
//
// Architecture: a fixed set of rules evaluated on each tick of the
// MetricsCollector, with per-knob cooldowns to prevent oscillation.
// No AI calls, no LLM round-trip, no external dependencies beyond
// the collector + tuner already in use.
//
// Rule set (evaluated in priority order, first match wins per tick):
//
//	IF memory_pct > 90:
//	    chunk_size *= 0.75   (back off under memory pressure)
//
//	ELIF queue_depth grew for 3 consecutive ticks:
//	    write_ahead_writers += 1   (consumer is the bottleneck —
//	                                add a writer to drain the queue)
//
//	ELIF error_rate > 0:
//	    write_ahead_writers -= 1   (target is rejecting work — back
//	                                off parallelism)
//
//	ELIF cpu_pct < 50 AND throughput stable for 3 ticks:
//	    chunk_size *= 1.10   (safe to grow chunks for throughput)
//
// Hysteresis: 90s cooldown per knob (chunk_size and
// write_ahead_writers cool down independently). Within a knob's
// cooldown window, that knob's rules don't fire — the other knob's
// rules can still fire if their cooldown has expired.
//
// The controller is a Go struct; tests construct it directly with
// synthetic metric traces and call Evaluate() / Apply() to verify
// rule firing without spinning up the ticker goroutine.

package monitor

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
)

// Controller is the rule-based runtime adjuster. Same lifecycle as
// AIMonitor (Start runs a ticker that evaluates rules + applies
// adjustments) but without the AI round-trip on each tick.
type Controller struct {
	tuner     transfer.RuntimeTuner
	collector *MetricsCollector
	interval  time.Duration

	// Per-knob cooldown deadlines. A rule for the corresponding knob
	// is suppressed until time.Now() >= the deadline.
	chunkSizeCooldownUntil  time.Time
	writeAheadCooldownUntil time.Time

	// Caps on parameters. Zero values mean "no cap" (use defensive
	// defaults: max WAW = 8, max chunk_size = unlimited).
	maxChunkSize int
	maxWAW       int

	// nowFn is the clock — overridable in tests via NewController's
	// options-style setter so cooldown logic can be exercised
	// deterministically without sleeping.
	nowFn func() time.Time
}

// ControllerOptions wraps the optional knobs the rule controller
// uses for clamping. Zero values fall back to defensive defaults.
type ControllerOptions struct {
	// MaxChunkSize caps the chunk_size knob's growth rule. The driver
	// layer's HardChunkLimit (#166) is the natural source. Zero =
	// unlimited.
	MaxChunkSize int

	// MaxWAW caps the write_ahead_writers knob's growth rule. Zero
	// falls back to defaultMaxWAW (8) — high enough that real
	// workloads rarely hit the cap, low enough that runaway
	// queue-depth-growth doesn't push WAW into the contention zone.
	MaxWAW int
}

// defaultMaxWAW is the cap the controller uses when ControllerOptions
// doesn't set one. Mirrors internal/tuning's maxWAWForGrid (8) — most
// parallel-write workloads peak well below 8 and additional writers
// just add contention.
const defaultMaxWAW = 8

// controllerCooldown is the per-knob hysteresis window. The same
// 90s the AI loop used; preserved intentionally so the controller
// adjustment cadence isn't materially different from what users
// observed pre-#172.
const controllerCooldown = 90 * time.Second

// memoryPressureThreshold is the memory-percent floor that triggers
// the chunk-shrink rule. ≥90% means PG (or MSSQL) is close to OOM;
// shrinking the chunk lets in-flight rows drain faster.
const memoryPressureThreshold = 90.0

// idleCPUThreshold below which the chunk-grow rule is allowed to
// fire. CPU < 50% means we have room to do more work per chunk
// before the host saturates.
const idleCPUThreshold = 50.0

// throughputStabilityRange is the maximum (max-min)/average of the
// last 3 throughput samples for the "stable" predicate to hold. 10%
// is loose enough to ignore tick-to-tick noise but tight enough that
// genuinely-trending throughput doesn't trigger growth.
const throughputStabilityRange = 0.10

// queueGrowthLookback is the number of ticks the controller looks
// back when checking "queue_depth grew for N consecutive ticks."
// Three ticks at the 30s default tick = 90s of sustained growth
// before the controller intervenes — long enough that one stalled
// chunk doesn't trigger a writer add.
const queueGrowthLookback = 3

// throughputStabilityLookback is the equivalent for the
// "throughput stable for N ticks" criterion.
const throughputStabilityLookback = 3

// chunkShrinkFactor is the multiplier applied to chunk_size when
// memory pressure fires. 0.75 backs off enough to make a difference
// without oscillating between sizes.
const chunkShrinkFactor = 0.75

// chunkGrowFactor is the multiplier applied to chunk_size when the
// idle-CPU + stable-throughput rule fires. 1.10 is gentle growth —
// enough to converge on a higher steady-state over several ticks
// without a single huge jump.
const chunkGrowFactor = 1.10

// NewController builds a Controller. The MetricsCollector should be
// the same one the rest of the migration pipeline uses (don't
// construct a separate one — they need shared metric history).
func NewController(tuner transfer.RuntimeTuner, collector *MetricsCollector, interval time.Duration, opts ControllerOptions) *Controller {
	if opts.MaxWAW == 0 {
		opts.MaxWAW = defaultMaxWAW
	}
	return &Controller{
		tuner:        tuner,
		collector:    collector,
		interval:     interval,
		maxChunkSize: opts.MaxChunkSize,
		maxWAW:       opts.MaxWAW,
		nowFn:        time.Now,
	}
}

// SetClock overrides the controller's time source. Tests use this to
// exercise cooldown logic without sleeping; production callers don't
// need it.
func (c *Controller) SetClock(now func() time.Time) {
	c.nowFn = now
}

// Start runs the controller's tick loop. Mirrors AIMonitor.Start so
// the wiring layer (transfer_runner) can swap implementations
// without further surgery in #172b.
//
// The collector should be started separately (typically by the same
// caller, in a sibling goroutine) so the ticker fires against
// already-populated metrics.
func (c *Controller) Start(ctx context.Context) {
	logging.Debug("rule controller started (interval=%v, max_waw=%d, max_chunk_size=%d)",
		c.interval, c.maxWAW, c.maxChunkSize)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.tick()
		case <-ctx.Done():
			logging.Debug("rule controller stopped")
			return
		}
	}
}

// tick is one evaluation cycle: read recent metrics, evaluate rules,
// apply the first matching adjustment. Exposed via Evaluate for
// tests; the production ticker calls this directly.
func (c *Controller) tick() {
	decision := c.Evaluate(c.nowFn())
	if decision == nil {
		return
	}
	if err := c.apply(decision); err != nil {
		logging.Warn("rule controller: apply failed: %v", err)
		return
	}
	logging.Debug("rule controller: %s — %s", decision.Knob, decision.Reasoning)
}

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
		next := scaleClampedChunk(cur.ChunkSize, chunkShrinkFactor, c.maxChunkSize)
		if next != cur.ChunkSize {
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
	if c.knobReady("write_ahead_writers", now) && queueGrew(recent) {
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
		next := scaleClampedChunk(cur.ChunkSize, chunkGrowFactor, c.maxChunkSize)
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

// apply persists the decision via the tuner and starts the per-knob
// cooldown. Returns the tuner's error verbatim so callers can log
// it; the caller is the production tick() and the test harness.
func (c *Controller) apply(d *Decision) error {
	update := transfer.RuntimeUpdate{}
	switch d.Knob {
	case "chunk_size":
		update.ChunkSize = &d.NewValue
		c.chunkSizeCooldownUntil = c.nowFn().Add(controllerCooldown)
	case "write_ahead_writers":
		update.WriteAheadWriters = &d.NewValue
		c.writeAheadCooldownUntil = c.nowFn().Add(controllerCooldown)
	default:
		return fmt.Errorf("unknown knob %q", d.Knob)
	}
	return c.tuner.Update(update)
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

// scaleClampedChunk multiplies cur by factor and clamps the result
// to [1, maxChunkSize]. maxChunkSize == 0 means no upper bound (the
// driver-layer HardChunkLimit isn't always known to the controller).
func scaleClampedChunk(cur int, factor float64, maxChunkSize int) int {
	scaled := int(float64(cur) * factor)
	if scaled < 1 {
		scaled = 1
	}
	if maxChunkSize > 0 && scaled > maxChunkSize {
		scaled = maxChunkSize
	}
	return scaled
}
