// Rule-based runtime parameter controller — the deterministic
// replacement for Adjuster (#172 of the AI-optional epic #167).
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
//	ELIF queue_depth grew for 3 consecutive ticks
//	   AND memory_pct < 80                       (#199 interlock)
//	   AND prior writer-add improved throughput  (#199 throughput-aware):
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
// Rule 2 has two extra suppression gates added after #199:
//   - Memory interlock: if memory% ≥ 80, adding a writer compounds
//     pressure on the target DB. Rule 1's 90% threshold targets
//     OOM-imminent; the 80% interlock is the broader "PG is already
//     swamped" guard that spans the gap between Rule 1 and a healthy
//     baseline.
//   - Throughput-aware: after a writer-add, if the recent mean
//     throughput hasn't improved by ≥2%, the writer didn't help —
//     the bottleneck is somewhere else (PG WAL flush, disk I/O,
//     etc.). Don't add another. Reset on rule fires that change
//     other knobs, since chunk_size changes invalidate the prior
//     baseline.
//
// Both gates are SUPPRESS-only — they don't fire a rule of their
// own, they just decline Rule 2's add. When suppressed, control
// falls through to Rules 3 and 4 normally.
//
// The controller is a Go struct; tests construct it directly with
// synthetic metric traces and call Evaluate() / apply() to verify
// rule firing without spinning up the ticker goroutine.

package monitor

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
	"time"
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

	// lastWAWAddThroughput is the recent-window mean throughput captured
	// at the moment of the most recent successful write_ahead_writers add
	// (#199 throughput-aware gate). lastWAWAddThroughputSet is the
	// "has baseline" flag — needed because zero is a valid baseline
	// (writers stalled at the moment of the prior add). Without the
	// flag, treating zero as "no baseline" would let stack-of-adds
	// proceed during a full stall (Codex review on PR #201, second
	// round).
	lastWAWAddThroughput    float64
	lastWAWAddThroughputSet bool

	// Caps + floors on parameters. Defaults applied in NewController.
	maxChunkSize int
	minChunkSize int
	maxWAW       int
	safetyOnly   bool

	// nowFn is the clock — overridable in tests via NewController's
	// options-style setter so cooldown logic can be exercised
	// deterministically without sleeping.
	nowFn func() time.Time

	runID                string
	adjustmentRecorder   AdjustmentRecorder
	adjustmentNumber     int
	nextAdjustmentNumber func() int
}

// ControllerOptions wraps the optional knobs the rule controller
// uses for clamping. Zero values fall back to defensive defaults.
type ControllerOptions struct {
	// SafetyOnly suppresses performance-growth Rules 2 and 4 while keeping
	// the memory/error backoff Rules 1 and 3 active. Exploration probes use
	// this mode so safety still wins without contaminating clean cohorts.
	SafetyOnly bool

	// MaxChunkSize caps the chunk_size knob's growth rule. The driver
	// layer's HardChunkLimit (#166) is the natural source. Zero =
	// unlimited.
	MaxChunkSize int

	// MinChunkSize floors the chunk_size knob's shrink rule.
	// Without a floor, sustained memory pressure could shrink
	// chunk_size all the way to 1, where per-row overhead dominates
	// and migrations crawl. The old AI adjuster floored at 5000;
	// preserved as the default here. Zero falls back to
	// defaultMinChunkSize (Copilot review on PR #194).
	MinChunkSize int

	// MaxWAW caps the write_ahead_writers knob's growth rule. Zero
	// falls back to defaultMaxWAW (8) — high enough that real
	// workloads rarely hit the cap, low enough that runaway
	// queue-depth-growth doesn't push WAW into the contention zone.
	MaxWAW int

	// RunID and AdjustmentRecorder persist successful runtime decisions
	// so later analysis can explain why knobs changed during the run.
	RunID                   string
	InitialAdjustmentNumber int
	NextAdjustmentNumber    func() int
	AdjustmentRecorder      AdjustmentRecorder
}

// AdjustmentRecorder persists a successful runtime controller decision.
// The controller treats recorder failures as telemetry failures: the
// runtime adjustment has already succeeded and is not rolled back.
type AdjustmentRecorder func(runID string, record AdjustmentRecord) error

// AdjustmentRecord is the monitor package's persistence-neutral shape for
// a runtime controller decision.
type AdjustmentRecord struct {
	AdjustmentNumber int
	Timestamp        time.Time
	Action           string
	Adjustments      map[string]int
	ThroughputBefore float64
	ThroughputAfter  float64
	EffectPercent    float64
	CPUBefore        float64
	CPUAfter         float64
	MemoryBefore     float64
	MemoryAfter      float64
	Reasoning        string
	Confidence       string
}

// defaultMaxWAW is the cap the controller uses when ControllerOptions
// doesn't set one. Mirrors internal/tuning's maxWAWForGrid (8) — most
// parallel-write workloads peak well below 8 and additional writers
// just add contention.
const defaultMaxWAW = 8

// DefaultMinChunkSize is the floor for the memory-pressure shrink
// rule when ControllerOptions doesn't set one. Below 5000 rows per
// chunk, per-row overhead dominates and throughput degrades sharply.
// Matches the old AI adjuster's floor (Copilot review on PR #194).
//
// Exported so callers (e.g. orchestrator/transfer_runner clamping
// MinChunkSize under a probe-derived hard cap, #166) can reference
// the single source of truth instead of duplicating the literal 5000.
const DefaultMinChunkSize = 5000

// defaultMinChunkSize is kept as a private alias for in-package use
// so the rest of this file doesn't churn across the export.
const defaultMinChunkSize = DefaultMinChunkSize

// controllerCooldown is the per-knob hysteresis window. The same
// 90s the AI loop used; preserved intentionally so the controller
// adjustment cadence isn't materially different from what users
// observed pre-#172.
const controllerCooldown = 90 * time.Second

// memoryPressureThreshold is the memory-percent floor that triggers
// the chunk-shrink rule. The Rule 1 condition is `MemoryPercent >
// memoryPressureThreshold` (strict), so a memory% above 90 (not
// equal) means PG (or MSSQL) is close to OOM and shrinking the
// chunk lets in-flight rows drain faster.
const memoryPressureThreshold = 90.0

// writerAddMemoryInterlockThreshold is the upper bound on memory% for
// Rule 2 to fire (#199). Rule 1's chunk-shrink threshold is 90% (OOM-
// imminent); this 80% gate suppresses writer-adds in the broader
// "target DB is already under pressure" zone where a new writer would
// compound memory contention without draining the queue. Set lower
// than memoryPressureThreshold so the gate fires before Rule 1 does.
const writerAddMemoryInterlockThreshold = 80.0

// writerAddMinImprovementRatio is the minimum throughput improvement
// the throughput-aware gate (#199) requires before allowing another
// Rule 2 fire. 1.02 = +2%; ratios at or below this means the prior
// writer-add didn't deliver, so adding another isn't likely to either.
// 2% is loose enough that tick-to-tick noise doesn't suppress a real
// payoff but tight enough to catch the "throughput flat after add"
// failure mode observed in the #199 sweep.
const writerAddMinImprovementRatio = 1.02

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
// Three ticks at the 5s default tick = 15s of sustained growth
// before the controller intervenes — long enough that one stalled
// chunk doesn't trigger a writer add, short enough that real
// queue pressure gets a response within typical migration runtimes.
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
	if opts.MinChunkSize == 0 {
		opts.MinChunkSize = defaultMinChunkSize
	}
	return &Controller{
		tuner:        tuner,
		collector:    collector,
		interval:     interval,
		maxChunkSize: opts.MaxChunkSize,
		minChunkSize: opts.MinChunkSize,
		maxWAW:       opts.MaxWAW,
		safetyOnly:   opts.SafetyOnly,
		nowFn:        time.Now,

		runID:                opts.RunID,
		adjustmentNumber:     opts.InitialAdjustmentNumber,
		nextAdjustmentNumber: opts.NextAdjustmentNumber,
		adjustmentRecorder:   opts.AdjustmentRecorder,
	}
}

// SetClock overrides the controller's time source. Tests use this to
// exercise cooldown logic without sleeping; production callers don't
// need it.
func (c *Controller) SetClock(now func() time.Time) {
	c.nowFn = now
}

// UpdateRowsProcessed forwards the live row count to the underlying
// MetricsCollector. Mirrors AIMonitor.UpdateRowsProcessed so the
// transfer runner can update rows processed without knowing which
// monitor implementation is in use.
func (c *Controller) UpdateRowsProcessed(count int64) {
	c.collector.UpdateRowCount(count)
}

// Start runs the controller's tick loop AND spawns the collector
// goroutine — drop-in replacement for AIMonitor.Start so wiring
// (transfer_runner) is a one-line swap in 172b.
//
// The collector goroutine is started here (not by the caller) so
// the contract is "construct + Start = working monitor." Mirrors
// AIMonitor.Start which has the same shape (Copilot review on PR
// #194 — earlier draft documented "collector started separately"
// but didn't enforce it, leading to silent no-ticks if the caller
// forgot).
func (c *Controller) Start(ctx context.Context) {
	logging.Debug("rule controller started (interval=%v, max_waw=%d, max_chunk_size=%d, min_chunk_size=%d)",
		c.interval, c.maxWAW, c.maxChunkSize, c.minChunkSize)

	go c.collector.Start(ctx)

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
	if !c.safetyOnly && c.knobReady("write_ahead_writers", now) && queueGrew(recent) {
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
	if !c.safetyOnly && latest.CPUPercent < idleCPUThreshold && c.knobReady("chunk_size", now) && throughputStable(recent) {
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

// apply persists the decision via the tuner and starts the per-knob
// cooldown. Cooldown is set ONLY after the tuner update succeeds —
// without that ordering, a failed Update would leave the knob in
// cooldown for 90s, suppressing retries even though no change took
// effect (Copilot review on PR #194).
func (c *Controller) apply(d *Decision) error {
	before := c.latestSnapshot()
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
	c.recordAdjustment(d, before)
	return nil
}

func (c *Controller) latestSnapshot() PerformanceSnapshot {
	recent := c.collector.GetRecentMetrics(1)
	if len(recent) == 0 {
		return PerformanceSnapshot{}
	}
	return recent[len(recent)-1]
}

func (c *Controller) recordAdjustment(d *Decision, before PerformanceSnapshot) {
	if c.adjustmentRecorder == nil || c.runID == "" || d == nil {
		return
	}
	record := AdjustmentRecord{
		AdjustmentNumber: c.nextAdjustmentNumberValue(),
		Timestamp:        c.nowFn(),
		Action:           d.Knob,
		Adjustments:      map[string]int{d.Knob: d.NewValue},
		ThroughputBefore: before.Throughput,
		CPUBefore:        before.CPUPercent,
		MemoryBefore:     before.MemoryPercent,
		Reasoning:        d.Reasoning,
		Confidence:       "deterministic",
	}
	if err := c.adjustmentRecorder(c.runID, record); err != nil {
		logging.Warn("rule controller: failed to persist runtime adjustment: %v", err)
	}
}

func (c *Controller) nextAdjustmentNumberValue() int {
	if c.nextAdjustmentNumber != nil {
		return c.nextAdjustmentNumber()
	}
	c.adjustmentNumber++
	return c.adjustmentNumber
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
