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
