// Tests for the rule-based runtime controller (#172). Exercise each
// rule in isolation with synthetic snapshot traces, plus cooldown
// hysteresis. The Evaluate() function is pure (returns a Decision
// without mutating state), so tests don't need a ticker goroutine.

package monitor

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/transfer"
)

// fixedClock returns a function suitable for Controller.SetClock
// that always returns the given instant. Lets tests advance time
// by constructing a new clock between Evaluate calls.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// newTestController builds a Controller with fresh tuner +
// collector at the given baseline params, ready for tests to
// inject metrics via the collector's append (test-only) helper.
func newTestController(t *testing.T, opts ControllerOptions) (*Controller, *MetricsCollector, transfer.RuntimeTuner) {
	t.Helper()
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{
		ChunkSize:         50000,
		WriteAheadWriters: 2,
		ReadAheadBuffers:  4,
		ParallelReaders:   2,
	})
	collector := NewMetricsCollector(tuner, 30*time.Second)
	c := NewController(tuner, collector, 30*time.Second, opts)
	return c, collector, tuner
}

// pushSnapshots appends synthetic snapshots to the collector's
// internal slice. Bypasses the live collectSnapshot path (which
// requires gopsutil + a real tuner with metrics) — tests instead
// directly drive the rule evaluator with hand-crafted traces.
func pushSnapshots(c *MetricsCollector, snaps ...PerformanceSnapshot) {
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	c.metrics = append(c.metrics, snaps...)
}

// snap builds a PerformanceSnapshot with the given knobs. Unset
// fields stay zero — rules that read them treat zero as "no
// signal" (queue depth, errors, throughput).
func snap(memPct, cpuPct float64, queueDepth, errorCount int, throughput float64, chunk, waw int) PerformanceSnapshot {
	return PerformanceSnapshot{
		Timestamp:     time.Now(),
		MemoryPercent: memPct,
		CPUPercent:    cpuPct,
		QueueDepth:    queueDepth,
		ErrorCount:    errorCount,
		Throughput:    throughput,
		CurrentConfig: ConfigSnapshot{
			ChunkSize:         chunk,
			WriteAheadWriters: waw,
		},
	}
}

// ---------- Rule 1: memory pressure → shrink chunk_size ----------

func TestController_MemoryPressure_ShrinksChunkSize(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(95.0, 30, 0, 0, 500_000, 50000, 2))

	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.Knob != "chunk_size" {
		t.Errorf("knob: got %q, want chunk_size", d.Knob)
	}
	want := int(50000 * chunkShrinkFactor) // 37500
	if d.NewValue != want {
		t.Errorf("new chunk_size: got %d, want %d (50000 × %.2f)", d.NewValue, want, chunkShrinkFactor)
	}
}

// TestController_MemoryPressure_FlooredAtMinChunkSize — Copilot review
// on PR #194. Without a floor, sustained memory pressure could
// shrink chunk_size all the way to 1, where per-row overhead
// dominates and migrations crawl. Default floor is 5000 (matching
// the old AI adjuster); this test pins that we don't go below it
// even when the shrink-factor would normally take us further.
func TestController_MemoryPressure_FlooredAtMinChunkSize(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	// Already-small chunk_size (6000); shrink-factor 0.75 would
	// take it to 4500, below the default floor of 5000.
	pushSnapshots(col, snap(95.0, 30, 0, 0, 500_000, 6000, 2))

	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.NewValue != defaultMinChunkSize {
		t.Errorf("new chunk_size: got %d, want floor at %d", d.NewValue, defaultMinChunkSize)
	}
}

// TestController_MemoryPressure_AlreadyAtFloor_NoFire — once we're
// at the floor, the rule has nothing to do. NewValue would equal
// PreviousValue; Evaluate should return nil rather than emit a
// no-op Decision.
func TestController_MemoryPressure_AlreadyAtFloor_NoFire(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(95.0, 30, 0, 0, 500_000, defaultMinChunkSize, 2))

	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("at floor — should not emit no-op Decision; got %+v", d)
	}
}

func TestController_MemoryBelowThreshold_NoFire(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(85.0, 30, 0, 0, 500_000, 50000, 2)) // 85% < 90%

	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("memory below threshold should not trigger any rule; got %+v", d)
	}
}

// ---------- Rule 2: queue depth grew → add writer ----------

func TestController_QueueGrew_AddsWriter(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	// Sub-90% memory, sub-50% CPU, queue strictly increasing.
	pushSnapshots(col,
		snap(50, 30, 5, 0, 500_000, 50000, 2),
		snap(50, 30, 8, 0, 500_000, 50000, 2),
		snap(50, 30, 12, 0, 500_000, 50000, 2),
	)

	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.Knob != "write_ahead_writers" {
		t.Errorf("knob: got %q, want write_ahead_writers", d.Knob)
	}
	if d.NewValue != 3 {
		t.Errorf("new WAW: got %d, want 3", d.NewValue)
	}
}

func TestController_QueueFlat_DoesNotAddWriter(t *testing.T) {
	// CPU=80 disables rule 4 (idle-CPU grow). With flat queue + no
	// errors + sub-90% memory, no rule should fire.
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("flat queue + busy CPU should not fire any rule; got %+v", d)
	}
}

func TestController_QueueGrewButHitsCap_DoesNotAddWriter(t *testing.T) {
	// WAW already at cap. CPU=80 disables rule 4 so we can isolate
	// rule 2's "no-add when capped" behavior.
	c, col, _ := newTestController(t, ControllerOptions{MaxWAW: 2})
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("WAW at MaxWAW=2 should suppress add; got %+v", d)
	}
}

// TestController_QueueGrewAtCap_FallsThroughToGrowChunk — sanity guard
// that when the WAW rule is suppressed (capped) but CPU is idle and
// throughput is stable, rule 4 (grow chunk) fires instead. This is
// actually the right response: can't add writers, try larger chunks
// per writer.
func TestController_QueueGrewAtCap_FallsThroughToGrowChunk(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{MaxWAW: 2})
	pushSnapshots(col,
		snap(50, 30, 5, 0, 500_000, 50000, 2),
		snap(50, 30, 8, 0, 510_000, 50000, 2),
		snap(50, 30, 12, 0, 495_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil || d.Knob != "chunk_size" {
		t.Errorf("WAW capped + idle CPU + stable throughput: should fall through to grow-chunk; got %+v", d)
	}
}

// ---------- Rule 3: NEW errors → back off writers ----------

func TestController_NewErrors_ReducesWAW(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	// Two snapshots: prior shows 0 errors, latest shows 3 → delta=3.
	pushSnapshots(col,
		snap(50, 80, 0, 0, 500_000, 50000, 4),
		snap(50, 80, 0, 3, 500_000, 50000, 4),
	)
	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.Knob != "write_ahead_writers" {
		t.Errorf("knob: got %q, want write_ahead_writers", d.Knob)
	}
	if d.NewValue != 3 {
		t.Errorf("new WAW: got %d, want 3 (4 - 1)", d.NewValue)
	}
}

// TestController_StaleErrors_NoFire — Codex P2 regression on PR #194.
// ErrorCount is cumulative; a naive `latest.ErrorCount > 0` check
// would fire the back-off rule on every cooldown after a single
// historical failure, pinning WAW at 1. The fix uses a delta against
// the previous snapshot, so unchanged cumulative counts don't fire.
func TestController_StaleErrors_NoFire(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	// Both snapshots show ErrorCount=3 — no NEW errors since last tick.
	pushSnapshots(col,
		snap(50, 80, 0, 3, 500_000, 50000, 4),
		snap(50, 80, 0, 3, 500_000, 50000, 4),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("stale cumulative error count should not fire back-off; got %+v (#194 regression)", d)
	}
}

func TestController_NewErrorsButWAW1_NoFire(t *testing.T) {
	// WAW already at floor — can't go lower.
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 80, 0, 0, 500_000, 50000, 1),
		snap(50, 80, 0, 3, 500_000, 50000, 1),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("WAW=1 + new errors should not fire (floor reached); got %+v", d)
	}
}

// TestController_FirstTick_NoFireOnErrors — boundary case: with only
// one snapshot, there's no prior to delta against, so the rule
// shouldn't fire even if ErrorCount > 0. Next tick has a baseline.
func TestController_FirstTick_NoFireOnErrors(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(50, 80, 0, 5, 500_000, 50000, 4))
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("single snapshot should not fire error rule (no baseline); got %+v", d)
	}
}

func TestNewErrorsSinceLast(t *testing.T) {
	cases := []struct {
		name     string
		errCount []int // per-snapshot ErrorCount
		want     int
	}{
		{"no_history", nil, 0},
		{"single_snapshot", []int{5}, 0},
		{"no_change", []int{3, 3}, 0},
		{"single_new_error", []int{0, 1}, 1},
		{"three_new_errors", []int{2, 5}, 3},
		{"three_history_window_takes_last_two", []int{0, 2, 5}, 3},
		{"defensive_decrease_returns_zero", []int{5, 3}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := make([]PerformanceSnapshot, len(tc.errCount))
			for i, n := range tc.errCount {
				snaps[i].ErrorCount = n
			}
			if got := newErrorsSinceLast(snaps); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// ---------- Rule 4: idle CPU + stable throughput → grow chunk ----------

func TestController_IdleCPUStableThroughput_GrowsChunk(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 30, 0, 0, 500_000, 50000, 2),
		snap(50, 30, 0, 0, 510_000, 50000, 2),
		snap(50, 30, 0, 0, 495_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.Knob != "chunk_size" {
		t.Errorf("knob: got %q, want chunk_size", d.Knob)
	}
	want := int(50000 * chunkGrowFactor) // 55000
	if d.NewValue != want {
		t.Errorf("new chunk_size: got %d, want %d", d.NewValue, want)
	}
}

func TestController_CPUNotIdle_NoFire(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 80, 0, 0, 500_000, 50000, 2),
		snap(50, 80, 0, 0, 510_000, 50000, 2),
		snap(50, 80, 0, 0, 495_000, 50000, 2),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("CPU 80%% > 50%% threshold should not trigger grow; got %+v", d)
	}
}

func TestController_ThroughputUnstable_NoFire(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 30, 0, 0, 100_000, 50000, 2), // wide variance
		snap(50, 30, 0, 0, 500_000, 50000, 2),
		snap(50, 30, 0, 0, 800_000, 50000, 2),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("variance 100K-800K (>10%%) should not trigger grow; got %+v", d)
	}
}

func TestController_ChunkGrowClampedByMaxChunkSize(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{MaxChunkSize: 51000})
	pushSnapshots(col,
		snap(50, 30, 0, 0, 500_000, 50000, 2),
		snap(50, 30, 0, 0, 500_000, 50000, 2),
		snap(50, 30, 0, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.NewValue != 51000 {
		t.Errorf("new chunk_size should clamp at MaxChunkSize=51000; got %d", d.NewValue)
	}
}

// TestController_ChunkGrow_FromSmallValue_AlwaysIncreases — Copilot
// review on PR #194. With int truncation, small chunk_size × 1.10
// could round down to the same value, never recovering. growChunk
// rounds half-up AND guarantees +1 minimum so growth always
// happens. Test: starting at chunk_size=10, we should see at least 11.
func TestController_ChunkGrow_FromSmallValue_AlwaysIncreases(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(50, 30, 0, 0, 500_000, 10, 2),
		snap(50, 30, 0, 0, 500_000, 10, 2),
		snap(50, 30, 0, 0, 500_000, 10, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil {
		t.Fatal("expected a Decision; got nil")
	}
	if d.NewValue <= 10 {
		t.Errorf("growChunk(10, 1.10) must increase, not stay at 10; got %d (#194 regression)", d.NewValue)
	}
}

// ---------- Rule priority ----------

func TestController_RulePriority_MemoryWinsOverQueue(t *testing.T) {
	// Both memory pressure AND queue growing are eligible. Memory
	// should win because it's checked first.
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(95, 30, 5, 0, 500_000, 50000, 2),
		snap(95, 30, 8, 0, 500_000, 50000, 2),
		snap(95, 30, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil || d.Knob != "chunk_size" {
		t.Errorf("memory pressure should win over queue growth; got %+v", d)
	}
}

// ---------- Cooldown ----------

func TestController_ChunkSizeCooldown_BlocksRepeatedFires(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})

	// Tick 1 — memory pressure fires, applies, sets cooldown.
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))
	pushSnapshots(col, snap(95, 30, 0, 0, 500_000, 50000, 2))
	d := c.Evaluate(t0)
	if d == nil {
		t.Fatal("first tick should fire memory rule")
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Tick 2 (60s later, still within 90s cooldown) — same condition,
	// should NOT fire.
	t1 := t0.Add(60 * time.Second)
	pushSnapshots(col, snap(95, 30, 0, 0, 500_000, 50000, 2))
	if d := c.Evaluate(t1); d != nil {
		t.Errorf("within cooldown — chunk_size rule should be suppressed; got %+v", d)
	}

	// Tick 3 (after cooldown expires) — should fire again.
	t2 := t0.Add(91 * time.Second)
	pushSnapshots(col, snap(95, 30, 0, 0, 50000, 50000, 2))
	if d := c.Evaluate(t2); d == nil {
		t.Error("post-cooldown — chunk_size rule should fire again")
	}
}

func TestController_PerKnobCooldown_Independent(t *testing.T) {
	// chunk_size cooldown should NOT block write_ahead_writers
	// rules. Rule 1 fires + sets chunk_size cooldown; on the next
	// tick with queue-growth conditions, WAW rule should still fire.
	c, col, _ := newTestController(t, ControllerOptions{})

	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: memory pressure fires.
	pushSnapshots(col, snap(95, 30, 0, 0, 500_000, 50000, 2))
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "chunk_size" {
		t.Fatalf("setup: expected chunk_size fire, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Tick 2 (still within chunk cooldown): queue-growth scenario.
	// WAW rule should still fire because it has its own cooldown.
	t1 := t0.Add(30 * time.Second)
	pushSnapshots(col,
		snap(50, 30, 5, 0, 500_000, 50000, 2),
		snap(50, 30, 8, 0, 500_000, 50000, 2),
	)
	d = c.Evaluate(t1)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Errorf("WAW cooldown is independent — rule should fire despite chunk_size cooldown; got %+v", d)
	}
}

// ---------- #199: Rule 2 memory interlock + throughput-aware add ----------

// TestController_QueueGrew_MemoryInterlock_SuppressesAdd: memory% ≥ 80
// (but < 90 so Rule 1 doesn't fire) suppresses Rule 2's writer-add even
// when queue is growing. CPU=80 disables Rule 4 so we can isolate Rule 2.
func TestController_QueueGrew_MemoryInterlock_SuppressesAdd(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(85, 80, 5, 0, 500_000, 50000, 2),
		snap(85, 80, 8, 0, 500_000, 50000, 2),
		snap(85, 80, 12, 0, 500_000, 50000, 2),
	)
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("memory %% ≥ 80 should suppress writer-add; got %+v", d)
	}
}

// TestController_QueueGrew_MemoryJustBelowInterlock_AddsWriter:
// memory% just under 80 still allows Rule 2 to fire. Pinpoints the
// boundary between the suppression gate and a normal add.
func TestController_QueueGrew_MemoryJustBelowInterlock_AddsWriter(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col,
		snap(79.9, 80, 5, 0, 500_000, 50000, 2),
		snap(79.9, 80, 8, 0, 500_000, 50000, 2),
		snap(79.9, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("memory just under interlock should permit writer-add; got %+v", d)
	}
}

// TestController_WAWAdd_ThroughputDidNotImprove_SuppressesNextAdd:
// after a successful WAW add, if the next evaluation's recent-mean
// throughput hasn't improved by ≥2%, Rule 2 must NOT fire again.
// CPU=80 + flat throughput → Rule 4 also doesn't fire.
func TestController_WAWAdd_ThroughputDidNotImprove_SuppressesNextAdd(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: queue growing, throughput flat at 500K.
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("setup: expected first WAW add, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Past the 90s WAW cooldown.
	t1 := t0.Add(91 * time.Second)
	// Throughput stayed flat at 500K (no improvement from the writer add).
	// Queue still growing — would fire again under the old rules.
	pushSnapshots(col,
		snap(50, 80, 15, 0, 500_000, 50000, 3),
		snap(50, 80, 18, 0, 500_000, 50000, 3),
		snap(50, 80, 22, 0, 500_000, 50000, 3),
	)
	if d := c.Evaluate(t1); d != nil {
		t.Errorf("throughput unchanged after prior WAW add — Rule 2 should suppress; got %+v", d)
	}
}

// TestController_WAWAdd_ThroughputImproved_AllowsNextAdd: after a
// successful WAW add, if recent-mean throughput improved by ≥2%, Rule 2
// is allowed to fire again on continued queue growth.
func TestController_WAWAdd_ThroughputImproved_AllowsNextAdd(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: queue growing, throughput at 500K.
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("setup: expected first WAW add, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Past cooldown. Throughput jumped to 520K (+4%, well above the 2%
	// gate). Queue still growing.
	t1 := t0.Add(91 * time.Second)
	pushSnapshots(col,
		snap(50, 80, 15, 0, 520_000, 50000, 3),
		snap(50, 80, 18, 0, 520_000, 50000, 3),
		snap(50, 80, 22, 0, 520_000, 50000, 3),
	)
	d = c.Evaluate(t1)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Errorf("throughput improved — Rule 2 should fire again; got %+v", d)
	}
}

// TestController_WAWAdd_ZeroBaselineThenStillStalled_SuppressesNextAdd
// is the third Codex-review case. If the FIRST writer-add happens at
// throughput=0 (writers stalled before the add), the baseline is
// recorded as 0 with the "set" flag true. On the next evaluation, if
// throughput is STILL 0, the gate must suppress — adding more writers
// during a full stall just stacks them up without progress. The pure
// proportional check (0 < 0*1.02) wouldn't fire; the gate's zero-case
// branch handles this explicitly.
func TestController_WAWAdd_ZeroBaselineThenStillStalled_SuppressesNextAdd(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: first WAW add at throughput=0 (stalled writers, queue
	// growing). Baseline is recorded as 0.
	pushSnapshots(col,
		snap(50, 80, 5, 0, 0, 50000, 2),
		snap(50, 80, 8, 0, 0, 50000, 2),
		snap(50, 80, 12, 0, 0, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("setup: expected first WAW add (no baseline yet); got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if c.lastWAWAddThroughput != 0 || !c.lastWAWAddThroughputSet {
		t.Fatalf("setup: baseline should be 0 with set=true; got value=%v set=%v",
			c.lastWAWAddThroughput, c.lastWAWAddThroughputSet)
	}

	// Tick 2 (past cooldown): writers still stalled, queue still growing.
	// The proportional gate (0 < 0*1.02) wouldn't catch this — the zero-
	// case branch must suppress.
	t1 := t0.Add(91 * time.Second)
	pushSnapshots(col,
		snap(50, 80, 15, 0, 0, 50000, 3),
		snap(50, 80, 18, 0, 0, 50000, 3),
		snap(50, 80, 22, 0, 0, 50000, 3),
	)
	if d := c.Evaluate(t1); d != nil {
		t.Errorf("zero baseline + still-zero throughput must suppress; got %+v", d)
	}
}

// TestController_FirstWAWAdd_NoBaseline_NotGated: lastWAWAddThroughputSet
// starts false; the throughput-aware gate must skip the comparison
// when no baseline exists, so the very first writer-add is never
// suppressed by it.
func TestController_FirstWAWAdd_NoBaseline_NotGated(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	// No prior apply has happened — c.lastWAWAddThroughput is 0.
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(time.Now())
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Errorf("first WAW add should fire (no baseline yet); got %+v", d)
	}
}

// TestController_WAWAdd_StalledWriters_SuppressesNextAdd is the Codex-
// review case: a prior WAW add set a positive baseline, then writers
// stall (throughput=0) while queue keeps growing. The throughput-aware
// gate must suppress the next add — zero is the strongest possible "no
// improvement" signal, not a reason to skip the comparison.
func TestController_WAWAdd_StalledWriters_SuppressesNextAdd(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: WAW add at throughput=500K (baseline).
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("setup: expected first WAW add, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Past cooldown. Writers stalled (throughput=0). Queue still growing
	// — under the original buggy guard the gate would skip suppression
	// and add another writer.
	t1 := t0.Add(91 * time.Second)
	pushSnapshots(col,
		snap(50, 80, 15, 0, 0, 50000, 3),
		snap(50, 80, 18, 0, 0, 50000, 3),
		snap(50, 80, 22, 0, 0, 50000, 3),
	)
	if d := c.Evaluate(t1); d != nil {
		t.Errorf("stalled writers (throughput=0) — Rule 2 should suppress; got %+v", d)
	}
}

// TestController_WAWDecrease_ClearsThroughputBaseline is the second
// Codex-review case (initial fix) plus the follow-up: a Rule 3 fire
// (error back-off) decreases WAW. Two correctness requirements:
//   - The decrease must NOT set a new baseline (it isn't an add to
//     validate).
//   - The decrease must CLEAR any prior baseline (the WAW count
//     just changed, so prior-add throughput is no longer comparable
//     and carrying it forward could suppress a legitimate later
//     recovery re-add indefinitely).
//
// Test sequence: prior add sets baseline=500K; errors trigger Rule 3
// backoff at the same throughput; baseline must be cleared (not
// preserved at 500K). Verifies a subsequent recovery add isn't
// suppressed by the stale 500K.
func TestController_WAWDecrease_ClearsThroughputBaseline(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: prior Rule 2 add sets baseline=500K (WAW 2→3).
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" || d.NewValue <= d.PreviousValue {
		t.Fatalf("setup tick 1: expected WAW add, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply tick 1: %v", err)
	}
	if c.lastWAWAddThroughput == 0 {
		t.Fatalf("setup: prior WAW add should have set baseline")
	}
	priorBaseline := c.lastWAWAddThroughput

	// Tick 2 (past cooldown): errors arrive → Rule 3 fires (WAW 3 → 2).
	// Baseline must be cleared, not preserved.
	t1 := t0.Add(91 * time.Second)
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 3),
		snap(50, 80, 5, 5, 500_000, 50000, 3),
	)
	d = c.Evaluate(t1)
	if d == nil || d.Knob != "write_ahead_writers" || d.NewValue >= d.PreviousValue {
		t.Fatalf("setup tick 2: expected Rule 3 decrease, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply tick 2: %v", err)
	}
	if c.lastWAWAddThroughput != 0 || c.lastWAWAddThroughputSet {
		t.Errorf("WAW decrease should CLEAR baseline (was %v); got value=%v set=%v",
			priorBaseline, c.lastWAWAddThroughput, c.lastWAWAddThroughputSet)
	}

	// Tick 3 (past cooldown): errors clear, queue grows. Recovery add
	// must NOT be suppressed by the stale 500K baseline. Throughput at
	// the same 500K — without the clear, comparison would be
	// 500K < 500K*1.02=510K → suppress, blocking recovery.
	t2 := t1.Add(91 * time.Second)
	pushSnapshots(col,
		snap(50, 80, 8, 5, 500_000, 50000, 2),
		snap(50, 80, 12, 5, 500_000, 50000, 2),
		snap(50, 80, 18, 5, 500_000, 50000, 2),
	)
	d = c.Evaluate(t2)
	if d == nil || d.Knob != "write_ahead_writers" || d.NewValue <= d.PreviousValue {
		t.Errorf("recovery add post-backoff should not be suppressed by stale baseline; got %+v", d)
	}
}

// TestController_ChunkSizeChange_ResetsWAWThroughputBaseline: a
// chunk_size adjustment alters the throughput surface, so the prior
// WAW-add baseline is no longer comparable. After a chunk_size apply,
// the next WAW add should NOT be suppressed by a stale baseline.
func TestController_ChunkSizeChange_ResetsWAWThroughputBaseline(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	t0 := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c.SetClock(fixedClock(t0))

	// Tick 1: WAW add at throughput=500K (sets baseline).
	pushSnapshots(col,
		snap(50, 80, 5, 0, 500_000, 50000, 2),
		snap(50, 80, 8, 0, 500_000, 50000, 2),
		snap(50, 80, 12, 0, 500_000, 50000, 2),
	)
	d := c.Evaluate(t0)
	if d == nil || d.Knob != "write_ahead_writers" {
		t.Fatalf("setup tick 1: expected WAW add, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply tick 1: %v", err)
	}
	if c.lastWAWAddThroughput == 0 {
		t.Fatalf("setup: lastWAWAddThroughput should be set after WAW apply")
	}

	// Tick 2 (past WAW cooldown): memory pressure → chunk_size shrink.
	// This should reset lastWAWAddThroughput to 0.
	t1 := t0.Add(91 * time.Second)
	pushSnapshots(col, snap(95, 80, 5, 0, 500_000, 50000, 3))
	d = c.Evaluate(t1)
	if d == nil || d.Knob != "chunk_size" {
		t.Fatalf("setup tick 2: expected chunk_size shrink, got %+v", d)
	}
	if err := c.apply(d); err != nil {
		t.Fatalf("apply tick 2: %v", err)
	}
	if c.lastWAWAddThroughput != 0 || c.lastWAWAddThroughputSet {
		t.Errorf("chunk_size apply should reset WAW throughput baseline; got value=%v set=%v",
			c.lastWAWAddThroughput, c.lastWAWAddThroughputSet)
	}
}

// TestMeanThroughput verifies the helper used by both apply() and the
// throughput-aware gate. Empty input returns 0; non-empty returns the
// arithmetic mean (which is also 0 if every snapshot's throughput is
// 0 — that's a legitimate "stall" baseline, not "no baseline yet".
// Callers distinguish the two via lastWAWAddThroughputSet, not by
// inspecting the value.
func TestMeanThroughput(t *testing.T) {
	cases := []struct {
		name        string
		throughputs []float64
		want        float64
	}{
		{"empty", nil, 0},
		{"single", []float64{500_000}, 500_000},
		{"three", []float64{400_000, 500_000, 600_000}, 500_000},
		{"includes zero", []float64{0, 500_000}, 250_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := make([]PerformanceSnapshot, len(tc.throughputs))
			for i, v := range tc.throughputs {
				snaps[i].Throughput = v
			}
			if got := meanThroughput(snaps); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------- Helper functions ----------

func TestQueueGrew_StrictMonotonic(t *testing.T) {
	cases := []struct {
		name   string
		depths []int
		want   bool
	}{
		{"strict_growth", []int{1, 2, 3}, true},
		{"flat", []int{2, 2, 2}, false},
		{"two_increases_one_flat", []int{1, 2, 2}, false}, // not strict
		{"decreasing", []int{3, 2, 1}, false},
		{"peak_in_middle", []int{1, 5, 3}, false},
		{"too_few_samples", []int{1, 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := make([]PerformanceSnapshot, len(tc.depths))
			for i, d := range tc.depths {
				snaps[i].QueueDepth = d
			}
			if got := queueGrew(snaps); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestThroughputStable_Detection(t *testing.T) {
	cases := []struct {
		name        string
		throughputs []float64
		want        bool
	}{
		{"stable_5pct", []float64{500_000, 510_000, 495_000}, true},        // (515-495)/501 ≈ 4%
		{"unstable_60pct", []float64{100_000, 500_000, 800_000}, false},    // huge spread
		{"unstable_15pct", []float64{500_000, 600_000, 460_000}, false},    // (600-460)/520 ≈ 27%
		{"stable_at_zero_returns_false", []float64{0, 0, 0}, false},
		{"too_few_samples", []float64{500_000, 510_000}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := make([]PerformanceSnapshot, len(tc.throughputs))
			for i, th := range tc.throughputs {
				snaps[i].Throughput = th
			}
			if got := throughputStable(snaps); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShrinkChunk(t *testing.T) {
	cases := []struct {
		name              string
		cur, minChunkSize int
		factor            float64
		want              int
	}{
		{"basic_shrink", 10000, 0, 0.75, 7500},
		{"floored_at_default_when_no_min_set", 10000, 5000, 0.75, 7500},
		{"hits_floor", 6000, 5000, 0.75, 5000},
		{"defensive_floor_at_1_when_min_zero", 1, 0, 0.75, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shrinkChunk(tc.cur, tc.factor, tc.minChunkSize); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGrowChunk(t *testing.T) {
	cases := []struct {
		name         string
		cur          int
		factor       float64
		maxChunkSize int
		want         int
	}{
		{"basic_grow", 1000, 1.10, 0, 1100},
		{"capped", 1000, 1.10, 1050, 1050},
		{"small_value_rounds_up_min_plus_one", 10, 1.10, 0, 11}, // 10×1.10 = 11 exactly with rounding
		{"min_increment_guard_at_cur_1", 1, 1.10, 0, 2},          // (1×1.10)+0.5 = 1.6, int = 1; guard kicks → 2
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := growChunk(tc.cur, tc.factor, tc.maxChunkSize); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestController_Apply_FailedTunerUpdate_NoCooldown — Copilot review
// on PR #194. If tuner.Update fails, the cooldown deadline must NOT
// be set, otherwise the knob is silently locked out for 90s
// despite no actual change taking effect.
func TestController_Apply_FailedTunerUpdate_NoCooldown(t *testing.T) {
	c, _, _ := newTestController(t, ControllerOptions{})
	c.tuner = &failingTuner{} // override with one that always errors
	c.SetClock(fixedClock(time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)))

	d := &Decision{Knob: "chunk_size", PreviousValue: 50000, NewValue: 37500}
	if err := c.apply(d); err == nil {
		t.Fatal("expected error from failingTuner.Update")
	}
	// Cooldown deadline should still be the zero value — apply()
	// only sets cooldown after Update succeeds.
	if !c.chunkSizeCooldownUntil.IsZero() {
		t.Errorf("cooldown should not be set when Update fails; got %v (#194 regression)", c.chunkSizeCooldownUntil)
	}
}

// failingTuner is a stub that errors on Update; everything else
// satisfies the interface but is unused in the apply-error test.
type failingTuner struct{}

func (failingTuner) Snapshot() transfer.RuntimeSnapshot { return transfer.RuntimeSnapshot{} }
func (failingTuner) Update(transfer.RuntimeUpdate) error {
	return errFailingTuner
}
func (failingTuner) TableChunkSize(string) (int, bool)       { return 0, false }
func (failingTuner) SetTableChunkSize(string, int)           {}
func (failingTuner) TableBatchSize(string) (int, bool)       { return 0, false }
func (failingTuner) SetTableBatchSize(string, int)           {}
func (failingTuner) ReportActiveJobs(int)                    {}
func (failingTuner) ReportQueueDepth(int)                    {}
func (failingTuner) ReportError()                            {}
func (failingTuner) ReportChunkRetry()                       {}
func (failingTuner) ReportTransferTime(int64, int64, int64, int64) {}
func (failingTuner) Metrics() transfer.RuntimeMetrics        { return transfer.RuntimeMetrics{} }

var errFailingTuner = errSentinel("tuner failed")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
