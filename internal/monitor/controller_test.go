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

// ---------- Rule 3: errors → back off writers ----------

func TestController_Errors_ReducesWAW(t *testing.T) {
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(50, 30, 0, 3, 500_000, 50000, 4))

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

func TestController_ErrorsButWAW1_NoFire(t *testing.T) {
	// WAW already at floor — can't go lower.
	c, col, _ := newTestController(t, ControllerOptions{})
	pushSnapshots(col, snap(50, 30, 0, 3, 500_000, 50000, 1))
	if d := c.Evaluate(time.Now()); d != nil {
		t.Errorf("WAW=1 + errors should not fire (floor reached); got %+v", d)
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

func TestScaleClampedChunk(t *testing.T) {
	cases := []struct {
		cur, max int
		factor   float64
		want     int
	}{
		{1000, 0, 0.75, 750},
		{1000, 800, 0.75, 750}, // factor result < cap, no clamp
		{1000, 0, 1.10, 1100},
		{1000, 1050, 1.10, 1050}, // factor result > cap → clamp
		{1, 0, 0.75, 1},          // floor at 1
		{0, 0, 0.75, 1},          // floor at 1 from any small value
	}
	for _, tc := range cases {
		got := scaleClampedChunk(tc.cur, tc.factor, tc.max)
		if got != tc.want {
			t.Errorf("scaleClampedChunk(cur=%d, factor=%.2f, max=%d): got %d, want %d",
				tc.cur, tc.factor, tc.max, got, tc.want)
		}
	}
}
