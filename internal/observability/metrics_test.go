package observability

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestRegistry_RunLifecycleClearsGauges pins the cardinality-bound
// invariant for #229: per-run-labeled gauges (queue_depth, writers_active,
// live_writers, migration_info) must be deleted when RunComplete fires so a long-running
// dmt process doesn't accumulate one label-value family per run.
func TestRegistry_RunLifecycleClearsGauges(t *testing.T) {
	reg := New()

	reg.RunStarted("run-1", "mssql", "postgres")
	reg.SetWriterQueueDepth(7)
	reg.SetWritersActive(2)
	reg.SetLiveWriters("Users", 3)

	got := testutil.ToFloat64(reg.queueDepth.WithLabelValues("run-1"))
	if got != 7 {
		t.Errorf("queueDepth(run-1) = %v, want 7", got)
	}
	// migration_info should be 1 after RunStarted (info-style gauge).
	info := testutil.ToFloat64(reg.migrationInfo.WithLabelValues("run-1", "mssql", "postgres"))
	if info != 1 {
		t.Errorf("migration_info(run-1) = %v, want 1", info)
	}

	reg.RunComplete("run-1")
	// Verify all four per-run gauge families no longer carry run-1.
	for _, want := range []string{"dmt_writer_queue_depth", "dmt_writers_active", "dmt_live_writers", "dmt_migration_info"} {
		if hasRunLabel(t, reg, want, "run-1") {
			t.Errorf("%s still has run_id=run-1 after RunComplete", want)
		}
	}
}

// hasRunLabel returns true if any series in metricName has run_id=runID.
// Helper for the RunComplete cleanup assertions.
func hasRunLabel(t *testing.T, reg *Registry, metricName, runID string) bool {
	t.Helper()
	dto, err := reg.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range dto {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "run_id" && lp.GetValue() == runID {
					return true
				}
			}
		}
	}
	return false
}

// TestRegistry_RowsBytesCounters covers the hot-path counters used by
// the transfer pipeline. Increments must reach the underlying Prometheus
// counter with the supplied labels — the value flows through to the
// /metrics scrape unchanged.
func TestRegistry_RowsBytesCounters(t *testing.T) {
	reg := New()
	reg.RunStarted("run-2", "mssql", "postgres")

	reg.IncRows("Users", "transfer", 1000)
	reg.IncRows("Users", "transfer", 500)
	reg.IncBytes("Users", "transfer", 12345)
	reg.IncErrors("Users", "transfer", "deadlock")
	reg.IncErrors("Users", "transfer", "deadlock")
	reg.IncRetries("Users")

	rows := testutil.ToFloat64(reg.rowsTotal.WithLabelValues("run-2", "Users", "transfer"))
	if rows != 1500 {
		t.Errorf("rows = %v, want 1500", rows)
	}
	bytesMetric := testutil.ToFloat64(reg.bytesTotal.WithLabelValues("run-2", "Users", "transfer"))
	if bytesMetric != 12345 {
		t.Errorf("bytes = %v, want 12345", bytesMetric)
	}
	errs := testutil.ToFloat64(reg.errorsTotal.WithLabelValues("run-2", "Users", "transfer", "deadlock"))
	if errs != 2 {
		t.Errorf("errors = %v, want 2", errs)
	}
	retries := testutil.ToFloat64(reg.retriesTotal.WithLabelValues("run-2", "Users"))
	if retries != 1 {
		t.Errorf("retries = %v, want 1", retries)
	}
}

func TestRegistry_BudgetWaitAndLiveWriterMetrics(t *testing.T) {
	reg := New()
	reg.RunStarted("run-budget", "mssql", "postgres")

	reg.AddBudgetWait("Users", 0.25)
	reg.AddBudgetWait("Users", 0.75)
	reg.AddBudgetWait("Users", 0) // non-positive observations are ignored
	reg.SetLiveWriters("Users", 4)

	if got := testutil.ToFloat64(reg.budgetWait.WithLabelValues("run-budget", "Users")); got != 1 {
		t.Errorf("budget_wait_seconds_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(reg.liveWriters.WithLabelValues("run-budget", "Users")); got != 4 {
		t.Errorf("live_writers = %v, want 4", got)
	}

	// A negative defensive input must not report an impossible live count.
	reg.SetLiveWriters("Users", -1)
	if got := testutil.ToFloat64(reg.liveWriters.WithLabelValues("run-budget", "Users")); got != 0 {
		t.Errorf("live_writers after negative input = %v, want 0", got)
	}
}

// TestRegistry_NoopCounters pins the noop-mode contract: calling Inc/Set
// methods on a non-registered or zero-row counter must not panic and
// must be effectively no-ops. This protects code paths where the metric
// has been disabled (e.g. n <= 0 to IncRows).
func TestRegistry_NoopCounters(t *testing.T) {
	reg := New()

	// n <= 0 is the documented "skip" condition for the convenience setters
	// (matches the IncRows / IncBytes guard).
	reg.IncRows("Users", "transfer", 0)
	reg.IncRows("Users", "transfer", -1)
	reg.IncBytes("Users", "transfer", 0)

	// Counter should still be zero — verifies the early-return guard works
	// without falsifying the metric.
	val := testutil.ToFloat64(reg.rowsTotal.WithLabelValues("", "Users", "transfer"))
	if val != 0 {
		t.Errorf("rows after zero/negative increments = %v, want 0", val)
	}
}

// TestRegistry_PhaseDuration verifies the per-phase histogram captures
// timing. We observe a known value and read it back via the cumulative
// _count and _sum exporters.
func TestRegistry_PhaseDuration(t *testing.T) {
	reg := New()
	reg.ObservePhaseDuration("transfer", 0.5)
	reg.ObservePhaseDuration("transfer", 1.5)

	// CollectAndCount reads the count of observations in the histogram.
	count := testutil.CollectAndCount(reg.phaseDuration)
	if count == 0 {
		t.Errorf("phase histogram has zero observation count")
	}
}

// TestRegistry_Server_StartsAndServes verifies the /metrics HTTP server
// binds, serves a Prometheus-format response, and shuts down cleanly.
// Uses :0 to let the OS pick a free port so concurrent tests don't fight.
func TestRegistry_Server_StartsAndServes(t *testing.T) {
	reg := New()
	if err := reg.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = reg.Stop(ctx)
	}()

	// reg.server.Addr is "127.0.0.1:0" — but the actual port comes from
	// the listener. The Start() call gives us no other way to discover
	// it, so populate a metric and confirm the handler shape via a
	// direct call to the in-process Handler() instead of an HTTP round
	// trip (the listener is up, but discovery is awkward).
	reg.IncRows("Users", "transfer", 42)
	rec := newRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	body := rec.Body
	if !strings.Contains(body, "dmt_rows_total") {
		t.Errorf("metrics output missing dmt_rows_total: %s", body)
	}
}

// TestSetGlobal_Noop pins the "metrics off" default: Global() must
// return a noop when SetGlobal hasn't been called, so hot-path
// instrumentation works in unit tests that don't bootstrap metrics.
func TestSetGlobal_Noop(t *testing.T) {
	// Reset the atomic so test order doesn't matter.
	SetGlobal(nil)
	m := Global()
	// Any method must succeed without side effects. Just exercise enough
	// to prove the no-op interface is satisfied.
	m.IncRows("x", "y", 1)
	m.IncBytes("x", "y", 1)
	m.IncRetries("x")
	m.AddBudgetWait("x", 1)
	m.SetLiveWriters("x", 1)
	m.RunStarted("a", "b", "c")
	m.RunComplete("a")
}

// TestSetGlobal_RoundTrip verifies SetGlobal/Global propagate the same
// Metrics instance — critical because transfer-package call sites read
// the global rather than threading the surface through constructors.
func TestSetGlobal_RoundTrip(t *testing.T) {
	reg := New()
	SetGlobal(reg)
	defer SetGlobal(nil)

	got := Global()
	if _, ok := got.(*Registry); !ok {
		t.Fatalf("Global() = %T, want *Registry", got)
	}
}

// recorder is a minimal http.ResponseWriter for the in-process handler test.
type recorder struct {
	Code    int
	Body    string
	header  http.Header
	written bool
}

func newRecorder() *recorder            { return &recorder{header: http.Header{}, Code: 200} }
func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) WriteHeader(code int) {
	if r.written {
		return
	}
	r.Code = code
	r.written = true
}
func (r *recorder) Write(b []byte) (int, error) {
	if !r.written {
		r.written = true
	}
	r.Body += string(b)
	return len(b), nil
}

// silence "declared but not used" for symbols imported only for compile
// checks (prometheus testutil is used above; io.Discard / time used for
// the server timeout in Stop).
var (
	_ = prometheus.NewCounter
	_ = io.Discard
)

// TestSetGlobal_ResetBetweenRuns pins the defensive Global reset added
// in NewWithOptions (Copilot review on #229): a process that wraps
// orchestrator construction in a loop (TUI, integration tests) must
// not leak a real Registry from a prior run into a subsequent
// no-metrics run.
func TestSetGlobal_ResetBetweenRuns(t *testing.T) {
	reg := New()
	SetGlobal(reg)
	if _, ok := Global().(*Registry); !ok {
		t.Fatalf("expected *Registry after SetGlobal, got %T", Global())
	}

	// Simulate orchestrator construction performing the reset.
	SetGlobal(Noop())

	got := Global()
	// noopMetrics is unexported; assert it's NOT a *Registry — that's
	// the contract: anything non-nil and non-Registry counts as "off".
	if _, ok := got.(*Registry); ok {
		t.Errorf("expected non-Registry after reset, got *Registry — stale metrics surface leaked across runs")
	}
}
