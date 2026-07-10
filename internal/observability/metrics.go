// Package observability exposes dmt's runtime to the operator's SRE stack
// via three coordinated surfaces (#229):
//
//   - Structured JSON logs (already supported in internal/logging) carry
//     base attributes — run_id, phase, source_db, target_db — on every
//     line so log aggregators can filter by run.
//   - Prometheus /metrics serves counters/gauges/histograms scraped by
//     Prometheus/Datadog/etc. Per-run labeled gauges are cleared at
//     Run/Resume completion to bound cardinality.
//   - OpenTelemetry traces (in this package's tracer.go) export spans
//     for each phase and table via OTLP HTTP.
//
// All three surfaces share a few invariants. They emit when configured,
// no-op when not (zero overhead in default deployments). They never block
// the migration on observability failures — a downed Prometheus scrape or
// trace exporter degrades to silent telemetry, never a failed migration.
// And they use the same dimension names (run_id, phase, table, source_db,
// target_db) so an operator can pivot between log, metric, and trace
// views in their tooling without re-mapping.
package observability

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/johndauphine/dmt/internal/logging"
)

// globalMetrics is the process-wide Metrics handle used by hot-path
// call sites that can't easily thread the surface through their
// constructors. Stored in an atomic.Value so loads (the hot path) are
// lock-free.
var globalMetrics atomic.Value

// Metrics is the dmt metrics surface scraped by Prometheus. Implementations
// satisfy the interface so callers can pass a no-op stand-in when metrics
// are disabled — every instrumentation site calls m.X() unconditionally,
// and m is just nil-but-safe in the default deployment.
type Metrics interface {
	// Counters
	IncRows(table, phase string, n int64)
	IncBytes(table, phase string, n int64)
	IncErrors(table, phase, class string)
	IncRetries(table string)
	IncTuningAdjustment(rule, direction string)
	IncAIFallback(surface string)
	AddBudgetWait(table string, seconds float64)

	// Histograms
	ObserveChunkDuration(table string, seconds float64)
	ObservePhaseDuration(phase string, seconds float64)

	// Gauges (per-run labels — cleared on RunComplete)
	SetWriterQueueDepth(depth int)
	SetWritersActive(active int)
	SetLiveWriters(table string, count int)

	// Lifecycle
	RunStarted(runID, sourceDB, targetDB string)
	RunComplete(runID string)
}

// Registry holds the Prometheus registry and the metric instruments. It's
// also the concrete implementation of Metrics — the orchestrator pulls
// the interface to keep the surface mockable in tests; everywhere else
// can use *Registry directly.
type Registry struct {
	registry *prometheus.Registry

	rowsTotal       *prometheus.CounterVec
	bytesTotal      *prometheus.CounterVec
	errorsTotal     *prometheus.CounterVec
	retriesTotal    *prometheus.CounterVec
	budgetWait      *prometheus.CounterVec
	chunkDuration   *prometheus.HistogramVec
	phaseDuration   *prometheus.HistogramVec
	queueDepth      *prometheus.GaugeVec
	writersActive   *prometheus.GaugeVec
	liveWriters     *prometheus.GaugeVec
	tuningAdjust    *prometheus.CounterVec
	aiFallbackTotal *prometheus.CounterVec
	migrationInfo   *prometheus.GaugeVec // info-style gauge carrying source_db/target_db

	mu               sync.Mutex
	currentRunID     string
	currentSourceDB  string
	currentTargetDB  string
	liveWriterTables map[string]map[string]struct{}
	server           *http.Server
}

// New constructs a Registry with all metric instruments pre-registered.
// Returns the Metrics interface (so callers can swap in noopMetrics for
// tests) plus the concrete Registry for callers that need to start the
// HTTP server.
func New() *Registry {
	r := prometheus.NewRegistry()

	const ns = "dmt"
	// Per-run labels: run_id is bounded by "in-flight runs" (~1 typically)
	// and cleared on RunComplete. table/phase are the dimensions operators
	// pivot on. errors_total gets an error_class label so the dashboard
	// can stack by failure type.
	reg := &Registry{
		registry: r,
		rowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "rows_total",
			Help: "Rows transferred to the target. Labels: run_id, table, phase.",
		}, []string{"run_id", "table", "phase"}),
		bytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "bytes_total",
			Help: "Bytes transferred to the target (estimated from row size). Labels: run_id, table, phase.",
		}, []string{"run_id", "table", "phase"}),
		errorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "errors_total",
			Help: "Errors raised during migration. Labels: run_id, table, phase, error_class.",
		}, []string{"run_id", "table", "phase", "error_class"}),
		retriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "retries_total",
			Help: "Chunk-level retries. Labels: run_id, table.",
		}, []string{"run_id", "table"}),
		budgetWait: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "budget_wait_seconds_total",
			Help: "Time readers wait for the shared in-flight memory budget. Labels: run_id, table.",
		}, []string{"run_id", "table"}),
		chunkDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "chunk_duration_seconds",
			Help:    "Wall-clock time to WRITE one chunk to the target (does not include the source read; reads are pipelined separately). Labels: run_id, table.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~40s
		}, []string{"run_id", "table"}),
		phaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "phase_duration_seconds",
			Help:    "Wall-clock duration of each orchestrator phase. Labels: phase.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 14), // 100ms .. ~3h
		}, []string{"phase"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "writer_queue_depth",
			Help: "Current depth of the writer pool's input queue. Labels: run_id.",
		}, []string{"run_id"}),
		writersActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "writers_active",
			Help: "Active writer goroutines. Labels: run_id.",
		}, []string{"run_id"}),
		liveWriters: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "live_writers",
			Help: "Live writer goroutines, including downscaled workers draining a final write. Labels: run_id, table.",
		}, []string{"run_id", "table"}),
		tuningAdjust: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "runtime_tuning_adjustments_total",
			Help: "Runtime parameter adjustments emitted by the rule-based controller. Labels: rule_name, direction.",
		}, []string{"rule_name", "direction"}),
		aiFallbackTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "ai_fallback_total",
			Help: "AI fallbacks fired during a run. Labels: surface (typemap|errordiag|smartconfig).",
		}, []string{"surface"}),
		migrationInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "migration_info",
			Help: "Info-style gauge always 1; carries source_db/target_db labels keyed by run_id so dashboards can join other metrics by run_id and pivot on driver. Labels: run_id, source_db, target_db.",
		}, []string{"run_id", "source_db", "target_db"}),
		liveWriterTables: make(map[string]map[string]struct{}),
	}
	r.MustRegister(
		reg.rowsTotal, reg.bytesTotal, reg.errorsTotal, reg.retriesTotal, reg.budgetWait,
		reg.chunkDuration, reg.phaseDuration, reg.queueDepth, reg.writersActive, reg.liveWriters,
		reg.tuningAdjust, reg.aiFallbackTotal, reg.migrationInfo,
	)
	return reg
}

// Start spins up the HTTP listener on addr (e.g. ":9090") and returns an
// error only on bind failure; ongoing scrape errors are swallowed by the
// Prometheus client lib. The caller is expected to invoke Stop() before
// the process exits so the listener releases the port for the next run.
func (r *Registry) Start(addr string) error {
	if addr == "" {
		return errors.New("observability: empty metrics address")
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{
		// Don't kill the scrape on a single broken collector — return what
		// we have and log the rest. Prometheus is more useful with stale
		// data than with no data.
		ErrorHandling: promhttp.ContinueOnError,
	}))
	r.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := newListener(addr)
	if err != nil {
		return err
	}
	go func() {
		if err := r.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logging.Warn("metrics server stopped: %v", err)
		}
	}()
	logging.InfoEvent("metrics endpoint serving",
		"addr", addr, "path", "/metrics")
	return nil
}

// Stop shuts down the HTTP server. Idempotent; safe to call when Start
// was never invoked.
func (r *Registry) Stop(ctx context.Context) error {
	if r == nil || r.server == nil {
		return nil
	}
	return r.server.Shutdown(ctx)
}

// RunStarted snapshots the run-scoped labels. The first call after a fresh
// run wins; if a second RunStarted lands before RunComplete (shouldn't
// happen in practice), the new run_id takes over and the prior gauges
// keep their last value until cleared by RunComplete. We also stamp
// migration_info{run_id, source_db, target_db} = 1 so dashboards can
// join other run-scoped metrics and pivot on driver pair.
func (r *Registry) RunStarted(runID, sourceDB, targetDB string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentRunID = runID
	r.currentSourceDB = sourceDB
	r.currentTargetDB = targetDB
	if _, ok := r.liveWriterTables[runID]; !ok {
		r.liveWriterTables[runID] = make(map[string]struct{})
	}
	// Initialize per-run gauges so /metrics shows the run even before
	// the first writer dispatches.
	r.queueDepth.WithLabelValues(runID).Set(0)
	r.writersActive.WithLabelValues(runID).Set(0)
	r.migrationInfo.WithLabelValues(runID, sourceDB, targetDB).Set(1)
}

// RunComplete clears every per-run-labeled series so the cardinality
// stays bounded across long-running dmt processes (TUI sessions, sidecar
// deployments). Aggregate counters (rows_total etc.) are NOT cleared —
// Prometheus expects monotonic counters; the run_id label scopes them
// already, and stale labels would survive anyway because counters can't
// be deleted mid-process. Per-run GAUGES (queue_depth, writers_active)
// ARE deletable since gauges don't have the monotonic constraint.
func (r *Registry) RunComplete(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queueDepth.DeleteLabelValues(runID)
	r.writersActive.DeleteLabelValues(runID)
	for table := range r.liveWriterTables[runID] {
		r.liveWriters.DeleteLabelValues(runID, table)
	}
	delete(r.liveWriterTables, runID)
	// migration_info uses the captured source/target labels because
	// DeleteLabelValues requires the exact label tuple a series was
	// registered with. We track them on the Registry for this reason.
	if r.currentSourceDB != "" && r.currentTargetDB != "" {
		r.migrationInfo.DeleteLabelValues(runID, r.currentSourceDB, r.currentTargetDB)
	}
	if r.currentRunID == runID {
		r.currentRunID = ""
		r.currentSourceDB = ""
		r.currentTargetDB = ""
	}
}

// IncRows / IncBytes / IncErrors / IncRetries are the hot-path counters.
// They use the registry's current run_id; calling them outside a run
// (e.g. during analyze) emits with run_id="" which Prometheus accepts
// but operators will see as an unlabeled time series.
func (r *Registry) IncRows(table, phase string, n int64) {
	if n <= 0 {
		return
	}
	r.rowsTotal.WithLabelValues(r.runID(), table, phase).Add(float64(n))
}

// IncBytes increments bytes_total for the given table/phase pair.
func (r *Registry) IncBytes(table, phase string, n int64) {
	if n <= 0 {
		return
	}
	r.bytesTotal.WithLabelValues(r.runID(), table, phase).Add(float64(n))
}

// IncErrors increments errors_total with the operator-friendly error class
// label. Class is one of: timeout, deadlock, constraint, network,
// unknown — kept low-cardinality so dashboards can stack by class without
// label explosion.
func (r *Registry) IncErrors(table, phase, class string) {
	r.errorsTotal.WithLabelValues(r.runID(), table, phase, class).Inc()
}

// IncRetries bumps the chunk-retry counter for a specific table.
func (r *Registry) IncRetries(table string) {
	r.retriesTotal.WithLabelValues(r.runID(), table).Inc()
}

// AddBudgetWait increments the cumulative time readers spent blocked on the
// shared in-flight memory budget. It mirrors the runtime-tuner signal so
// Prometheus and controller diagnostics describe the same contention.
func (r *Registry) AddBudgetWait(table string, seconds float64) {
	if seconds <= 0 {
		return
	}
	r.budgetWait.WithLabelValues(r.runID(), table).Add(seconds)
}

// IncTuningAdjustment records a parameter change emitted by the runtime
// controller. rule_name identifies the rule that fired (queue_growth,
// throughput_stable, etc.); direction is "up" or "down".
func (r *Registry) IncTuningAdjustment(rule, direction string) {
	r.tuningAdjust.WithLabelValues(rule, direction).Inc()
}

// IncAIFallback bumps the AI fallback counter for a specific surface.
// Cross-references #176 (AI fallback observability).
func (r *Registry) IncAIFallback(surface string) {
	r.aiFallbackTotal.WithLabelValues(surface).Inc()
}

// ObserveChunkDuration records the time taken to read+write one chunk.
func (r *Registry) ObserveChunkDuration(table string, seconds float64) {
	r.chunkDuration.WithLabelValues(r.runID(), table).Observe(seconds)
}

// ObservePhaseDuration records the wall-clock duration of an
// orchestrator phase. Useful for spotting "extract_schema took 10 minutes
// on a slow source" without parsing logs.
func (r *Registry) ObservePhaseDuration(phase string, seconds float64) {
	r.phaseDuration.WithLabelValues(phase).Observe(seconds)
}

// SetWriterQueueDepth / SetWritersActive update the per-run gauges. Called
// from the writer pool's metrics tap.
func (r *Registry) SetWriterQueueDepth(depth int) {
	r.queueDepth.WithLabelValues(r.runID()).Set(float64(depth))
}

// SetWritersActive sets the count of active writer goroutines.
func (r *Registry) SetWritersActive(active int) {
	r.writersActive.WithLabelValues(r.runID()).Set(float64(active))
}

// SetLiveWriters records the writer goroutines actually alive for one table.
// Unlike writers_active, it includes downscaled workers that are still
// draining their final write, so it can temporarily exceed the desired count.
func (r *Registry) SetLiveWriters(table string, count int) {
	if count < 0 {
		count = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	runID := r.currentRunID
	if _, ok := r.liveWriterTables[runID]; !ok {
		r.liveWriterTables[runID] = make(map[string]struct{})
	}
	r.liveWriterTables[runID][table] = struct{}{}
	r.liveWriters.WithLabelValues(runID, table).Set(float64(count))
}

// runID returns the current run_id under lock. Hot-path code calls this
// per increment so we keep it tight; the mutex contention is negligible
// vs the metric increment cost itself.
func (r *Registry) runID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentRunID
}

// Handler returns the bare /metrics HTTP handler so callers can mount it
// into an existing mux (TUI, embedded server). Most users start their own
// listener via Start() instead.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// noopMetrics is the safe fallback when metrics are disabled. Every
// instrumentation site calls m.IncX() unconditionally; the noop avoids
// nil checks at every call.
type noopMetrics struct{}

// Noop returns a Metrics that drops all updates on the floor. Use this
// when --metrics-addr was not set so instrumentation sites don't need
// nil checks.
func Noop() Metrics { return noopMetrics{} }

// SetGlobal installs m as the process-wide Metrics surface accessible via
// Global(). Hot-path call sites in transfer/writer pool / etc. use this
// rather than threading the surface through every constructor — the
// dispatch cost is a single atomic.Value load per call, which is
// negligible against an INSERT or COPY. Pass Noop() to clear.
//
// Safe for concurrent callers and re-callable: the orchestrator's
// SetMetrics path also calls SetGlobal so transfer-package code sees
// the same instance as the orchestrator's direct method calls.
func SetGlobal(m Metrics) {
	if m == nil {
		globalMetrics.Store(metricsHolder{m: Noop()})
		return
	}
	globalMetrics.Store(metricsHolder{m: m})
}

// Global returns the process-wide Metrics surface. Returns a noop when
// nothing has been set so call sites can use it unconditionally.
func Global() Metrics {
	v := globalMetrics.Load()
	if v == nil {
		return Noop()
	}
	return v.(metricsHolder).m
}

// metricsHolder wraps Metrics so atomic.Value can store an interface
// (which requires concrete-type uniformity across stores).
type metricsHolder struct{ m Metrics }

func (noopMetrics) IncRows(string, string, int64)        {}
func (noopMetrics) IncBytes(string, string, int64)       {}
func (noopMetrics) IncErrors(string, string, string)     {}
func (noopMetrics) IncRetries(string)                    {}
func (noopMetrics) IncTuningAdjustment(string, string)   {}
func (noopMetrics) IncAIFallback(string)                 {}
func (noopMetrics) AddBudgetWait(string, float64)        {}
func (noopMetrics) ObserveChunkDuration(string, float64) {}
func (noopMetrics) ObservePhaseDuration(string, float64) {}
func (noopMetrics) SetWriterQueueDepth(int)              {}
func (noopMetrics) SetWritersActive(int)                 {}
func (noopMetrics) SetLiveWriters(string, int)           {}
func (noopMetrics) RunStarted(string, string, string)    {}
func (noopMetrics) RunComplete(string)                   {}
