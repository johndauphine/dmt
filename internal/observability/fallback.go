package observability

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

// AI fallback surfaces. These are the labels recorded on the
// dmt_ai_fallback_total Prometheus counter (#229) and the keys used
// for the per-run counts surfaced in `dmt status` (#176). The set is
// closed: instrumentation must use one of these constants so the
// downstream dashboards / catalog-growth review pipelines see a
// stable, low-cardinality label space.
const (
	// SurfaceTypemap covers column-level type mapping fallback —
	// vendor-specific source types (KindRaw) that the deterministic
	// catalog can't render in the target dialect (e.g. PG inet
	// targeting MSSQL). Fingerprint shape: "<source-dialect>:<udt-name>".
	SurfaceTypemap = "typemap"

	// SurfaceDDL covers table- and finalization-DDL fallback — the
	// holistic CREATE TABLE / CREATE INDEX paths that route to AI
	// when the deterministic emitter can't faithfully render the
	// surface (Raw-bearing tables, approximate-column tables when
	// approx_type_action=ai_fallback, unsupported finalization DDL
	// like clustered / covering / filtered indexes). Fingerprint
	// shape: short tag identifying the trigger
	// ("raw_columns", "approx_columns", "table_ddl_error",
	// "finalization:<index-feature>").
	SurfaceDDL = "ddl"

	// SurfaceErrordiag covers error diagnosis. dmt no longer ships
	// an AI fallback for error messages (PII-egress concern), so
	// "fallback fires" here means "deterministic catalog had no
	// matching pattern" — the catalog-growth signal that lets
	// maintainers add a pattern for the long tail. Fingerprint
	// shape: scrubbed message prefix + sha256-hex hash (the same
	// pair errordiag_dispatch already computes for its log line).
	SurfaceErrordiag = "errordiag"
)

// fallbackCounts is the process-wide counter map surfaced by Counts()
// for the in-memory side of `dmt status` (the durable cross-process
// view goes through the checkpoint backend). Guarded by fallbackMu —
// the prior atomic-pointer / copy-on-write design churned a fresh map
// per increment for no measurable benefit, since `dmt status` is not
// latency-critical (Copilot review on PR #285).
var (
	fallbackMu     sync.Mutex
	fallbackCounts = map[string]int64{}
)

// FallbackSink is the persistence hook a caller registers so
// RecordFallback writes show up on cross-process status reads. Without
// a sink, RecordFallback bumps only in-process state — fine for the
// TUI / sidecar process, but the separate `dmt status` invocation in
// an Airflow polling workflow would see zero (#176). The orchestrator
// installs a checkpoint-backed sink at Run/Resume time so the
// persisted counts are available to anyone with the same state file.
type FallbackSink interface {
	SaveFallbackEvent(surface, fingerprint string) error
}

var fallbackSink atomic.Pointer[fallbackSinkHolder]

// fallbackSinkHolder boxes the interface so atomic.Pointer has a
// concrete element type to hold.
type fallbackSinkHolder struct{ sink FallbackSink }

// SetFallbackSink installs s as the persistence destination for
// future RecordFallback calls. Pass nil to clear (the orchestrator
// does this at Run/Resume teardown so a TUI session that finishes a
// run doesn't keep writing to the prior run's state file). Safe to
// call concurrently; only the most recently installed sink receives
// events.
//
// Single-run-per-process assumption: dmt's Run/Resume orchestration
// is one migration at a time inside any single process (CLI, TUI,
// k8s sidecar). The package-level sink reflects that: if two Run()
// goroutines ever overlap, the second SetFallbackSink replaces the
// first and the first run's late events would route to the second
// run's row in the state file. The Run code path opens a fresh
// orchestrator per migration, so concurrent runs would require a
// caller architectural change first; this contract documents the
// assumption so a future multi-tenant rework knows to revisit the
// sink plumbing (Copilot review on PR #285).
func SetFallbackSink(s FallbackSink) {
	if s == nil {
		fallbackSink.Store(nil)
		return
	}
	fallbackSink.Store(&fallbackSinkHolder{sink: s})
}

// fingerprintRingSize bounds the per-surface fingerprint sample kept
// in memory so a pathological migration (10K Raw columns, 10K
// unmatched errors) doesn't grow the surface unbounded. The first N
// distinct fingerprints win; once full we just stop recording new
// ones. Counters are unbounded (cheap int64 increments); the
// fingerprint sample is the catalog-growth signal and N=64 is plenty
// for review.
const fingerprintRingSize = 64

var (
	fingerprintsMu sync.Mutex
	fingerprints   = map[string]map[string]struct{}{} // surface -> set of distinct fingerprints
)

// RecordFallback is the single entry point every AI-fallback call
// site invokes when the deterministic path falls through to AI (or,
// for errordiag, when the deterministic catalog has no match — the
// "fallback" there is the human reviewer who adds a pattern later).
//
// It does four things, in order of cost:
//
//  1. Bumps a process-local counter keyed by surface so `dmt status`
//     (in-process callers) can report "AI fallback fired N times."
//  2. Bumps the Prometheus dmt_ai_fallback_total{surface=...} counter
//     for external observability (#229). Cheap and lock-free in the
//     hot path (atomic.Value load).
//  3. Records the fingerprint in a bounded per-surface sample and
//     emits a debug log line. Debug-level so the catalog-growth
//     pipeline can grep it without flooding INFO-level operator
//     output during a large migration.
//  4. If a FallbackSink is installed, persists the event to the
//     checkpoint backend so cross-process `dmt status` polling sees
//     the running migration's counts (#176).
//
// Call-site frequency: fires per-column at DDL-emit time for typemap
// surfaces and per-error for errordiag, NOT per-row. Bound is the
// source schema's vocabulary, not the row count — so even a 100K-table
// migration produces O(distinct unmapped types) calls, typically <100.
// The synchronous SQLite UPSERT / YAML rewrite is acceptable at that
// scale; if a future surface fires per-row, the sink should batch
// (Copilot review on PR #285).
//
// surface must be one of the Surface* constants; an unknown surface
// is still recorded but the counter will appear under that label,
// which would mark a bug at the call site (fix the call site, not
// the recorder).
func RecordFallback(surface, fingerprint string) {
	if surface == "" {
		return
	}

	incCount(surface)

	Global().IncAIFallback(surface)

	recordFingerprint(surface, fingerprint)

	if h := fallbackSink.Load(); h != nil && h.sink != nil {
		// Persistence errors are non-fatal — the in-memory counter and
		// the Prometheus surface still observed the event, and the
		// migration itself shouldn't fail because the status write
		// errored. Log at debug to keep the operator-visible signal
		// quiet on the happy path.
		if err := h.sink.SaveFallbackEvent(surface, fingerprint); err != nil {
			logging.Debug("fallback sink: SaveFallbackEvent surface=%s err=%v", surface, err)
		}
	}

	logging.Debug("ai fallback fired: surface=%s fingerprint=%q", surface, fingerprint)
}

func incCount(surface string) {
	fallbackMu.Lock()
	fallbackCounts[surface]++
	fallbackMu.Unlock()
}

func recordFingerprint(surface, fp string) {
	if fp == "" {
		return
	}
	fingerprintsMu.Lock()
	defer fingerprintsMu.Unlock()
	set, ok := fingerprints[surface]
	if !ok {
		set = map[string]struct{}{}
		fingerprints[surface] = set
	}
	if len(set) >= fingerprintRingSize {
		return
	}
	set[fp] = struct{}{}
}

// Counts returns a snapshot of per-surface fallback counts since the
// last Reset (or process start). The returned map is a copy — callers
// can mutate it freely. Surfaces with zero fallbacks are omitted, so
// the typical "no AI fired" run returns an empty map.
//
// Consumed by orchestrator.GetStatusResult so `dmt status --json`
// includes per-surface counts alongside row counts; the Prometheus
// counter is the source of truth for external dashboards.
func Counts() map[string]int64 {
	fallbackMu.Lock()
	defer fallbackMu.Unlock()
	out := make(map[string]int64, len(fallbackCounts))
	for k, v := range fallbackCounts {
		out[k] = v
	}
	return out
}

// Fingerprints returns a sorted slice of distinct fingerprints
// recorded for a given surface, capped at fingerprintRingSize. Used
// by `dmt status` to show which inputs triggered the fallback so a
// maintainer reviewing catalog growth can see "PG inet fired 47
// times, here are the 3 distinct columns it hit."
func Fingerprints(surface string) []string {
	fingerprintsMu.Lock()
	defer fingerprintsMu.Unlock()
	set, ok := fingerprints[surface]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(set))
	for fp := range set {
		out = append(out, fp)
	}
	sort.Strings(out)
	return out
}

// ResetFallbackState clears all counters and fingerprint samples.
// Called at run start so per-run status counts don't carry over from
// a prior run in the same process (TUI / long-running sidecar). The
// Prometheus counter is monotonic and intentionally not reset — it
// represents "since process start" for external dashboards.
func ResetFallbackState() {
	fallbackMu.Lock()
	fallbackCounts = map[string]int64{}
	fallbackMu.Unlock()

	fingerprintsMu.Lock()
	fingerprints = map[string]map[string]struct{}{}
	fingerprintsMu.Unlock()
}
