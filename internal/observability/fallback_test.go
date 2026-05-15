package observability

import (
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

var errBoom = errors.New("boom")

type recordedEvent struct {
	Surface     string
	Fingerprint string
}

// recordingSink captures events for the sink-wiring tests. err, if
// non-nil, is returned from every call so the policy "sink errors
// don't poison the in-memory path" can be pinned.
type recordingSink struct {
	mu     sync.Mutex
	events []recordedEvent
	err    error
}

func (s *recordingSink) SaveFallbackEvent(surface, fingerprint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, recordedEvent{Surface: surface, Fingerprint: fingerprint})
	return s.err
}

// TestRecordFallback_IncrementsCountsAndFingerprints covers the core
// contract from #176: each call bumps the surface counter, the
// fingerprint is retained for catalog-growth review, and Counts/Fingerprints
// see them.
func TestRecordFallback_IncrementsCountsAndFingerprints(t *testing.T) {
	ResetFallbackState()

	RecordFallback(SurfaceTypemap, "postgres:inet")
	RecordFallback(SurfaceTypemap, "postgres:inet") // duplicate fingerprint still bumps count
	RecordFallback(SurfaceTypemap, "mssql:hierarchyid")
	RecordFallback(SurfaceDDL, "raw_columns")
	RecordFallback(SurfaceErrordiag, "postgres|relation does not exist|abc123")

	counts := Counts()
	if counts[SurfaceTypemap] != 3 {
		t.Errorf("typemap count = %d, want 3 (every call bumps regardless of dedup)", counts[SurfaceTypemap])
	}
	if counts[SurfaceDDL] != 1 {
		t.Errorf("ddl count = %d, want 1", counts[SurfaceDDL])
	}
	if counts[SurfaceErrordiag] != 1 {
		t.Errorf("errordiag count = %d, want 1", counts[SurfaceErrordiag])
	}

	fps := Fingerprints(SurfaceTypemap)
	if len(fps) != 2 {
		t.Fatalf("typemap fingerprints = %v, want 2 distinct", fps)
	}
	if fps[0] != "mssql:hierarchyid" || fps[1] != "postgres:inet" {
		t.Errorf("typemap fingerprints = %v, want sorted [mssql:hierarchyid postgres:inet]", fps)
	}
}

// TestRecordFallback_FeedsPrometheusCounter pins the wiring to the
// Prometheus surface in #229: every RecordFallback call must also
// bump dmt_ai_fallback_total{surface=...} so external dashboards see
// the same number as `dmt status`.
func TestRecordFallback_FeedsPrometheusCounter(t *testing.T) {
	ResetFallbackState()

	reg := New()
	prev := Global()
	SetGlobal(reg)
	defer SetGlobal(prev)

	RecordFallback(SurfaceTypemap, "postgres:inet")
	RecordFallback(SurfaceTypemap, "mssql:hierarchyid")
	RecordFallback(SurfaceDDL, "raw_columns")

	if got := testutil.ToFloat64(reg.aiFallbackTotal.WithLabelValues(SurfaceTypemap)); got != 2 {
		t.Errorf("prometheus typemap counter = %v, want 2", got)
	}
	if got := testutil.ToFloat64(reg.aiFallbackTotal.WithLabelValues(SurfaceDDL)); got != 1 {
		t.Errorf("prometheus ddl counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(reg.aiFallbackTotal.WithLabelValues(SurfaceErrordiag)); got != 0 {
		t.Errorf("prometheus errordiag counter = %v, want 0 (no call placed)", got)
	}
}

// TestResetFallbackState_ClearsCountsAndFingerprints pins the
// per-run reset behavior wired by orchestrator Run/Resume so a
// long-running process (TUI) doesn't carry counts across runs.
func TestResetFallbackState_ClearsCountsAndFingerprints(t *testing.T) {
	ResetFallbackState()
	RecordFallback(SurfaceTypemap, "postgres:inet")
	RecordFallback(SurfaceDDL, "raw_columns")

	if len(Counts()) == 0 {
		t.Fatal("setup: expected non-empty counts before reset")
	}
	ResetFallbackState()

	if got := Counts(); len(got) != 0 {
		t.Errorf("after reset: Counts() = %v, want empty", got)
	}
	if got := Fingerprints(SurfaceTypemap); got != nil {
		t.Errorf("after reset: typemap fingerprints = %v, want nil", got)
	}
}

// TestRecordFallback_FingerprintRingBound proves the ring cap holds
// under a pathological flood — counts keep climbing but fingerprints
// stop at fingerprintRingSize so a 10K-Raw-column migration doesn't
// grow the in-memory sample unbounded.
func TestRecordFallback_FingerprintRingBound(t *testing.T) {
	ResetFallbackState()

	const flood = fingerprintRingSize * 4
	for i := 0; i < flood; i++ {
		RecordFallback(SurfaceTypemap, fingerprintFor(i))
	}

	if got := Counts()[SurfaceTypemap]; got != int64(flood) {
		t.Errorf("count = %d, want %d (counter is unbounded)", got, flood)
	}
	if got := len(Fingerprints(SurfaceTypemap)); got != fingerprintRingSize {
		t.Errorf("fingerprint sample size = %d, want %d (ring cap)", got, fingerprintRingSize)
	}
}

// TestRecordFallback_EmptyArgsAreSafe covers the defensive paths so a
// buggy call site doesn't panic or pollute the surface with empty
// strings.
func TestRecordFallback_EmptyArgsAreSafe(t *testing.T) {
	ResetFallbackState()

	RecordFallback("", "anything")                  // dropped — empty surface
	RecordFallback(SurfaceTypemap, "")              // counted, no fingerprint recorded
	RecordFallback(SurfaceTypemap, "postgres:inet") // normal call

	if got := Counts()[""]; got != 0 {
		t.Errorf("empty surface count = %d, want 0 (dropped)", got)
	}
	if got := Counts()[SurfaceTypemap]; got != 2 {
		t.Errorf("typemap count = %d, want 2", got)
	}
	fps := Fingerprints(SurfaceTypemap)
	if len(fps) != 1 || fps[0] != "postgres:inet" {
		t.Errorf("typemap fingerprints = %v, want [postgres:inet] (empty dropped)", fps)
	}
}

// TestRecordFallback_FeedsRegisteredSink pins the persistence wiring
// (#176, codex follow-up): every RecordFallback must also forward to
// the registered FallbackSink so cross-process status reads can see
// the running migration's counts. A nil sink is the default and
// drops cleanly.
func TestRecordFallback_FeedsRegisteredSink(t *testing.T) {
	ResetFallbackState()
	SetFallbackSink(nil) // defensive — clear any leftover
	defer SetFallbackSink(nil)

	sink := &recordingSink{}
	SetFallbackSink(sink)

	RecordFallback(SurfaceTypemap, "postgres:inet")
	RecordFallback(SurfaceDDL, "raw_columns")
	RecordFallback(SurfaceDDL, "raw_columns") // duplicate fingerprint forwards twice

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 3 {
		t.Fatalf("sink received %d events, want 3", len(sink.events))
	}
	want := []recordedEvent{
		{SurfaceTypemap, "postgres:inet"},
		{SurfaceDDL, "raw_columns"},
		{SurfaceDDL, "raw_columns"},
	}
	for i, w := range want {
		if sink.events[i] != w {
			t.Errorf("event[%d] = %+v, want %+v", i, sink.events[i], w)
		}
	}
}

// TestRecordFallback_SinkErrorsDoNotPanic covers the policy from #176:
// a persistence failure must not bring down the migration or panic
// the call site. The Prometheus + in-memory surfaces still observe
// the event even when the sink errors.
func TestRecordFallback_SinkErrorsDoNotPanic(t *testing.T) {
	ResetFallbackState()
	defer SetFallbackSink(nil)
	SetFallbackSink(&recordingSink{err: errBoom})

	RecordFallback(SurfaceTypemap, "postgres:inet")

	if got := Counts()[SurfaceTypemap]; got != 1 {
		t.Errorf("in-memory count = %d, want 1 (sink error must not block the in-memory write)", got)
	}
}

// TestRecordFallback_ConcurrentCallsAreCorrect runs RecordFallback
// from many goroutines to surface any race on the counter / ring.
// The race detector catches data races; the assertion catches lost
// increments.
func TestRecordFallback_ConcurrentCallsAreCorrect(t *testing.T) {
	ResetFallbackState()

	const goroutines = 32
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				RecordFallback(SurfaceTypemap, fingerprintFor(id*perGoroutine+i))
			}
		}(g)
	}
	wg.Wait()

	if got := Counts()[SurfaceTypemap]; got != int64(goroutines*perGoroutine) {
		t.Errorf("typemap count = %d, want %d (concurrent increments)", got, goroutines*perGoroutine)
	}
}

func fingerprintFor(i int) string {
	// Distinct per call so the ring cap test sees fresh fingerprints
	// rather than dedup hits.
	return "fp-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	const digits = "0123456789"
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	return string(b[pos:])
}
