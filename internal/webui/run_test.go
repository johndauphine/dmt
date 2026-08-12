package webui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/progress"
)

func TestRunManagerSingleFlight(t *testing.T) {
	m := newRunManager()
	_, ok := m.start(kindRun, "run-1", func() {})
	if !ok {
		t.Fatal("first start should succeed")
	}
	if _, ok := m.start(kindRun, "run-2", func() {}); ok {
		t.Fatal("second start should be rejected while one is active")
	}
	// After finishing, a new run may start.
	m.finish("completed", "")
	if _, ok := m.start(kindResume, "run-3", func() {}); !ok {
		t.Fatal("start should succeed after the prior run finished")
	}
}

func TestRunManagerCancelAndState(t *testing.T) {
	m := newRunManager()
	if m.cancel() {
		t.Error("cancel with no active run should return false")
	}
	if st := m.state(); st != nil {
		t.Error("state should be nil before any run")
	}

	cancelled := false
	m.start(kindRun, "run-1", func() { cancelled = true })
	if st := m.state(); st == nil || st.Status != "running" {
		t.Fatalf("expected running state, got %+v", st)
	}
	if !m.cancel() {
		t.Fatal("cancel should return true for an active run")
	}
	if !cancelled {
		t.Error("cancel func was not invoked")
	}

	st := m.finish("cancelled", "context canceled")
	if st.Status != "cancelled" || st.EndedAt == "" {
		t.Errorf("finish did not record terminal state: %+v", st)
	}
	if last := m.state(); last == nil || last.Status != "cancelled" {
		t.Errorf("state should return the last finished run, got %+v", last)
	}
}

func TestEventHubDeliversAndSnapshots(t *testing.T) {
	h := newEventHub()
	h.setLive(true)
	id, ch := h.subscribe()
	defer h.unsubscribe(id)

	h.Report(progress.ProgressUpdate{Phase: "transferring", TablesComplete: 2})
	select {
	case e := <-ch:
		if e.Type != eventProgress || e.Progress == nil || e.Progress.Phase != "transferring" {
			t.Fatalf("unexpected event: %+v", e)
		}
	default:
		t.Fatal("expected a progress event")
	}

	// Snapshot retains the last progress event for late joiners.
	snap := h.snapshot()
	if snap == nil || snap.Progress.TablesComplete != 2 {
		t.Fatalf("snapshot missing/wrong: %+v", snap)
	}
}

func TestEventHubDropsWhenFull(t *testing.T) {
	h := newEventHub()
	h.setLive(true)
	id, ch := h.subscribe()
	defer h.unsubscribe(id)

	// Publish far more than the buffer without reading; publish must never
	// block, and the channel must not exceed its capacity.
	for i := 0; i < 500; i++ {
		h.Report(progress.ProgressUpdate{TablesComplete: i})
	}
	if len(ch) > cap(ch) {
		t.Fatalf("channel exceeded capacity: len=%d cap=%d", len(ch), cap(ch))
	}
	// Snapshot still reflects the most recent publish.
	if snap := h.snapshot(); snap == nil || snap.Progress.TablesComplete != 499 {
		t.Fatalf("snapshot stale: %+v", snap)
	}
}

func TestEventHubGatesProgressOutsideRun(t *testing.T) {
	h := newEventHub()
	id, ch := h.subscribe()
	defer h.unsubscribe(id)

	// Not live → progress dropped (mirrors a tracker tick after the run ended).
	h.Report(progress.ProgressUpdate{Phase: "stray"})
	if len(ch) != 0 {
		t.Fatal("progress should be dropped when no run is live")
	}
	// Lifecycle events always pass, even when not live.
	h.publish(event{Type: eventDone, Run: &runState{ID: "r1", Status: "completed"}})
	if len(ch) != 1 {
		t.Fatal("lifecycle events must pass regardless of live state")
	}
	// Ending a run clears the retained snapshot.
	h.setLive(true)
	h.Report(progress.ProgressUpdate{Phase: "transferring"})
	h.setLive(false)
	if h.snapshot() != nil {
		t.Error("snapshot should be cleared when a run ends")
	}
}

// hasEverSubscribed backs the --gui idle watchdog's arming decision
// (idle_watchdog.go): it must latch true the instant a client connects and
// stay true after that client disconnects — unlike subscriberCount(), which
// reflects only the current moment.
func TestEventHubHasEverSubscribed(t *testing.T) {
	h := newEventHub()
	if h.hasEverSubscribed() {
		t.Fatal("hasEverSubscribed should be false before any subscriber")
	}
	id, _ := h.subscribe()
	if !h.hasEverSubscribed() {
		t.Fatal("hasEverSubscribed should be true immediately on subscribe")
	}
	h.unsubscribe(id)
	if !h.hasEverSubscribed() {
		t.Error("hasEverSubscribed must stay true after the subscriber disconnects")
	}
}

func TestEventHubUnsubscribeClosesChannel(t *testing.T) {
	h := newEventHub()
	id, ch := h.subscribe()
	h.unsubscribe(id)
	if _, open := <-ch; open {
		t.Error("channel should be closed after unsubscribe")
	}
	// Publishing after everyone left must not panic.
	h.Report(progress.ProgressUpdate{})
}

func TestRunStateIdleEndpoint(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/run", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/run = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"active":false`) {
		t.Errorf("expected active:false, got %s", rec.Body.String())
	}
}

func TestCancelNoActiveRun(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/run/cancel", ""))
	if rec.Code != http.StatusConflict {
		t.Fatalf("cancel with no run = %d, want 409", rec.Code)
	}
}

func TestSSEStreamsEvents(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	srv := httptest.NewServer(s.buildHandler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect SSE: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	// Publish an event and confirm the client receives it in SSE framing.
	h := s.hub
	h.setLive(true)
	go func() { h.ReportImmediate(progress.ProgressUpdate{Phase: "extracting"}) }()

	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("read SSE: %v", err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "event: progress") || !strings.Contains(got, "extracting") {
		t.Errorf("unexpected SSE payload: %q", got)
	}
}
