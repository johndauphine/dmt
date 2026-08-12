package webui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/progress"
)

// TestHubConcurrentChurn hammers the event hub with concurrent publishers and
// a large number of churning subscribers, then asserts the subscriber map
// fully drains. Most valuable under -race (subscribe/publish/unsubscribe all
// contend on the hub mutex). Fast — always runs.
func TestHubConcurrentChurn(t *testing.T) {
	h := newEventHub()
	h.setLive(true)

	stop := make(chan struct{})
	var pubWg sync.WaitGroup
	for p := 0; p < 4; p++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					h.Report(progress.ProgressUpdate{Phase: "transferring"})
				}
			}
		}()
	}

	var subWg sync.WaitGroup
	for c := 0; c < 400; c++ {
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			id, ch := h.subscribe()
			for i := 0; i < 3; i++ {
				select {
				case <-ch:
				case <-time.After(time.Millisecond):
				}
			}
			h.unsubscribe(id)
		}()
	}
	subWg.Wait()
	close(stop)
	pubWg.Wait()

	h.mu.Lock()
	n := len(h.subs)
	h.mu.Unlock()
	if n != 0 {
		t.Errorf("hub leaked %d subscribers after churn", n)
	}
}

// TestSoakSSEConnectionChurn opens and tears down many real SSE connections
// and asserts the server's subscriber set drains back to zero — guarding the
// per-connection subscribe/unsubscribe lifecycle in handleEvents.
func TestSoakSSEConnectionChurn(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	srv := httptest.NewServer(s.buildHandler())
	defer srv.Close()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	const conns = 60
	for i := 0; i < conns; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("sse connect %d: %v", i, err)
		}
		// Do() returns after the handler flushes headers, i.e. after it has
		// subscribed. Let it settle briefly, then drop the connection.
		time.Sleep(2 * time.Millisecond)
		cancel()
		resp.Body.Close()
	}

	// The server needs a moment to observe each disconnect and unsubscribe.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.hub.mu.Lock()
		n := len(s.hub.subs)
		s.hub.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.hub.mu.Lock()
	n := len(s.hub.subs)
	s.hub.mu.Unlock()
	t.Errorf("hub retained %d subscribers after %d churned SSE connections", n, conns)
}

// TestSoakRepeatedMigrations runs many migrations through the API back-to-back
// and asserts the goroutine count doesn't grow — the regression guard for the
// tracker goroutine leak (#580) and the per-run orchestrator lifecycle across a
// long-lived server. Skipped in -short (exercises the full pipeline N times).
func TestSoakRepeatedMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("soak: repeated migrations")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	seedSource(t, src, 40)
	cfg := writeMigrationConfig(t, dir, src, dst, stateDir)

	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfg})
	srv := httptest.NewServer(s.buildHandler())
	defer srv.Close()

	runOnce := func() {
		// confirm_backup lets drop_recreate re-run against the now-populated
		// target on every round after the first.
		body := fmt.Sprintf(`{"config":%q,"confirm_backup":true}`, cfg)
		if code, resp := apiPost(t, srv, "/api/run", body); code != http.StatusAccepted {
			t.Fatalf("run = %d, want 202; %s", code, resp)
		}
		if st := waitForRun(t, srv, 30*time.Second); st != "completed" {
			t.Fatalf("run ended %q, want completed", st)
		}
	}

	// Warm up so first-run lazy goroutines exist before the baseline.
	runOnce()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	const rounds = 15
	for i := 0; i < rounds; i++ {
		runOnce()
	}

	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	after := runtime.NumGoroutine()
	// A per-run leak would add ~one goroutine per migration; allow generous
	// slack for transient HTTP/runtime goroutines.
	if after > before+10 {
		t.Errorf("goroutines grew across %d migrations (leak suspected): before=%d after=%d", rounds, before, after)
	}
}
