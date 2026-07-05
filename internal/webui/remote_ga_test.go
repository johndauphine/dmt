package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestRemoteTokenMinLength: a non-loopback bind refuses a short operator token
// (#594) but accepts a sufficiently long one; loopback binds are unaffected.
func TestRemoteTokenMinLength(t *testing.T) {
	if _, err := New(Options{Addr: "0.0.0.0:8484", AuthToken: "short", Insecure: true}); err == nil {
		t.Error("expected a short remote token to be rejected")
	}
	if _, err := New(Options{Addr: "0.0.0.0:8484", AuthToken: "this-token-is-long-enough", Insecure: true}); err != nil {
		t.Errorf("a long remote token should be accepted: %v", err)
	}
	// A loopback bind places no length floor (auto or short both fine).
	if _, err := New(Options{Addr: "127.0.0.1:8484", AuthToken: "short"}); err != nil {
		t.Errorf("loopback short token should be accepted: %v", err)
	}
}

// TestLoginLimiter: repeated failures lock the IP out with 429, and a success
// resets the bucket.
func TestLoginLimiter(t *testing.T) {
	l := newLoginLimiter()
	ip := "10.0.0.9"
	for i := 0; i < loginMaxFailures-1; i++ {
		l.fail(ip)
		if ok, _ := l.allow(ip); !ok {
			t.Fatalf("locked out early after %d failures", i+1)
		}
	}
	l.fail(ip) // the Nth failure trips the lockout
	if ok, retry := l.allow(ip); ok || retry <= 0 {
		t.Fatalf("expected lockout after %d failures, got ok=%v retry=%v", loginMaxFailures, ok, retry)
	}
	l.reset(ip)
	if ok, _ := l.allow(ip); !ok {
		t.Error("reset should clear the lockout")
	}
}

// TestLoginEndpointRateLimited: the HTTP login endpoint returns 429 with
// Retry-After after too many bad tokens from one client.
func TestLoginEndpointRateLimited(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: "topsecret"})
	h := s.buildHandler()
	var last *httptest.ResponseRecorder
	for i := 0; i < loginMaxFailures+2; i++ {
		last = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://localhost/api/login", strings.NewReader(`{"token":"wrong"}`))
		req.RemoteAddr = "203.0.113.7:5555"
		h.ServeHTTP(last, req)
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after repeated failures, got %d", last.Code)
	}
	if last.Header().Get("Retry-After") == "" {
		t.Error("429 should carry a Retry-After header")
	}
}

// TestBearerBruteForceThrottled: repeated bad bearer tokens against an
// authenticated endpoint trip the same limiter as login (#594 review fix), so
// the throttle can't be bypassed by brute-forcing the bearer path. An
// anonymous request (no credential) from the same IP is not throttled.
func TestBearerBruteForceThrottled(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: "topsecret"})
	h := s.buildHandler()
	var last *httptest.ResponseRecorder
	for i := 0; i < loginMaxFailures+2; i++ {
		last = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://localhost/api/health", nil)
		req.RemoteAddr = "198.51.100.4:4444"
		req.Header.Set("Authorization", "Bearer wrong")
		h.ServeHTTP(last, req)
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("bearer brute-force = %d, want 429", last.Code)
	}
	// A credential-less request from the same IP is NOT throttled — 401, not 429.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/health", nil)
	req.RemoteAddr = "198.51.100.4:4444"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("anonymous request should be 401 (not throttled), got %d", rec.Code)
	}
}

// TestSessionSlides: validating a session pushes its expiry forward, so an
// in-use session outlives its original TTL (#601).
func TestSessionSlides(t *testing.T) {
	// Generous margins: the inter-touch gap (40ms) is far below the TTL
	// (400ms), so only a >400ms scheduler stall could flake the active leg.
	const ttl = 400 * time.Millisecond
	store := newSessionStore(ttl)
	id, err := store.create()
	if err != nil {
		t.Fatal(err)
	}
	// Touch it well past the original TTL; it must stay valid.
	deadline := time.Now().Add(ttl + 300*time.Millisecond)
	for time.Now().Before(deadline) {
		if !store.valid(id) {
			t.Fatal("active session expired despite being touched")
		}
		time.Sleep(40 * time.Millisecond)
	}
	// Stop touching; it should lapse after the TTL.
	time.Sleep(ttl + 200*time.Millisecond)
	if store.valid(id) {
		t.Error("idle session should expire after its TTL")
	}
}

// TestSessionAbsoluteCap: a continuously-touched session still dies at the
// absolute lifetime cap (a stolen cookie can't be renewed forever).
func TestSessionAbsoluteCap(t *testing.T) {
	store := newSessionStore(time.Hour)
	id, err := store.create()
	if err != nil {
		t.Fatal(err)
	}
	// Force the entry's created time past the cap, then touch it.
	store.mu.Lock()
	e := store.m[id]
	e.created = time.Now().Add(-sessionMaxLife - time.Minute)
	store.m[id] = e
	store.mu.Unlock()
	if store.valid(id) {
		t.Error("session past the absolute lifetime cap should be invalid even if idle-TTL is fresh")
	}
}

// TestRequestSlidesCookie: an authenticated cookie request re-issues the
// session cookie (sliding the browser MaxAge).
func TestRequestSlidesCookie(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: "topsecret"})
	sid, _ := s.sessions.create()
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/health", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: sid})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cookie auth = %d, want 200", rec.Code)
	}
	if sessionCookieFrom(rec.Result().Cookies()) == nil {
		t.Error("authenticated cookie request should re-issue the session cookie")
	}
}
