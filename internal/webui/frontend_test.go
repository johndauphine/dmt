package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/version"
)

// TestSPAAssetsServed guards that the embedded SPA (#582) ships in the binary:
// the shell, stylesheet, and script are all served with sane content types,
// and unknown client-routes fall back to the shell.
func TestSPAAssetsServed(t *testing.T) {
	s := newTestServer(t, Options{})
	h := s.buildHandler()

	cases := []struct {
		path, wantType, wantSubstr string
	}{
		{"/", "text/html", "<div id=\"app\">"},
		{"/app.css", "text/css", "--accent"},
		{"/app.js", "text/javascript", "EventSource"},
		{"/icon-192.png", "image/png", ""},
		{"/icon-512.png", "image/png", ""},
		{"/icon-512-maskable.png", "image/png", ""},
		{"/apple-touch-icon.png", "image/png", ""},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost"+c.path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", c.path, rec.Code)
			continue
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, c.wantType) {
			t.Errorf("GET %s content-type = %q, want %q", c.path, ct, c.wantType)
		}
		if !strings.Contains(rec.Body.String(), c.wantSubstr) {
			t.Errorf("GET %s body missing %q", c.path, c.wantSubstr)
		}
	}

	// Deep-link client route falls back to the SPA shell (index.html).
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost/profiles", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<div id=\"app\">") {
		t.Errorf("SPA fallback for /profiles: code=%d", rec.Code)
	}
}

// TestRunEndAnnouncementIsDeduped guards the fix for the stacking completed-run
// toast (#599): applyRunState is re-entered by SSE, the run-pill poll, and each
// dashboard mount, so the terminal-state toast and desktop notification have to
// go through the transition guard rather than firing on "status is terminal".
// There is no JS runtime in this build, so this asserts the shape of the source
// — the same approach TestServiceWorkerVersionStamped takes for sw.js.
func TestRunEndAnnouncementIsDeduped(t *testing.T) {
	s := newTestServer(t, Options{})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost/app.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /app.js = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{"function announceRunEnd(", "lastRunSeen"} {
		if !strings.Contains(body, want) {
			t.Errorf("app.js missing the run-end dedup guard %q", want)
		}
	}
	// One announcement site, reached only through announceRunEnd.
	if n := strings.Count(body, "toast(`Migration "); n != 1 {
		t.Errorf("`Migration ...` toast appears %d times, want exactly 1 (inside announceRunEnd)", n)
	}
	if n := strings.Count(body, "notifyRunEnded("); n != 2 { // the definition + the single call
		t.Errorf("notifyRunEnded referenced %d times, want 2 (definition + call in announceRunEnd)", n)
	}
}

// TestManifestServedWithCorrectMIME guards against Go's builtin mime table
// (mime/type.go) not knowing the .webmanifest extension: without the explicit
// override in assets.go, http.FileServer would serve it with a guessed or
// empty Content-Type, and some browsers refuse to treat an untyped response
// as an installable PWA manifest.
func TestManifestServedWithCorrectMIME(t *testing.T) {
	s := newTestServer(t, Options{})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost/manifest.webmanifest", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /manifest.webmanifest = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/manifest+json" {
		t.Errorf("Content-Type = %q, want application/manifest+json", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{`"name": "dmt console"`, `"display": "standalone"`, "icon-512-maskable.png"} {
		if !strings.Contains(body, want) {
			t.Errorf("manifest missing %q", want)
		}
	}
}

// TestServiceWorkerVersionStamped guards the mechanism that keeps a dmt
// upgrade from being served an old sw.js/app.js pair indefinitely: sw.js's
// cache name must reflect the running build's version, not the literal
// placeholder, and it must never take over the /api/ path.
func TestServiceWorkerVersionStamped(t *testing.T) {
	s := newTestServer(t, Options{})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost/sw.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /sw.js = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/javascript") {
		t.Errorf("Content-Type = %q, want text/javascript", ct)
	}
	body := rec.Body.String()
	if strings.Contains(body, "__DMT_VERSION__") {
		t.Error("sw.js still contains the unsubstituted __DMT_VERSION__ placeholder")
	}
	if !strings.Contains(body, version.Version) {
		t.Errorf("sw.js cache name does not reference the running version %q", version.Version)
	}
	if !strings.Contains(body, `startsWith("/api/")`) {
		t.Error("sw.js does not appear to exempt /api/ from caching")
	}
}
