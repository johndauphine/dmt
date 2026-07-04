package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
