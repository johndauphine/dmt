package webui

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewRejectsInsecureRemoteBind(t *testing.T) {
	_, err := New(Options{Addr: "0.0.0.0:8484"})
	if err == nil {
		t.Fatal("expected New to reject a remote bind without a token")
	}
	if !strings.Contains(err.Error(), "--webui-auth-token") {
		t.Errorf("unexpected error: %v", err)
	}
	// Config failures must map to the config exit code (1), not the
	// transfer-error default, via the ExitCoder interface.
	var coder interface{ ExitCode() int }
	if !errors.As(err, &coder) {
		t.Fatal("expected the error to implement ExitCode()")
	}
	if got := coder.ExitCode(); got != 1 {
		t.Errorf("ExitCode() = %d, want 1 (config error)", got)
	}
}

func TestNewDefaultsAddr(t *testing.T) {
	s, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.opts.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want default %q", s.opts.Addr, DefaultAddr)
	}
	if !s.tokenAuto {
		t.Error("expected auto-generated token on default loopback bind")
	}
}

func TestStaticHandlerServesIndex(t *testing.T) {
	s := newTestServer(t, Options{})
	h := s.buildHandler()

	// Root serves the placeholder shell.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "dmt WebUI") {
		t.Error("index.html did not render expected content")
	}

	// SPA fallback: an unknown deep-link path serves the shell, not 404.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status/some-run-id", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("SPA fallback status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "dmt WebUI") {
		t.Error("SPA fallback did not serve the shell")
	}
}

func TestDisplayHost(t *testing.T) {
	bound, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:5555")
	cases := map[string]string{
		"127.0.0.1:8484": "127.0.0.1:8484",
		"0.0.0.0:8484":   "localhost:8484",
		":8484":          "localhost:8484",
		"[::]:8484":      "localhost:8484",
	}
	for in, want := range cases {
		if got := displayHost(in, bound); got != want {
			t.Errorf("displayHost(%q) = %q, want %q", in, got, want)
		}
	}
}
