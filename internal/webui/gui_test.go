package webui

import (
	"errors"
	"net"
	"strings"
	"testing"
)

// --gui must never auto-open a browser against a non-loopback bind: that
// would hand a token-bearing URL to a local process launcher on what may be a
// shared server.
func TestNewRejectsGUIOnNonLoopbackBind(t *testing.T) {
	_, err := New(Options{
		Addr:        "0.0.0.0:8484",
		AuthToken:   "a-sufficiently-long-remote-token-value",
		Insecure:    true,
		OpenBrowser: true,
	})
	if err == nil {
		t.Fatal("expected New to reject --gui (OpenBrowser) on a non-loopback bind")
	}
	if !strings.Contains(err.Error(), "loopback") {
		t.Errorf("unexpected error: %v", err)
	}
	var coder interface{ ExitCode() int }
	if !errors.As(err, &coder) {
		t.Fatal("expected the error to implement ExitCode()")
	}
	if got := coder.ExitCode(); got != 1 {
		t.Errorf("ExitCode() = %d, want 1 (config error)", got)
	}
}

func TestNewAllowsGUIOnLoopbackBind(t *testing.T) {
	s, err := New(Options{Addr: "127.0.0.1:0", OpenBrowser: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !s.opts.OpenBrowser {
		t.Error("OpenBrowser was not preserved on the server options")
	}
}

// loginURL is what Run() hands to the browser launcher (server.go) and what
// printBanner prints — they must always agree.
func TestLoginURLMatchesTokenMode(t *testing.T) {
	// displayHost() (security.go) reads the port from the *configured* addr,
	// not from bound, so these must agree for the test to reflect real usage
	// (production always binds an explicit port, never ":0").
	const addr = "127.0.0.1:5555"
	bound, _ := net.ResolveTCPAddr("tcp", addr)

	auto := newTestServer(t, Options{Addr: addr})
	if !auto.tokenAuto {
		t.Fatal("expected an auto-generated token on a default loopback bind")
	}
	if got, want := auto.loginURL(bound), "http://"+addr+"/?token="+auto.token; got != want {
		t.Errorf("loginURL (auto token) = %q, want %q", got, want)
	}

	configured := newTestServer(t, Options{Addr: "127.0.0.1:5556", AuthToken: "operator-chosen-token"})
	boundConfigured, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:5556")
	if configured.tokenAuto {
		t.Fatal("expected a configured (non-auto) token")
	}
	if got, want := configured.loginURL(boundConfigured), "http://127.0.0.1:5556/"; got != want {
		t.Errorf("loginURL (configured token) = %q, want %q (no token in the URL)", got, want)
	}
}
