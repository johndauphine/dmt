// Package webui serves dmt's browser front-end (epic #577). It is the third
// front-end alongside the CLI and TUI, launched with the global --webui flag.
// The package owns the HTTP server lifecycle, embedded SPA serving, the
// security baseline (bind-address rules, shared-secret auth, session cookies,
// security headers, DNS-rebinding guard), and the authenticated /api surface:
// read/advisory endpoints (#579), run/resume with SSE live progress (#580),
// and the interactive setup/profiles/session flows (#581). Handlers call the
// same internal/orchestrator + internal/command surfaces the TUI does — no
// forked business logic. Command parity with the registry is machine-checked
// by parity_surface_test.go (#583).
//
// Security posture: the server is loopback-only by default with an
// auto-generated token printed at startup. Binding a non-loopback address
// (a server deployment) requires an explicit --webui-auth-token and either
// TLS (--webui-tls-cert/--webui-tls-key) or --webui-insecure for a
// TLS-terminating reverse proxy. See validateBindSecurity.
package webui

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmt/v5/internal/desktop"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/version"
)

// DefaultAddr is the loopback bind address used when --webui-addr is empty.
const DefaultAddr = "127.0.0.1:8484"

// sessionTTL bounds how long a browser session cookie stays valid before the
// operator must re-authenticate.
const sessionTTL = 12 * time.Hour

// Options configures the WebUI server. It is populated from the global
// --webui* flags in cmd/dmt.
type Options struct {
	// Addr is the TCP bind address (host:port). Loopback hosts get relaxed
	// security; any other host is treated as remotely reachable.
	Addr string
	// AuthToken is the shared-secret bearer token. It may contain
	// ${env:VAR}/${file:/path} templates, expanded at startup. Empty is only
	// valid for a loopback bind, where a token is auto-generated.
	AuthToken string
	// TLSCert and TLSKey enable native HTTPS when both are set.
	TLSCert string
	TLSKey  string
	// Insecure permits a non-loopback bind over plaintext HTTP (reverse-proxy
	// TLS termination). The auth token is still required.
	Insecure bool
	// TrustedProxies lists CIDRs (or bare IPs) whose X-Forwarded-For header is
	// trusted, so the login/auth limiter and audit logging attribute requests
	// to the real client instead of a shared reverse-proxy IP (#604). Off by
	// default: XFF is ignored unless the direct peer is in this set.
	TrustedProxies []string

	// Origin context threaded from the global flags so later WebUI issues
	// (#579/#580) can resolve configs/profiles the way the CLI and TUI do.
	// Stored by the foundation; not yet consumed.
	ConfigPath string
	StateFile  string
	Profile    string

	// GUI desktop-mode behavior (--gui, internal/desktop). All four are set
	// together by --gui; AppWindow additionally requires --app-window. They
	// are independent knobs (not a single bool) because a future caller may
	// want, say, single-instance handoff without the idle-exit auto-shutdown.
	//
	// OpenBrowser launches a browser at the server's URL once it starts
	// listening. AppWindow requests a chromeless app-style window (falling
	// back to OpenBrowser's default-browser behavior if no Chromium-family
	// browser is found). SingleInstance coordinates with any other running
	// `dmt --gui` via a lock file so a second launch hands off to the first
	// instead of failing to bind the same port. ExitWhenIdle shuts the server
	// down shortly after the last connected browser window closes, but never
	// while a migration is in flight.
	//
	// Only valid on a loopback bind — New rejects OpenBrowser on a
	// non-loopback --webui-addr, since auto-opening a browser would hand a
	// token-bearing URL to a local process launcher on what may be a shared
	// server.
	OpenBrowser    bool
	AppWindow      bool
	SingleInstance bool
	ExitWhenIdle   bool
}

// tlsEnabled reports whether native TLS is configured.
func (o Options) tlsEnabled() bool { return o.TLSCert != "" && o.TLSKey != "" }

// Server is a running (or ready-to-run) WebUI HTTP server.
type Server struct {
	opts      Options
	token     string
	tokenAuto bool // token was auto-generated (loopback convenience)
	sessions  *sessionStore
	logins    *loginLimiter // brute-force throttle on /api/login (#594)
	httpSrv   *http.Server

	// trustedProxies holds the parsed --webui-trusted-proxy networks whose
	// X-Forwarded-For is honored for client-IP attribution (#604).
	trustedProxies []*net.IPNet

	// allowedHosts is the DNS-rebinding guard's Host-header allowlist. It is
	// populated only for a loopback bind (where the rebinding attack applies:
	// a malicious page resolving its own hostname to 127.0.0.1 to reach the
	// operator's local server). For a remote bind it is nil — the public
	// hostname is unknowable and the mandatory token+TLS are the control.
	allowedHosts map[string]bool

	// hub fans migration progress/lifecycle events to SSE clients; runs
	// enforces single-flight migration execution (#580). runWg tracks the
	// background migration goroutine so shutdown can wait for it.
	hub   *eventHub
	runs  *runManager
	runWg sync.WaitGroup

	// sessionDefaults holds server-side /session defaults; setup is the
	// in-progress guided setup flow, if any (#581).
	sessionDefaults *sessionDefaults
	setup           *setupSession
	setupMu         sync.Mutex
}

// waitForRuns blocks until the background migration goroutine (if any) exits,
// or ctx expires. Called during shutdown so a cancelled run can flush its
// checkpoint before the process ends.
func (s *Server) waitForRuns(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.runWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// configErr marks a WebUI startup/configuration failure (bad flag
// combination, unresolvable token) so the CLI maps it to the config exit
// code (1) rather than the transfer-error default. It implements the
// exitcodes.ExitCoder interface structurally, avoiding an import dependency
// on the exitcodes package.
type configErr struct{ err error }

func (e *configErr) Error() string { return e.err.Error() }
func (e *configErr) Unwrap() error { return e.err }
func (e *configErr) ExitCode() int { return 1 }

// New validates the options and constructs a Server without binding a port.
// Binding and serving happen in Run.
func New(opts Options) (*Server, error) {
	if strings.TrimSpace(opts.Addr) == "" {
		opts.Addr = DefaultAddr
	}
	if err := validateBindSecurity(opts); err != nil {
		return nil, &configErr{err}
	}
	// hostFromAddr already succeeded inside validateBindSecurity.
	host, _ := hostFromAddr(opts.Addr)
	if opts.OpenBrowser && !isLoopbackHost(host) {
		return nil, &configErr{fmt.Errorf(
			"webui: --gui requires a loopback --webui-addr (got %q); opening a browser at a "+
				"token-bearing URL on a non-loopback bind would expose the token to the local launcher",
			opts.Addr)}
	}
	token, auto, err := resolveAuthToken(opts)
	if err != nil {
		return nil, &configErr{err}
	}
	// A non-loopback bind exposes the token to the network; refuse a weak
	// operator-chosen one (#594). Auto-generated tokens are long by
	// construction and exempt.
	if n := utf8.RuneCountInString(token); !isLoopbackHost(host) && !auto && n < minRemoteTokenLen {
		return nil, &configErr{fmt.Errorf("webui: --webui-auth-token must be at least %d characters for a non-loopback bind (got %d)", minRemoteTokenLen, n)}
	}
	trusted, err := parseTrustedProxies(opts.TrustedProxies)
	if err != nil {
		return nil, &configErr{err}
	}
	s := &Server{
		opts:            opts,
		token:           token,
		tokenAuto:       auto,
		sessions:        newSessionStore(sessionTTL),
		logins:          newLoginLimiter(),
		trustedProxies:  trusted,
		allowedHosts:    buildAllowedHosts(host, isLoopbackHost(host)),
		hub:             newEventHub(),
		runs:            newRunManager(),
		sessionDefaults: newSessionDefaults(),
	}
	s.httpSrv = &http.Server{
		Handler:           s.buildHandler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: later issues stream progress over SSE, which must
		// not be capped by a per-response deadline.
	}
	return s, nil
}

// Start builds and runs a WebUI server, blocking until the process is
// signalled to stop. It is the entry point called from cmd/dmt.
func Start(opts Options) error {
	s, err := New(opts)
	if err != nil {
		return err
	}
	return s.Run()
}

// Run binds the address, serves until SIGINT/SIGTERM (or, in --gui mode, an
// idle timeout), then shuts down gracefully. It blocks for the lifetime of
// the front-end, mirroring how the TUI owns the foreground.
func (s *Server) Run() error {
	// Single-instance handoff happens before any port is bound: if another
	// `dmt --gui` already holds the lock, open a window at its URL and exit
	// rather than racing it for the same address (--webui server deployments
	// don't opt into this — SingleInstance is --gui-only).
	var inst *desktop.Instance
	if s.opts.SingleInstance {
		var handedOff bool
		inst, handedOff = s.tryHandoffToRunningInstance()
		if handedOff {
			return nil
		}
	}
	if inst != nil {
		defer inst.Release()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// baseCtx backs every request context. Cancelling it on shutdown unblocks
	// long-lived handlers (the SSE stream loops until its request context is
	// done) so httpSrv.Shutdown doesn't wait the full timeout for an open
	// EventSource connection.
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	s.httpSrv.BaseContext = func(net.Listener) context.Context { return baseCtx }

	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("webui: cannot bind %s: %w", s.opts.Addr, err)
	}

	s.printBanner(ln.Addr())

	loginURL := s.loginURL(ln.Addr())
	if inst != nil {
		if pubErr := inst.PublishURL(loginURL); pubErr != nil {
			logging.WarnEvent("webui: could not publish GUI handoff info", "error", pubErr)
		}
	}
	if s.opts.OpenBrowser {
		go s.openBrowser(loginURL)
	}

	serveErr := make(chan error, 1)
	go func() {
		var err error
		if s.opts.tlsEnabled() {
			err = s.httpSrv.ServeTLS(ln, s.opts.TLSCert, s.opts.TLSKey)
		} else {
			err = s.httpSrv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	var idleDone chan struct{}
	if s.opts.ExitWhenIdle {
		idleDone = make(chan struct{})
		go func() {
			runIdleWatchdog(baseCtx, s.hub, s.runs, idleWatchdogPoll, idleWatchdogGrace)
			close(idleDone)
		}()
	}

	select {
	case <-ctx.Done():
		logging.Info("webui: shutting down")
		return s.shutdown(baseCancel)
	case <-idleDone: // nil when !ExitWhenIdle; a nil channel never selects
		logging.InfoEvent("webui: no browser window open and idle — exiting")
		return s.shutdown(baseCancel)
	case err := <-serveErr:
		return err
	}
}

// shutdown cancels any in-flight migration, unblocks long-lived requests, and
// gracefully stops the HTTP server. Shared by the signal-triggered and
// idle-triggered exit paths in Run.
func (s *Server) shutdown(baseCancel context.CancelFunc) error {
	// Cancel any in-flight migration so its checkpoint is left resumable and
	// the process can exit cleanly. A no-op (returns false) when idle-exit is
	// what triggered this, since idleWatchdog never fires while a run is
	// active.
	if s.runs.cancel() {
		logging.Info("webui: cancelling active migration")
	}
	// Cancel request contexts first so open SSE streams return and Shutdown
	// doesn't block on them for the full timeout.
	baseCancel()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(shutCtx)
	// Give a cancelled migration a bounded moment to flush its final
	// checkpoint state before the process exits.
	s.waitForRuns(shutCtx)
	return err
}

// tryHandoffToRunningInstance checks whether another `dmt --gui` already
// holds the single-instance lock. If so, it opens a window at that instance's
// URL and reports handedOff=true so the caller exits without ever binding a
// port. Otherwise it returns the acquired (or nil, on an unexpected local
// failure) Instance for the caller to hold and eventually Release.
func (s *Server) tryHandoffToRunningInstance() (inst *desktop.Instance, handedOff bool) {
	inst, err := desktop.NewInstance()
	if err != nil {
		logging.WarnEvent("webui: GUI single-instance check unavailable", "error", err)
		return nil, false
	}
	acquired, handoffURL, err := inst.Acquire()
	if err != nil {
		logging.WarnEvent("webui: GUI single-instance check failed", "error", err)
		return nil, false
	}
	if acquired {
		return inst, false
	}
	if handoffURL != "" {
		logging.InfoEvent("webui: another dmt --gui instance is already running; opening a window there")
		// Called synchronously, unlike the primary launch path's `go
		// s.openBrowser(...)`: this process is about to return and exit, so
		// there is no server loop left to run concurrently with. openBrowser
		// itself still returns immediately either way — it only execs the
		// opener (cmd.Start(), never Wait()) and never blocks on the browser.
		s.openBrowser(handoffURL)
	} else {
		logging.WarnEvent("webui: another dmt --gui instance appears to be starting; try again shortly")
	}
	return nil, true
}

// openBrowser launches url in a browser per s.opts.AppWindow. Failures are
// logged, never fatal — the server keeps running and the banner already
// printed remains the fallback way to reach it.
func (s *Server) openBrowser(url string) {
	l := desktop.New()
	if s.opts.AppWindow {
		fallback, err := l.OpenAppWindow(url)
		if err == nil {
			if fallback == desktop.FallbackSafari {
				logging.InfoEvent("webui: " + desktop.SafariHint)
			}
			return
		}
		logging.WarnEvent("webui: could not open an app window; falling back to the default browser", "reason", desktop.Describe(err))
	}
	if err := l.Open(url); err != nil {
		logging.WarnEvent("webui: could not open a browser automatically; use the link above", "reason", desktop.Describe(err))
	}
}

// baseURL builds the scheme+host portion of the server's address, shared by
// printBanner and loginURL so the banner and the auto-opened browser always
// agree.
func (s *Server) baseURL(bound net.Addr) string {
	scheme := "http"
	if s.opts.tlsEnabled() {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, displayHost(s.opts.Addr, bound))
}

// loginURL is the URL a browser should open: for an auto-generated (loopback)
// token this embeds the one-click ?token=, which the SPA consumes and scrubs
// from history on load (app.js boot()); for a configured token it is the bare
// root and the operator signs in manually.
func (s *Server) loginURL(bound net.Addr) string {
	base := s.baseURL(bound)
	if s.tokenAuto {
		return fmt.Sprintf("%s/?token=%s", base, s.token)
	}
	return base + "/"
}

// printBanner writes the startup URL to stdout.
func (s *Server) printBanner(bound net.Addr) {
	fmt.Printf("\n  dmt WebUI %s\n\n", version.Version)
	if s.tokenAuto {
		fmt.Printf("  ➜  %s\n\n", s.loginURL(bound))
	} else {
		fmt.Printf("  ➜  %s\n", s.loginURL(bound))
		fmt.Printf("     Sign in with your --webui-auth-token.\n\n")
	}
	fmt.Printf("  Press Ctrl+C to stop.\n\n")
}
