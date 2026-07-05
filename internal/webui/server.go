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

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/version"
)

// DefaultAddr is the loopback bind address used when --webui-addr is empty.
const DefaultAddr = "127.0.0.1:8484"

// sessionTTL bounds how long a browser session cookie stays valid before the
// operator must re-authenticate.
const sessionTTL = 12 * time.Hour

// Options configures the WebUI server. It is populated from the global
// --webui* flags in cmd/migrate.
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

	// Origin context threaded from the global flags so later WebUI issues
	// (#579/#580) can resolve configs/profiles the way the CLI and TUI do.
	// Stored by the foundation; not yet consumed.
	ConfigPath string
	StateFile  string
	Profile    string
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
	token, auto, err := resolveAuthToken(opts)
	if err != nil {
		return nil, &configErr{err}
	}
	// hostFromAddr already succeeded inside validateBindSecurity.
	host, _ := hostFromAddr(opts.Addr)
	// A non-loopback bind exposes the token to the network; refuse a weak
	// operator-chosen one (#594). Auto-generated tokens are long by
	// construction and exempt.
	if n := utf8.RuneCountInString(token); !isLoopbackHost(host) && !auto && n < minRemoteTokenLen {
		return nil, &configErr{fmt.Errorf("webui: --webui-auth-token must be at least %d characters for a non-loopback bind (got %d)", minRemoteTokenLen, n)}
	}
	s := &Server{
		opts:            opts,
		token:           token,
		tokenAuto:       auto,
		sessions:        newSessionStore(sessionTTL),
		logins:          newLoginLimiter(),
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
// signalled to stop. It is the entry point called from cmd/migrate.
func Start(opts Options) error {
	s, err := New(opts)
	if err != nil {
		return err
	}
	return s.Run()
}

// Run binds the address, serves until SIGINT/SIGTERM, then shuts down
// gracefully. It blocks for the lifetime of the front-end, mirroring how the
// TUI owns the foreground.
func (s *Server) Run() error {
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

	select {
	case <-ctx.Done():
		logging.Info("webui: shutting down")
		// Cancel any in-flight migration so its checkpoint is left resumable
		// and the process can exit cleanly.
		if s.runs.cancel() {
			logging.Info("webui: cancelling active migration")
		}
		// Cancel request contexts first so open SSE streams return and
		// Shutdown doesn't block on them for the full timeout.
		baseCancel()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := s.httpSrv.Shutdown(shutCtx)
		// Give the cancelled migration a bounded moment to flush its final
		// checkpoint state before the process exits.
		s.waitForRuns(shutCtx)
		return err
	case err := <-serveErr:
		return err
	}
}

// printBanner writes the startup URL to stdout. For an auto-generated
// (loopback) token the token is embedded in the link for one-click access;
// for a configured token the operator signs in with it.
func (s *Server) printBanner(bound net.Addr) {
	scheme := "http"
	if s.opts.tlsEnabled() {
		scheme = "https"
	}
	base := fmt.Sprintf("%s://%s", scheme, displayHost(s.opts.Addr, bound))
	fmt.Printf("\n  dmt WebUI %s\n\n", version.Version)
	if s.tokenAuto {
		fmt.Printf("  ➜  %s/?token=%s\n\n", base, s.token)
	} else {
		fmt.Printf("  ➜  %s/\n", base)
		fmt.Printf("     Sign in with your --webui-auth-token.\n\n")
	}
	fmt.Printf("  Press Ctrl+C to stop.\n\n")
}
