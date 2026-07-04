// Package webui serves dmt's browser front-end (#578, epic #577). It is the
// third front-end alongside the CLI and TUI and is launched with the global
// --webui flag. This foundation package owns the HTTP server lifecycle,
// embedded-asset serving, and the security baseline (bind-address rules,
// shared-secret auth, session cookies, security headers). The command
// surface — status/run/progress/wizards — is layered on in later issues
// (#579-#582) by registering handlers under /api/.
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
	"syscall"
	"time"

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
	httpSrv   *http.Server
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
	s := &Server{
		opts:      opts,
		token:     token,
		tokenAuto: auto,
		sessions:  newSessionStore(sessionTTL),
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
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutCtx)
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
