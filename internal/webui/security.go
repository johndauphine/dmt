package webui

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// validateBindSecurity enforces the remote-access rules before the server
// starts. A loopback bind is unrestricted (auto-token, plaintext OK). Any
// other bind is reachable from the network and must be protected: it requires
// a configured auth token AND either native TLS or an explicit --webui-insecure
// opt-out (for reverse-proxy TLS termination).
func validateBindSecurity(opts Options) error {
	// TLS cert/key must be provided as a pair regardless of bind address.
	if (opts.TLSCert == "") != (opts.TLSKey == "") {
		return fmt.Errorf("webui: --webui-tls-cert and --webui-tls-key must be provided together")
	}

	host, err := hostFromAddr(opts.Addr)
	if err != nil {
		return err
	}
	if isLoopbackHost(host) {
		return nil
	}

	// Non-loopback (remote-reachable) bind.
	if strings.TrimSpace(opts.AuthToken) == "" {
		return fmt.Errorf("webui: refusing to bind non-loopback address %q without --webui-auth-token; "+
			"remote access requires a shared secret", opts.Addr)
	}
	if !opts.tlsEnabled() && !opts.Insecure {
		return fmt.Errorf("webui: refusing plaintext HTTP on non-loopback address %q; "+
			"provide --webui-tls-cert/--webui-tls-key, or pass --webui-insecure when TLS is terminated by a reverse proxy",
			opts.Addr)
	}
	return nil
}

// hostFromAddr extracts the host portion of a host:port bind address.
func hostFromAddr(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("webui: invalid --webui-addr %q: %w", addr, err)
	}
	return host, nil
}

// isLoopbackHost reports whether binding host keeps the server unreachable
// from other machines. An empty or wildcard host (":8484", "0.0.0.0", "::")
// is remotely reachable and therefore NOT loopback.
func isLoopbackHost(host string) bool {
	if host == "" {
		return false // wildcard bind — reachable on all interfaces
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	// A named host that isn't localhost — treat conservatively as remote.
	return false
}

// displayHost produces a clickable host:port for the startup banner. Wildcard
// binds have no single canonical address, so localhost is shown for a working
// local link; remote operators reach the server by its own hostname/IP.
func displayHost(configured string, bound net.Addr) string {
	host, port, err := net.SplitHostPort(configured)
	if err != nil {
		return bound.String()
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort("localhost", port)
	}
	return net.JoinHostPort(host, port)
}

// securityHeaders sets defensive response headers on every response. The CSP
// keeps the bundle self-contained (no external fetches), which also lets the
// WebUI run on air-gapped servers. 'unsafe-inline' covers the placeholder's
// inline script/style; the #582 SPA build can tighten this to hashed assets.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy",
			"default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "+
				"script-src 'self' 'unsafe-inline'; connect-src 'self'; font-src 'self'; "+
				"base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// originGuard rejects cross-origin state-changing requests as CSRF defense in
// depth (SameSite=Strict cookies are the primary control). Requests without an
// Origin header (curl, API clients using the bearer token) are allowed; the
// bearer path has no ambient-authority CSRF exposure.
func (s *Server) originGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isStateChanging(r.Method) {
			if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) {
				writeError(w, http.StatusForbidden, "origin_forbidden", "cross-origin request rejected")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// isStateChanging reports whether the method mutates server state and thus
// needs CSRF protection.
func isStateChanging(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

// sameOrigin compares an Origin header's host to the request Host.
func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, host)
}
