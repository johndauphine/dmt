package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateBindSecurity(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr string // substring; "" means no error
	}{
		{name: "loopback default", opts: Options{Addr: "127.0.0.1:8484"}},
		{name: "loopback ipv6", opts: Options{Addr: "[::1]:8484"}},
		{name: "localhost host", opts: Options{Addr: "localhost:8484"}},
		{name: "loopback needs no token", opts: Options{Addr: "127.0.0.1:8484", AuthToken: ""}},
		{
			name:    "remote without token",
			opts:    Options{Addr: "0.0.0.0:8484"},
			wantErr: "without --webui-auth-token",
		},
		{
			name:    "remote with token but no tls",
			opts:    Options{Addr: "0.0.0.0:8484", AuthToken: "secret"},
			wantErr: "refusing plaintext HTTP",
		},
		{
			name: "remote with token and tls",
			opts: Options{Addr: "0.0.0.0:8484", AuthToken: "secret", TLSCert: "c.pem", TLSKey: "k.pem"},
		},
		{
			name: "remote with token and insecure optout",
			opts: Options{Addr: "0.0.0.0:8484", AuthToken: "secret", Insecure: true},
		},
		{
			name:    "remote named host without token",
			opts:    Options{Addr: "db-server:8484"},
			wantErr: "without --webui-auth-token",
		},
		{
			name:    "tls cert without key",
			opts:    Options{Addr: "127.0.0.1:8484", TLSCert: "c.pem"},
			wantErr: "must be provided together",
		},
		{
			name:    "invalid addr",
			opts:    Options{Addr: "not-an-addr"},
			wantErr: "invalid --webui-addr",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBindSecurity(tc.opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestIsLoopbackHost(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1": true,
		"::1":       true,
		"localhost": true,
		"LOCALHOST": true,
		"":          false, // wildcard
		"0.0.0.0":   false,
		"::":        false,
		"10.0.0.5":  false,
		"db-server": false,
	}
	for host, want := range cases {
		if got := isLoopbackHost(host); got != want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	h := securityHeaders(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	want := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	}
	for k, v := range want {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
	csp := rec.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "default-src 'self'") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Errorf("unexpected CSP: %q", csp)
	}
}

func TestHostGuardLoopback(t *testing.T) {
	s := newTestServer(t, Options{Addr: "127.0.0.1:8484"})
	h := s.buildHandler()

	tests := []struct {
		host string
		want int
	}{
		{host: "127.0.0.1:8484", want: http.StatusOK},
		{host: "localhost:8484", want: http.StatusOK},
		{host: "localhost", want: http.StatusOK},
		{host: "evil.example.com", want: http.StatusForbidden}, // DNS rebinding
		{host: "evil.example.com:8484", want: http.StatusForbidden},
	}
	for _, tc := range tests {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://x/", nil)
		req.Host = tc.host
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("Host %q → %d, want %d", tc.host, rec.Code, tc.want)
		}
	}
}

func TestHostGuardDisabledForRemoteBind(t *testing.T) {
	// A remote bind can't enumerate its public hostname, so the host guard is
	// off there (token+TLS are the controls). Any Host must pass.
	s := newTestServer(t, Options{Addr: "0.0.0.0:8484", AuthToken: "a-strong-remote-token", Insecure: true})
	if s.allowedHosts != nil {
		t.Fatalf("expected nil allowedHosts for remote bind, got %v", s.allowedHosts)
	}
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	req.Host = "anything.example.com"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("remote bind blocked Host %q with %d", req.Host, rec.Code)
	}
}

func TestRequestIsHTTPS(t *testing.T) {
	// Plaintext, no proxy header.
	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	if requestIsHTTPS(r) {
		t.Error("plaintext request should not be HTTPS")
	}
	// Reverse proxy terminated TLS.
	r = httptest.NewRequest(http.MethodGet, "http://x/", nil)
	r.Header.Set("X-Forwarded-Proto", "https")
	if !requestIsHTTPS(r) {
		t.Error("X-Forwarded-Proto: https should count as HTTPS")
	}
}

func TestOriginGuard(t *testing.T) {
	s := &Server{}
	guarded := s.originGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name   string
		method string
		origin string
		want   int
	}{
		{name: "GET no origin", method: http.MethodGet, want: http.StatusOK},
		{name: "POST no origin (curl)", method: http.MethodPost, want: http.StatusOK},
		{name: "POST same origin", method: http.MethodPost, origin: "http://example.test", want: http.StatusOK},
		{name: "POST cross origin", method: http.MethodPost, origin: "http://evil.test", want: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "http://example.test/api/x", nil)
			req.Host = "example.test"
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := httptest.NewRecorder()
			guarded.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}
