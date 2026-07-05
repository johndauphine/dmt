package webui

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func proxyReq(remote, xff string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://localhost/api/health", nil)
	r.RemoteAddr = remote
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

// TestClientIPTrustedProxy covers the client-IP attribution matrix (#604).
func TestClientIPTrustedProxy(t *testing.T) {
	cases := []struct {
		name, remote, xff, want string
		cidrs                   []string
	}{
		{"no trusted proxies → direct peer", "203.0.113.9:5555", "1.2.3.4", "203.0.113.9", nil},
		{"trusted peer, single client", "10.0.0.1:5555", "198.51.100.7", "198.51.100.7", []string{"10.0.0.0/8"}},
		{"trusted peer, chained trusted proxies", "10.0.0.1:5555", "198.51.100.7, 10.0.0.2", "198.51.100.7", []string{"10.0.0.0/8"}},
		// A client that prepends a forged (untrusted) XFF entry can't frame it:
		// the proxy appends the real client to the right, and the right-to-left
		// walk returns that real entry, ignoring the forged leftmost one (#604).
		{"forged leftmost XFF ignored", "10.0.0.1:5555", "9.9.9.9, 198.51.100.7", "198.51.100.7", []string{"10.0.0.0/8"}},
		{"IPv6 trusted peer + client", "[fd00::1]:5555", "2001:db8::42", "2001:db8::42", []string{"fd00::/8"}},
		{"IPv6 untrusted peer ignores XFF", "[2001:db8::99]:5555", "2001:db8::42", "2001:db8::99", []string{"fd00::/8"}},
		{"untrusted peer ignores XFF (anti-spoof)", "203.0.113.9:5555", "198.51.100.7", "203.0.113.9", []string{"10.0.0.0/8"}},
		{"trusted peer, no XFF", "10.0.0.1:5555", "", "10.0.0.1", []string{"10.0.0.0/8"}},
		{"trusted peer, all-trusted XFF", "10.0.0.1:5555", "10.0.0.2, 10.0.0.3", "10.0.0.1", []string{"10.0.0.0/8"}},
		{"bare-IP trusted proxy", "192.168.1.5:5555", "198.51.100.7", "198.51.100.7", []string{"192.168.1.5"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newTestServer(t, Options{AuthToken: testToken, TrustedProxies: c.cidrs})
			if got := s.clientIP(proxyReq(c.remote, c.xff)); got != c.want {
				t.Errorf("clientIP = %q, want %q", got, c.want)
			}
		})
	}
}

// TestTrustedProxyInvalidCIDR: a malformed trusted-proxy value fails startup.
func TestTrustedProxyInvalidCIDR(t *testing.T) {
	if _, err := New(Options{Addr: "127.0.0.1:8484", TrustedProxies: []string{"not-a-cidr"}}); err == nil {
		t.Error("expected an error for an invalid trusted-proxy CIDR")
	}
}

// TestTrustedProxyPerClientLimiter proves the point of #604: with a trusted
// proxy configured, two distinct real clients behind the same proxy get
// independent limiter buckets, so one being locked out doesn't lock the other.
func TestTrustedProxyPerClientLimiter(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: "topsecret", TrustedProxies: []string{"10.0.0.0/8"}})
	h := s.buildHandler()

	badLogin := func(xff string) int {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost/api/login", strings.NewReader(`{"token":"wrong"}`))
		r.RemoteAddr = "10.0.0.1:5555" // the trusted reverse proxy
		r.Header.Set("X-Forwarded-For", xff)
		h.ServeHTTP(rec, r)
		return rec.Code
	}

	var last int
	for i := 0; i < loginMaxFailures+2; i++ {
		last = badLogin("198.51.100.7")
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("client A should be locked out, got %d", last)
	}
	// A different real client via the same proxy is unaffected.
	if code := badLogin("203.0.113.50"); code == http.StatusTooManyRequests {
		t.Error("a different client behind the same proxy must not share the lockout")
	}
}

// TestTrustedProxySpoofBlocked: an attacker connecting directly (peer not in
// the trusted set) cannot escape the limiter by rotating X-Forwarded-For — the
// limiter still keys on their real peer IP.
func TestTrustedProxySpoofBlocked(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: "topsecret", TrustedProxies: []string{"10.0.0.0/8"}})
	h := s.buildHandler()

	var last int
	for i := 0; i < loginMaxFailures+2; i++ {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost/api/login", strings.NewReader(`{"token":"wrong"}`))
		r.RemoteAddr = "203.0.113.99:5555"                        // NOT a trusted proxy
		r.Header.Set("X-Forwarded-For", "1.2.3."+strconv.Itoa(i)) // rotating spoof
		h.ServeHTTP(rec, r)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Error("an untrusted peer rotating XFF must still be rate-limited on its real IP")
	}
}
