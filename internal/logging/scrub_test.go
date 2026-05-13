package logging

import (
	"errors"
	"strings"
	"testing"
)

// TestScrub_DSNPasswords covers the three DSN dialects dmt produces
// (Postgres URL form, MSSQL URL form, MySQL Go-driver form). The
// sentinel password "hunter2" must not survive in any of them — that's
// the load-bearing assertion. The surrounding host/database structure
// is preserved so operators still get useful context from the log.
func TestScrub_DSNPasswords(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "postgres URL",
			input: "postgres://migrator:hunter2@db.example.com:5432/sales?sslmode=require",
		},
		{
			name:  "postgresql URL alias",
			input: "postgresql://migrator:hunter2@db.example.com:5432/sales",
		},
		{
			name:  "sqlserver URL",
			input: "sqlserver://sa:hunter2@db.example.com:1433?database=Reports",
		},
		{
			name:  "mssql URL alias",
			input: "mssql://sa:hunter2@db.example.com:1433?database=Reports",
		},
		{
			name:  "MySQL go-driver",
			input: "migrator:hunter2@tcp(db.example.com:3306)/sales?parseTime=true",
		},
		{
			name:  "DSN tail with password query param",
			input: "postgres://db.example.com:5432/sales?user=migrator&password=hunter2&sslmode=require",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Scrub(tc.input)
			if strings.Contains(out, "hunter2") {
				t.Fatalf("password leaked in scrubbed output: %q", out)
			}
			if !strings.Contains(out, RedactedToken) {
				t.Fatalf("expected %s placeholder, got %q", RedactedToken, out)
			}
			// Host should survive so debug context is preserved.
			if !strings.Contains(out, "db.example.com") {
				t.Fatalf("host stripped from scrubbed output: %q", out)
			}
		})
	}
}

// TestScrub_KeyValueCredentials covers space-separated and equals-form
// credential strings that appear in driver-library errors and operator-
// pasted config snippets (e.g. libpq-style "host=... password=...").
func TestScrub_KeyValueCredentials(t *testing.T) {
	cases := []struct {
		name  string
		input string
		leak  string
	}{
		{
			name:  "libpq style",
			input: "host=db port=5432 user=migrator password=hunter2 sslmode=require",
			leak:  "hunter2",
		},
		{
			name:  "api_key= form",
			input: "config: api_key=abc123def456 model=claude",
			leak:  "abc123def456",
		},
		{
			name:  "passwd= form",
			input: "auth failure: passwd=secret123 user=root",
			leak:  "secret123",
		},
		// Copilot review on #231: the kv_credential rule used to
		// hard-code `=` as the separator, silently rewriting YAML-ish
		// inputs from `:` to `=`. The pattern now captures the
		// separator (including surrounding whitespace) so the original
		// structural form survives. Pin both shapes.
		{
			name:  "colon separator preserved",
			input: "config: password: hunter2 model=claude",
			leak:  "hunter2",
		},
		{
			name:  "equals separator preserved",
			input: "config: password=hunter2 model=claude",
			leak:  "hunter2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Scrub(tc.input)
			if strings.Contains(out, tc.leak) {
				t.Fatalf("secret leaked: %q", out)
			}
			if !strings.Contains(out, RedactedToken) {
				t.Fatalf("expected redaction token: %q", out)
			}
		})
	}
}

// TestScrub_KeyValueSeparatorPreserved pins the Copilot-review fix on
// #231: the kv_credential rule used to hard-code `=` regardless of the
// original separator, so `password: hunter2` became `password=[REDACTED]`
// and confused log parsers expecting YAML shape. Now the separator is
// captured and emitted verbatim.
func TestScrub_KeyValueSeparatorPreserved(t *testing.T) {
	cases := []struct{ in, want string }{
		{"password=hunter2", "password=" + RedactedToken},
		{"password = hunter2", "password = " + RedactedToken},
		{"password: hunter2", "password: " + RedactedToken},
		{"password:hunter2", "password:" + RedactedToken},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := Scrub(tc.in); got != tc.want {
				t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestScrub_AuthHeader covers a Bearer-token authorization line as it
// would appear in an HTTP error string (e.g. from a misbehaving AI
// provider returning the original request headers in its error body).
func TestScrub_AuthHeader(t *testing.T) {
	out := Scrub("request failed: Authorization: Bearer sk-ant-api03-AAAABBBBCCCCDDDDEEEEFFFF was rejected")
	if strings.Contains(out, "AAAABBBBCCCCDDDDEEEEFFFF") {
		t.Fatalf("bearer token tail leaked: %q", out)
	}
	if !strings.Contains(out, "Bearer "+RedactedToken) {
		t.Fatalf("expected Bearer %s, got %q", RedactedToken, out)
	}
}

// TestScrub_APIKey covers raw sk- / sk-ant- API keys that aren't
// behind a header — e.g. a logging slip-up that emits an API key
// in an error message body.
func TestScrub_APIKey(t *testing.T) {
	cases := []struct {
		name  string
		input string
		leak  string
	}{
		{
			name:  "anthropic key",
			input: "model call failed for key sk-ant-api03-AAAABBBBCCCCDDDDEEEEFFFF",
			leak:  "AAAABBBBCCCCDDDDEEEEFFFF",
		},
		{
			name:  "openai key",
			input: "openai request: sk-abcdefghijklmnopqrstuvwxyz0123456789",
			leak:  "sk-abcdefghijklmnopqrstuvwxyz0123456789",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Scrub(tc.input)
			if strings.Contains(out, tc.leak) {
				t.Fatalf("API key leaked: %q", out)
			}
			if !strings.Contains(out, RedactedToken) {
				t.Fatalf("expected redaction token, got %q", out)
			}
		})
	}
}

// TestScrub_SlackWebhook covers Slack incoming-webhook URLs. The
// service path is the credential — there is no separate password
// field — so the entire path must be replaced. The host portion is
// preserved so operators can still tell a Slack-host failure from a
// proxy-host failure.
func TestScrub_SlackWebhook(t *testing.T) {
	in := `Post "https://hooks.slack.com/services/T0123ABCDEF/B98765WXYZAB/qwertyuiopasdfghjklzxcvbnm" failed: dial tcp: lookup hooks.slack.com: no such host`
	out := Scrub(in)
	if strings.Contains(out, "T0123ABCDEF") || strings.Contains(out, "qwertyuiopasdfghjklzxcvbnm") {
		t.Fatalf("Slack webhook tokens leaked: %q", out)
	}
	if !strings.Contains(out, "hooks.slack.com") {
		t.Fatalf("expected hooks.slack.com hostname to survive: %q", out)
	}
	if !strings.Contains(out, RedactedToken) {
		t.Fatalf("expected redaction token: %q", out)
	}
}

// TestScrub_NoSecrets is the negative case: a string with no secret
// patterns must pass through unchanged. This pins the "conservative
// by default" invariant — Scrub never mangles a non-secret error.
func TestScrub_NoSecrets(t *testing.T) {
	in := "table users: column id not found (SQLState 42703)"
	out := Scrub(in)
	if out != in {
		t.Fatalf("non-secret string was modified:\n  in:  %q\n  out: %q", in, out)
	}
}

// TestScrub_Empty asserts the empty-string fast path doesn't allocate
// or panic.
func TestScrub_Empty(t *testing.T) {
	if got := Scrub(""); got != "" {
		t.Fatalf("Scrub(\"\") = %q, want empty", got)
	}
}

// TestScrubError_NilPassthrough — nil in, nil out, mirrors fmt.Errorf
// convention so callers can do `return logging.ScrubError(err)` without
// a nil check.
func TestScrubError_NilPassthrough(t *testing.T) {
	if got := ScrubError(nil); got != nil {
		t.Fatalf("ScrubError(nil) = %v, want nil", got)
	}
}

// TestScrubError_PreservesSentinel asserts errors.Is still works on
// the wrapped error so callers can scrub a sentinel-bearing error
// without breaking type-based handling.
func TestScrubError_PreservesSentinel(t *testing.T) {
	sentinel := errors.New("connect failed")
	wrapped := errors.Join(sentinel, errors.New("dsn=postgres://u:hunter2@h:5432/d"))
	scrubbed := ScrubError(wrapped)
	if scrubbed == nil {
		t.Fatal("ScrubError dropped non-nil error")
	}
	if strings.Contains(scrubbed.Error(), "hunter2") {
		t.Fatalf("password leaked through ScrubError: %q", scrubbed.Error())
	}
	if !errors.Is(scrubbed, sentinel) {
		t.Fatalf("sentinel lost after scrub: %v", scrubbed)
	}
}

// TestScrubError_NoChangeReturnsOriginal asserts the "no scrub needed"
// fast path returns the original error pointer rather than allocating
// a wrapper.
func TestScrubError_NoChangeReturnsOriginal(t *testing.T) {
	orig := errors.New("table users not found")
	got := ScrubError(orig)
	if got != orig {
		t.Fatalf("expected pointer-equal passthrough on no-secret error, got new error: %v", got)
	}
}
