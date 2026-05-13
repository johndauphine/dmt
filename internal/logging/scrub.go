// Package logging — secrets scrubbing (#231).
//
// The Scrub function is the centralized "redact known secret patterns"
// helper used by error-formatting and log-formatting call sites that
// could otherwise emit credentials, connection strings, or webhook
// URLs. The patterns covered are the ones an operator can verifiably
// describe in docs/SECURITY.md — the goal is "no driver-library error
// wrapping a DSN leaks the password" and "no Slack failure prints the
// webhook," not heuristic PII scrubbing of row content (which we
// address by structurally never logging row content; see ScrubError
// for the structural invariant).
//
// Design notes:
//   - Patterns are compiled once at package init via sync.Once so the
//     hot path (every wrapped error) doesn't pay regexp compile cost.
//   - Each pattern leaves a stable token in place of the secret
//     (e.g. "password=[REDACTED]") so downstream regex/grep based
//     log-monitoring still works against the structural shape.
//   - The replacement token mirrors config.DebugDump's existing
//     "[REDACTED]" convention rather than introducing a new sentinel —
//     operators already grep their logs for "[REDACTED]" and we don't
//     want to fragment that.
package logging

import (
	"regexp"
	"sync"
)

// RedactedToken is the stable replacement for any scrubbed secret.
// Exported so tests across packages can assert against the same value
// rather than duplicating the literal.
const RedactedToken = "[REDACTED]"

// scrubPatterns is the compiled regex list applied in order by Scrub.
// Initialized once via the sync.Once below so Scrub itself is allocation-
// and lock-free on the hot path.
var (
	scrubPatternsOnce sync.Once
	scrubPatterns     []scrubRule
)

// scrubRule pairs a compiled regex with a replacement template. The
// template uses the standard regexp ReplaceAll $N syntax so a rule can
// preserve structural bytes around the secret (e.g. the host portion
// of a DSN) while replacing only the credential.
type scrubRule struct {
	name        string
	pattern     *regexp.Regexp
	replacement string
}

func initScrubPatterns() {
	// Pattern order matters: more-specific rules run first so a
	// generic "password=..." rule can't gobble part of a Bearer
	// header. Authorization-header and webhook patterns are tightly
	// shaped and idempotent on the structural surroundings, so they
	// can run before the generic kv_credential fallback.
	scrubPatterns = []scrubRule{
		// Bearer / Token authorization headers as they appear in error
		// messages from HTTP clients. Run before kv_credential because
		// the header line "Authorization: Bearer sk-..." would
		// otherwise be matched by the "authorization=..." rule (with
		// the Bearer prefix swallowed into the redaction).
		{
			name:        "bearer_token",
			pattern:     regexp.MustCompile(`(?i)(Authorization:\s*)(Bearer|Token)\s+\S+`),
			replacement: `$1$2 ` + RedactedToken,
		},
		// Slack incoming-webhook URLs. The full path uniquely identifies
		// the webhook and is the credential — there is no separate
		// "password" part. Preserve the hostname so the log still tells
		// the operator which webhook host failed (e.g. corporate vs.
		// hooks.slack.com proxy).
		{
			name:        "slack_webhook",
			pattern:     regexp.MustCompile(`https?://hooks\.slack\.com/services/[A-Za-z0-9/_-]+`),
			replacement: `https://hooks.slack.com/services/` + RedactedToken,
		},
		// PostgreSQL / MSSQL URL-style DSN: scheme://user:PASSWORD@host
		// Covers postgres://, postgresql://, sqlserver://, mssql://,
		// mysql:// (the URL form some operators paste). The user portion
		// is preserved because it's not a secret on its own and operators
		// rely on it for debugging "is the right service account
		// connecting?".
		{
			name:        "url_userinfo",
			pattern:     regexp.MustCompile(`(?i)((?:postgres(?:ql)?|sqlserver|mssql|mysql|mariadb)://[^:/\s@]+):([^@\s]+)@`),
			replacement: `$1:` + RedactedToken + `@`,
		},
		// MySQL DSN (go-sql-driver/mysql) form: user:PASSWORD@tcp(host:port)
		// MySQL's DSN doesn't use a URL scheme prefix, so it isn't caught
		// by the rule above. Match user:pass@tcp(... explicitly. The
		// negated character classes intentionally exclude '@' and '/'
		// from the password to keep this rule from over-matching when
		// the password contains unusual characters that should have
		// been URL-encoded.
		{
			name:        "mysql_dsn",
			pattern:     regexp.MustCompile(`([A-Za-z0-9_.+\-]+):([^@/\s]+)@tcp\(`),
			replacement: `$1:` + RedactedToken + `@tcp(`,
		},
		// Generic key=value with secret-looking names. Used by error
		// messages that quote DSN tail parameters (e.g.
		// "?password=hunter2&sslmode=require") and by structured logs
		// that emit raw connection strings. Matches password, passwd,
		// pwd, api_key, apikey, api-key, secret, token. Authorization
		// is handled separately above (so the Bearer/Token prefix is
		// preserved). The value extends to the next ;, &, ', ", or
		// whitespace.
		{
			name:        "kv_credential",
			pattern:     regexp.MustCompile(`(?i)\b(password|passwd|pwd|api[_\-]?key|secret|token)\s*[=:]\s*("[^"]*"|'[^']*'|[^\s;&'"]+)`),
			replacement: `$1=` + RedactedToken,
		},
		// Anthropic API keys (sk-ant-...) and OpenAI keys (sk-...). The
		// full key length varies by provider but the high-entropy tail
		// is uniformly long. Match sk- followed by at least 20 of the
		// charset providers use for their keys.
		{
			name:        "anthropic_openai_key",
			pattern:     regexp.MustCompile(`sk-(ant-)?[A-Za-z0-9_\-]{20,}`),
			replacement: RedactedToken,
		},
	}
}

// Scrub returns s with known secret patterns replaced by RedactedToken.
// Safe to call on every log line; the regex set is compiled once and
// the function itself only reads the package-global slice.
//
// Scrub is intentionally conservative: it errs on the side of leaving
// non-secret strings untouched. The cost of a missed pattern (a
// password reaches a log) is much higher than the cost of a non-redacted
// non-secret (an HTTP error message stays readable). The structural
// fix — never logging a DSN in the first place — happens at the call
// sites via the [REDACTED] convention; Scrub is the defense-in-depth
// layer for driver-library error messages we don't directly control.
func Scrub(s string) string {
	if s == "" {
		return s
	}
	scrubPatternsOnce.Do(initScrubPatterns)
	for _, rule := range scrubPatterns {
		s = rule.pattern.ReplaceAllString(s, rule.replacement)
	}
	return s
}

// ScrubError returns a new error whose Error() string has had Scrub
// applied to it. The returned error wraps the input via fmt.Errorf
// semantics so errors.Is / errors.As still work for sentinel checks —
// the scrubbed string only affects the display surface, not error
// identity.
//
// Returns nil when err is nil (matches the pass-through convention of
// fmt.Errorf wrappers). Returns the original error untouched when
// Scrub would be a no-op, so a non-secret-bearing error path doesn't
// pay an extra wrapper allocation.
func ScrubError(err error) error {
	if err == nil {
		return nil
	}
	raw := err.Error()
	scrubbed := Scrub(raw)
	if scrubbed == raw {
		return err
	}
	return &scrubbedError{inner: err, msg: scrubbed}
}

// scrubbedError wraps a driver/library error so callers can keep
// using errors.Is/As to test against the underlying sentinel while
// any code that prints the error sees the scrubbed message.
type scrubbedError struct {
	inner error
	msg   string
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.inner }
