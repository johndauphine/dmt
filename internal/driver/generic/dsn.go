package generic

import (
	"fmt"
	"net/url"
	"strings"
)

// dsnFunc builds a connection string for engines whose DSN logic is
// genuinely imperative (the audit's strategy-selected escape hatch).
type dsnFunc func(host string, port int, database, user, password string, opts map[string]any) string

// dsnStrategies is the named registry catalogs select from via
// connection.dsn_strategy. Load-time validation rejects unknown names.
var dsnStrategies = map[string]dsnFunc{
	"sqlite_file":    sqliteFileDSN,
	"clickhouse_url": clickhouseURLDSN,
	"mysql_tcp":      mysqlTCPDSN,
}

// mysqlTCPDSN is the hand-written mysql dialect's BuildDSN moved
// verbatim (#509): go-sql-driver TCP DSN with credential escaping and
// the #252 TLS-mode normalization (fail-closed defaults).
func mysqlTCPDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// MySQL DSN format: user:password@tcp(host:port)/database?params
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)

	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("multiStatements", "true")
	params.Set("interpolateParams", "true")

	// Handle SSL/TLS mode. Accept both `ssl_mode` (MySQL convention)
	// and `sslmode` (Postgres convention — what dbconfig.DSNOptions
	// actually emits today, see #252). Reading both keys is the
	// Codex-recommended normalization so an operator's configured TLS
	// mode isn't silently ignored.
	sslMode, ok := opts["ssl_mode"].(string)
	if !ok || sslMode == "" {
		// Fall back to the Postgres key that dbconfig still uses.
		if v, vok := opts["sslmode"].(string); vok {
			sslMode = v
			ok = true
		}
	}
	if ok && sslMode != "" {
		switch strings.ToLower(sslMode) {
		case "disable", "disabled", "false":
			params.Set("tls", "false")
		case "require", "required", "true":
			params.Set("tls", "true")
		case "verify-ca", "verify_ca":
			// go-sql-driver/mysql has no built-in "verify CA but not
			// hostname" mode. The pre-#252 mapping was `skip-verify`
			// (TLS with NO verification) — the direct inverse of what
			// the operator asked for. Map to `tls=true` (CA + hostname
			// verification) instead: strictly stricter than verify-ca,
			// which is safe; the alternative would be a custom
			// RegisterTLSConfig path we don't yet have config plumbing
			// for. Users who need genuine CA-only verification should
			// use verify-full and accept the hostname constraint, or
			// register a custom TLS profile (future).
			params.Set("tls", "true")
		case "verify-full", "verify_full", "verify-identity", "verify_identity":
			params.Set("tls", "true")
		case "preferred":
			// Explicit opt-in to downgradeable TLS: try TLS, fall back
			// to plaintext on connection failure. NOT a safe default
			// (#252) — operators with this setting are knowingly
			// trading security for compatibility with mixed-TLS
			// environments. The driver and setup-wizard defaults
			// changed away from "preferred" in #252; this branch only
			// fires when the operator explicitly typed it.
			params.Set("tls", "preferred")
		default:
			// Unknown values used to fall through to `preferred`
			// (downgradeable to plaintext). Default to `tls=true`
			// instead — the operator clearly intended TLS but we
			// don't recognize the mode name; failing closed is safer
			// than failing open.
			params.Set("tls", "true")
		}
	} else {
		// No ssl_mode configured. Pre-#252 default was `preferred`
		// (silent downgrade to plain). Default to `tls=true` instead
		// (require + verify). Operators who explicitly want plain
		// can set `ssl_mode: disable`.
		params.Set("tls", "true")
	}

	// Handle charset
	if charset, ok := opts["charset"].(string); ok && charset != "" {
		params.Set("charset", charset)
	} else {
		params.Set("charset", "utf8mb4")
	}

	// Handle collation
	if collation, ok := opts["collation"].(string); ok && collation != "" {
		params.Set("collation", collation)
	}

	// Handle timezone
	if loc, ok := opts["loc"].(string); ok && loc != "" {
		params.Set("loc", loc)
	} else {
		params.Set("loc", "UTC")
	}

	// Set read/write timeouts to prevent indefinite hangs on large batch inserts.
	// These are go-sql-driver/mysql DSN parameters that set deadlines on the
	// underlying TCP connection. 5 minutes is generous for bulk operations.
	if _, ok := opts["writeTimeout"]; !ok {
		params.Set("writeTimeout", "5m")
	}
	if _, ok := opts["readTimeout"]; !ok {
		params.Set("readTimeout", "5m")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		encodedUser, encodedPassword, host, port, database, params.Encode())

	return dsn
}

// clickhouseURLDSN builds the clickhouse-go v2 URL form with proper
// escaping — a raw template would misparse credentials containing
// URL-reserved characters (codex on #507).
func clickhouseURLDSN(host string, port int, database, user, password string, _ map[string]any) string {
	q := url.Values{}
	q.Set("username", user)
	q.Set("password", password)
	u := url.URL{
		Scheme:   "clickhouse",
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + database,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// sqliteFileDSN mirrors the hand-written sqlite dialect's BuildDSN:
// path from the database field, WAL/busy-timeout/foreign-keys/
// synchronous pragmas baked in, optional extra pragmas via
// opts["pragmas"], and the file: URI form for :memory: so query
// parameters are honored consistently across connections.
func sqliteFileDSN(_ string, _ int, database, _, _ string, opts map[string]any) string {
	if database == "" {
		database = ":memory:"
	}

	params := url.Values{}
	params.Add("_pragma", "journal_mode(WAL)")
	params.Add("_pragma", "busy_timeout(30000)")
	params.Add("_pragma", "foreign_keys(ON)")
	params.Add("_pragma", "synchronous(NORMAL)")

	if extra, ok := opts["pragmas"].([]string); ok {
		for _, p := range extra {
			params.Add("_pragma", p)
		}
	}

	prefix := ""
	if database == ":memory:" || strings.HasPrefix(database, "file:") {
		if !strings.HasPrefix(database, "file:") {
			prefix = "file:"
		}
	}

	return fmt.Sprintf("%s%s?%s", prefix, database, params.Encode())
}
