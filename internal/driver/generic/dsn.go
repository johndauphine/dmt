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
