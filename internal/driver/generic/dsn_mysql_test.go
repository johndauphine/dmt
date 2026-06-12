package generic

import (
	"strings"
	"testing"
)

// DSN tests ported from the hand-written mysql dialect with its
// removal (#509 cleanup) — they pin the #252 fail-closed TLS contract
// and the read/write timeout defaults on mysqlTCPDSN.

func TestMysqlDSNTimeouts(t *testing.T) {
	dsn := mysqlTCPDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{})

	if !strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN missing writeTimeout: %s", dsn)
	}
	if !strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN missing readTimeout: %s", dsn)
	}
}

func TestMysqlDSNTimeoutOverride(t *testing.T) {
	dsn := mysqlTCPDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{
		"writeTimeout": "10m",
		"readTimeout":  "10m",
	})

	if strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN should not override user writeTimeout: %s", dsn)
	}
	if strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN should not override user readTimeout: %s", dsn)
	}
}

// TestMysqlDSN_SSLMode_KeyNormalization is the core regression guard
// for #252: dbconfig.DSNOptions emits the Postgres-style key `sslmode`,
// but the MySQL DSN builder historically read only `ssl_mode`, so
// configured TLS settings were silently ignored. Both keys must
// produce byte-for-byte identical DSNs.
func TestMysqlDSN_SSLMode_KeyNormalization(t *testing.T) {
	mysqlKey := mysqlTCPDSN("localhost", 3306, "db", "u", "p", map[string]any{"ssl_mode": "require"})
	pgKey := mysqlTCPDSN("localhost", 3306, "db", "u", "p", map[string]any{"sslmode": "require"})

	if mysqlKey != pgKey {
		t.Errorf("ssl_mode and sslmode keys produced different DSNs:\n  ssl_mode: %s\n  sslmode : %s", mysqlKey, pgKey)
	}
	if !strings.Contains(mysqlKey, "tls=true") {
		t.Errorf("normalized DSN doesn't include expected tls=true: %s", mysqlKey)
	}
}

func TestMysqlDSN_SSLMode_AllModes(t *testing.T) {
	cases := []struct {
		mode    string
		wantTLS string
		why     string
	}{
		{"disable", "false", "disable = no TLS"},
		{"disabled", "false", "alias for disable"},
		{"false", "false", "alias for disable"},
		{"require", "true", "TLS required, verify CA+host"},
		{"required", "true", "alias for require"},
		{"true", "true", "alias for require"},
		{"verify-ca", "true", "#252: mapped to verify-full instead of pre-fix skip-verify (which was no verification at all)"},
		{"verify_ca", "true", "underscore variant"},
		{"verify-full", "true", "CA + hostname verification"},
		{"verify_full", "true", "underscore variant"},
		{"verify-identity", "true", "MySQL term for verify-full"},
		{"preferred", "preferred", "#252: explicit opt-in to downgradeable TLS"},
		{"bogus-mode", "true", "#252: unknown values default to tls=true (require) instead of pre-fix tls=preferred (downgrade)"},
		{"", "true", "#252: empty/unset defaults to tls=true instead of pre-fix tls=preferred"},
	}

	for _, tc := range cases {
		t.Run(tc.mode+"_"+tc.why, func(t *testing.T) {
			opts := map[string]any{}
			if tc.mode != "" {
				opts["ssl_mode"] = tc.mode
			}
			dsn := mysqlTCPDSN("localhost", 3306, "db", "u", "p", opts)
			want := "tls=" + tc.wantTLS
			if !strings.Contains(dsn, want) {
				t.Errorf("ssl_mode=%q produced DSN %s; want it to contain %q", tc.mode, dsn, want)
			}
			// Downgradeable TLS only on explicit operator opt-in.
			if tc.mode != "preferred" && strings.Contains(dsn, "tls=preferred") {
				t.Errorf("ssl_mode=%q produced downgradeable tls=preferred unexpectedly; DSN: %s", tc.mode, dsn)
			}
			// skip-verify (TLS without verification) must never appear.
			if strings.Contains(dsn, "tls=skip-verify") {
				t.Errorf("ssl_mode=%q produced tls=skip-verify (no verification); DSN: %s", tc.mode, dsn)
			}
		})
	}
}

// The #227 resume contract: mysql converts duplicate-key conflicts to
// no-ops via PK self-assignment, never INSERT IGNORE (which also masks
// data-conversion errors, silently dropping rows on resume). The live
// idempotent-replay behavior is pinned by the integration test; this
// pins the catalog declaration that drives it.
func TestMysqlCatalogIdempotentSuffix(t *testing.T) {
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	if cat.Bulk.IdempotentVerb != "" {
		t.Errorf("mysql must use the suffix form, not a verb (got %q)", cat.Bulk.IdempotentVerb)
	}
	want := "ON DUPLICATE KEY UPDATE {pk} = {pk}"
	if cat.Bulk.IdempotentSuffix != want {
		t.Errorf("idempotent_suffix = %q, want %q", cat.Bulk.IdempotentSuffix, want)
	}
	if strings.Contains(strings.ToUpper(cat.Bulk.IdempotentSuffix+cat.Bulk.IdempotentVerb), "IGNORE") {
		t.Error("INSERT IGNORE is forbidden — it masks data-conversion errors")
	}
}
