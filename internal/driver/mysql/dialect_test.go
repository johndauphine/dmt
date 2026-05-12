package mysql

import (
	"strings"
	"testing"
)

func TestBuildDSNTimeouts(t *testing.T) {
	d := &Dialect{}
	dsn := d.BuildDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{})

	if !strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN missing writeTimeout: %s", dsn)
	}
	if !strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN missing readTimeout: %s", dsn)
	}
}

func TestBuildDSNTimeoutOverride(t *testing.T) {
	d := &Dialect{}
	dsn := d.BuildDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{
		"writeTimeout": "10m",
		"readTimeout":  "10m",
	})

	// User-provided values should not be overridden
	if strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN should not override user writeTimeout: %s", dsn)
	}
	if strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN should not override user readTimeout: %s", dsn)
	}
}

// TestBuildDSN_SSLMode_KeyNormalization is the core regression guard
// for #252: dbconfig.DSNOptions emits the Postgres-style key
// `sslmode`, but the MySQL dialect historically read only `ssl_mode`,
// so configured TLS settings were silently ignored. Both keys must
// now produce byte-for-byte identical DSNs — asserting equality is
// stricter than asserting both contain `tls=true`, and would catch a
// regression where e.g. one path picked up a different default.
func TestBuildDSN_SSLMode_KeyNormalization(t *testing.T) {
	d := &Dialect{}

	mysqlKey := d.BuildDSN("localhost", 3306, "db", "u", "p", map[string]any{"ssl_mode": "require"})
	pgKey := d.BuildDSN("localhost", 3306, "db", "u", "p", map[string]any{"sslmode": "require"})

	if mysqlKey != pgKey {
		t.Errorf("ssl_mode and sslmode keys produced different DSNs:\n  ssl_mode: %s\n  sslmode : %s", mysqlKey, pgKey)
	}
	// Sanity check: the normalized DSN still honors the configured mode.
	if !strings.Contains(mysqlKey, "tls=true") {
		t.Errorf("normalized DSN doesn't include expected tls=true: %s", mysqlKey)
	}
}

func TestBuildDSN_SSLMode_AllModes(t *testing.T) {
	d := &Dialect{}

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
			dsn := d.BuildDSN("localhost", 3306, "db", "u", "p", opts)
			want := "tls=" + tc.wantTLS
			if !strings.Contains(dsn, want) {
				t.Errorf("ssl_mode=%q produced DSN %s; want it to contain %q", tc.mode, dsn, want)
			}
			// `tls=preferred` (downgradeable) must only ever appear
			// when the operator explicitly typed "preferred" — never
			// from any other configured value or from the default.
			if tc.mode != "preferred" && strings.Contains(dsn, "tls=preferred") {
				t.Errorf("ssl_mode=%q produced downgradeable tls=preferred unexpectedly; DSN: %s", tc.mode, dsn)
			}
			// `skip-verify` (TLS without any verification) was the
			// pre-#252 mapping for verify-ca. It should never appear
			// from any configured mode.
			if strings.Contains(dsn, "tls=skip-verify") {
				t.Errorf("ssl_mode=%q produced tls=skip-verify (no verification); DSN: %s", tc.mode, dsn)
			}
		})
	}
}
