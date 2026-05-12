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
// produce the same DSN.
func TestBuildDSN_SSLMode_KeyNormalization(t *testing.T) {
	d := &Dialect{}

	mysqlKey := d.BuildDSN("localhost", 3306, "db", "u", "p", map[string]any{"ssl_mode": "require"})
	pgKey := d.BuildDSN("localhost", 3306, "db", "u", "p", map[string]any{"sslmode": "require"})

	if !strings.Contains(mysqlKey, "tls=true") {
		t.Errorf("ssl_mode key not honored; DSN: %s", mysqlKey)
	}
	if !strings.Contains(pgKey, "tls=true") {
		t.Errorf("sslmode key (Postgres-style) not honored; DSN: %s", pgKey)
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
			// Production-default sanity: the downgradeable
			// `tls=preferred` must never appear, on any mode.
			if strings.Contains(dsn, "tls=preferred") {
				t.Errorf("ssl_mode=%q produced downgradeable tls=preferred; DSN: %s", tc.mode, dsn)
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
