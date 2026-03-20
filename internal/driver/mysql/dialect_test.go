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
