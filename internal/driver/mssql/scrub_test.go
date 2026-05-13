package mssql

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// TestNewReader_DoesNotLeakPasswordOnFailure pins the scrubbing
// contract for the SQL Server reader (#231): when the underlying
// go-mssqldb open/ping fails, the surfaced error must not contain
// the sentinel password. Connection is pointed at an unroutable
// host so the failure path is deterministic.
func TestNewReader_DoesNotLeakPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"
	cfg := &dbconfig.SourceConfig{
		Host:     "127.0.0.1",
		Port:     1,
		Database: "nonexistent",
		User:     "scrubtest",
		Password: sentinelPassword,
	}

	_, err := NewReader(cfg, 4)
	if err == nil {
		t.Skip("expected NewReader to fail; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through MSSQL NewReader error: %q", err.Error())
	}
}

// TestNewWriter_DoesNotLeakPasswordOnFailure is the writer-side mirror
// of the reader scrub test (#231).
func TestNewWriter_DoesNotLeakPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"
	cfg := &dbconfig.TargetConfig{
		Host:     "127.0.0.1",
		Port:     1,
		Database: "nonexistent",
		User:     "scrubtest",
		Password: sentinelPassword,
	}

	_, err := NewWriter(cfg, 4, driver.WriterOptions{})
	if err == nil {
		t.Skip("expected NewWriter to fail; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through MSSQL NewWriter error: %q", err.Error())
	}
}
