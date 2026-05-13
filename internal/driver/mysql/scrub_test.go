package mysql

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// TestNewReader_DoesNotLeakPasswordOnFailure pins the scrubbing
// contract for the MySQL reader (#231): when the underlying
// go-sql-driver/mysql open/ping fails, the surfaced error must not
// contain the sentinel password. Connection is pointed at an
// unroutable host so the failure path is deterministic.
func TestNewReader_DoesNotLeakPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"
	cfg := &dbconfig.SourceConfig{
		Host:     "127.0.0.1",
		Port:     1,
		Database: "nonexistent",
		User:     "scrubtest",
		Password: sentinelPassword,
		// MySQL TLS defaults to require+verify (#252) — for an
		// unreachable test host we don't need TLS to "work", but the
		// driver still needs to parse a valid DSN, so we set the
		// SSL mode explicitly to a recognized value.
		SSLMode: "disable",
	}

	_, err := NewReader(cfg, 4)
	if err == nil {
		t.Skip("expected NewReader to fail; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through MySQL NewReader error: %q", err.Error())
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
		SSLMode:  "disable",
	}

	_, err := NewWriter(cfg, 4, driver.WriterOptions{})
	if err == nil {
		t.Skip("expected NewWriter to fail; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through MySQL NewWriter error: %q", err.Error())
	}
}
