package postgres

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// TestNewReader_DoesNotLeakPasswordOnFailure pins the scrubbing
// contract for the PostgreSQL reader (#231): when the underlying
// pgxpool / pgx layer rejects the connection, the surfaced error must
// not contain the sentinel password. We point at an unroutable host so
// the failure path is deterministic without needing a live database.
//
// The test intentionally uses a host that the pgx parser will accept
// but pool creation will fail to dial — the goal is to exercise the
// scrubbing wrapper on a real driver error, not to test pgx's parser.
func TestNewReader_DoesNotLeakPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"
	cfg := &dbconfig.SourceConfig{
		Host:     "127.0.0.1",
		Port:     1, // port 1 is reserved; the dial will fail fast
		Database: "nonexistent",
		User:     "scrubtest",
		Password: sentinelPassword,
	}

	_, err := NewReader(cfg, 4)
	if err == nil {
		t.Skip("expected NewReader to fail against unroutable host; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through PostgreSQL NewReader error: %q", err.Error())
	}
}

// TestNewWriter_DoesNotLeakPasswordOnFailure is the writer-side mirror
// of the reader test above. Same intent (#231): scrubbing applies to
// every NewWriter error path that wraps a pgx/pgxpool error.
func TestNewWriter_DoesNotLeakPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"
	cfg := &dbconfig.TargetConfig{
		Host:     "127.0.0.1",
		Port:     1,
		Database: "nonexistent",
		User:     "scrubtest",
		Password: sentinelPassword,
	}

	// Pass an empty WriterOptions; the pgxpool dial fails on the
	// unroutable host before the TypeMapper check runs, so the
	// scrubbing surface gets exercised.
	_, err := NewWriter(cfg, 4, driver.WriterOptions{})
	if err == nil {
		t.Skip("expected NewWriter to fail; environment unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), sentinelPassword) {
		t.Fatalf("password leaked through PostgreSQL NewWriter error: %q", err.Error())
	}
}
