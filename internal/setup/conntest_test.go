package setup

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestTestConnection_ScrubsPasswordOnFailure pins the scrubbing
// contract for the setup-wizard's TestConnection helper (#231).
// When the driver library returns an error that quotes the DSN, the
// surfaced ConnTestResult.Error must not contain the sentinel
// password — that string ends up rendered to the TUI and may be
// captured into operator support tickets.
func TestTestConnection_ScrubsPasswordOnFailure(t *testing.T) {
	const sentinelPassword = "hunter2-canary-string"

	// Bound the test so a flaky DNS resolver / slow CI box can't hang
	// the suite. The driver should fail well inside this budget.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Point at an unroutable port so the failure is deterministic
	// without requiring a live database in CI.
	cases := []struct {
		name   string
		dbType string
		opts   map[string]any
	}{
		{name: "postgres", dbType: "postgres", opts: map[string]any{"sslmode": "disable"}},
		{name: "mssql", dbType: "mssql", opts: map[string]any{"encrypt": false}},
		{name: "mysql", dbType: "mysql", opts: map[string]any{"sslmode": "disable"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := TestConnection(ctx, tc.dbType, "127.0.0.1", 1,
				"nonexistent", "scrubtest", sentinelPassword, tc.opts)

			if result.Connected {
				t.Skip("expected connection to fail against unroutable host")
			}
			if strings.Contains(result.Error, sentinelPassword) {
				t.Fatalf("password leaked through ConnTestResult.Error for %s: %q",
					tc.dbType, result.Error)
			}
		})
	}
}
