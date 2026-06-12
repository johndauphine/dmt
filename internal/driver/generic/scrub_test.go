package generic

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// Credential-scrub contract (#231), ported from the hand-written
// driver packages with their removal: when open/ping fails, the
// surfaced error must not contain the password — some backend drivers
// echo the raw DSN in their error text. Connections point at an
// unroutable host/port so the failure path is deterministic. One
// catalog per backend style covers the strategy surface.

const scrubSentinel = "hunter2-canary-string"

func scrubCatalogs(t *testing.T) []string {
	t.Helper()
	return []string{"mysql", "postgres", "mssql"}
}

func TestNewReader_DoesNotLeakPasswordOnFailure(t *testing.T) {
	for _, name := range scrubCatalogs(t) {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			cfg := &dbconfig.SourceConfig{
				Host: "127.0.0.1", Port: 1,
				Database: "nonexistent", User: "scrubtest", Password: scrubSentinel,
				SSLMode: "disable",
			}
			_, err = NewReader(cat, cfg, 4)
			if err == nil {
				t.Skip("expected NewReader to fail; environment unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), scrubSentinel) {
				t.Fatalf("password leaked through %s NewReader error: %q", name, err.Error())
			}
		})
	}
}

func TestNewWriter_DoesNotLeakPasswordOnFailure(t *testing.T) {
	for _, name := range scrubCatalogs(t) {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			cfg := &dbconfig.TargetConfig{
				Host: "127.0.0.1", Port: 1,
				Database: "nonexistent", User: "scrubtest", Password: scrubSentinel,
				SSLMode: "disable",
			}
			_, err = NewWriter(cat, cfg, 4, driver.WriterOptions{})
			if err == nil {
				t.Skip("expected NewWriter to fail; environment unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), scrubSentinel) {
				t.Fatalf("password leaked through %s NewWriter error: %q", name, err.Error())
			}
		})
	}
}
