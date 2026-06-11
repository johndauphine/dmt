package tui

import (
	"strings"
	"testing"
)

// #440: /preflight and its /health-check alias share one parser.
func TestParsePreflightArgs(t *testing.T) {
	m := &Model{}

	cf, pn, skip, ai, err := m.parsePreflightArgs("/preflight", []string{
		"/preflight", "@my.yaml", "--skip-preflight", "privileges", "--ai-review",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cf != "my.yaml" || pn != "" || skip != "privileges" || !ai {
		t.Fatalf("got cf=%q pn=%q skip=%q ai=%v", cf, pn, skip, ai)
	}

	_, _, skip, ai, err = m.parsePreflightArgs("/health-check", []string{"/health-check"})
	if err != nil {
		t.Fatal(err)
	}
	if skip != "" || ai {
		t.Fatalf("defaults: skip=%q ai=%v", skip, ai)
	}

	_, _, _, _, err = m.parsePreflightArgs("/health-check", []string{"/health-check", "--bogus"})
	if err == nil || !strings.Contains(err.Error(), "/health-check: unknown flag --bogus") {
		t.Fatalf("err = %v", err)
	}
}
