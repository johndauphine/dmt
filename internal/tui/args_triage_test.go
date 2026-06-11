package tui

import (
	"strings"
	"testing"
	"time"
)

// #441: /validate gains --ai-triage and --timeout.
func TestParseValidateArgs(t *testing.T) {
	m := &Model{}

	cf, _, ai, timeout, err := m.parseValidateArgs([]string{
		"/validate", "@my.yaml", "--ai-triage", "--timeout", "120s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cf != "my.yaml" || !ai || timeout != 2*time.Minute {
		t.Fatalf("got cf=%q ai=%v timeout=%v", cf, ai, timeout)
	}

	_, _, ai, timeout, err = m.parseValidateArgs([]string{"/validate"})
	if err != nil {
		t.Fatal(err)
	}
	if ai || timeout != 0 {
		t.Fatalf("defaults: ai=%v timeout=%v", ai, timeout)
	}

	_, _, _, _, err = m.parseValidateArgs([]string{"/validate", "--timeout", "soon"})
	if err == nil || !strings.Contains(err.Error(), "invalid --timeout") {
		t.Fatalf("err = %v", err)
	}
}

// #441: /diagnose parses --run, --ai-triage, --timeout.
func TestParseDiagnoseArgs(t *testing.T) {
	m := &Model{}

	_, _, runID, ai, timeout, err := m.parseDiagnoseArgs([]string{
		"/diagnose", "--run", "run-42", "--ai-triage", "--timeout=45s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if runID != "run-42" || !ai || timeout != 45*time.Second {
		t.Fatalf("got run=%q ai=%v timeout=%v", runID, ai, timeout)
	}

	_, _, runID, ai, _, err = m.parseDiagnoseArgs([]string{"/diagnose"})
	if err != nil {
		t.Fatal(err)
	}
	if runID != "" || ai {
		t.Fatalf("defaults: run=%q ai=%v", runID, ai)
	}

	_, _, _, _, _, err = m.parseDiagnoseArgs([]string{"/diagnose", "--bogus"})
	if err == nil || !strings.Contains(err.Error(), "/diagnose: unknown flag --bogus") {
		t.Fatalf("err = %v", err)
	}
}
