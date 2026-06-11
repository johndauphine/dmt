package tui

import (
	"strings"
	"testing"
	"time"
)

// #442: /ai config-review parsing; --request consumes the rest of the
// line because slash-command input is whitespace-split (no quoting).
func TestParseAIConfigReviewArgs(t *testing.T) {
	m := &Model{}

	cf, _, request, timeout, err := parseAIConfigReviewArgs(m, []string{
		"/ai config-review", "@my.yaml", "--timeout", "60s",
		"--request", "tune", "for", "low", "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cf != "my.yaml" || timeout != time.Minute {
		t.Fatalf("cf=%q timeout=%v", cf, timeout)
	}
	if request != "tune for low memory" {
		t.Fatalf("request=%q", request)
	}

	// --request= form also takes the remainder.
	_, _, request, _, err = parseAIConfigReviewArgs(m, []string{
		"/ai runbook", "--request=explain", "the", "plan",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request != "explain the plan" {
		t.Fatalf("request=%q", request)
	}

	_, _, _, _, err = parseAIConfigReviewArgs(m, []string{"/ai config-review", "--request"})
	if err == nil || !strings.Contains(err.Error(), "--request requires a value") {
		t.Fatalf("err = %v", err)
	}

	_, _, _, _, err = parseAIConfigReviewArgs(m, []string{"/ai config-review", "--bogus"})
	if err == nil || !strings.Contains(err.Error(), "/ai config-review: unknown flag --bogus") {
		t.Fatalf("err = %v", err)
	}
}

// #442: /analyze gains --ai-explain alongside --apply.
func TestParseAnalyzeArgsAIExplain(t *testing.T) {
	m := &Model{}
	_, _, apply, aiExplain, err := m.parseAnalyzeArgs([]string{"/analyze", "--apply", "--ai-explain"})
	if err != nil {
		t.Fatal(err)
	}
	if !apply || !aiExplain {
		t.Fatalf("apply=%v aiExplain=%v", apply, aiExplain)
	}
	_, _, _, aiExplain, err = m.parseAnalyzeArgs([]string{"/analyze"})
	if err != nil || aiExplain {
		t.Fatalf("default aiExplain=%v err=%v", aiExplain, err)
	}
}

// #442: /ai dispatch errors are actionable.
func TestHandleAICommand(t *testing.T) {
	m := &Model{}
	if out := runCmd(t, m.handleAICommand([]string{"/ai"})); !strings.Contains(out, "usage: /ai config-review") {
		t.Fatalf("usage output: %q", out)
	}
	if out := runCmd(t, m.handleAICommand([]string{"/ai", "evals"})); !strings.Contains(out, "CLI-only") {
		t.Fatalf("evals output: %q", out)
	}
}
