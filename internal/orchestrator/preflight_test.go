package orchestrator

import (
	"math"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
)

func TestEstimatedTargetBytesSaturatesOverflow(t *testing.T) {
	o := &Orchestrator{tables: []driver.Table{
		{Name: "huge", RowCount: math.MaxInt64, EstimatedRowSize: math.MaxInt64},
		{Name: "more", RowCount: 10, EstimatedRowSize: 10},
	}}
	if got := o.estimatedTargetBytes(); got != math.MaxInt64 {
		t.Fatalf("overflowing target-byte estimate = %d, want MaxInt64 saturation", got)
	}
}

// TestPreFlightSkipSet pins the parse rules for --skip-preflight: comma
// splitting, whitespace trim, case-fold, and the dropping of empty tokens.
// Drivers emit lowercase dotted check names; matching is case-insensitive
// to be friendly to operators who type "Privileges" or "PRIVILEGES" in
// CLI flags.
func TestPreFlightSkipSet(t *testing.T) {
	cases := []struct {
		in   []string
		want []string
	}{
		{in: nil, want: nil},
		{in: []string{""}, want: nil},
		{in: []string{"all"}, want: []string{"all"}},
		{in: []string{"privileges,disk.estimate"}, want: []string{"disk.estimate", "privileges"}},
		{in: []string{"privileges", "disk.estimate"}, want: []string{"disk.estimate", "privileges"}},
		{in: []string{" PRIVILEGES.create_table , Disk "}, want: []string{"disk", "privileges.create_table"}},
		{in: []string{"a,,b,"}, want: []string{"a", "b"}},
	}
	for _, tc := range cases {
		got := preFlightSkipSet(tc.in)
		keys := keysSorted(got)
		if !equalStringSlices(keys, tc.want) {
			t.Errorf("preFlightSkipSet(%v) = %v, want %v", tc.in, keys, tc.want)
		}
	}
}

// TestShouldSkip pins the exact-vs-prefix matching for skip names. The
// dotted prefix form is the operator's lever for skipping a category
// without listing every leaf — "privileges" should silence every
// "privileges.*" finding but must NOT silence "pool.headroom".
func TestShouldSkip(t *testing.T) {
	cases := []struct {
		check string
		skip  map[string]bool
		want  bool
	}{
		{check: "privileges.create_table", skip: map[string]bool{"privileges.create_table": true}, want: true},
		{check: "privileges.create_table", skip: map[string]bool{"privileges": true}, want: true},
		{check: "privileges.schema_create", skip: map[string]bool{"privileges": true}, want: true},
		{check: "pool.headroom", skip: map[string]bool{"privileges": true}, want: false},
		{check: "version.server", skip: map[string]bool{"version.server.foo": true}, want: false},
		{check: "pool.headroom", skip: map[string]bool{}, want: false},
		{check: "deeply.nested.check.name", skip: map[string]bool{"deeply.nested": true}, want: true},
		{check: "deeply.nested.check.name", skip: map[string]bool{"deeply": true}, want: true},
		// Non-dotted name with non-matching skip
		{check: "encoding", skip: map[string]bool{"version": true}, want: false},
	}
	for _, tc := range cases {
		got := shouldSkip(tc.check, tc.skip)
		if got != tc.want {
			t.Errorf("shouldSkip(%q, %v) = %v, want %v", tc.check, tc.skip, got, tc.want)
		}
	}
}

// TestApplyPreFlightSkips covers the policy layer: error findings whose
// check matches the skip set get downgraded to info (and tagged so the
// log line still shows operator intent), and Aborted recomputes to
// reflect the remaining errors. Warnings stay warnings — the policy is
// "skipping silences errors but doesn't suppress warnings."
func TestApplyPreFlightSkips(t *testing.T) {
	in := preFlightResult{
		Findings: []driver.PreFlightFinding{
			{Severity: driver.SeverityError, Check: "privileges.create_table", Side: driver.PreFlightSideTarget, Message: "no CREATE"},
			{Severity: driver.SeverityError, Check: "pool.headroom", Side: driver.PreFlightSideTarget, Message: "too few"},
			{Severity: driver.SeverityWarn, Check: "encoding.server", Side: driver.PreFlightSideSource, Message: "non-utf8"},
		},
		Aborted: true,
	}
	out := applyPreFlightSkips(in, map[string]bool{"privileges": true})

	// Privileges error -> info; pool error remains; warn unchanged.
	if out.Findings[0].Severity != driver.SeverityInfo {
		t.Errorf("privileges error not downgraded to info: %+v", out.Findings[0])
	}
	if !strings.HasPrefix(out.Findings[0].Message, "(skipped via --skip-preflight)") {
		t.Errorf("privileges message not tagged with skip prefix: %+v", out.Findings[0])
	}
	if out.Findings[1].Severity != driver.SeverityError {
		t.Errorf("pool error wrongly downgraded: %+v", out.Findings[1])
	}
	if out.Findings[2].Severity != driver.SeverityWarn {
		t.Errorf("warn finding wrongly mutated: %+v", out.Findings[2])
	}
	if !out.Aborted {
		t.Error("Aborted should remain true while pool.headroom error is unskipped")
	}

	// Skip the remaining error too — Aborted should flip to false.
	out2 := applyPreFlightSkips(out, map[string]bool{"pool.headroom": true})
	if out2.Aborted {
		t.Error("Aborted should be false after all errors skipped")
	}
}

// TestPreFlightError_ExitCode pins the ConfigError mapping. Without this,
// the CLI's exit-code classifier might match preflight error messages
// against unrelated heuristics (e.g. the word "connection" routing to
// ConnectionError) and report a recoverable code for a non-recoverable
// failure. Codex review on prior PRs called this out as load-bearing.
func TestPreFlightError_ExitCode(t *testing.T) {
	err := &PreFlightError{Findings: []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "privileges.schema_create",
		Side:     driver.PreFlightSideTarget,
		Message:  "no CREATE",
		Remedy:   "GRANT CREATE",
	}}}
	if got := err.ExitCode(); got != exitcodes.ConfigError {
		t.Errorf("ExitCode = %d, want %d (ConfigError)", got, exitcodes.ConfigError)
	}
	if !strings.Contains(err.Error(), "no CREATE") {
		t.Errorf("error message missing finding text: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "GRANT CREATE") {
		t.Errorf("error message missing remedy: %q", err.Error())
	}
}

// TestContainsAbortingFinding covers the severity gate. Only
// SeverityError counts; warn and info findings are observational.
func TestContainsAbortingFinding(t *testing.T) {
	cases := []struct {
		findings []driver.PreFlightFinding
		want     bool
	}{
		{findings: nil, want: false},
		{findings: []driver.PreFlightFinding{{Severity: driver.SeverityWarn}}, want: false},
		{findings: []driver.PreFlightFinding{{Severity: driver.SeverityInfo}}, want: false},
		{findings: []driver.PreFlightFinding{{Severity: driver.SeverityError}}, want: true},
		{findings: []driver.PreFlightFinding{
			{Severity: driver.SeverityWarn},
			{Severity: driver.SeverityError},
			{Severity: driver.SeverityInfo},
		}, want: true},
	}
	for i, tc := range cases {
		got := containsAbortingFinding(tc.findings)
		if got != tc.want {
			t.Errorf("case %d: containsAbortingFinding = %v, want %v", i, got, tc.want)
		}
	}
}

func keysSorted(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Stable sort for test stability.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
