package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
)

func TestDriftGateArgs(t *testing.T) {
	cases := []struct {
		name string
		gate config.DriftGateConfig
		want []string
	}{
		{
			name: "config path",
			gate: config.DriftGateConfig{SMTConfig: "/etc/smt.yaml"},
			want: []string{"--config", "/etc/smt.yaml", "drift"},
		},
		{
			name: "profile",
			gate: config.DriftGateConfig{SMTProfile: "prod-pair"},
			want: []string{"--profile", "prod-pair", "drift"},
		},
		{
			name: "destructive-only passthrough",
			gate: config.DriftGateConfig{SMTConfig: "c.yaml", FailOnDestructiveOnly: true},
			want: []string{"--config", "c.yaml", "drift", "--fail-on-destructive-only"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := driftGateArgs(&tc.gate)
			if len(got) != len(tc.want) {
				t.Fatalf("args = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("args = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestDriftGateFindings(t *testing.T) {
	cases := []struct {
		name         string
		code         int
		output       string
		runErr       error
		wantSeverity driver.PreFlightSeverity
		wantContains string
		wantRemedy   string
	}{
		{
			name:         "in sync passes as info",
			code:         0,
			wantSeverity: driver.SeverityInfo,
			wantContains: "in sync",
		},
		{
			name:         "drift detected blocks with sync remedy",
			code:         8,
			output:       "Drift: 2 table(s) changed\n  orders: column status missing on target\n",
			wantSeverity: driver.SeverityError,
			wantContains: "column status missing on target",
			wantRemedy:   "smt sync",
		},
		{
			name:         "config error fails closed",
			code:         1,
			output:       "config: source.type is required\n",
			wantSeverity: driver.SeverityError,
			wantContains: "drift state unknown",
			wantRemedy:   "--skip-preflight drift.gate",
		},
		{
			name:         "connection error fails closed",
			code:         2,
			wantSeverity: driver.SeverityError,
			wantContains: "exit 2",
		},
		{
			name:         "binary missing fails closed with install remedy",
			code:         -1,
			runErr:       errors.New(`exec: "smt": executable file not found in $PATH`),
			wantSeverity: driver.SeverityError,
			wantContains: "could not run",
			wantRemedy:   "install smt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := driftGateFindings("smt", tc.code, tc.output, tc.runErr)
			if len(fs) != 1 {
				t.Fatalf("findings = %d, want 1", len(fs))
			}
			f := fs[0]
			if f.Severity != tc.wantSeverity {
				t.Errorf("Severity = %q, want %q", f.Severity, tc.wantSeverity)
			}
			if f.Check != driftGateCheck {
				t.Errorf("Check = %q, want %q", f.Check, driftGateCheck)
			}
			if f.Side != driver.PreFlightSideTarget {
				t.Errorf("Side = %q, want target", f.Side)
			}
			if !strings.Contains(f.Message, tc.wantContains) {
				t.Errorf("Message = %q, want it to contain %q", f.Message, tc.wantContains)
			}
			if tc.wantRemedy != "" && !strings.Contains(f.Remedy, tc.wantRemedy) {
				t.Errorf("Remedy = %q, want it to contain %q", f.Remedy, tc.wantRemedy)
			}
		})
	}
}

func TestRunDriftGate_InvokesConfiguredBinary(t *testing.T) {
	orig := runDriftGateCommand
	defer func() { runDriftGateCommand = orig }()

	var gotBin string
	var gotArgs []string
	runDriftGateCommand = func(_ context.Context, bin string, args []string) (int, string, error) {
		gotBin = bin
		gotArgs = args
		return 0, "", nil
	}

	fs := runDriftGate(context.Background(), &config.DriftGateConfig{
		SMTBinary: "/opt/smt/bin/smt",
		SMTConfig: "pair.yaml",
	})
	if gotBin != "/opt/smt/bin/smt" {
		t.Errorf("bin = %q, want configured path", gotBin)
	}
	if len(gotArgs) == 0 || gotArgs[len(gotArgs)-1] != "drift" {
		t.Errorf("args = %v, want trailing drift subcommand", gotArgs)
	}
	if len(fs) != 1 || fs[0].Severity != driver.SeverityInfo {
		t.Fatalf("findings = %+v, want single info", fs)
	}
}

func TestRunDriftGate_DefaultBinaryName(t *testing.T) {
	orig := runDriftGateCommand
	defer func() { runDriftGateCommand = orig }()

	var gotBin string
	runDriftGateCommand = func(_ context.Context, bin string, _ []string) (int, string, error) {
		gotBin = bin
		return 0, "", nil
	}
	runDriftGate(context.Background(), &config.DriftGateConfig{SMTConfig: "c.yaml"})
	if gotBin != "smt" {
		t.Errorf("bin = %q, want PATH-resolved default \"smt\"", gotBin)
	}
}

func TestRunDriftGate_TimeoutReportsTimeout(t *testing.T) {
	orig := runDriftGateCommand
	defer func() { runDriftGateCommand = orig }()

	runDriftGateCommand = func(ctx context.Context, _ string, _ []string) (int, string, error) {
		<-ctx.Done() // simulate the child being killed by the deadline
		return -1, "", ctx.Err()
	}

	fs := runDriftGate(context.Background(), &config.DriftGateConfig{
		SMTConfig:      "c.yaml",
		TimeoutSeconds: 1,
	})
	if len(fs) != 1 || fs[0].Severity != driver.SeverityError {
		t.Fatalf("findings = %+v, want single error", fs)
	}
	if !strings.Contains(fs[0].Message, "timed out") {
		t.Errorf("Message = %q, want timeout wording", fs[0].Message)
	}
	if !strings.Contains(fs[0].Remedy, "timeout_seconds") {
		t.Errorf("Remedy = %q, want timeout_seconds pointer", fs[0].Remedy)
	}
}

func TestRunDriftGate_TimeoutDefaultApplied(t *testing.T) {
	orig := runDriftGateCommand
	defer func() { runDriftGateCommand = orig }()

	var gotDeadline time.Time
	runDriftGateCommand = func(ctx context.Context, _ string, _ []string) (int, string, error) {
		gotDeadline, _ = ctx.Deadline()
		return 0, "", nil
	}
	start := time.Now()
	runDriftGate(context.Background(), &config.DriftGateConfig{SMTConfig: "c.yaml"})
	remaining := time.Until(gotDeadline)
	_ = start
	if remaining < 9*time.Minute || remaining > 10*time.Minute+time.Second {
		t.Errorf("default deadline ~%s away, want ~10m", remaining)
	}
}

// TestRunDriftGateCommand_RealExec exercises the unstubbed exec seam with a
// script standing in for smt: the exit code must round-trip through
// exec.ExitError (8 = drift), stdout must be captured, and a missing binary
// must surface as a launch error, not an exit code.
func TestRunDriftGateCommand_RealExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture; unix only")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "fake-smt")
	script := "#!/bin/sh\necho \"Drift: 1 table(s) changed\"\nexit 8\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	code, out, err := runDriftGateCommand(context.Background(), fake, []string{"drift"})
	if err != nil {
		t.Fatalf("runDriftGateCommand: %v", err)
	}
	if code != smtDriftDetectedExit {
		t.Errorf("exit code = %d, want %d", code, smtDriftDetectedExit)
	}
	if !strings.Contains(out, "Drift: 1 table(s) changed") {
		t.Errorf("output = %q, want captured drift summary", out)
	}

	_, _, err = runDriftGateCommand(context.Background(), filepath.Join(dir, "no-such-binary"), nil)
	if err == nil {
		t.Fatal("missing binary: err = nil, want launch error")
	}
}

// TestAppendDriftGateFindings pins the run-only-if-needed policy: the gate
// never runs when the section is absent, when skipped (by name, prefix, or
// "all"), or when the cheap checks already aborted.
func TestAppendDriftGateFindings(t *testing.T) {
	orig := runDriftGateCommand
	defer func() { runDriftGateCommand = orig }()

	var calls int
	runDriftGateCommand = func(_ context.Context, _ string, _ []string) (int, string, error) {
		calls++
		return smtDriftDetectedExit, "Drift: 1 table(s) changed", nil
	}

	gate := &config.DriftGateConfig{SMTConfig: "pair.yaml"}
	newOrch := func(g *config.DriftGateConfig) *Orchestrator {
		return &Orchestrator{config: &config.Config{
			Migration: config.MigrationConfig{DriftGate: g},
		}}
	}

	cases := []struct {
		name      string
		gate      *config.DriftGateConfig
		in        preFlightResult
		skip      map[string]bool
		wantRun   bool
		wantAbort bool
	}{
		{name: "absent section never runs", gate: nil},
		{name: "aborted result short-circuits", gate: gate, in: preFlightResult{Aborted: true}, wantAbort: true},
		{name: "skip by exact name", gate: gate, skip: map[string]bool{"drift.gate": true}},
		{name: "skip by prefix", gate: gate, skip: map[string]bool{"drift": true}},
		{name: "skip all", gate: gate, skip: map[string]bool{"all": true}},
		{name: "runs and aborts on drift", gate: gate, wantRun: true, wantAbort: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls = 0
			out := newOrch(tc.gate).appendDriftGateFindings(context.Background(), tc.in, tc.skip)
			if ran := calls > 0; ran != tc.wantRun {
				t.Errorf("gate ran = %v, want %v", ran, tc.wantRun)
			}
			if out.Aborted != tc.wantAbort {
				t.Errorf("Aborted = %v, want %v", out.Aborted, tc.wantAbort)
			}
		})
	}
}

func TestOutputTail(t *testing.T) {
	if got := outputTail(""); got != "" {
		t.Errorf("empty output tail = %q, want empty", got)
	}
	long := strings.Repeat("line\n", 20) + "final summary"
	tail := outputTail(long)
	if !strings.HasSuffix(tail, "final summary") {
		t.Errorf("tail = %q, want it to end with the last line", tail)
	}
	if strings.Count(tail, "\n") > 6 {
		t.Errorf("tail has %d newlines, want <= 6", strings.Count(tail, "\n"))
	}
}
