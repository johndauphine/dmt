package orchestrator

// Drift-gate preflight (#575): shell out to the external `smt drift` binary
// and refuse to transfer when the LIVE TARGET schema has drifted from the
// source. This is the workflow bridge to SMT (the schema migration tool) for
// the shared user base — rows must not be pumped into a target whose schema
// no longer matches.
//
// Contract (smt docs/cli.md, stable as of smt#214): exit 0 = in sync,
// exit 8 = drift detected (domain result), anything else = execution error.
// The gate fails closed on execution errors — the operator opted in, so an
// unrunnable check blocks rather than silently passing.
//
// Not to be confused with migration.fail_on_schema_drift (#305), which gates
// on SOURCE-vs-last-snapshot using DMT's internal drift report after schema
// extraction. This gate compares the live target before anything runs.

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
)

// driftGateCheck is the finding check name; skippable via
// `--skip-preflight drift.gate` (or the `drift` prefix).
const driftGateCheck = "drift.gate"

// smtDriftDetectedExit is smt's dedicated "drift detected" exit code.
const smtDriftDetectedExit = 8

// driftGateDefaultTimeout bounds the external check when the config doesn't:
// smt drift extracts both schemas, which can take minutes on wide catalogs.
const driftGateDefaultTimeout = 10 * time.Minute

// runDriftGateCommand is the exec seam, replaceable in tests. It returns the
// process exit code and combined output when the binary ran (successfully or
// not), or a non-nil error when it could not be started at all.
var runDriftGateCommand = func(ctx context.Context, bin string, args []string) (int, string, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out), nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out), nil
	}
	return -1, string(out), err
}

// runDriftGate executes the configured gate and translates the outcome into
// preflight findings. Package-level (not a method) so tests can drive it with
// just a config block.
func runDriftGate(ctx context.Context, gate *config.DriftGateConfig) []driver.PreFlightFinding {
	timeout := driftGateDefaultTimeout
	if gate.TimeoutSeconds > 0 {
		timeout = time.Duration(gate.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	bin := gate.SMTBinary
	if bin == "" {
		bin = "smt"
	}
	code, out, err := runDriftGateCommand(ctx, bin, driftGateArgs(gate))
	if ctxErr := ctx.Err(); ctxErr != nil && (err != nil || code != 0) {
		// Deadline/cancel kills the child mid-run; report the timeout, not
		// the child's confusing secondary symptom.
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    driftGateCheck,
			Side:     driver.PreFlightSideTarget,
			Message:  fmt.Sprintf("drift gate timed out after %s running %s drift", timeout, bin),
			Remedy:   "raise migration.drift_gate.timeout_seconds, or skip with --skip-preflight drift.gate",
		}}
	}
	return driftGateFindings(bin, code, out, err)
}

// driftGateArgs builds the smt invocation. Global flags (--config/--profile)
// precede the subcommand; drift's own flags follow it.
func driftGateArgs(gate *config.DriftGateConfig) []string {
	var args []string
	if gate.SMTConfig != "" {
		args = append(args, "--config", gate.SMTConfig)
	}
	if gate.SMTProfile != "" {
		args = append(args, "--profile", gate.SMTProfile)
	}
	args = append(args, "drift")
	if gate.FailOnDestructiveOnly {
		args = append(args, "--fail-on-destructive-only")
	}
	return args
}

// driftGateFindings is the pure exit-code → findings policy.
func driftGateFindings(bin string, code int, output string, runErr error) []driver.PreFlightFinding {
	switch {
	case runErr != nil:
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    driftGateCheck,
			Side:     driver.PreFlightSideTarget,
			Message:  fmt.Sprintf("drift gate could not run %q: %v", bin, runErr),
			Remedy:   "install smt (or set migration.drift_gate.smt_binary to its path), or skip with --skip-preflight drift.gate",
		}}
	case code == 0:
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    driftGateCheck,
			Side:     driver.PreFlightSideTarget,
			Message:  "smt drift: target schema in sync with source",
		}}
	case code == smtDriftDetectedExit:
		msg := "smt drift: target schema has drifted from the source"
		if tail := outputTail(output); tail != "" {
			msg += "\n" + tail
		}
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    driftGateCheck,
			Side:     driver.PreFlightSideTarget,
			Message:  msg,
			Remedy:   "reconcile the target with `smt sync` (review the plan first), or skip with --skip-preflight drift.gate to transfer anyway",
		}}
	default:
		// Any other exit is an execution failure (smt: 1 config, 2
		// connection, ...). Fail closed: an opted-in gate that can't run
		// must not silently pass.
		msg := fmt.Sprintf("smt drift failed (exit %d) — drift state unknown", code)
		if tail := outputTail(output); tail != "" {
			msg += "\n" + tail
		}
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    driftGateCheck,
			Side:     driver.PreFlightSideTarget,
			Message:  msg,
			Remedy:   "fix the smt invocation (check migration.drift_gate.smt_config / smt_profile and connectivity), or skip with --skip-preflight drift.gate",
		}}
	}
}

// outputTail returns the last few non-empty lines of the child's combined
// output, size-bounded, for embedding in a finding message.
func outputTail(output string) string {
	const maxLines, maxBytes = 6, 500
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var keep []string
	for i := len(lines) - 1; i >= 0 && len(keep) < maxLines; i-- {
		if s := strings.TrimRight(lines[i], " \t\r"); s != "" {
			keep = append([]string{s}, keep...)
		}
	}
	tail := strings.Join(keep, "\n")
	if len(tail) > maxBytes {
		tail = tail[len(tail)-maxBytes:]
	}
	return tail
}
