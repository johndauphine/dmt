package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
)

// preFlightSkipAll is the sentinel value that disables every check.
// Recognized in MigrationConfig.SkipPreflight or the --skip-preflight flag.
const preFlightSkipAll = "all"

// preFlightResult bundles findings + the decision for the caller. The
// orchestrator's Run() / Resume() turn an Aborted result into a
// PreFlightError; status / preflight subcommand uses the findings list
// for display without aborting.
type preFlightResult struct {
	Findings []driver.PreFlightFinding
	Aborted  bool // true when at least one un-skipped error-severity finding exists
}

// PreFlightError is returned when one or more preflight checks failed
// with severity error and the operator hasn't skipped them. It carries
// the structured findings for downstream rendering (CLI text, JSON
// output, future audit-log) and self-classifies its exit code as
// ConfigError — the environment isn't configured the way the migration
// needs it to be.
type PreFlightError struct {
	Findings []driver.PreFlightFinding
}

func (e *PreFlightError) Error() string {
	if len(e.Findings) == 0 {
		return "preflight failed"
	}
	var sb strings.Builder
	sb.WriteString("preflight failed:")
	for _, f := range e.Findings {
		if f.Severity != driver.SeverityError {
			continue
		}
		fmt.Fprintf(&sb, "\n  [%s] %s (%s): %s", f.Side, f.Check, f.Severity, f.Message)
		if f.Remedy != "" {
			fmt.Fprintf(&sb, "\n    remedy: %s", f.Remedy)
		}
	}
	return sb.String()
}

// ExitCode classifies preflight failures as ConfigError. The migration
// can't proceed because the environment doesn't meet declared assumptions
// — that's the same shape as a malformed YAML: non-recoverable, operator
// must fix something before retrying.
func (e *PreFlightError) ExitCode() int { return exitcodes.ConfigError }

// runPreFlight invokes the source and target drivers' PreFlight methods,
// applies the operator's skip list, logs each finding, and returns an
// error when any un-skipped finding has severity error. Empty findings
// means everything's clean; a non-aborted return with warn/info findings
// is fine — they're logged but don't block.
func (o *Orchestrator) runPreFlight(ctx context.Context) error {
	skipSet := preFlightSkipSet(o.config.Migration.SkipPreflight)
	if skipSet[preFlightSkipAll] {
		logging.Debug("Preflight: skipped (--skip-preflight=all)")
		return nil
	}

	result := o.collectPreFlightFindings(ctx)
	result = applyPreFlightSkips(result, skipSet)
	result = o.appendDriftGateFindings(ctx, result, skipSet)
	logPreFlightFindings(result.Findings)

	if result.Aborted {
		return &PreFlightError{Findings: result.Findings}
	}
	return nil
}

// appendDriftGateFindings runs the configured drift gate (#575) and merges
// its findings. The gate shells out to `smt drift`, which can take minutes —
// so unlike the cheap engine checks it is run-only-if-needed: never when the
// section is absent, never when skipped (skip means don't run, not
// run-then-downgrade), and never when the cheap checks already aborted the
// run. Shared by runPreFlight (the Run/Resume/DryRun gate) and HealthCheck
// (the advisory `dmt preflight` display).
func (o *Orchestrator) appendDriftGateFindings(ctx context.Context, r preFlightResult, skipSet map[string]bool) preFlightResult {
	gate := o.config.Migration.DriftGate
	if gate == nil || r.Aborted {
		return r
	}
	if skipSet[preFlightSkipAll] || shouldSkip(driftGateCheck, skipSet) {
		logging.Debug("Preflight: drift gate skipped (--skip-preflight)")
		return r
	}
	r.Findings = append(r.Findings, runDriftGate(ctx, gate)...)
	r.Aborted = containsAbortingFinding(r.Findings)
	return r
}

// collectPreFlightFindings runs source + target preflight in sequence
// (not parallel — they're cheap and serial gives stable log ordering)
// and gathers findings. Pool-level information (max connections etc.)
// is pulled from the config so checks see the same values the
// migration will use.
func (o *Orchestrator) collectPreFlightFindings(ctx context.Context) preFlightResult {
	var findings []driver.PreFlightFinding

	if o.sourcePool != nil {
		srcDrv, err := driver.Get(o.config.Source.Type)
		if err == nil && srcDrv != nil {
			req := driver.PreFlightRequest{
				Side:    driver.PreFlightSideSource,
				Schema:  o.config.Source.Schema,
				Workers: o.config.Migration.Workers,
			}
			findings = append(findings, srcDrv.PreFlight(ctx, o.sourcePool.DB(), req)...)
		} else if err != nil {
			findings = append(findings, driver.PreFlightFinding{
				Severity: driver.SeverityError,
				Check:    "driver.lookup",
				Side:     driver.PreFlightSideSource,
				Message:  fmt.Sprintf("could not resolve source driver %q: %v", o.config.Source.Type, err),
				Remedy:   "set source.type to one of: mssql, postgres, mysql",
			})
		}
	}

	if o.targetPool != nil {
		tgtDrv, err := driver.Get(o.config.Target.Type)
		if err == nil && tgtDrv != nil {
			req := driver.PreFlightRequest{
				Side:           driver.PreFlightSideTarget,
				Schema:         o.config.Target.Schema,
				TargetMode:     o.config.Migration.TargetMode,
				Workers:        o.config.Migration.Workers,
				EstimatedBytes: o.estimatedTargetBytes(),
				ConfirmBackup:  o.config.Migration.ConfirmBackup,
			}
			findings = append(findings, tgtDrv.PreFlight(ctx, o.targetPool.DB(), req)...)
		} else if err != nil {
			findings = append(findings, driver.PreFlightFinding{
				Severity: driver.SeverityError,
				Check:    "driver.lookup",
				Side:     driver.PreFlightSideTarget,
				Message:  fmt.Sprintf("could not resolve target driver %q: %v", o.config.Target.Type, err),
				Remedy:   "set target.type to one of: mssql, postgres, mysql",
			})
		}
	}

	// Stable order: by side, then check name. Keeps log output and tests
	// deterministic across the parallel-ish per-driver calls.
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Side != findings[j].Side {
			return findings[i].Side < findings[j].Side
		}
		return findings[i].Check < findings[j].Check
	})

	return preFlightResult{Findings: findings, Aborted: containsAbortingFinding(findings)}
}

// estimatedTargetBytes sums known source-table sizes so the target-side
// disk-space check has a meaningful number. Preflight runs before
// ExtractSchema in Run(), so o.tables is normally empty here; we return 0
// in that case and the driver-side check emits an info-only finding rather
// than guessing. (Resume() already has o.tables populated and the check
// shows actionable numbers.)
func (o *Orchestrator) estimatedTargetBytes() int64 {
	if len(o.tables) == 0 {
		return 0
	}
	var total int64
	for _, t := range o.tables {
		total += t.RowCount * t.EstimatedRowSize
	}
	return total
}

// applyPreFlightSkips downgrades skipped error-severity findings to info
// (so they still show in logs as "we skipped this") and recomputes the
// Aborted flag. Skip names match Finding.Check exactly OR by dotted-prefix
// — "privileges" matches every "privileges.*" check. The literal "all"
// downgrades every error regardless of check name; used by `dmt preflight`
// to show findings without enforcing the gate (`dmt run` short-circuits
// the whole phase 0 on "all" so the checks never run).
func applyPreFlightSkips(r preFlightResult, skip map[string]bool) preFlightResult {
	if len(skip) == 0 {
		return r
	}
	skipAll := skip[preFlightSkipAll]
	for i := range r.Findings {
		f := &r.Findings[i]
		if !skipAll && !shouldSkip(f.Check, skip) {
			continue
		}
		if f.Severity == driver.SeverityError {
			f.Severity = driver.SeverityInfo
			f.Message = "(skipped via --skip-preflight) " + f.Message
		}
	}
	r.Aborted = containsAbortingFinding(r.Findings)
	return r
}

// shouldSkip returns true when check is in skip exactly, or any prefix
// segment of check is in skip. Example: "privileges.create_table" is
// skipped by either {"privileges.create_table"} or {"privileges"}.
func shouldSkip(check string, skip map[string]bool) bool {
	if skip[check] {
		return true
	}
	// Walk dotted prefixes — "privileges.schema_create" → "privileges".
	for i := strings.LastIndexByte(check, '.'); i > 0; i = strings.LastIndexByte(check[:i], '.') {
		if skip[check[:i]] {
			return true
		}
	}
	return false
}

func containsAbortingFinding(findings []driver.PreFlightFinding) bool {
	for _, f := range findings {
		if f.Severity == driver.SeverityError {
			return true
		}
	}
	return false
}

// preFlightSkipSet normalizes a config/CLI skip list into a lookup set.
// Accepts comma-joined strings ("a,b") and individual entries; trims
// whitespace; lowercases for case-insensitive matching against the
// driver-side dotted check names (which are always lowercase).
func preFlightSkipSet(raw []string) map[string]bool {
	out := make(map[string]bool, len(raw))
	for _, s := range raw {
		for _, tok := range strings.Split(s, ",") {
			tok = strings.ToLower(strings.TrimSpace(tok))
			if tok != "" {
				out[tok] = true
			}
		}
	}
	return out
}

// logPreFlightFindings emits one log line per finding at the matching
// level. Errors go to Error, warns to Warn, info to Debug — we don't want
// info-level findings spamming default-verbosity logs but they still need
// to be present for the operator who runs --verbosity=debug.
func logPreFlightFindings(findings []driver.PreFlightFinding) {
	for _, f := range findings {
		head := fmt.Sprintf("preflight %s/%s: %s", f.Side, f.Check, f.Message)
		switch f.Severity {
		case driver.SeverityError:
			logging.Error("%s", head)
			if f.Remedy != "" {
				logging.Error("  remedy: %s", f.Remedy)
			}
		case driver.SeverityWarn:
			logging.Warn("%s", head)
			if f.Remedy != "" {
				logging.Warn("  remedy: %s", f.Remedy)
			}
		default:
			logging.Debug("%s", head)
		}
	}
}
