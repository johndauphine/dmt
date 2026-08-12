package shared

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

const defaultPreFlightPoolMargin = 5

// PreFlightCheck is one composable preflight probe.
type PreFlightCheck func(context.Context, *sql.DB, driver.PreFlightRequest) []driver.PreFlightFinding

// PreFlightRunConfig controls shared preflight runner behavior that still
// needs driver-owned wording.
type PreFlightRunConfig struct {
	NilDatabaseMessage string
	NilDatabaseRemedy  string
}

// RunPreFlight runs checks in order after applying the common nil-handle guard.
func RunPreFlight(
	ctx context.Context,
	db *sql.DB,
	req driver.PreFlightRequest,
	cfg PreFlightRunConfig,
	checks ...PreFlightCheck,
) []driver.PreFlightFinding {
	if db == nil {
		message := cfg.NilDatabaseMessage
		if message == "" {
			message = "no database handle supplied to preflight"
		}
		return []driver.PreFlightFinding{ErrorFinding(
			"connection.handle",
			req.Side,
			message,
			cfg.NilDatabaseRemedy,
		)}
	}

	var findings []driver.PreFlightFinding
	for _, check := range checks {
		if check == nil {
			continue
		}
		findings = append(findings, check(ctx, db, req)...)
	}
	return findings
}

// Finding builds a preflight finding while keeping concrete probes readable.
func Finding(
	severity driver.PreFlightSeverity,
	check string,
	side driver.PreFlightSide,
	message string,
	remedy string,
) driver.PreFlightFinding {
	return driver.PreFlightFinding{
		Severity: severity,
		Check:    check,
		Side:     side,
		Message:  message,
		Remedy:   remedy,
	}
}

// ErrorFinding builds an error-severity preflight finding.
func ErrorFinding(check string, side driver.PreFlightSide, message string, remedy string) driver.PreFlightFinding {
	return Finding(driver.SeverityError, check, side, message, remedy)
}

// WarnFinding builds a warning-severity preflight finding.
func WarnFinding(check string, side driver.PreFlightSide, message string, remedy string) driver.PreFlightFinding {
	return Finding(driver.SeverityWarn, check, side, message, remedy)
}

// InfoFinding builds an info-severity preflight finding.
func InfoFinding(check string, side driver.PreFlightSide, message string, remedy string) driver.PreFlightFinding {
	return Finding(driver.SeverityInfo, check, side, message, remedy)
}

// SingleFinding converts optional pointer findings into the slice shape used
// by PreFlightCheck.
func SingleFinding(finding *driver.PreFlightFinding) []driver.PreFlightFinding {
	if finding == nil {
		return nil
	}
	return []driver.PreFlightFinding{*finding}
}

// AppendNonNilFinding appends an optional pointer finding.
func AppendNonNilFinding(
	findings []driver.PreFlightFinding,
	finding *driver.PreFlightFinding,
) []driver.PreFlightFinding {
	if finding == nil {
		return findings
	}
	return append(findings, *finding)
}

// ConnectionCheckConfig keeps driver-owned connection wording outside the
// generic ping helper.
type ConnectionCheckConfig struct {
	Query         string
	Check         string
	MessagePrefix string
	Remedy        string
}

// CheckConnection runs a simple scalar query and returns a finding on failure.
func CheckConnection(
	ctx context.Context,
	db *sql.DB,
	side driver.PreFlightSide,
	cfg ConnectionCheckConfig,
) *driver.PreFlightFinding {
	if cfg.Query == "" {
		cfg.Query = "SELECT 1"
	}
	if cfg.Check == "" {
		cfg.Check = "connection.ping"
	}
	if cfg.MessagePrefix == "" {
		cfg.MessagePrefix = "connection ping failed"
	}

	if db == nil {
		finding := ErrorFinding(
			cfg.Check,
			side,
			fmt.Sprintf("%s: database handle is nil", cfg.MessagePrefix),
			cfg.Remedy,
		)
		return &finding
	}

	var one int
	if err := db.QueryRowContext(ctx, cfg.Query).Scan(&one); err != nil {
		finding := ErrorFinding(
			cfg.Check,
			side,
			fmt.Sprintf("%s: %v", cfg.MessagePrefix, err),
			cfg.Remedy,
		)
		return &finding
	}
	return nil
}

// BackupAcknowledgmentRequired returns true when drop_recreate must be
// explicitly acknowledged before a target-side destructive migration.
//
// It does not fire when a resume owns the target (ResumeOwnsTarget): the tables
// were (re)created by the run being resumed, so their contents are that run's
// own output. Re-gating on it every recovery makes the safety check cry wolf —
// and worse, points at a --confirm-backup flag that `resume` doesn't define
// (#623). Resuming is the acknowledgment. The orchestrator scopes
// ResumeOwnsTarget narrowly so a run that never reached transfer, or a drifted
// --force-resume, still faces the gate.
func BackupAcknowledgmentRequired(req driver.PreFlightRequest) bool {
	return req.Side == driver.PreFlightSideTarget &&
		strings.ToLower(strings.TrimSpace(req.TargetMode)) == "drop_recreate" &&
		!req.ConfirmBackup &&
		!req.ResumeOwnsTarget
}

// TargetModeOrDefault returns a trimmed, lower-case target mode.
func TargetModeOrDefault(mode string, fallback string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return fallback
	}
	return mode
}

// PoolHeadroomFinding returns the shared insufficient-headroom finding after
// a concrete driver has read max/current connection counts.
func PoolHeadroomFinding(
	req driver.PreFlightRequest,
	maxConnections int64,
	currentConnections int64,
	remedy string,
) []driver.PreFlightFinding {
	needed := int64(req.Workers) + int64(defaultPreFlightPoolMargin)
	if needed <= 0 {
		return nil
	}

	available := maxConnections - currentConnections
	if available >= needed {
		return nil
	}

	return []driver.PreFlightFinding{ErrorFinding(
		"pool.headroom",
		req.Side,
		fmt.Sprintf(
			"only %d of %d connections free; need %d (workers=%d + %d margin)",
			available,
			maxConnections,
			needed,
			req.Workers,
			defaultPreFlightPoolMargin,
		),
		remedy,
	)}
}
