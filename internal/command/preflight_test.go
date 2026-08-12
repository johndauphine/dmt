package command

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/orchestrator"
)

// #440: the shared renderer must carry the readiness facts — both the
// CLI and /preflight print exactly this string. The blocking finding's
// [error] tag is what distinguishes it from warnings in both surfaces.
func TestFormatHealthCheckResult(t *testing.T) {
	out := FormatHealthCheckResult(&orchestrator.HealthCheckResult{
		SourceDBType:     "mssql",
		SourceConnected:  true,
		SourceLatencyMs:  12,
		SourceTableCount: 9,
		TargetDBType:     "postgres",
		TargetConnected:  false,
		TargetError:      "connection refused",
		Healthy:          false,
		PreFlightFindings: []driver.PreFlightFinding{
			{
				Severity: driver.SeverityError,
				Check:    "privileges.create_table",
				Side:     driver.PreFlightSideTarget,
				Message:  "missing CREATE TABLE privilege",
				Remedy:   "GRANT CREATE ON SCHEMA public",
			},
			{
				Severity: driver.SeverityWarn,
				Check:    "version.compatibility",
				Side:     driver.PreFlightSideSource,
				Message:  "old server version",
			},
		},
		AIPreflightReview: &aicopilot.PreflightReview{
			Status:    "ok",
			Readiness: "not_ready",
			Summary:   "target lacks DDL privileges",
		},
	})

	for _, want := range []string{
		"Source (mssql): OK (12ms)",
		"Tables: 9",
		"Target (postgres): FAILED",
		"Error: connection refused",
		"[error] target/privileges.create_table: missing CREATE TABLE privilege",
		"remedy: GRANT CREATE ON SCHEMA public",
		"[warn] source/version.compatibility: old server version",
		"AI readiness review:",
		"(readiness: NOT_READY)",
		"Summary: target lacks DDL privileges",
		"Overall: UNHEALTHY",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// Provider-unavailable AI review must render its error without hiding
// the deterministic results around it.
func TestFormatHealthCheckResultAIError(t *testing.T) {
	out := FormatHealthCheckResult(&orchestrator.HealthCheckResult{
		SourceDBType: "sqlite", SourceConnected: true,
		TargetDBType: "sqlite", TargetConnected: true,
		Healthy: true,
		AIPreflightReview: &aicopilot.PreflightReview{
			Status: "error",
			Error:  "no AI provider configured",
		},
	})
	for _, want := range []string{"Status: error", "Error: no AI provider configured", "Overall: HEALTHY"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
