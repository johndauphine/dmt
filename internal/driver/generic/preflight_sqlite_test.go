package generic

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

// TestSqliteCheckBackupAckResumeSkipsPopulatedTarget reproduces #623: a
// drop_recreate resume of a run that already transferred rows finds that
// partial data in the target (the interrupted run's own output). On `run` that
// trips the backup-acknowledgment gate; on a resume-with-progress it must not —
// resuming is the acknowledgment, and `resume` has no --confirm-backup flag to
// satisfy the gate.
func TestSqliteCheckBackupAckResumeSkipsPopulatedTarget(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE big_events (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO big_events (v) VALUES ('a'), ('b'), ('c')`); err != nil {
		t.Fatalf("seeding rows: %v", err)
	}

	base := driver.PreFlightRequest{
		Side:       driver.PreFlightSideTarget,
		TargetMode: "drop_recreate",
	}

	// `run` (IsResume=false, unconfirmed): the gate fires with the
	// --confirm-backup remedy that only `run` defines.
	runReq := base
	findings := sqliteCheckBackupAck(ctx, db, runReq)
	if len(findings) != 1 {
		t.Fatalf("run path: got %d findings, want 1: %+v", len(findings), findings)
	}
	if findings[0].Check != "backup.acknowledgment" || findings[0].Severity != driver.SeverityError {
		t.Fatalf("run path: unexpected finding metadata: %+v", findings[0])
	}
	if !strings.Contains(findings[0].Remedy, "--confirm-backup") {
		t.Fatalf("run path: remedy should name --confirm-backup: %q", findings[0].Remedy)
	}
	// #623: the remedy must also name the resume-side hatch, so a resume that
	// legitimately hits the gate (legacy config, drift) is never dead-ended on
	// a flag `resume` lacks.
	if !strings.Contains(findings[0].Remedy, "--skip-preflight backup") {
		t.Fatalf("run path: remedy should also name --skip-preflight backup for resume: %q", findings[0].Remedy)
	}

	// resume that owns the target: the same populated target must pass clean.
	resumeReq := base
	resumeReq.ResumeOwnsTarget = true
	if findings := sqliteCheckBackupAck(ctx, db, resumeReq); len(findings) != 0 {
		t.Fatalf("resume path: got %d findings, want 0 (resume is the acknowledgment): %+v", len(findings), findings)
	}
}
