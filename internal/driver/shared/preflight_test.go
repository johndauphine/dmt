package shared

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"

	_ "modernc.org/sqlite"
)

func TestRunPreFlightNilDatabaseHandle(t *testing.T) {
	findings := RunPreFlight(
		context.Background(),
		nil,
		driver.PreFlightRequest{Side: driver.PreFlightSideTarget},
		PreFlightRunConfig{
			NilDatabaseMessage: "custom nil db",
			NilDatabaseRemedy:  "custom remedy",
		},
		func(context.Context, *sql.DB, driver.PreFlightRequest) []driver.PreFlightFinding {
			t.Fatal("check should not run with nil db")
			return nil
		},
	)

	if len(findings) != 1 {
		t.Fatalf("RunPreFlight nil db produced %d findings, want 1", len(findings))
	}
	got := findings[0]
	if got.Severity != driver.SeverityError ||
		got.Check != "connection.handle" ||
		got.Side != driver.PreFlightSideTarget ||
		got.Message != "custom nil db" ||
		got.Remedy != "custom remedy" {
		t.Fatalf("RunPreFlight nil db finding = %+v", got)
	}
}

func TestRunPreFlightStableOrder(t *testing.T) {
	db := openSharedSQLite(t)
	ctx := context.Background()
	req := driver.PreFlightRequest{Side: driver.PreFlightSideSource}

	findings := RunPreFlight(
		ctx,
		db,
		req,
		PreFlightRunConfig{},
		func(context.Context, *sql.DB, driver.PreFlightRequest) []driver.PreFlightFinding {
			return []driver.PreFlightFinding{InfoFinding("first", req.Side, "one", "")}
		},
		nil,
		func(context.Context, *sql.DB, driver.PreFlightRequest) []driver.PreFlightFinding {
			return []driver.PreFlightFinding{WarnFinding("second", req.Side, "two", "")}
		},
	)

	if len(findings) != 2 {
		t.Fatalf("RunPreFlight produced %d findings, want 2", len(findings))
	}
	if findings[0].Check != "first" || findings[1].Check != "second" {
		t.Fatalf("RunPreFlight order = [%s, %s], want [first, second]", findings[0].Check, findings[1].Check)
	}
}

func TestCheckConnection(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	if finding := CheckConnection(ctx, db, driver.PreFlightSideSource, ConnectionCheckConfig{}); finding != nil {
		t.Fatalf("CheckConnection open db returned finding: %+v", finding)
	}

	nilFinding := CheckConnection(ctx, nil, driver.PreFlightSideSource, ConnectionCheckConfig{})
	if nilFinding == nil {
		t.Fatal("CheckConnection nil db returned nil finding")
	}
	if nilFinding.Message != "connection ping failed: database handle is nil" {
		t.Fatalf("CheckConnection nil db message = %q", nilFinding.Message)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("closing sqlite db: %v", err)
	}

	finding := CheckConnection(ctx, db, driver.PreFlightSideSource, ConnectionCheckConfig{
		Check:         "connection.alive",
		MessagePrefix: "could not query database",
		Remedy:        "verify database access",
	})
	if finding == nil {
		t.Fatal("CheckConnection closed db returned nil finding")
	}
	if finding.Check != "connection.alive" {
		t.Fatalf("CheckConnection check = %q, want connection.alive", finding.Check)
	}
	if !strings.HasPrefix(finding.Message, "could not query database: ") {
		t.Fatalf("CheckConnection message = %q", finding.Message)
	}
	if finding.Remedy != "verify database access" {
		t.Fatalf("CheckConnection remedy = %q", finding.Remedy)
	}
}

func TestBackupAcknowledgmentRequired(t *testing.T) {
	tests := []struct {
		name string
		req  driver.PreFlightRequest
		want bool
	}{
		{
			name: "target drop recreate without confirmation",
			req: driver.PreFlightRequest{
				Side:       driver.PreFlightSideTarget,
				TargetMode: " drop_recreate ",
			},
			want: true,
		},
		{
			name: "source side skipped",
			req: driver.PreFlightRequest{
				Side:       driver.PreFlightSideSource,
				TargetMode: "drop_recreate",
			},
		},
		{
			name: "upsert skipped",
			req: driver.PreFlightRequest{
				Side:       driver.PreFlightSideTarget,
				TargetMode: "upsert",
			},
		},
		{
			name: "confirmed skipped",
			req: driver.PreFlightRequest{
				Side:          driver.PreFlightSideTarget,
				TargetMode:    "drop_recreate",
				ConfirmBackup: true,
			},
		},
		{
			// #623: a resume that owns the target (its run created the
			// tables) must not fire the gate — that data is the run's own
			// output, and `resume` has no --confirm-backup flag to satisfy it.
			name: "resume that owns target skipped even without confirmation",
			req: driver.PreFlightRequest{
				Side:             driver.PreFlightSideTarget,
				TargetMode:       "drop_recreate",
				ResumeOwnsTarget: true,
			},
		},
		{
			// A resume that doesn't own the target (killed before it created
			// tables, or a drifted --force-resume) has acknowledged nothing —
			// the gate must still fire so an unconfirmed drop_recreate can't
			// destroy pre-existing data.
			name: "resume that doesn't own target still gated",
			req: driver.PreFlightRequest{
				Side:       driver.PreFlightSideTarget,
				TargetMode: "drop_recreate",
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BackupAcknowledgmentRequired(tc.req)
			if got != tc.want {
				t.Fatalf("BackupAcknowledgmentRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTargetModeOrDefault(t *testing.T) {
	if got := TargetModeOrDefault(" UPSERT ", "drop_recreate"); got != "upsert" {
		t.Fatalf("TargetModeOrDefault trimmed value = %q, want upsert", got)
	}
	if got := TargetModeOrDefault("", "drop_recreate"); got != "drop_recreate" {
		t.Fatalf("TargetModeOrDefault fallback = %q, want drop_recreate", got)
	}
}

func TestPoolHeadroomFinding(t *testing.T) {
	req := driver.PreFlightRequest{
		Side:    driver.PreFlightSideTarget,
		Workers: 4,
	}

	if findings := PoolHeadroomFinding(req, 20, 10, "reduce workers"); len(findings) != 0 {
		t.Fatalf("PoolHeadroomFinding enough capacity = %+v, want none", findings)
	}

	findings := PoolHeadroomFinding(req, 12, 5, "reduce workers")
	if len(findings) != 1 {
		t.Fatalf("PoolHeadroomFinding low capacity produced %d findings, want 1", len(findings))
	}
	got := findings[0]
	if got.Severity != driver.SeverityError || got.Check != "pool.headroom" || got.Side != driver.PreFlightSideTarget {
		t.Fatalf("PoolHeadroomFinding metadata = %+v", got)
	}
	wantMessage := "only 7 of 12 connections free; need 9 (workers=4 + 5 margin)"
	if got.Message != wantMessage {
		t.Fatalf("PoolHeadroomFinding message = %q, want %q", got.Message, wantMessage)
	}
	if got.Remedy != "reduce workers" {
		t.Fatalf("PoolHeadroomFinding remedy = %q", got.Remedy)
	}
}

func TestPoolHeadroomFindingSkipsUnspecifiedWorkers(t *testing.T) {
	req := driver.PreFlightRequest{Side: driver.PreFlightSideTarget, Workers: -5}
	if findings := PoolHeadroomFinding(req, 0, 100, "reduce workers"); len(findings) != 0 {
		t.Fatalf("PoolHeadroomFinding workers=-5 = %+v, want none", findings)
	}
}
