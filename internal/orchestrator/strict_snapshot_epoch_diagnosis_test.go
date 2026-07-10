package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
)

func TestDiagnoseSnapshotEpochErrorUsesSourceCatalog(t *testing.T) {
	var received *driver.ErrorDiagnosis
	driver.SetDiagnosisHandler(func(diag *driver.ErrorDiagnosis) { received = diag })
	t.Cleanup(func() { driver.SetDiagnosisHandler(nil) })

	cfg := &config.Config{
		Source: config.SourceConfig{Type: "mssql"},
		Target: config.TargetConfig{Type: "postgres"},
		Migration: config.MigrationConfig{
			TargetMode: "drop_recreate",
		},
	}
	diagnoseSnapshotEpochError(context.Background(), cfg, errors.New("creating SQL Server strict snapshot [dmt_strict_abc12345]: mssql: CREATE DATABASE permission denied in database 'master'"))

	if received == nil {
		t.Fatal("snapshot epoch error did not emit a diagnosis")
	}
	combined := received.Cause + " " + strings.Join(received.Suggestions, " ")
	if !strings.Contains(combined, "CREATE ANY DATABASE") || strings.Contains(received.Cause, "No automatic diagnosis") {
		t.Fatalf("snapshot diagnosis = %+v, want SQL Server database-snapshot catalog entry", received)
	}
}

func TestBeginMigrationSnapshotEpochLeavesPostgresAcquisitionToTransferRunner(t *testing.T) {
	orch := &Orchestrator{config: &config.Config{
		Source: config.SourceConfig{Type: "postgres"},
		Migration: config.MigrationConfig{
			StrictConsistency:      true,
			StrictConsistencyScope: "migration",
		},
	}}
	epoch, err := orch.beginMigrationSnapshotEpoch(context.Background(), "pg-run", false)
	if err != nil || epoch != nil {
		t.Fatalf("early PostgreSQL epoch = (%v, %v), want nil epoch for TransferRunner acquisition", epoch, err)
	}
}
