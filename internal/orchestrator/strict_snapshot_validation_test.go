package orchestrator

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"

	_ "modernc.org/sqlite"
)

// strictSnapshotValidationSource and target override only the count APIs used
// by validation. Embedding keeps these unit tests focused on the validation
// decision rather than a full migration driver setup.
type strictSnapshotValidationSource struct {
	pool.SourcePool
	db *sql.DB
}

func (p *strictSnapshotValidationSource) DB() *sql.DB    { return p.db }
func (p *strictSnapshotValidationSource) DBType() string { return "sqlite" }
func (p *strictSnapshotValidationSource) GetRowCountExact(ctx context.Context, _, table string, _ bool) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	return count, err
}
func (p *strictSnapshotValidationSource) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	return p.GetRowCountExact(ctx, schema, table, false)
}

type strictSnapshotValidationTarget struct {
	pool.TargetPool
	db *sql.DB
}

func (p *strictSnapshotValidationTarget) DB() *sql.DB    { return p.db }
func (p *strictSnapshotValidationTarget) DBType() string { return "sqlite" }
func (p *strictSnapshotValidationTarget) GetRowCountExact(ctx context.Context, _, table string, _ bool) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	return count, err
}
func (p *strictSnapshotValidationTarget) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	return p.GetRowCountExact(ctx, schema, table, false)
}

func TestValidateStrictSnapshotUsesPinnedCountAndReportsLiveDrift(t *testing.T) {
	orch := newStrictSnapshotValidationOrchestrator(t, 6, 5, true, int64Pointer(5), "")
	result, err := orch.ValidateDetailed(context.Background())
	if err != nil {
		t.Fatalf("ValidateDetailed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("validation rows = %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if row.Failed || row.SourceCount != 5 || row.TargetCount != 5 {
		t.Fatalf("snapshot validation row = %+v, want snapshot-target match", row)
	}
	if row.SnapshotRowCount == nil || *row.SnapshotRowCount != 5 ||
		row.LiveSourceCount == nil || *row.LiveSourceCount != 6 ||
		row.LiveSourceDrift == nil || *row.LiveSourceDrift != 1 {
		t.Fatalf("snapshot drift evidence = %+v, want snapshot=5 live=6 drift=+1", row)
	}
}

func TestValidateStrictSnapshotWithoutFullTableCountKeepsLiveValidation(t *testing.T) {
	// An incremental DateFilter transfer has no full-table snapshot count, so
	// it must retain the existing live source/target comparison policy.
	orch := newStrictSnapshotValidationOrchestrator(t, 6, 5, true, nil, "run-664")
	result, err := orch.ValidateDetailed(context.Background())
	if err == nil || !strings.Contains(err.Error(), "validation failed") {
		t.Fatalf("ValidateDetailed error = %v, want live-count validation failure", err)
	}
	row := result.Rows[0]
	if row.SnapshotRowCount != nil || row.SourceCount != 6 || row.TargetCount != 5 {
		t.Fatalf("count-less strict validation row = %+v, want live source=6 target=5", row)
	}
}

func TestValidateNonStrictRowCountPolicyIsUnchanged(t *testing.T) {
	orch := newStrictSnapshotValidationOrchestrator(t, 6, 5, false, nil, "")
	result, err := orch.ValidateDetailed(context.Background())
	if err == nil || !strings.Contains(err.Error(), "validation failed") {
		t.Fatalf("ValidateDetailed error = %v, want live-count validation failure", err)
	}
	row := result.Rows[0]
	if row.SnapshotRowCount != nil || row.SourceCount != 6 || row.TargetCount != 5 {
		t.Fatalf("non-strict validation row = %+v, want source=6 target=5", row)
	}
}

func TestValidateStrictSnapshotKeepsUpsertTargetSupersetPolicy(t *testing.T) {
	orch := newStrictSnapshotValidationOrchestrator(t, 6, 7, true, int64Pointer(5), "run-664")
	orch.config.Migration.TargetMode = "upsert"
	result, err := orch.ValidateDetailed(context.Background())
	if err != nil {
		t.Fatalf("ValidateDetailed: %v", err)
	}
	if got := result.Rows[0]; got.Failed || got.SnapshotRowCount == nil || *got.SnapshotRowCount != 5 || got.TargetCount != 7 {
		t.Fatalf("upsert snapshot validation row = %+v, want allowed target superset", got)
	}
}

func newStrictSnapshotValidationOrchestrator(
	t *testing.T,
	sourceRows, targetRows int,
	strict bool,
	snapshotCount *int64,
	validationRunID string,
) *Orchestrator {
	t.Helper()
	sourceDB := openStrictSnapshotValidationDB(t, sourceRows)
	targetDB := openStrictSnapshotValidationDB(t, targetRows)
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })

	const runID = "run-664"
	if err := state.CreateRun(runID, "", "", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if strict {
		if err := state.SetRunStrictConsistency(runID, true); err != nil {
			t.Fatal(err)
		}
	}
	identity := checkpoint.TransferTaskIdentity{Table: "items"}
	taskID, err := state.CreateTransferTask(runID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if snapshotCount != nil {
		if err := state.SaveStrictSnapshotRowCount(taskID, *snapshotCount); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.MarkTransferTaskComplete(runID, identity); err != nil {
		t.Fatal(err)
	}

	return &Orchestrator{
		config: &config.Config{Migration: config.MigrationConfig{
			StrictConsistency: strict,
			TargetMode:        "drop_recreate",
		}},
		sourcePool:      &strictSnapshotValidationSource{db: sourceDB},
		targetPool:      &strictSnapshotValidationTarget{db: targetDB},
		state:           state,
		tables:          []source.Table{{Name: "items"}},
		validationRunID: validationRunID,
	}
}

func openStrictSnapshotValidationDB(t *testing.T, rows int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= rows; id++ {
		if _, err := db.Exec(`INSERT INTO items (id) VALUES (?)`, id); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
