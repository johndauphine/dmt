package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/progress"
	"github.com/johndauphine/dmt/v5/internal/source"
	"github.com/johndauphine/dmt/v5/internal/transfer"
)

// firstPeriodicFailureSaver fails the first (asynchronous) checkpoint save and
// delegates the later synchronous final save to durable checkpoint state.
type firstPeriodicFailureSaver struct {
	inner transfer.ProgressSaver
	err   error

	mu    sync.Mutex
	calls int
}

func (s *firstPeriodicFailureSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	s.mu.Lock()
	s.calls++
	fail := s.calls == 1
	s.mu.Unlock()
	if fail {
		return s.err
	}
	return s.inner.SaveProgress(taskID, tableName, partitionID, lastPK, rowsDone, rowsTotal, rangeState)
}

func (s *firstPeriodicFailureSaver) GetProgress(taskID int64) (any, int64, string, error) {
	return s.inner.GetProgress(taskID)
}

func TestPeriodicCheckpointDegradationCompletesTaskAndResumeSkipsIt(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "source.db")
	tgtPath := filepath.Join(t.TempDir(), "target.db")
	cfg := &config.Config{
		Source: config.SourceConfig{Type: "sqlite", Database: srcPath},
		Target: config.TargetConfig{Type: "sqlite", Database: tgtPath, ChunkSize: 1},
		Migration: config.MigrationConfig{
			ChunkSize:            1,
			TargetMode:           "drop_recreate",
			Workers:              1,
			ParallelReaders:      1,
			WriteAheadWriters:    1,
			ReadAheadBuffers:     1,
			CheckpointFrequency:  1,
			MaxSourceConnections: 2,
			MaxTargetConnections: 1,
		},
	}
	typeMapper, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatalf("GetTypeMapper: %v", err)
	}
	srcPool, err := pool.NewSourcePool(&cfg.Source, cfg.Migration.MaxSourceConnections)
	if err != nil {
		t.Fatalf("NewSourcePool: %v", err)
	}
	defer srcPool.Close()
	tgtPool, err := pool.NewTargetPool(&cfg.Target, cfg.Migration.MaxTargetConnections, "sqlite", typeMapper)
	if err != nil {
		t.Fatalf("NewTargetPool: %v", err)
	}
	defer tgtPool.Close()

	for _, setup := range []struct {
		name string
		db   *sql.DB
	}{
		{name: "source", db: srcPool.DB()},
		{name: "target", db: tgtPool.DB()},
	} {
		if _, err := setup.db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
			t.Fatalf("create %s table: %v", setup.name, err)
		}
	}
	if _, err := srcPool.DB().ExecContext(ctx, `INSERT INTO items VALUES (1, 'a')`); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()
	const runID = "periodic-degradation"
	if err := state.CreateRun(runID, "", "", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	table := source.Table{
		Name:       "items",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		RowCount: 1,
	}
	table.PopulatePKColumns()
	identity := checkpoint.TransferTaskIdentity{Table: table.Name}
	taskID, err := state.CreateTransferTask(runID, identity)
	if err != nil {
		t.Fatalf("CreateTransferTask: %v", err)
	}
	periodicErr := errors.New("transient checkpoint store error")
	job := transfer.Job{
		Table:  table,
		TaskID: taskID,
		Saver: &firstPeriodicFailureSaver{
			inner: checkpoint.NewProgressSaver(state),
			err:   periodicErr,
		},
	}
	var auditTypes []string
	targetMode := NewTargetModeStrategy("drop_recreate", tgtPool, "", false, false, false, "sqlite", "sqlite")
	runner := NewTransferRunner(srcPool, tgtPool, state, cfg, progress.New(), nil, targetMode)
	runner.auditEvent = func(typeName string, _ map[string]any) {
		auditTypes = append(auditTypes, typeName)
	}
	result, err := runner.Run(ctx, runID, &BuildResult{
		Jobs:           []transfer.Job{job},
		TableJobCounts: map[string]int{table.Name: 1},
	}, []source.Table{table}, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.TableFailures) != 0 {
		t.Fatalf("table failures = %+v, want none after successful final checkpoint", result.TableFailures)
	}
	if len(auditTypes) != 1 || auditTypes[0] != "checkpoint_periodic_save_degraded" {
		t.Fatalf("audit events = %v, want checkpoint_periodic_save_degraded", auditTypes)
	}

	completed, err := state.GetCompletedTables(runID)
	if err != nil {
		t.Fatalf("GetCompletedTables: %v", err)
	}
	scheduled, skipped := selectResumeTables(
		[]source.Table{table},
		completed,
		func(t source.Table) string {
			return checkpoint.TransferTaskKeyForBackend(state, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name})
		},
		func(source.Table) (int64, error) { return 1, nil },
	)
	if len(scheduled) != 0 || len(skipped) != 1 || skipped[0] != table.Name {
		t.Fatalf("resume selection = scheduled:%v skipped:%v, want no scheduled jobs and items skipped", scheduled, skipped)
	}
}
