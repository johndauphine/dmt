package transfer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
)

type oneChunkCheckpointProducer struct{}

func (oneChunkCheckpointProducer) readerCount() int { return 1 }

func (oneChunkCheckpointProducer) produce(ctx context.Context, _ pipelineEnv, out chan<- chunkResult) {
	sendChunkOrCancel(ctx, out, chunkResult{
		rows:     [][]any{{int64(1), "payload"}},
		lastPK:   int64(1),
		readEnd:  time.Now(),
		readerID: 0,
		seq:      0,
	})
}

type scriptedCheckpointSaver struct {
	mu       sync.Mutex
	outcomes []error
	calls    []savedProgress
}

func (s *scriptedCheckpointSaver) SaveProgress(_ int64, _ string, _ *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, savedProgress{lastPK: lastPK, rowsDone: rowsDone, rowsTotal: rowsTotal, rangeState: rangeState})
	call := len(s.calls) - 1
	if call < len(s.outcomes) {
		return s.outcomes[call]
	}
	return nil
}

func (s *scriptedCheckpointSaver) GetProgress(int64) (any, int64, string, error) {
	return nil, 0, "", nil
}

func (s *scriptedCheckpointSaver) saved() []savedProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]savedProgress(nil), s.calls...)
}

type auditRecord struct {
	typeName string
	fields   map[string]any
}

func runCheckpointDegradationPipeline(t *testing.T, saver *scriptedCheckpointSaver, saveFinal func(*scriptedCheckpointSaver) (bool, error)) (*TransferStats, []auditRecord, error) {
	t.Helper()
	table := driver.Table{
		Name:             "items",
		PrimaryKey:       []string{"id"},
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	var audits []auditRecord
	job := Job{
		Table:  table,
		TaskID: 665,
		Saver:  saver,
		AuditEvent: func(typeName string, fields map[string]any) {
			audits = append(audits, auditRecord{typeName: typeName, fields: fields})
		},
	}
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			ParallelReaders:   1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}
	stats, err := runPipeline(context.Background(), pipelineConfig{
		cfg:             cfg,
		job:             job,
		tgtPool:         &keysetRuntimeTargetPool{updated: true},
		prog:            progress.New(),
		producer:        oneChunkCheckpointProducer{},
		targetTableName: "items",
		targetCols:      []string{"id", "payload"},
		colTypes:        []string{"integer", "text"},
		colSRIDs:        []int{0, 0},
		newAckHandler: func(_ tunerCallbacks, periodic ProgressSaver) func(writeAck) ackRelease {
			return func(ack writeAck) ackRelease {
				_ = periodic.SaveProgress(665, "items", nil, ack.lastPK, ack.rows, 1, "")
				return ackRelease{jobs: 1}
			}
		},
		saveFinal: func(_ chunkResult, _ int64) (bool, error) {
			return saveFinal(saver)
		},
	})
	return stats, audits, err
}

func TestRunPipelineFinalCheckpointSupersedesPeriodicSaveFailure(t *testing.T) {
	periodicErr := errors.New("periodic sqlite busy")
	saver := &scriptedCheckpointSaver{outcomes: []error{periodicErr, nil}}
	stats, audits, err := runCheckpointDegradationPipeline(t, saver, func(s *scriptedCheckpointSaver) (bool, error) {
		err := s.SaveProgress(665, "items", nil, int64(1), 1, 1, "")
		return err == nil, err
	})
	if err != nil {
		t.Fatalf("runPipeline = %v, want final checkpoint to supersede periodic failure", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("stats.Rows = %d, want 1", stats.Rows)
	}
	saves := saver.saved()
	if len(saves) != 2 || saves[1].rowsDone != 1 || saves[1].lastPK != int64(1) {
		t.Fatalf("checkpoint saves = %+v, want failed periodic then durable final watermark", saves)
	}
	if len(audits) != 1 || audits[0].typeName != "checkpoint_periodic_save_degraded" {
		t.Fatalf("audit events = %+v, want one checkpoint degradation event", audits)
	}
	if got := audits[0].fields["consecutive_failures"]; got != 1 {
		t.Fatalf("audit consecutive_failures = %v, want 1", got)
	}
	if got := audits[0].fields["table"]; got != "items" {
		t.Fatalf("audit table = %v, want items", got)
	}
}

func TestRunPipelinePeriodicSaveFailureFailsWhenFinalSaveFails(t *testing.T) {
	periodicErr := errors.New("periodic sqlite busy")
	finalErr := errors.New("final disk full")
	saver := &scriptedCheckpointSaver{outcomes: []error{periodicErr, finalErr}}
	_, audits, err := runCheckpointDegradationPipeline(t, saver, func(s *scriptedCheckpointSaver) (bool, error) {
		err := s.SaveProgress(665, "items", nil, int64(1), 1, 1, "")
		return err == nil, err
	})
	if !errors.Is(err, periodicErr) || !errors.Is(err, finalErr) {
		t.Fatalf("runPipeline error = %v, want joined periodic and final errors", err)
	}
	if len(audits) != 0 {
		t.Fatalf("audit events = %+v, want none when final checkpoint failed", audits)
	}
}

func TestRunPipelinePeriodicSaveFailureFailsWhenFinalSaveSkipped(t *testing.T) {
	periodicErr := errors.New("periodic sqlite busy")
	saver := &scriptedCheckpointSaver{outcomes: []error{periodicErr}}
	_, audits, err := runCheckpointDegradationPipeline(t, saver, func(*scriptedCheckpointSaver) (bool, error) {
		return false, nil
	})
	if !errors.Is(err, periodicErr) {
		t.Fatalf("runPipeline error = %v, want periodic failure when final save was skipped", err)
	}
	if len(audits) != 0 {
		t.Fatalf("audit events = %+v, want none when final checkpoint was skipped", audits)
	}
}

func TestRunPipelineAllCheckpointSavesSucceed(t *testing.T) {
	saver := &scriptedCheckpointSaver{outcomes: []error{nil, nil}}
	_, audits, err := runCheckpointDegradationPipeline(t, saver, func(s *scriptedCheckpointSaver) (bool, error) {
		err := s.SaveProgress(665, "items", nil, int64(1), 1, 1, "")
		return err == nil, err
	})
	if err != nil {
		t.Fatalf("runPipeline = %v, want success", err)
	}
	if len(audits) != 0 {
		t.Fatalf("audit events = %+v, want none without periodic degradation", audits)
	}
}
