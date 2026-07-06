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

// gateSaver records every persisted snapshot and can block the first call
// on a gate so a test can deterministically fill the mailbox behind it.
type gateSaver struct {
	mu       sync.Mutex
	rowsDone []int64
	err      error
	calls    int

	gate     chan struct{} // if non-nil, the first SaveProgress blocks on it
	entered  chan struct{} // if non-nil, closed when the first call is dequeued (before it blocks on gate)
	gateOnce sync.Once
	each     chan struct{} // if non-nil, receives after every call
}

func (g *gateSaver) SaveProgress(_ int64, _ string, _ *int, _ any, rowsDone, _ int64, _ string) error {
	if g.gate != nil {
		g.gateOnce.Do(func() {
			if g.entered != nil {
				close(g.entered)
			}
			<-g.gate
		})
	}
	g.mu.Lock()
	g.rowsDone = append(g.rowsDone, rowsDone)
	g.calls++
	err := g.err
	g.mu.Unlock()
	if g.each != nil {
		g.each <- struct{}{}
	}
	return err
}

func (g *gateSaver) GetProgress(int64) (any, int64, string, error) { return nil, 0, "", nil }

func (g *gateSaver) persisted() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.rowsDone...)
}

func post(s *asyncSaver, rowsDone int64) {
	_ = s.SaveProgress(1, "t", nil, rowsDone, rowsDone, 100, "")
}

// TestAsyncSaverLatestWins proves the single-slot mailbox coalesces: while
// the persistence goroutine is blocked on the first snapshot, later posts
// collapse to only the newest, and that newest is still persisted (#620).
func TestAsyncSaverLatestWins(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	inner := &gateSaver{gate: gate, entered: entered}
	s := newAsyncSaver(inner)
	s.start()

	post(s, 1) // goroutine picks this up and blocks on the gate
	<-entered  // guarantee snapshot 1 is dequeued before the next posts land behind it
	post(s, 2) // sits in the mailbox
	post(s, 3) // full → discards 2, leaves 3 (latest wins)
	post(s, 4) // full → discards 3, leaves 4

	close(gate) // release snapshot 1; goroutine then drains 4
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got := inner.persisted()
	if len(got) == 0 || got[0] != 1 {
		t.Fatalf("first persisted = %v, want it to start with 1", got)
	}
	if last := got[len(got)-1]; last != 4 {
		t.Fatalf("last persisted = %d, want 4 (newest must not be lost)", last)
	}
	// Coalescing must have dropped at least one intermediate snapshot.
	if len(got) > 3 {
		t.Fatalf("persisted %v — expected coalescing to drop intermediates (≤3, ideally [1 4])", got)
	}
}

// TestAsyncSaverSurfacesRepeatedFailures verifies close() reports the saver
// as unhealthy after enough consecutive failures, so a run never silently
// loses all checkpointing (#620). Snapshots are fed one at a time (waiting
// for each to be consumed) so none coalesce away.
func TestAsyncSaverSurfacesRepeatedFailures(t *testing.T) {
	each := make(chan struct{})
	inner := &gateSaver{err: errors.New("disk full"), each: each}
	s := newAsyncSaver(inner)
	s.start()

	for i := 0; i < asyncSaverFailThreshold; i++ {
		post(s, int64(i+1))
		<-each // wait for this snapshot to be persisted before posting the next
	}

	err := s.close()
	if err == nil {
		t.Fatal("close() = nil, want an unhealthy-saver error after repeated failures")
	}
}

// TestAsyncSaverRecoversFromTransientFailure confirms a single failure
// followed by successes does NOT surface as unhealthy — consecutive count
// resets on success (#620).
func TestAsyncSaverRecoversFromTransientFailure(t *testing.T) {
	each := make(chan struct{})
	inner := &gateSaver{each: each}
	s := newAsyncSaver(inner)
	s.start()

	inner.mu.Lock()
	inner.err = errors.New("transient")
	inner.mu.Unlock()
	post(s, 1)
	<-each

	inner.mu.Lock()
	inner.err = nil
	inner.mu.Unlock()
	for i := 0; i < asyncSaverFailThreshold; i++ {
		post(s, int64(i+2))
		<-each
	}

	if err := s.close(); err != nil {
		t.Fatalf("close() = %v, want nil (successes reset the failure count)", err)
	}
}

// TestAsyncSaverCloseIsIdempotent guards the drain path calling close on
// every exit route.
func TestAsyncSaverCloseIsIdempotent(t *testing.T) {
	s := newAsyncSaver(&gateSaver{})
	s.start()
	post(s, 1)
	if err := s.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// slowSaver wraps a ProgressSaver, delaying each persist to model a slow
// SQLite fsync / YAML rewrite. Synchronous saves would serialize this delay
// into the ack path (and, via ackChan, the writers); async must not.
type slowSaver struct {
	inner ProgressSaver
	delay time.Duration
}

func (s *slowSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	time.Sleep(s.delay)
	return s.inner.SaveProgress(taskID, tableName, partitionID, lastPK, rowsDone, rowsTotal, rangeState)
}
func (s *slowSaver) GetProgress(id int64) (any, int64, string, error) { return s.inner.GetProgress(id) }

// TestSlowSaverKeepsFinalCheckpointDurable drives a full keyset transfer
// with checkpoint_frequency=1 (a periodic save posted for every chunk)
// against a deliberately slow saver. The transfer must complete, move every
// row, and leave a durable final checkpoint at the last PK — proving the
// async saver never drops the final value behind coalesced intermediates
// and that a slow saver doesn't stall the pipeline into incorrectness (#620).
func TestSlowSaverKeepsFinalCheckpointDurable(t *testing.T) {
	const totalRows = 500

	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}
	recording := &keysetRuntimeProgressSaver{}
	saver := &slowSaver{inner: recording, delay: 3 * time.Millisecond}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         totalRows,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:           10, // 50 chunks → 50 periodic saves posted
			ParallelReaders:     2,
			WriteAheadWriters:   2,
			TargetMode:          "drop_recreate",
			CheckpointFrequency: 1,
		},
	}
	job := Job{Table: table, TaskID: 620, Saver: saver}

	stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, job, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != totalRows {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, totalRows)
	}

	ids, _ := tgtPool.snapshot()
	if len(ids) != totalRows {
		t.Fatalf("wrote %d rows, want %d", len(ids), totalRows)
	}

	last, ok := recording.last()
	if !ok {
		t.Fatal("no checkpoint persisted")
	}
	lastPK, ok := keysetRuntimeInt(last.lastPK)
	if !ok || int64(lastPK) != totalRows {
		t.Fatalf("final checkpoint lastPK = %v, want %d", last.lastPK, totalRows)
	}
	if last.rowsDone != totalRows {
		t.Fatalf("final checkpoint rowsDone = %d, want %d", last.rowsDone, totalRows)
	}
}
