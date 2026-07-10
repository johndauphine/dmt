package transfer

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

func memBudgetTestTable() driver.Table {
	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	return table
}

func memBudgetTestConfig() *config.Config {
	return &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         50,
			ParallelReaders:   4,
			WriteAheadWriters: 2,
			TargetMode:        "drop_recreate",
		},
	}
}

// TestMemBudgetFullyReleasedOnSuccess drives a real keyset transfer with a
// small shared budget and asserts every reserved byte is returned after the
// transfer completes — the exactly-once release guarantee on the happy path
// (#617). The budget is deliberately smaller than the table's total
// in-flight bytes, so readers actually block on and cycle through it.
func TestMemBudgetFullyReleasedOnSuccess(t *testing.T) {
	const totalRows = 4000
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}

	table := memBudgetTestTable()
	table.RowCount = totalRows
	cfg := memBudgetTestConfig()

	budget := NewMemBudget(64 * 1024) // 64 KB — far below the whole table in flight
	job := Job{Table: table, MemBudget: budget}

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
	if !budget.fullyReleased() {
		t.Fatal("budget not fully released after a successful transfer — reservation leaked")
	}
}

// TestMemBudgetContentionReportsTunerAndPrometheus drives a real transfer
// through a one-byte shared budget. A blocked writer keeps later readers in
// acquire long enough to distinguish genuine budget starvation from timer
// noise, while the live-writer gauge is sampled before the drain completes.
func TestMemBudgetContentionReportsTunerAndPrometheus(t *testing.T) {
	const totalRows = 400
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	writeStarted := make(chan struct{}, 1)
	releaseWrites := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(releaseWrites)
		}
	})
	tgtPool := &keysetRuntimeTargetPool{
		updated:      true,
		writeStarted: writeStarted,
		writeGate:    releaseWrites,
	}

	table := memBudgetTestTable()
	table.RowCount = totalRows
	cfg := memBudgetTestConfig()
	budget := NewMemBudget(1)
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: cfg.Migration.ChunkSize, WriteAheadWriters: cfg.Migration.WriteAheadWriters})

	reg := observability.New()
	reg.RunStarted("budget-contention", "sqlite", "sqlite")
	observability.SetGlobal(reg)
	t.Cleanup(func() {
		observability.SetGlobal(nil)
		reg.RunComplete("budget-contention")
	})

	done := make(chan error, 1)
	go func() {
		_, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table, MemBudget: budget}, progress.New(), tuner)
		done <- err
	}()

	select {
	case <-writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}

	// The first write is held, so the sample must show the configured writers
	// alive before the pool's final drain changes the gauge to zero. The writer
	// can begin just before the consumer reaches its post-submit sample, so
	// wait for that chunk-boundary publication rather than racing it.
	waitForLiveWriterGauge(t, reg, "budget-contention", 2)

	// Keep the writer blocked long enough for another reader to be waiting in
	// acquire. Its elapsed wait is reported only after the reservation frees.
	time.Sleep(50 * time.Millisecond)
	close(releaseWrites)
	released = true

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transfer did not finish after writers were released")
	}

	m := tuner.Metrics()
	if m.BudgetWaitCount == 0 || m.BudgetWaitNs < int64(25*time.Millisecond) {
		t.Fatalf("budget waits = (%d ns, %d count), want contention of at least 25ms", m.BudgetWaitNs, m.BudgetWaitCount)
	}
	if !budget.fullyReleased() {
		t.Fatal("budget not fully released after contention transfer")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	metricsText := rec.Body.String()
	if !strings.Contains(metricsText, `dmt_budget_wait_seconds_total{run_id="budget-contention",table="items"}`) {
		t.Fatalf("budget wait metric missing after contention transfer:\n%s", metricsText)
	}
}

// TestMemBudgetNilTunerStillAcquires ensures the timed-acquire path remains
// safe for direct callers that deliberately do not install runtime tuning.
func TestMemBudgetNilTunerStillAcquires(t *testing.T) {
	const totalRows = 100
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}
	table := memBudgetTestTable()
	table.RowCount = totalRows
	budget := NewMemBudget(1)

	stats, err := Execute(context.Background(), srcPool, tgtPool, memBudgetTestConfig(), Job{Table: table, MemBudget: budget}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != totalRows {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, totalRows)
	}
	if !budget.fullyReleased() {
		t.Fatal("budget not fully released with a nil tuner")
	}
}

// TestMemBudgetNilBudgetDoesNotReportWait ensures a tuner alone does not
// fabricate contention when in-flight byte accounting is disabled.
func TestMemBudgetNilBudgetDoesNotReportWait(t *testing.T) {
	const totalRows = 100
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}
	table := memBudgetTestTable()
	table.RowCount = totalRows

	reg := observability.New()
	reg.RunStarted("no-budget", "sqlite", "sqlite")
	observability.SetGlobal(reg)
	t.Cleanup(func() {
		observability.SetGlobal(nil)
		reg.RunComplete("no-budget")
	})

	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 50, WriteAheadWriters: 2})
	stats, err := Execute(context.Background(), srcPool, tgtPool, memBudgetTestConfig(), Job{Table: table}, progress.New(), tuner)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.Rows != totalRows {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, totalRows)
	}
	if m := tuner.Metrics(); m.BudgetWaitNs != 0 || m.BudgetWaitCount != 0 {
		t.Fatalf("nil budget reported waits = (%d ns, %d count)", m.BudgetWaitNs, m.BudgetWaitCount)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "dmt_budget_wait_seconds_total") {
		t.Fatalf("disabled budget unexpectedly emitted wait metric:\n%s", rec.Body.String())
	}
}

// TestLiveWriterMetricTracksScaleDownDrain verifies the value sampled by the
// pipeline has the intended semantics: a retired writer remains visible while
// its target write drains, then converges to the desired worker count.
func TestLiveWriterMetricTracksScaleDownDrain(t *testing.T) {
	reg := observability.New()
	reg.RunStarted("writer-drain", "sqlite", "sqlite")

	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{}, 2)
	wp := pool.NewWriterPool(context.Background(), pool.WriterPoolConfig{
		NumWriters:    2,
		BufferSize:    1,
		JobBufferSize: 3,
		WriteFunc: func(context.Context, int, [][]any) error {
			started <- struct{}{}
			<-release // model a target write that must drain before it can retire
			return nil
		},
	})
	wp.Start()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		wp.Wait()
		reg.RunComplete("writer-drain")
	})

	for i := 0; i < 2; i++ {
		if ok := wp.Submit(pool.WriteJob{Rows: [][]any{{i}}}); !ok {
			t.Fatal("Submit returned false")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("writers did not begin their blocked writes")
		}
	}

	if err := wp.ScaleWorkers(1); err != nil {
		t.Fatalf("ScaleWorkers(1): %v", err)
	}
	reg.SetLiveWriters("items", wp.GetLiveWorkerCount())
	assertLiveWriterGauge(t, reg, "writer-drain", 2)

	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(2 * time.Second)
	for wp.GetLiveWorkerCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := wp.GetLiveWorkerCount(); got != 1 {
		t.Fatalf("live workers after retired write drained = %d, want 1", got)
	}
	reg.SetLiveWriters("items", wp.GetLiveWorkerCount())
	assertLiveWriterGauge(t, reg, "writer-drain", 1)
}

func assertLiveWriterGauge(t *testing.T, reg *observability.Registry, runID string, want int) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	wantLine := `dmt_live_writers{run_id="` + runID + `",table="items"} ` + fmt.Sprint(want)
	if !strings.Contains(rec.Body.String(), wantLine) {
		t.Fatalf("live writer gauge = not %d:\n%s", want, rec.Body.String())
	}
}

func waitForLiveWriterGauge(t *testing.T, reg *observability.Registry, runID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/metrics", nil)
		reg.Handler().ServeHTTP(rec, req)
		wantLine := `dmt_live_writers{run_id="` + runID + `",table="items"} ` + fmt.Sprint(want)
		if strings.Contains(rec.Body.String(), wantLine) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("live writer gauge did not reach %d", want)
}

// TestMemBudgetFullyReleasedOnWriteError asserts the residual-release
// backstop returns every byte even when the writer fails partway and chunks
// are abandoned in the channels (#617). Without the drain-time residual
// release, the abandoned chunks' reservations would leak and shrink the
// shared budget for the rest of the run.
func TestMemBudgetFullyReleasedOnWriteError(t *testing.T) {
	const totalRows = 4000
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{
		updated:   true,
		writeErr:  errors.New("simulated write failure"),
		failAfter: 3, // let a few chunks through, then fail — leaves chunks in flight
	}

	table := memBudgetTestTable()
	table.RowCount = totalRows
	cfg := memBudgetTestConfig()

	budget := NewMemBudget(64 * 1024)
	job := Job{Table: table, MemBudget: budget}

	_, err := Execute(context.Background(), srcPool, tgtPool, cfg, job, progress.New(), nil)
	if err == nil {
		t.Fatal("Execute succeeded, want a write error")
	}
	if !budget.fullyReleased() {
		t.Fatal("budget not fully released after a failed transfer — abandoned-chunk reservation leaked")
	}
}

// TestMemBudgetTightBudgetWriteErrorDoesNotDeadlock reproduces the tight-
// budget write-failure hazard (#617 codex review): with a budget that admits
// roughly one chunk, a chunk that fails to write must still release its
// reservation so readers blocked in acquireMem are freed and the transfer
// surfaces the error instead of hanging. Runs Execute under a hard timeout
// so a regression fails fast rather than wedging the suite.
func TestMemBudgetTightBudgetWriteErrorDoesNotDeadlock(t *testing.T) {
	const totalRows = 8000
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{
		updated:   true,
		writeErr:  errors.New("simulated write failure"),
		failAfter: 1, // first chunk succeeds, then fail while the budget is saturated
	}

	table := memBudgetTestTable()
	table.RowCount = totalRows
	cfg := memBudgetTestConfig()
	cfg.Migration.ParallelReaders = 4 // several readers contend for the tiny budget

	// A budget so small it admits only about one chunk at a time, so a
	// failed chunk holding it would wedge every waiting reader.
	budget := NewMemBudget(4 * 1024)
	job := Job{Table: table, MemBudget: budget}

	done := make(chan error, 1)
	go func() {
		_, err := Execute(context.Background(), srcPool, tgtPool, cfg, job, progress.New(), nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Execute succeeded, want the simulated write error")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Execute deadlocked on a tight budget with a write error (#617)")
	}
	if !budget.fullyReleased() {
		t.Fatal("budget not fully released after tight-budget write error")
	}
}

// TestMemBudgetFullyReleasedOnCancel asserts a mid-transfer cancellation
// leaves no reservation behind: readers blocked in acquire reserve nothing,
// and chunks abandoned in flight are released by the drain (#617).
func TestMemBudgetFullyReleasedOnCancel(t *testing.T) {
	const totalRows = 8000
	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}
	tgtPool := &keysetRuntimeTargetPool{updated: true}

	table := memBudgetTestTable()
	table.RowCount = totalRows
	cfg := memBudgetTestConfig()

	budget := NewMemBudget(64 * 1024)
	job := Job{Table: table, MemBudget: budget}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel almost immediately so the transfer aborts with chunks in flight.
	cancel()
	_, _ = Execute(ctx, srcPool, tgtPool, cfg, job, progress.New(), nil)

	if !budget.fullyReleased() {
		t.Fatal("budget not fully released after cancellation — reservation leaked")
	}
}
