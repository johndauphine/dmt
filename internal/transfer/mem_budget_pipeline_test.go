package transfer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
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
