package progress

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/schollz/progressbar/v3"
)

// Tracker tracks migration progress
type Tracker struct {
	bar       *progressbar.ProgressBar
	total     atomic.Int64
	current   atomic.Int64
	startTime time.Time

	// Track active tables for accurate display
	mu           sync.Mutex
	activeTables map[string]int // table name -> active job count

	// Table counts for progress reporting
	tablesTotal    atomic.Int64
	tablesComplete atomic.Int32
	tablesFailed   atomic.Int32

	// JSON progress reporting
	reporter   Reporter
	jsonMode   bool // When true, disable progress bar
	phase      string
	reporterMu sync.Mutex
	stopReport chan struct{}
	reportWg   sync.WaitGroup
	stopOnce   sync.Once
}

// New creates a new progress tracker
func New() *Tracker {
	return &Tracker{
		startTime:    time.Now(),
		activeTables: make(map[string]int),
		phase:        "initializing",
	}
}

// SetReporter sets the progress reporter for JSON output.
// When a reporter is set, the progress bar is disabled.
func (t *Tracker) SetReporter(reporter Reporter, interval time.Duration) {
	// Stop any loop a prior SetReporter started so it can't leak, then re-arm
	// the once-guard for the new loop. stopLoop must run outside reporterMu
	// (the loop's emitProgress takes it).
	t.stopLoop()

	t.reporterMu.Lock()
	defer t.reporterMu.Unlock()

	t.reporter = reporter
	t.jsonMode = reporter != nil

	// Start background reporting goroutine
	if t.reporter != nil && interval > 0 {
		t.stopReport = make(chan struct{})
		t.stopOnce = sync.Once{}
		t.reportWg.Add(1)
		go t.reportLoop(interval)
	}
}

// reportLoop emits periodic progress updates
func (t *Tracker) reportLoop(interval time.Duration) {
	defer t.reportWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.emitProgress()
		case <-t.stopReport:
			return
		}
	}
}

// emitProgress sends current progress to the reporter
func (t *Tracker) emitProgress() {
	t.reporterMu.Lock()
	reporter := t.reporter
	phase := t.phase
	startTime := t.startTime
	t.reporterMu.Unlock()

	if reporter == nil {
		return
	}

	t.mu.Lock()
	activeTables := make([]string, 0, len(t.activeTables))
	for name := range t.activeTables {
		activeTables = append(activeTables, name)
	}
	tablesRunning := len(t.activeTables)
	t.mu.Unlock()

	current := t.current.Load()
	total := t.total.Load()
	var progressPct float64
	if total > 0 {
		progressPct = float64(current) / float64(total) * 100
	}

	var rowsPerSec int64
	elapsed := time.Since(startTime).Seconds()
	if elapsed > 0 {
		rowsPerSec = int64(float64(current) / elapsed)
	}

	update := ProgressUpdate{
		Phase:           phase,
		TablesComplete:  int(t.tablesComplete.Load()),
		TablesTotal:     int(t.tablesTotal.Load()),
		TablesRunning:   tablesRunning,
		RowsTransferred: current,
		RowsTotal:       total,
		ProgressPct:     progressPct,
		RowsPerSecond:   rowsPerSec,
		CurrentTables:   activeTables,
		ErrorCount:      int(t.tablesFailed.Load()),
	}

	reporter.Report(update)
}

// SetPhase updates the current phase and emits an immediate progress update.
// When entering the "transfer" phase, the start time is reset so that
// "Transfer complete" reports transfer-only duration (excluding schema
// extraction, DDL creation, and AI tuning overhead).
func (t *Tracker) SetPhase(phase string) {
	t.reporterMu.Lock()
	if phase == "transfer" {
		t.startTime = time.Now()
	}
	t.phase = phase
	t.reporterMu.Unlock()
	t.emitProgressImmediate()
}

// emitProgressImmediate sends progress update immediately (for phase changes)
func (t *Tracker) emitProgressImmediate() {
	t.reporterMu.Lock()
	reporter := t.reporter
	phase := t.phase
	startTime := t.startTime
	t.reporterMu.Unlock()

	if reporter == nil {
		return
	}

	t.mu.Lock()
	activeTables := make([]string, 0, len(t.activeTables))
	for name := range t.activeTables {
		activeTables = append(activeTables, name)
	}
	tablesRunning := len(t.activeTables)
	t.mu.Unlock()

	current := t.current.Load()
	total := t.total.Load()
	var progressPct float64
	if total > 0 {
		progressPct = float64(current) / float64(total) * 100
	}

	var rowsPerSec int64
	elapsed := time.Since(startTime).Seconds()
	if elapsed > 0 {
		rowsPerSec = int64(float64(current) / elapsed)
	}

	update := ProgressUpdate{
		Phase:           phase,
		TablesComplete:  int(t.tablesComplete.Load()),
		TablesTotal:     int(t.tablesTotal.Load()),
		TablesRunning:   tablesRunning,
		RowsTransferred: current,
		RowsTotal:       total,
		ProgressPct:     progressPct,
		RowsPerSecond:   rowsPerSec,
		CurrentTables:   activeTables,
		ErrorCount:      int(t.tablesFailed.Load()),
	}

	reporter.ReportImmediate(update)
}

// SetTablesTotal sets the total number of tables to transfer
func (t *Tracker) SetTablesTotal(total int) {
	t.tablesTotal.Store(int64(total))
}

// SetTotal sets the total number of rows to transfer
func (t *Tracker) SetTotal(total int64) {
	t.total.Store(total)

	// Only create progress bar if not in JSON mode
	t.reporterMu.Lock()
	jsonMode := t.jsonMode
	t.reporterMu.Unlock()

	if !jsonMode {
		t.bar = progressbar.NewOptions64(
			total,
			progressbar.OptionSetDescription("Transferring"),
			progressbar.OptionShowBytes(false),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(40),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionShowIts(),
			progressbar.OptionSetItsString("rows"),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetRenderBlankState(true),
		)
	}
}

// Add increments the progress counter
func (t *Tracker) Add(n int64) {
	t.current.Add(n)
	if t.bar != nil {
		t.bar.Add64(n)
	}
}

// StartTable marks a table as actively transferring
func (t *Tracker) StartTable(tableName string) {
	t.mu.Lock()
	t.activeTables[tableName]++
	tableCount := len(t.activeTables)
	t.mu.Unlock()

	if t.bar != nil {
		if tableCount == 1 {
			t.bar.Describe(fmt.Sprintf("Transferring %s", tableName))
		} else {
			t.bar.Describe(fmt.Sprintf("Transferring (%d tables)", tableCount))
		}
		t.bar.RenderBlank()
	}
}

// EndTable marks a table job as done transferring
func (t *Tracker) EndTable(tableName string) {
	t.mu.Lock()
	t.activeTables[tableName]--
	if t.activeTables[tableName] <= 0 {
		delete(t.activeTables, tableName)
	}
	tableCount := len(t.activeTables)
	// Get remaining table name if only one left
	var remaining string
	for name := range t.activeTables {
		remaining = name
		break
	}
	t.mu.Unlock()

	if t.bar != nil && tableCount > 0 {
		if tableCount == 1 {
			t.bar.Describe(fmt.Sprintf("Transferring %s", remaining))
		} else {
			t.bar.Describe(fmt.Sprintf("Transferring (%d tables)", tableCount))
		}
	}
}

// TableComplete marks a table as successfully completed
func (t *Tracker) TableComplete() {
	t.tablesComplete.Add(1)
}

// TableFailed marks a table as failed
func (t *Tracker) TableFailed() {
	t.tablesFailed.Add(1)
}

// TablesTotal returns the total number of tables to transfer.
func (t *Tracker) TablesTotal() int {
	return int(t.tablesTotal.Load())
}

// TablesComplete returns the number of tables successfully completed.
func (t *Tracker) TablesComplete() int {
	return int(t.tablesComplete.Load())
}

// TablesFailed returns the number of tables that failed to transfer.
func (t *Tracker) TablesFailed() int {
	return int(t.tablesFailed.Load())
}

// Current returns the current count
func (t *Tracker) Current() int64 {
	return t.current.Load()
}

// stopLoop stops the periodic reporting goroutine exactly once. It is safe to
// call from both Finish() and Close() and when no loop was ever started. It
// must not hold reporterMu while waiting: the loop's emitProgress acquires
// reporterMu, so holding it here would deadlock.
func (t *Tracker) stopLoop() {
	t.stopOnce.Do(func() {
		if t.stopReport != nil {
			close(t.stopReport)
			t.reportWg.Wait()
		}
	})
}

// Finish marks the progress as complete
func (t *Tracker) Finish() {
	t.stopLoop()

	if t.bar != nil {
		t.bar.Finish()
	}

	t.reporterMu.Lock()
	startTime := t.startTime
	t.phase = "completed"
	jsonMode := t.jsonMode
	t.reporterMu.Unlock()

	elapsed := time.Since(startTime)
	var rowsPerSec float64
	if elapsed.Seconds() > 0 {
		rowsPerSec = float64(t.current.Load()) / elapsed.Seconds()
	}
	t.emitProgressImmediate()

	if !jsonMode {
		fmt.Println()
	}
	logging.Info("Transfer complete: %d rows in %s (%.0f rows/sec)",
		t.current.Load(), elapsed.Round(time.Second), rowsPerSec)
}

// Close stops the reporting goroutine and cleans up the reporter. Safe to call
// after Finish() (stopLoop is idempotent). This is the teardown a long-lived
// process (e.g. the WebUI server, which creates one orchestrator per run) must
// invoke so the periodic goroutine started by SetReporter does not leak.
func (t *Tracker) Close() {
	t.stopLoop()

	t.reporterMu.Lock()
	defer t.reporterMu.Unlock()

	if t.reporter != nil {
		t.reporter.Close()
	}
}
