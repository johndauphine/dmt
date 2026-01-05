package progress

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/schollz/progressbar/v3"
)

// Tracker tracks migration progress
type Tracker struct {
	bar       *progressbar.ProgressBar
	total     int64
	current   atomic.Int64
	startTime time.Time

	// Track active tables for accurate display
	mu           sync.Mutex
	activeTables map[string]int // table name -> active job count

	// Table counts for progress reporting
	tablesTotal    int
	tablesComplete atomic.Int32
	tablesFailed   atomic.Int32

	// JSON progress reporting
	reporter   Reporter
	jsonMode   bool // When true, disable progress bar
	phase      string
	reporterMu sync.Mutex
	stopReport chan struct{}
	reportWg   sync.WaitGroup
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
	t.reporterMu.Lock()
	defer t.reporterMu.Unlock()

	t.reporter = reporter
	t.jsonMode = reporter != nil

	// Start background reporting goroutine
	if t.reporter != nil && interval > 0 {
		t.stopReport = make(chan struct{})
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
	var progressPct float64
	if t.total > 0 {
		progressPct = float64(current) / float64(t.total) * 100
	}

	var rowsPerSec int64
	elapsed := time.Since(t.startTime).Seconds()
	if elapsed > 0 {
		rowsPerSec = int64(float64(current) / elapsed)
	}

	update := ProgressUpdate{
		Phase:           phase,
		TablesComplete:  int(t.tablesComplete.Load()),
		TablesTotal:     t.tablesTotal,
		TablesRunning:   tablesRunning,
		RowsTransferred: current,
		RowsTotal:       t.total,
		ProgressPct:     progressPct,
		RowsPerSecond:   rowsPerSec,
		CurrentTables:   activeTables,
		ErrorCount:      int(t.tablesFailed.Load()),
	}

	reporter.Report(update)
}

// SetPhase updates the current phase and emits an immediate progress update
func (t *Tracker) SetPhase(phase string) {
	t.reporterMu.Lock()
	t.phase = phase
	t.reporterMu.Unlock()
	t.emitProgressImmediate()
}

// emitProgressImmediate sends progress update immediately (for phase changes)
func (t *Tracker) emitProgressImmediate() {
	t.reporterMu.Lock()
	reporter := t.reporter
	phase := t.phase
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
	var progressPct float64
	if t.total > 0 {
		progressPct = float64(current) / float64(t.total) * 100
	}

	var rowsPerSec int64
	elapsed := time.Since(t.startTime).Seconds()
	if elapsed > 0 {
		rowsPerSec = int64(float64(current) / elapsed)
	}

	update := ProgressUpdate{
		Phase:           phase,
		TablesComplete:  int(t.tablesComplete.Load()),
		TablesTotal:     t.tablesTotal,
		TablesRunning:   tablesRunning,
		RowsTransferred: current,
		RowsTotal:       t.total,
		ProgressPct:     progressPct,
		RowsPerSecond:   rowsPerSec,
		CurrentTables:   activeTables,
		ErrorCount:      int(t.tablesFailed.Load()),
	}

	reporter.ReportImmediate(update)
}

// SetTablesTotal sets the total number of tables to transfer
func (t *Tracker) SetTablesTotal(total int) {
	t.tablesTotal = total
}

// SetTotal sets the total number of rows to transfer
func (t *Tracker) SetTotal(total int64) {
	t.total = total

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

// SetTable updates the progress bar description with the current table name
// Deprecated: Use StartTable/EndTable for accurate multi-table tracking
func (t *Tracker) SetTable(tableName string) {
	t.StartTable(tableName)
}

// Current returns the current count
func (t *Tracker) Current() int64 {
	return t.current.Load()
}

// Finish marks the progress as complete
func (t *Tracker) Finish() {
	// Stop the reporting goroutine
	if t.stopReport != nil {
		close(t.stopReport)
		t.reportWg.Wait()
	}

	if t.bar != nil {
		t.bar.Finish()
	}

	elapsed := time.Since(t.startTime)
	rowsPerSec := float64(t.current.Load()) / elapsed.Seconds()

	// Emit final progress update
	t.reporterMu.Lock()
	t.phase = "completed"
	jsonMode := t.jsonMode
	t.reporterMu.Unlock()
	t.emitProgressImmediate()

	if !jsonMode {
		fmt.Println()
	}
	logging.Info("Transfer complete: %d rows in %s (%.0f rows/sec)",
		t.current.Load(), elapsed.Round(time.Second), rowsPerSec)
}

// Close cleans up the reporter
func (t *Tracker) Close() {
	t.reporterMu.Lock()
	defer t.reporterMu.Unlock()

	if t.reporter != nil {
		t.reporter.Close()
	}
}
