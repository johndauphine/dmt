package progress

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
)

// ProgressUpdate represents a JSON progress update for automation/Airflow.
type ProgressUpdate struct {
	Timestamp       string  `json:"timestamp"`
	Phase           string  `json:"phase"`
	TablesComplete  int     `json:"tables_complete"`
	TablesTotal     int     `json:"tables_total"`
	TablesRunning   int     `json:"tables_running"`
	RowsTransferred int64   `json:"rows_transferred"`
	RowsTotal       int64   `json:"rows_total,omitempty"`
	ProgressPct     float64 `json:"progress_pct"`
	RowsPerSecond   int64   `json:"rows_per_second,omitempty"`
	CurrentTables   []string `json:"current_tables,omitempty"`
	ErrorCount      int     `json:"error_count,omitempty"`
}

// Reporter defines the interface for progress reporting.
type Reporter interface {
	// Report emits a progress update (may be throttled)
	Report(update ProgressUpdate)
	// ReportImmediate emits a progress update immediately, bypassing throttling
	ReportImmediate(update ProgressUpdate)
	// Close cleans up any resources
	Close()
}

// JSONReporter outputs JSON progress updates to a writer (typically stderr).
type JSONReporter struct {
	writer   io.Writer
	mu       sync.Mutex
	interval time.Duration
	lastReport time.Time
	closed   bool
}

// NewJSONReporter creates a new JSON progress reporter.
// interval specifies the minimum time between updates (to avoid flooding).
func NewJSONReporter(writer io.Writer, interval time.Duration) *JSONReporter {
	if writer == nil {
		writer = os.Stderr
	}
	return &JSONReporter{
		writer:   writer,
		interval: interval,
	}
}

// Report emits a JSON progress update to the writer.
// Updates are throttled based on the configured interval.
func (r *JSONReporter) Report(update ProgressUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	// Throttle updates based on interval (except for phase changes)
	now := time.Now()
	if r.interval > 0 && now.Sub(r.lastReport) < r.interval {
		return
	}
	r.lastReport = now

	// Set timestamp if not already set
	if update.Timestamp == "" {
		update.Timestamp = now.Format(time.RFC3339)
	}

	data, err := json.Marshal(update)
	if err != nil {
		logging.Warn("Failed to marshal progress update: %v", err)
		return
	}

	fmt.Fprintln(r.writer, string(data))
}

// ReportImmediate emits a progress update immediately, bypassing throttling.
// Use for important state changes like phase transitions.
func (r *JSONReporter) ReportImmediate(update ProgressUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	if update.Timestamp == "" {
		update.Timestamp = time.Now().Format(time.RFC3339)
	}

	data, err := json.Marshal(update)
	if err != nil {
		logging.Warn("Failed to marshal progress update: %v", err)
		return
	}

	fmt.Fprintln(r.writer, string(data))
	r.lastReport = time.Now()
}

// Close marks the reporter as closed.
func (r *JSONReporter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}

// NullReporter is a no-op reporter for when progress reporting is disabled.
type NullReporter struct{}

// Report does nothing.
func (r *NullReporter) Report(update ProgressUpdate) {}

// ReportImmediate does nothing.
func (r *NullReporter) ReportImmediate(update ProgressUpdate) {}

// Close does nothing.
func (r *NullReporter) Close() {}
