package progress

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tracker := New()
	if tracker == nil {
		t.Fatal("expected tracker, got nil")
	}
	if tracker.activeTables == nil {
		t.Error("expected activeTables map to be initialized")
	}
	if tracker.phase != "initializing" {
		t.Errorf("phase = %q, want %q", tracker.phase, "initializing")
	}
	if tracker.Current() != 0 {
		t.Errorf("current = %d, want 0", tracker.Current())
	}
}

func TestTracker_SetTotal(t *testing.T) {
	t.Run("sets total without progress bar in JSON mode", func(t *testing.T) {
		tracker := New()
		// Enable JSON mode to prevent progress bar creation
		tracker.SetReporter(&NullReporter{}, 0)
		tracker.SetTotal(1000)

		if got := tracker.total.Load(); got != 1000 {
			t.Errorf("total = %d, want 1000", got)
		}
		if tracker.bar != nil {
			t.Error("expected no progress bar in JSON mode")
		}
	})
}

func TestTracker_SetTablesTotal(t *testing.T) {
	tracker := New()
	tracker.SetTablesTotal(10)

	if got := tracker.tablesTotal.Load(); got != 10 {
		t.Errorf("tablesTotal = %d, want 10", got)
	}
}

func TestTracker_Add(t *testing.T) {
	tracker := New()
	tracker.SetReporter(&NullReporter{}, 0) // Disable progress bar

	tracker.Add(100)
	if got := tracker.Current(); got != 100 {
		t.Errorf("Current() = %d, want 100", got)
	}

	tracker.Add(50)
	if got := tracker.Current(); got != 150 {
		t.Errorf("Current() = %d, want 150", got)
	}
}

func TestTracker_Add_Concurrent(t *testing.T) {
	tracker := New()
	tracker.SetReporter(&NullReporter{}, 0)

	var wg sync.WaitGroup
	numGoroutines := 100
	incrementsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				tracker.Add(1)
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * incrementsPerGoroutine)
	if got := tracker.Current(); got != expected {
		t.Errorf("Current() = %d, want %d", got, expected)
	}
}

func TestTracker_StartEndTable(t *testing.T) {
	tracker := New()
	tracker.SetReporter(&NullReporter{}, 0)

	// Start a table
	tracker.StartTable("users")
	tracker.mu.Lock()
	if count := tracker.activeTables["users"]; count != 1 {
		t.Errorf("activeTables[users] = %d, want 1", count)
	}
	tracker.mu.Unlock()

	// Start same table again (parallel partition)
	tracker.StartTable("users")
	tracker.mu.Lock()
	if count := tracker.activeTables["users"]; count != 2 {
		t.Errorf("activeTables[users] = %d, want 2", count)
	}
	tracker.mu.Unlock()

	// End one job for the table
	tracker.EndTable("users")
	tracker.mu.Lock()
	if count := tracker.activeTables["users"]; count != 1 {
		t.Errorf("activeTables[users] = %d, want 1", count)
	}
	tracker.mu.Unlock()

	// End the last job
	tracker.EndTable("users")
	tracker.mu.Lock()
	if _, exists := tracker.activeTables["users"]; exists {
		t.Error("expected users to be removed from activeTables")
	}
	tracker.mu.Unlock()
}

func TestTracker_MultipleTables(t *testing.T) {
	tracker := New()
	tracker.SetReporter(&NullReporter{}, 0)

	tracker.StartTable("users")
	tracker.StartTable("posts")
	tracker.StartTable("comments")

	tracker.mu.Lock()
	if len(tracker.activeTables) != 3 {
		t.Errorf("activeTables count = %d, want 3", len(tracker.activeTables))
	}
	tracker.mu.Unlock()

	tracker.EndTable("posts")
	tracker.mu.Lock()
	if len(tracker.activeTables) != 2 {
		t.Errorf("activeTables count = %d, want 2", len(tracker.activeTables))
	}
	tracker.mu.Unlock()
}

func TestTracker_TableComplete(t *testing.T) {
	tracker := New()

	tracker.TableComplete()
	tracker.TableComplete()
	tracker.TableComplete()

	if got := tracker.tablesComplete.Load(); got != 3 {
		t.Errorf("tablesComplete = %d, want 3", got)
	}
}

func TestTracker_TableFailed(t *testing.T) {
	tracker := New()

	tracker.TableFailed()
	tracker.TableFailed()

	if got := tracker.tablesFailed.Load(); got != 2 {
		t.Errorf("tablesFailed = %d, want 2", got)
	}
}

func TestTracker_SetPhase(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 0)

	tracker.SetPhase("transferring")

	if tracker.phase != "transferring" {
		t.Errorf("phase = %q, want %q", tracker.phase, "transferring")
	}

	// Check that immediate report was sent
	output := buf.String()
	if !strings.Contains(output, `"phase":"transferring"`) {
		t.Errorf("expected phase in output, got: %s", output)
	}
}

func TestTracker_SetReporter(t *testing.T) {
	tracker := New()

	if tracker.jsonMode {
		t.Error("expected jsonMode to be false initially")
	}

	tracker.SetReporter(&NullReporter{}, 0)

	if !tracker.jsonMode {
		t.Error("expected jsonMode to be true after setting reporter")
	}
}

func TestTracker_SetReporter_WithInterval(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 50*time.Millisecond)
	tracker.SetTotal(1000)
	tracker.SetTablesTotal(5)

	// Wait for at least one tick
	time.Sleep(100 * time.Millisecond)

	// Stop the reporter
	tracker.Finish()

	output := buf.String()
	if output == "" {
		t.Error("expected some output from periodic reporting")
	}
}

func TestTracker_EmitProgress(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 0)
	tracker.SetTotal(1000)
	tracker.SetTablesTotal(10)
	tracker.SetPhase("transferring")

	tracker.Add(500)
	tracker.StartTable("users")
	tracker.TableComplete()
	tracker.TableFailed()

	// Force emit
	tracker.emitProgress()

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 1 {
		t.Fatal("expected at least one JSON line")
	}

	// Parse the last line
	var update ProgressUpdate
	lastLine := lines[len(lines)-1]
	if err := json.Unmarshal([]byte(lastLine), &update); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if update.Phase != "transferring" {
		t.Errorf("phase = %q, want %q", update.Phase, "transferring")
	}
	if update.RowsTransferred != 500 {
		t.Errorf("rows_transferred = %d, want 500", update.RowsTransferred)
	}
	if update.RowsTotal != 1000 {
		t.Errorf("rows_total = %d, want 1000", update.RowsTotal)
	}
	if update.TablesTotal != 10 {
		t.Errorf("tables_total = %d, want 10", update.TablesTotal)
	}
	if update.TablesComplete != 1 {
		t.Errorf("tables_complete = %d, want 1", update.TablesComplete)
	}
	if update.ErrorCount != 1 {
		t.Errorf("error_count = %d, want 1", update.ErrorCount)
	}
	if update.ProgressPct != 50.0 {
		t.Errorf("progress_pct = %f, want 50.0", update.ProgressPct)
	}
}

func TestTracker_Finish(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 50*time.Millisecond)
	tracker.SetTotal(100)
	tracker.Add(100)

	tracker.Finish()

	// Check that final update was sent with "completed" phase
	output := buf.String()
	if !strings.Contains(output, `"phase":"completed"`) {
		t.Errorf("expected completed phase in output, got: %s", output)
	}
}

func TestTracker_Close(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 0)
	tracker.Close()

	// Reporter should be closed
	if !reporter.closed {
		t.Error("expected reporter to be closed")
	}
}

func TestTracker_NoReporter(t *testing.T) {
	tracker := New()

	// These should not panic with nil reporter
	tracker.SetPhase("testing")
	tracker.emitProgress()
	tracker.emitProgressImmediate()
}

// TestTracker_Stress_ReporterAndMutators exercises the race detector against
// concurrent mutation of every shared field while a reporter goroutine is
// emitting. Regression guard for #249.
func TestTracker_Stress_ReporterAndMutators(t *testing.T) {
	tracker := New()
	tracker.SetReporter(&NullReporter{}, time.Millisecond)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	mutators := []func(){
		func() { tracker.SetTotal(1000) },
		func() { tracker.SetTablesTotal(10) },
		func() { tracker.SetPhase("transferring") },
		func() { tracker.Add(1) },
		func() { tracker.StartTable("t") },
		func() { tracker.EndTable("t") },
		func() { tracker.TableComplete() },
		func() { tracker.TableFailed() },
		func() { _ = tracker.TablesTotal() },
		func() { _ = tracker.TablesComplete() },
		func() { _ = tracker.TablesFailed() },
		func() { _ = tracker.Current() },
	}

	for _, fn := range mutators {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					f()
				}
			}
		}(fn)
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	tracker.Finish()
}

func TestTracker_ProgressPct_ZeroTotal(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewJSONReporter(&buf, 0)

	tracker := New()
	tracker.SetReporter(reporter, 0)
	tracker.SetTotal(0) // Zero total

	tracker.emitProgress()

	output := buf.String()
	var update ProgressUpdate
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &update); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Should not panic and progress should be 0
	if update.ProgressPct != 0 {
		t.Errorf("progress_pct = %f, want 0", update.ProgressPct)
	}
}
