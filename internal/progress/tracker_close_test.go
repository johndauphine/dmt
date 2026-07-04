package progress

import (
	"runtime"
	"testing"
	"time"
)

// TestCloseStopsReportLoop guards the fix for the goroutine leak the WebUI
// exposed: SetReporter starts a periodic reportLoop, and a long-lived caller
// that builds one tracker per run must be able to stop it via Close().
func TestCloseStopsReportLoop(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		tr := New()
		tr.SetReporter(&NullReporter{}, 5*time.Millisecond)
		tr.Close()
	}
	// Give any still-winding-down loops a moment to exit.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Errorf("reportLoop goroutines leaked: before=%d after=%d", before, after)
	}
}

// TestFinishThenCloseIdempotent verifies stopLoop's once-guard: the transfer
// runner calls Finish() and the orchestrator's Close() also stops the loop;
// neither double-close of stopReport may panic.
func TestFinishThenCloseIdempotent(t *testing.T) {
	tr := New()
	tr.SetReporter(&NullReporter{}, 5*time.Millisecond)
	tr.Finish()
	tr.Close()
	tr.Close() // repeated Close is still safe
}

// TestCloseWithoutReporterLoop verifies Close is safe when no periodic loop was
// ever started (interval 0 / no reporter).
func TestCloseWithoutReporterLoop(t *testing.T) {
	tr := New()
	tr.Close()
	tr2 := New()
	tr2.SetReporter(&NullReporter{}, 0) // no loop
	tr2.Close()
}

// TestReusableAcrossSetReporter verifies a tracker can be re-armed: repeated
// SetReporter+Close cycles must not leak the reportLoop (SetReporter stops any
// prior loop and resets the once-guard).
func TestReusableAcrossSetReporter(t *testing.T) {
	before := runtime.NumGoroutine()
	tr := New()
	for i := 0; i < 30; i++ {
		tr.SetReporter(&NullReporter{}, 5*time.Millisecond)
		tr.Close()
	}
	// Re-arm without an intervening Close, then stop.
	tr.SetReporter(&NullReporter{}, 5*time.Millisecond)
	tr.SetReporter(&NullReporter{}, 5*time.Millisecond) // stops the first loop
	tr.Close()
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Errorf("reportLoop leaked across re-arm: before=%d after=%d", before, after)
	}
}
