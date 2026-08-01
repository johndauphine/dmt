package webui

import (
	"context"
	"sync"
	"time"
)

// runKind distinguishes a fresh migration from a resume.
type runKind string

const (
	kindRun    runKind = "run"
	kindResume runKind = "resume"
)

// runState is the serializable status of the current or last migration driven
// from the WebUI. The final counts are stamped at completion so a client that
// loads /api/run for a finished run (no live progress stream) can still show
// the totals (#591).
type runState struct {
	ID              string  `json:"id"`
	Kind            runKind `json:"kind"`
	Status          string  `json:"status"` // running | completed | failed | cancelled
	StartedAt       string  `json:"started_at"`
	EndedAt         string  `json:"ended_at,omitempty"`
	Error           string  `json:"error,omitempty"`
	RowsTransferred int64   `json:"rows_transferred,omitempty"`
	RowsPerSecond   int64   `json:"rows_per_second,omitempty"`
	TablesComplete  int     `json:"tables_complete,omitempty"`
	TablesTotal     int     `json:"tables_total,omitempty"`
	TablesFailed    int     `json:"tables_failed,omitempty"`
}

// runCounts is the final tally stamped onto a finished run.
type runCounts struct {
	rows, rps                             int64
	tablesDone, tablesTotal, tablesFailed int
}

// runManager enforces single-flight: at most one migration runs at a time
// (mirroring the TUI's ModeMigration guard). It owns the active run's cancel
// func and remembers the last finished run so late clients can sync state.
type runManager struct {
	mu     sync.Mutex
	active *activeRun
	last   *runState
}

type activeRun struct {
	state  runState
	cancel context.CancelFunc
}

func newRunManager() *runManager { return &runManager{} }

// start begins tracking a run. ok is false when one is already active, in which
// case the caller must not proceed (409).
func (m *runManager) start(kind runKind, id string, cancel context.CancelFunc) (runState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		return runState{}, false
	}
	st := runState{
		ID:        id,
		Kind:      kind,
		Status:    "running",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	m.active = &activeRun{state: st, cancel: cancel}
	return st, true
}

// complete records the run's terminal state and remembers it as the last run,
// but KEEPS the single-flight slot held (m.active stays set). This lets the
// caller quiesce the event hub and publish the terminal event before releasing
// the slot with clear(), so a successor run can't interleave and have its
// progress suppressed by this run's teardown. During the window a new run is
// correctly rejected with 409.
func (m *runManager) complete(status, errMsg string, c runCounts) runState {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := runState{}
	if m.active != nil {
		st = m.active.state
	}
	st.Status = status
	st.EndedAt = time.Now().UTC().Format(time.RFC3339)
	st.Error = errMsg
	st.RowsTransferred = c.rows
	st.RowsPerSecond = c.rps
	st.TablesComplete = c.tablesDone
	st.TablesTotal = c.tablesTotal
	st.TablesFailed = c.tablesFailed
	m.last = &st
	if m.active != nil {
		m.active.state = st
	}
	return st
}

// clear releases the single-flight slot after complete().
func (m *runManager) clear() {
	m.mu.Lock()
	m.active = nil
	m.mu.Unlock()
}

// finish is complete()+clear() in one step, for callers/tests that don't need
// the intervening window.
func (m *runManager) finish(status, errMsg string) runState {
	st := m.complete(status, errMsg, runCounts{})
	m.clear()
	return st
}

// cancel signals the active run's context. Returns false when nothing is
// running.
func (m *runManager) cancel() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		return false
	}
	m.active.cancel()
	return true
}

// state returns the active run, or the last finished run, or nil.
func (m *runManager) state() *runState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		s := m.active.state
		return &s
	}
	return m.last
}

// isRunning reports whether a migration is currently in flight. Unlike
// state(), which keeps returning the last finished run's terminal snapshot
// indefinitely, this is false as soon as a run completes — the correct signal
// for "is it safe to exit idle" (--gui's idle-shutdown watchdog).
func (m *runManager) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active != nil
}
