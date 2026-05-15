package orchestrator

import (
	"github.com/johndauphine/dmt/internal/checkpoint"
)

// fallbackSink is the observability.FallbackSink adapter that pins
// every RecordFallback write to the current run's row in the
// checkpoint's fallback_events table. The orchestrator installs an
// instance at Run/Resume start and clears it at teardown so a
// long-running process (TUI, sidecar) doesn't keep writing to a
// completed run's state.
//
// state is the backend the run is using (SQLite or FileState — both
// implement SaveFallbackEvent). runID scopes every write so cross-
// process `dmt status` queries can read just this run's counts (#176).
type fallbackSink struct {
	state checkpoint.StateBackend
	runID string
}

func newFallbackSink(state checkpoint.StateBackend, runID string) *fallbackSink {
	return &fallbackSink{state: state, runID: runID}
}

// SaveFallbackEvent forwards an in-process RecordFallback to the
// checkpoint backend. Errors propagate to the observability package,
// which logs them at debug — a state-write failure is not worth
// failing the migration over, and the in-memory + Prometheus counters
// still observed the event.
func (s *fallbackSink) SaveFallbackEvent(surface, fingerprint string) error {
	if s == nil || s.state == nil {
		return nil
	}
	return s.state.SaveFallbackEvent(s.runID, surface, fingerprint)
}
