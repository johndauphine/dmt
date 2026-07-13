package orchestrator

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/monitor"
)

type runtimeAdjustmentCaptureState struct {
	checkpoint.StateBackend
	runID string
	saved checkpoint.RuntimeAdjustmentRecord
}

func (*runtimeAdjustmentCaptureState) GetRuntimeAdjustments(int) ([]checkpoint.RuntimeAdjustmentRecord, error) {
	return nil, nil
}

func (s *runtimeAdjustmentCaptureState) SaveRuntimeAdjustment(runID string, record checkpoint.RuntimeAdjustmentRecord) error {
	s.runID = runID
	s.saved = record
	return nil
}

func TestRuntimeAdjustmentRecorderPropagatesEffectMeasured(t *testing.T) {
	state := &runtimeAdjustmentCaptureState{}
	recorder := newRuntimeAdjustmentRecorder(state, "run-1")
	now := time.Date(2026, 7, 12, 15, 30, 0, 0, time.UTC)

	err := recorder.record("ignored", monitor.AdjustmentRecord{
		AdjustmentNumber: 7,
		Timestamp:        now,
		Action:           "chunk_size",
		Adjustments:      map[string]int{"chunk_size": 7500},
		ThroughputBefore: 100,
		ThroughputAfter:  125,
		EffectPercent:    25,
		EffectMeasured:   true,
		CPUBefore:        30,
		CPUAfter:         35,
		MemoryBefore:     60,
		MemoryAfter:      55,
		Reasoning:        "measured test adjustment",
		Confidence:       "deterministic",
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if state.runID != "run-1" {
		t.Fatalf("saved run ID = %q, want run-1", state.runID)
	}
	if !state.saved.EffectMeasured {
		t.Fatalf("saved record lost measurement state: %+v", state.saved)
	}
	if state.saved.ThroughputAfter != 125 || state.saved.EffectPercent != 25 ||
		state.saved.CPUAfter != 35 || state.saved.MemoryAfter != 55 {
		t.Fatalf("saved measured values = %+v", state.saved)
	}
}
