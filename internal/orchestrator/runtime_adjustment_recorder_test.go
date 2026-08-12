package orchestrator

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/monitor"
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

func TestRuntimeAdjustmentRecorderCompletesCheckpointObservationInPlace(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.CreateRun("run-observed", "public", "dbo", nil, "", ""); err != nil {
		t.Fatal(err)
	}

	recorder := newRuntimeAdjustmentRecorder(state, "run-observed")
	number := recorder.nextNumber()
	initial := monitor.AdjustmentRecord{
		AdjustmentNumber: number,
		Timestamp:        time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
		Action:           "chunk_size",
		Adjustments:      map[string]int{"chunk_size": 7500},
		ThroughputBefore: 100,
		CPUBefore:        30,
		MemoryBefore:     60,
		Reasoning:        "initial deterministic decision",
		Confidence:       "deterministic",
	}
	if err := recorder.record("run-observed", initial); err != nil {
		t.Fatalf("initial record: %v", err)
	}
	measured := initial
	measured.EffectMeasured = true
	measured.ThroughputAfter = 125
	measured.EffectPercent = 25
	measured.CPUAfter = 35
	measured.MemoryAfter = 55
	measured.Reasoning = "observational three-sample delta"
	if err := recorder.record("run-observed", measured); err != nil {
		t.Fatalf("measured record: %v", err)
	}

	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("checkpoint observation rows = %d, want 1: %+v", len(records), records)
	}
	got := records[0]
	if got.AdjustmentNumber != number || got.Action != initial.Action || !got.EffectMeasured ||
		got.ThroughputBefore != 100 || got.CPUBefore != 30 || got.MemoryBefore != 60 ||
		got.ThroughputAfter != 125 || got.EffectPercent != 25 || got.CPUAfter != 35 || got.MemoryAfter != 55 {
		t.Fatalf("checkpoint observation round trip = %+v", got)
	}
	if got.Reasoning != initial.Reasoning {
		t.Fatalf("measured upsert replaced initial decision rationale: %q", got.Reasoning)
	}
	if next := recorder.nextNumber(); next != number+1 {
		t.Fatalf("measured upsert consumed adjustment number: next=%d want=%d", next, number+1)
	}
}
