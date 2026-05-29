package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/transfer"
)

type runtimeAdjustmentRecorder struct {
	state checkpoint.StateBackend
	runID string
	mu    sync.Mutex
	last  int
}

func newRuntimeAdjustmentRecorder(state checkpoint.StateBackend, runID string) *runtimeAdjustmentRecorder {
	r := &runtimeAdjustmentRecorder{state: state, runID: runID}
	records, err := state.GetAIAdjustments(0)
	if err != nil {
		logging.Warn("failed to load runtime adjustment history for %s: %v", runID, err)
		return r
	}
	for _, record := range records {
		if record.RunID == runID && record.AdjustmentNumber > r.last {
			r.last = record.AdjustmentNumber
		}
	}
	return r
}

func (r *runtimeAdjustmentRecorder) nextNumber() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last++
	return r.last
}

func (r *runtimeAdjustmentRecorder) record(_ string, record monitor.AdjustmentRecord) error {
	if r == nil || r.state == nil || r.runID == "" {
		return nil
	}
	if record.AdjustmentNumber <= 0 {
		record.AdjustmentNumber = r.nextNumber()
	}
	return r.state.SaveAIAdjustment(r.runID, checkpoint.AIAdjustmentRecord{
		AdjustmentNumber: record.AdjustmentNumber,
		Timestamp:        record.Timestamp,
		Action:           record.Action,
		Adjustments:      record.Adjustments,
		ThroughputBefore: record.ThroughputBefore,
		ThroughputAfter:  record.ThroughputAfter,
		EffectPercent:    record.EffectPercent,
		CPUBefore:        record.CPUBefore,
		CPUAfter:         record.CPUAfter,
		MemoryBefore:     record.MemoryBefore,
		MemoryAfter:      record.MemoryAfter,
		Reasoning:        record.Reasoning,
		Confidence:       record.Confidence,
	})
}

type recordingWriteErrorAdjuster struct {
	base     transfer.WriteErrorAdjuster
	recorder *runtimeAdjustmentRecorder
}

func (a recordingWriteErrorAdjuster) EvaluateWriteError(ctx context.Context, errCtx transfer.WriteErrorContext) int {
	next := a.base.EvaluateWriteError(ctx, errCtx)
	if next <= 0 || a.recorder == nil {
		return next
	}
	_ = a.recorder.record("", monitor.AdjustmentRecord{
		AdjustmentNumber: a.recorder.nextNumber(),
		Timestamp:        time.Now(),
		Action:           "chunk_size",
		Adjustments:      map[string]int{"chunk_size": next},
		Reasoning:        fmt.Sprintf("structural write error reduced chunk_size from %d to %d", errCtx.ChunkSize, next),
		Confidence:       "deterministic",
	})
	return next
}
