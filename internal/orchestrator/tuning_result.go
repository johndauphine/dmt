package orchestrator

import (
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

func (o *Orchestrator) recordSuccessfulTuningResult(totalRows int64, transferDuration time.Duration) {
	if o.state == nil {
		return
	}
	transferDurationSecs := transferDuration.Seconds()
	if transferDurationSecs <= 0 {
		return
	}

	transferThroughput := float64(totalRows) / transferDurationSecs
	if err := o.state.UpdateAITuningResult(transferThroughput, transferDurationSecs, o.lastChunkRetryCount); err != nil {
		logging.Debug("Failed to update AI tuning result: %v", err)
	}
}
