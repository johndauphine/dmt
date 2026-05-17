package orchestrator

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/source"
)

func (o *Orchestrator) transferAll(ctx context.Context, runID string, tables []source.Table, resume bool) ([]TableFailure, error) {
	// Build jobs using JobBuilder
	builder := NewJobBuilder(o.sourcePool, o.state, o.config)
	buildResult, err := builder.Build(ctx, runID, tables)
	if err != nil {
		return nil, fmt.Errorf("building jobs: %w", err)
	}

	// Execute jobs using TransferRunner. Error diagnosis runs through the
	// deterministic catalog in internal/driver/errordiag (#173); the
	// former AI-driven diagnoser was removed to avoid sending error
	// messages (which routinely contain row data) to a third-party LLM.
	runner := NewTransferRunner(
		o.sourcePool,
		o.targetPool,
		o.state,
		o.config,
		o.progress,
		o.notifier,
		o.targetMode,
	)

	result, err := runner.Run(ctx, runID, buildResult, tables, resume)
	if err != nil {
		return nil, err
	}

	// Stash chunk retry count so the orchestrator can persist it with the run's
	// final tuning result. Read by the UpdateAITuningResult call sites in Run/Resume.
	o.lastChunkRetryCount = result.ChunkRetryCount

	return result.TableFailures, nil
}
