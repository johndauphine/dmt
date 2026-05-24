package orchestrator

import (
	"context"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// ReviewPreflightWithAI asks the configured AI provider for an advisory
// readiness review over the deterministic health/preflight result. Failures
// never change the deterministic preflight outcome.
func (o *Orchestrator) ReviewPreflightWithAI(ctx context.Context, result *HealthCheckResult) *aicopilot.PreflightReview {
	var findings []driver.PreFlightFinding
	if result != nil {
		findings = result.PreFlightFindings
	}
	payload := aicopilot.BuildPreflightPayload(o.config, healthSummaryForAI(result), findings)
	payload.RecentRuns = o.recentRunsForAIReview()

	mapper := driver.GetAIMapper()
	if mapper == nil {
		return aicopilot.UnavailablePreflightReview("no AI provider configured in secrets", payload)
	}

	review, err := aicopilot.GeneratePreflightReview(ctx, mapper, payload)
	if err != nil {
		logging.Warn("AI preflight review failed: %v", err)
		return aicopilot.ErrorPreflightReview(mapper.ProviderName(), mapper.Model(), err, payload)
	}
	logging.Info("AI preflight review completed: readiness=%s advisory_findings=%d provider=%s model=%s",
		review.Readiness, len(review.Findings), review.Provider, review.Model)
	return review
}

func healthSummaryForAI(result *HealthCheckResult) aicopilot.HealthSummary {
	if result == nil {
		return aicopilot.HealthSummary{}
	}
	return aicopilot.HealthSummary{
		Timestamp:        result.Timestamp,
		SourceConnected:  result.SourceConnected,
		SourceLatencyMs:  result.SourceLatencyMs,
		SourceDBType:     result.SourceDBType,
		SourceTableCount: result.SourceTableCount,
		SourceError:      result.SourceError,
		TargetConnected:  result.TargetConnected,
		TargetLatencyMs:  result.TargetLatencyMs,
		TargetDBType:     result.TargetDBType,
		TargetError:      result.TargetError,
		Healthy:          result.Healthy,
	}
}

func (o *Orchestrator) recentRunsForAIReview() []aicopilot.RunHistorySummary {
	if o == nil || o.state == nil || o.config == nil {
		return nil
	}
	records, err := o.state.GetAITuningHistory(3, driver.Canonicalize(o.config.Source.Type), driver.Canonicalize(o.config.Target.Type))
	if err != nil {
		logging.Debug("AI preflight review: recent run history unavailable: %v", err)
		return nil
	}
	out := make([]aicopilot.RunHistorySummary, 0, len(records))
	for _, r := range records {
		out = append(out, runHistorySummaryForAI(r))
	}
	return out
}

func runHistorySummaryForAI(r checkpoint.AITuningRecord) aicopilot.RunHistorySummary {
	ts := ""
	if !r.Timestamp.IsZero() {
		ts = r.Timestamp.UTC().Format(time.RFC3339)
	}
	return aicopilot.RunHistorySummary{
		Timestamp:         ts,
		TotalTables:       r.TotalTables,
		TotalRows:         r.TotalRows,
		AvgRowSizeBytes:   r.AvgRowSizeBytes,
		Workers:           r.Workers,
		ChunkSize:         r.ChunkSize,
		WriteAheadWriters: r.WriteAheadWriters,
		ParallelReaders:   r.ParallelReaders,
		FinalThroughput:   r.FinalThroughput,
		FinalDurationSecs: r.FinalDurationSecs,
		ChunkRetryCount:   r.ChunkRetryCount,
	}
}
