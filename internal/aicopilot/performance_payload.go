package aicopilot

import (
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
)

func BuildPerformancePayload(input driver.AutoTuneInput, suggestions driver.SmartConfigSuggestions, recent []driver.AITuningRecord, adjustments []driver.AIAdjustmentRecord) PerformancePayload {
	payload := PerformancePayload{
		PromptVersion: PerformancePromptVersion,
		Task:          "Explain deterministic DMT smartconfig and runtime tuning choices. Deterministic knobs and values are authoritative; AI may rank evidence but must not invent knobs or values.",
		Workload: PerformanceWorkloadSummary{
			SourceDBType:          input.DatabaseType,
			TargetDBType:          input.TargetType,
			TargetMode:            input.TargetMode,
			Platform:              input.Platform,
			CPUCores:              input.CPUCores,
			MemoryGB:              input.MemoryGB,
			AvailableMemoryMB:     input.AvailableMemoryMB,
			MaxMemoryMB:           input.MaxMemoryMB,
			TotalTables:           input.TotalTables,
			TotalRows:             input.TotalRows,
			AvgRowBytes:           input.AvgRowBytes,
			UncappedAvgRowBytes:   input.UncappedAvgRowBytes,
			LargestTableBytes:     input.LargestTableBytes,
			EstimatedMemoryMB:     suggestions.EstimatedMemMB,
			SourceIdentityOmitted: true,
			TargetIdentityOmitted: true,
		},
		DeterministicTier:      logging.Scrub(suggestions.Tier),
		DeterministicReasoning: logging.Scrub(suggestions.Reasoning),
		DeterministicKnobs: PerformanceKnobs{
			Workers:              suggestions.Workers,
			ChunkSize:            suggestions.ChunkSizeRecommendation,
			ReadAheadBuffers:     suggestions.ReadAheadBuffers,
			WriteAheadWriters:    suggestions.WriteAheadWriters,
			ParallelReaders:      suggestions.ParallelReaders,
			MaxPartitions:        suggestions.MaxPartitions,
			LargeTableThreshold:  suggestions.LargeTableThreshold,
			MaxSourceConnections: suggestions.MaxSourceConnections,
			MaxTargetConnections: suggestions.MaxTargetConnections,
			UpsertMergeChunkSize: suggestions.UpsertMergeChunkSize,
			CheckpointFrequency:  suggestions.CheckpointFrequency,
			MaxRetries:           suggestions.MaxRetries,
		},
		AllowedKnobs: allowedPerformanceKnobs(),
		Redaction: RedactionSummary{
			OmittedFields: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database", "source.schema",
				"target.host", "target.port", "target.user", "target.password", "target.database", "target.schema",
				"runtime_adjustments.reasoning", "ai.api_key", "slack.webhook_url",
				"recent_runs.reasoning",
			},
			ScrubbedText: true,
		},
	}
	payload.RecentRuns = buildPerformanceHistoryRuns(recent, 5)
	payload.RuntimeAdjustments = buildRuntimeAdjustmentSummaries(adjustments, 5)
	payload.DatabaseTuning = buildDBTuningSummaries(suggestions, 5)
	payload.ObservedMetrics = buildObservedPerformanceMetrics(suggestions, recent, payload.RuntimeAdjustments)
	payload.AllowedFindingTargets = allowedPerformanceFindingTargets(payload.DatabaseTuning)
	return payload
}

func buildPerformanceHistoryRuns(recent []driver.AITuningRecord, limit int) []PerformanceHistoryRun {
	if limit <= 0 || len(recent) == 0 {
		return nil
	}
	if len(recent) < limit {
		limit = len(recent)
	}
	out := make([]PerformanceHistoryRun, 0, limit)
	for _, r := range recent[:limit] {
		out = append(out, PerformanceHistoryRun{
			Timestamp:    formatPerformanceTimestamp(r.Timestamp),
			SourceDBType: r.SourceDBType,
			TargetDBType: r.TargetDBType,
			Workload: PerformanceWorkloadSummary{
				SourceDBType:          r.SourceDBType,
				TargetDBType:          r.TargetDBType,
				Platform:              r.Platform,
				CPUCores:              r.CPUCores,
				MemoryGB:              r.MemoryGB,
				TotalTables:           r.TotalTables,
				TotalRows:             r.TotalRows,
				AvgRowBytes:           r.AvgRowSizeBytes,
				EstimatedMemoryMB:     r.EstimatedMemoryMB,
				SourceIdentityOmitted: true,
				TargetIdentityOmitted: true,
			},
			Knobs: PerformanceKnobs{
				Workers:              r.Workers,
				ChunkSize:            r.ChunkSize,
				ReadAheadBuffers:     r.ReadAheadBuffers,
				WriteAheadWriters:    r.WriteAheadWriters,
				ParallelReaders:      r.ParallelReaders,
				MaxPartitions:        r.MaxPartitions,
				LargeTableThreshold:  r.LargeTableThreshold,
				MaxSourceConnections: r.MaxSourceConnections,
				MaxTargetConnections: r.MaxTargetConnections,
			},
			FinalThroughput:      r.FinalThroughput,
			FinalDurationSeconds: r.FinalDurationSecs,
			ChunkRetryCount:      r.ChunkRetryCount,
		})
	}
	return out
}

func buildRuntimeAdjustmentSummaries(adjustments []driver.AIAdjustmentRecord, limit int) []RuntimeAdjustmentSummary {
	if limit <= 0 || len(adjustments) == 0 {
		return nil
	}
	if len(adjustments) < limit {
		limit = len(adjustments)
	}
	out := make([]RuntimeAdjustmentSummary, 0, limit)
	for _, a := range adjustments {
		if !isDeterministicRuntimeAdjustment(a.Confidence) {
			continue
		}
		out = append(out, RuntimeAdjustmentSummary{
			Action:           runtimeAdjustmentActionForPayload(a),
			Adjustments:      filterRuntimeAdjustments(a.Adjustments),
			ThroughputBefore: a.ThroughputBefore,
			ThroughputAfter:  a.ThroughputAfter,
			EffectPercent:    a.EffectPercent,
			CPUBefore:        a.CPUBefore,
			CPUAfter:         a.CPUAfter,
			MemoryBefore:     a.MemoryBefore,
			MemoryAfter:      a.MemoryAfter,
			Confidence:       normalizeRuntimeAdjustmentConfidence(a.Confidence),
		})
		if len(out) == limit {
			break
		}
	}
	return out
}

func runtimeAdjustmentActionForPayload(a driver.AIAdjustmentRecord) string {
	allowed := allowedPerformanceKnobSet()
	if allowed[a.Action] {
		return a.Action
	}
	for knob := range a.Adjustments {
		if allowed[knob] {
			return knob
		}
	}
	return ""
}

func normalizeRuntimeAdjustmentConfidence(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "deterministic":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func isDeterministicRuntimeAdjustment(confidence string) bool {
	return normalizeRuntimeAdjustmentConfidence(confidence) == "deterministic"
}

func filterRuntimeAdjustments(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	allowed := allowedPerformanceKnobSet()
	out := make(map[string]int, len(in))
	for k, v := range in {
		if allowed[k] {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildDBTuningSummaries(suggestions driver.SmartConfigSuggestions, limit int) []PerformanceDBTuningSummary {
	out := make([]PerformanceDBTuningSummary, 0, 2)
	appendTuning := func(tuning *dbtuning.DatabaseTuning) {
		if tuning == nil {
			return
		}
		summary := PerformanceDBTuningSummary{
			Role:             logging.Scrub(tuning.Role),
			DatabaseType:     logging.Scrub(tuning.DatabaseType),
			TuningPotential:  logging.Scrub(tuning.TuningPotential),
			EstimatedImpact:  limitText(logging.Scrub(tuning.EstimatedImpact), 300),
			RecommendationCt: len(tuning.Recommendations),
		}
		recs := tuning.Recommendations
		if limit > 0 && len(recs) > limit {
			recs = recs[:limit]
		}
		for _, rec := range recs {
			summary.Recommendations = append(summary.Recommendations, PerformanceDBTuningRecommendation{
				Parameter:        limitText(logging.Scrub(rec.Parameter), 120),
				CurrentValue:     scrubTuningValue(rec.CurrentValue),
				RecommendedValue: scrubTuningValue(rec.RecommendedValue),
				Impact:           limitText(logging.Scrub(rec.Impact), 80),
				Priority:         rec.Priority,
				CanApplyRuntime:  rec.CanApplyRuntime,
				RequiresRestart:  rec.RequiresRestart,
				Reason:           limitText(logging.Scrub(rec.Reason), 400),
			})
		}
		out = append(out, summary)
	}
	appendTuning(suggestions.SourceTuning)
	appendTuning(suggestions.TargetTuning)
	if len(out) == 0 {
		return nil
	}
	return out
}

func scrubTuningValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case string,
		bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return limitText(logging.Scrub(fmt.Sprint(v)), 200)
	default:
		return logging.RedactedToken
	}
}

func buildObservedPerformanceMetrics(suggestions driver.SmartConfigSuggestions, recent []driver.AITuningRecord, adjustments []RuntimeAdjustmentSummary) PerformanceObservedMetrics {
	metrics := PerformanceObservedMetrics{
		MaxSourceConnections:   suggestions.MaxSourceConnections,
		MaxTargetConnections:   suggestions.MaxTargetConnections,
		RuntimeAdjustmentCount: len(adjustments),
	}
	var throughputSum float64
	for _, r := range recent {
		if r.FinalThroughput > 0 {
			metrics.RecentSuccessfulRuns++
			throughputSum += r.FinalThroughput
			if r.FinalThroughput > metrics.BestThroughputRows {
				metrics.BestThroughputRows = r.FinalThroughput
			}
		}
		if r.ChunkRetryCount > 0 {
			metrics.RunsWithRetries++
			metrics.TotalChunkRetries += r.ChunkRetryCount
		}
	}
	if metrics.RecentSuccessfulRuns > 0 {
		metrics.AverageThroughputRows = throughputSum / float64(metrics.RecentSuccessfulRuns)
	}
	if len(recent) > 0 {
		metrics.ChunkRetryRate = float64(metrics.RunsWithRetries) / float64(len(recent))
	}
	return metrics
}

func allowedPerformanceKnobs() []string {
	knobs := []string{
		"workers",
		"chunk_size",
		"read_ahead_buffers",
		"write_ahead_writers",
		"parallel_readers",
		"max_partitions",
		"large_table_threshold",
		"max_source_connections",
		"max_target_connections",
		"upsert_merge_chunk_size",
		"checkpoint_frequency",
		"max_retries",
	}
	sort.Strings(knobs)
	return knobs
}

func allowedPerformanceFindingTargets(databaseTuning []PerformanceDBTuningSummary) []string {
	targets := append([]string{}, allowedPerformanceKnobs()...)
	for _, tuning := range databaseTuning {
		for _, rec := range tuning.Recommendations {
			parameter := strings.ToLower(strings.TrimSpace(rec.Parameter))
			if parameter != "" {
				targets = append(targets, "db_tuning."+parameter)
			}
		}
	}
	sort.Strings(targets)
	out := targets[:0]
	seen := map[string]bool{}
	for _, target := range targets {
		if seen[target] {
			continue
		}
		seen[target] = true
		out = append(out, target)
	}
	return out
}

func allowedPerformanceKnobSet() map[string]bool {
	out := make(map[string]bool)
	for _, k := range allowedPerformanceKnobs() {
		out[k] = true
	}
	return out
}
