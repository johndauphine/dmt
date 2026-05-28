package aicopilot

import (
	"sort"

	"github.com/johndauphine/dmt/internal/driver"
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
				"ai.api_key", "slack.webhook_url",
			},
			ScrubbedText: true,
		},
	}
	payload.RecentRuns = buildPerformanceHistoryRuns(recent, 5)
	payload.RuntimeAdjustments = buildRuntimeAdjustmentSummaries(adjustments, 5)
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
			Reasoning:            logging.Scrub(r.AIReasoning),
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
	for _, a := range adjustments[:limit] {
		out = append(out, RuntimeAdjustmentSummary{
			Action:           logging.Scrub(a.Action),
			Adjustments:      filterRuntimeAdjustments(a.Adjustments),
			ThroughputBefore: a.ThroughputBefore,
			ThroughputAfter:  a.ThroughputAfter,
			EffectPercent:    a.EffectPercent,
			Reasoning:        logging.Scrub(a.Reasoning),
		})
	}
	return out
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

func allowedPerformanceKnobSet() map[string]bool {
	out := make(map[string]bool)
	for _, k := range allowedPerformanceKnobs() {
		out[k] = true
	}
	return out
}
