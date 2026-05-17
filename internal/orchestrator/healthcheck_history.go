package orchestrator

import (
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
)

// stateHistoryAdapter adapts checkpoint.StateBackend to driver.TuningHistoryProvider.
type stateHistoryAdapter struct {
	state checkpoint.StateBackend
}

// GetAIAdjustments returns recent runtime AI adjustments from migrations.
func (a *stateHistoryAdapter) GetAIAdjustments(limit int) ([]driver.AIAdjustmentRecord, error) {
	records, err := a.state.GetAIAdjustments(limit)
	if err != nil {
		return nil, err
	}

	// Convert checkpoint records to driver records
	result := make([]driver.AIAdjustmentRecord, len(records))
	for i, r := range records {
		result[i] = driver.AIAdjustmentRecord{
			Action:           r.Action,
			Adjustments:      r.Adjustments,
			ThroughputBefore: r.ThroughputBefore,
			ThroughputAfter:  r.ThroughputAfter,
			EffectPercent:    r.EffectPercent,
			Reasoning:        r.Reasoning,
		}
	}
	return result, nil
}

// GetAITuningHistory returns recent tuning recommendations from analyze.
func (a *stateHistoryAdapter) GetAITuningHistory(limit int, sourceType, targetType string) ([]driver.AITuningRecord, error) {
	records, err := a.state.GetAITuningHistory(limit, sourceType, targetType)
	if err != nil {
		return nil, err
	}

	// Convert checkpoint records to driver records
	result := make([]driver.AITuningRecord, len(records))
	for i, r := range records {
		result[i] = driver.AITuningRecord{
			Timestamp:            r.Timestamp,
			SourceDBType:         r.SourceDBType,
			TargetDBType:         r.TargetDBType,
			TotalTables:          r.TotalTables,
			TotalRows:            r.TotalRows,
			AvgRowSizeBytes:      r.AvgRowSizeBytes,
			CPUCores:             r.CPUCores,
			MemoryGB:             r.MemoryGB,
			Workers:              r.Workers,
			ChunkSize:            r.ChunkSize,
			ReadAheadBuffers:     r.ReadAheadBuffers,
			WriteAheadWriters:    r.WriteAheadWriters,
			ParallelReaders:      r.ParallelReaders,
			MaxPartitions:        r.MaxPartitions,
			LargeTableThreshold:  r.LargeTableThreshold,
			MaxSourceConnections: r.MaxSourceConns,
			MaxTargetConnections: r.MaxTargetConns,
			EstimatedMemoryMB:    r.EstimatedMemoryMB,
			AIReasoning:          r.AIReasoning,
			WasAIUsed:            r.WasAIUsed,
			FinalThroughput:      r.FinalThroughput,
			FinalDurationSecs:    r.FinalDurationSecs,
			ChunkRetryCount:      r.ChunkRetryCount,
			// #144 regime fields
			Platform:                r.Platform,
			TargetSharedBuffersMB:   r.TargetSharedBuffersMB,
			TargetSyncCommit:        r.TargetSyncCommit,
			TargetFsync:             r.TargetFsync,
			TargetFullPageWrites:    r.TargetFullPageWrites,
			TargetMaxWALSizeMB:      r.TargetMaxWALSizeMB,
			TargetWALLevel:          r.TargetWALLevel,
			SourceMaxServerMemoryMB: r.SourceMaxServerMemoryMB,
			// #215 workload identity
			SourceHost:     r.SourceHost,
			SourcePort:     r.SourcePort,
			SourceDatabase: r.SourceDatabase,
			SourceSchema:   r.SourceSchema,
			TargetHost:     r.TargetHost,
			TargetPort:     r.TargetPort,
			TargetDatabase: r.TargetDatabase,
			TargetSchema:   r.TargetSchema,
		}
	}
	return result, nil
}

// SaveAITuning saves a tuning recommendation for future reference.
func (a *stateHistoryAdapter) SaveAITuning(record driver.AITuningRecord) error {
	return a.state.SaveAITuning(checkpoint.AITuningRecord{
		Timestamp:           record.Timestamp,
		SourceDBType:        record.SourceDBType,
		TargetDBType:        record.TargetDBType,
		TotalTables:         record.TotalTables,
		TotalRows:           record.TotalRows,
		AvgRowSizeBytes:     record.AvgRowSizeBytes,
		CPUCores:            record.CPUCores,
		MemoryGB:            record.MemoryGB,
		Workers:             record.Workers,
		ChunkSize:           record.ChunkSize,
		ReadAheadBuffers:    record.ReadAheadBuffers,
		WriteAheadWriters:   record.WriteAheadWriters,
		ParallelReaders:     record.ParallelReaders,
		MaxPartitions:       record.MaxPartitions,
		LargeTableThreshold: record.LargeTableThreshold,
		MaxSourceConns:      record.MaxSourceConnections,
		MaxTargetConns:      record.MaxTargetConnections,
		EstimatedMemoryMB:   record.EstimatedMemoryMB,
		AIReasoning:         record.AIReasoning,
		WasAIUsed:           record.WasAIUsed,
		// #144 regime fields
		Platform:                record.Platform,
		TargetSharedBuffersMB:   record.TargetSharedBuffersMB,
		TargetSyncCommit:        record.TargetSyncCommit,
		TargetFsync:             record.TargetFsync,
		TargetFullPageWrites:    record.TargetFullPageWrites,
		TargetMaxWALSizeMB:      record.TargetMaxWALSizeMB,
		TargetWALLevel:          record.TargetWALLevel,
		SourceMaxServerMemoryMB: record.SourceMaxServerMemoryMB,
		// #215 workload identity
		SourceHost:     record.SourceHost,
		SourcePort:     record.SourcePort,
		SourceDatabase: record.SourceDatabase,
		SourceSchema:   record.SourceSchema,
		TargetHost:     record.TargetHost,
		TargetPort:     record.TargetPort,
		TargetDatabase: record.TargetDatabase,
		TargetSchema:   record.TargetSchema,
	})
}

// UpdateAITuningResult updates the most recent tuning record with final migration throughput.
func (a *stateHistoryAdapter) UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int) error {
	return a.state.UpdateAITuningResult(throughput, durationSecs, chunkRetryCount)
}
