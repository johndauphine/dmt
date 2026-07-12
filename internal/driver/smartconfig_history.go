package driver

import (
	"github.com/johndauphine/dmt/internal/checkpoint"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
)

type pendingTuningSave struct {
	input     AutoTuneInput
	reasoning string
}

// ActualParams holds the actual migration parameters used after user overrides,
// plus effective DB tuning captured at run start (#144).
type ActualParams struct {
	Workers           int
	ChunkSize         int
	ReadAheadBuffers  int
	WriteAheadWriters int
	ParallelReaders   int
	MaxPartitions     int

	// Regime fields (#144). Populated by the orchestrator from
	// captureDBTuning before this struct reaches SaveTuningWithActualParams.
	Platform                string
	TargetSharedBuffersMB   int64
	TargetSyncCommit        string
	TargetFsync             string
	TargetFullPageWrites    string
	TargetMaxWALSizeMB      int64
	TargetWALLevel          string
	SourceMaxServerMemoryMB int64
}

// SaveTuningWithActualParams saves tuning history with the actual params
// used (after user overrides), not the recommendations.
func (s *SmartConfigAnalyzer) SaveTuningWithActualParams(actual ActualParams) int64 {
	if s.pendingSave == nil {
		return 0
	}
	ps := s.pendingSave
	s.pendingSave = nil

	s.suggestions.Workers = actual.Workers
	s.suggestions.ChunkSizeRecommendation = actual.ChunkSize
	s.suggestions.ReadAheadBuffers = actual.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = actual.WriteAheadWriters
	s.suggestions.ParallelReaders = actual.ParallelReaders
	s.suggestions.MaxPartitions = actual.MaxPartitions
	// Re-derive EstimatedMemMB so the persisted history reflects the
	// post-override params, not the pre-override estimate (#160).
	s.suggestions.EstimatedMemMB = tuning.EstimatedMemMB(
		actual.Workers,
		actual.ReadAheadBuffers,
		actual.WriteAheadWriters,
		actual.ChunkSize,
		ps.input.AvgRowBytes,
	)

	rowID, err := s.saveTuningResult(ps.input, ps.reasoning, actual)
	if err != nil {
		logging.Debug("Failed to save tuning history: %v", err)
		return 0
	}
	return rowID
}

// saveTuningResult saves the tuning recommendation to history.
func (s *SmartConfigAnalyzer) saveTuningResult(input AutoTuneInput, reasoning string, actual ActualParams) (int64, error) {
	if s.historyProvider == nil {
		return 0, nil
	}

	record := checkpoint.TuningRecord{
		Timestamp:               time.Now(),
		SourceDBType:            s.dbType,
		TargetDBType:            s.targetDBType,
		TotalTables:             s.suggestions.TotalTables,
		TotalRows:               s.suggestions.TotalRows,
		AvgRowSizeBytes:         s.suggestions.AvgRowSizeBytes,
		CPUCores:                input.CPUCores,
		MemoryGB:                input.MemoryGB,
		Workers:                 s.suggestions.Workers,
		ChunkSize:               s.suggestions.ChunkSizeRecommendation,
		ReadAheadBuffers:        s.suggestions.ReadAheadBuffers,
		WriteAheadWriters:       s.suggestions.WriteAheadWriters,
		ParallelReaders:         s.suggestions.ParallelReaders,
		MaxPartitions:           s.suggestions.MaxPartitions,
		LargeTableThreshold:     s.suggestions.LargeTableThreshold,
		MaxSourceConns:          s.suggestions.MaxSourceConnections,
		MaxTargetConns:          s.suggestions.MaxTargetConnections,
		EstimatedMemoryMB:       s.suggestions.EstimatedMemMB,
		Reasoning:               reasoning, // deterministic tuner's reasoning string
		WasAIUsed:               false,     // PR1 dropped the AI path entirely
		Platform:                firstNonEmpty(actual.Platform, input.Platform),
		TargetSharedBuffersMB:   actual.TargetSharedBuffersMB,
		TargetSyncCommit:        actual.TargetSyncCommit,
		TargetFsync:             actual.TargetFsync,
		TargetFullPageWrites:    actual.TargetFullPageWrites,
		TargetMaxWALSizeMB:      actual.TargetMaxWALSizeMB,
		TargetWALLevel:          actual.TargetWALLevel,
		SourceMaxServerMemoryMB: actual.SourceMaxServerMemoryMB,
		// Workload identity passthrough (#215). The values come from
		// the AutoTuneInput the orchestrator built when calling Tune,
		// so they reflect THIS run's exact endpoints. Pre-#215 callers
		// that don't set these fields leave them empty in the record,
		// which is the correct outcome â€” empty values can't satisfy
		// Tier 1's equality check.
		SourceHost:     input.SourceHost,
		SourcePort:     input.SourcePort,
		SourceDatabase: input.SourceDatabase,
		SourceSchema:   input.SourceSchema,
		TargetHost:     input.TargetHost,
		TargetPort:     input.TargetPort,
		TargetDatabase: input.TargetDatabase,
		TargetSchema:   input.TargetSchema,
	}

	return s.historyProvider.SaveTuningRecord(record)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// applyTableNameFilter trims the schema-returned table list to those
// the caller declared in-scope (#241). nil/empty filter is a no-op so
// the analyze CLI subcommand (which has no filtering context) behaves
