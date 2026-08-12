package driver

import (
	"fmt"
	"time"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/tuning"
)

type pendingTuningSave struct {
	input                  AutoTuneInput
	reasoning              string
	representativeRowBytes int64
	safetyRowBytes         int64
	safetyRowBytesKnown    bool
}

// ActualParams holds the global run policy used after user overrides, plus
// effective DB tuning captured at run start (#144). ChunkSize is the steady
// requested action. Target protocol limits can reduce it, and an applied
// runtime writer transition can ratchet later reader chunks and writer batches;
// those conditional limits remain disclosure metadata rather than replacement
// learned actions.
type ActualParams struct {
	Workers              int
	ChunkSize            int
	ReadAheadBuffers     int
	WriteAheadWriters    int
	ParallelReaders      int
	MaxPartitions        int
	MaxSourceConnections int
	MaxTargetConnections int

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

// SaveTuningWithActualParams saves tuning history with the global run policy
// used after user overrides, not the pre-override recommendation. The persisted
// reasoning distinguishes ordinary direct execution from target protocol limits
// and conditional writer-transition ratchets.
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
	s.suggestions.MaxSourceConnections = actual.MaxSourceConnections
	s.suggestions.MaxTargetConnections = actual.MaxTargetConnections
	s.suggestions.RepresentativeRowBytes = pendingRepresentativeRowBytes(ps)
	s.suggestions.SafetyRowBytes = pendingSafetyRowBytes(ps)
	s.suggestions.SafetyRowBytesKnown = pendingSafetyRowBytesKnown(ps)
	// Re-derive EstimatedMemMB so the persisted history reflects the
	// post-override params, not the pre-override estimate (#160). The global
	// diagnostic uses the row-count-weighted representative width, matching the
	// recommendation policy. Shared measured-byte admission and MemoryGuard
	// enforce steady transfer; complete-inventory checks gate writer transitions.
	// The legacy AvgRowSizeBytes remains the persisted regression feature, and
	// conditional projection accounting is stored separately.
	representativeRowBytes := pendingRepresentativeRowBytes(ps)
	s.suggestions.EstimatedMemMB = tuning.EstimatedMemMB(
		actual.Workers,
		actual.ReadAheadBuffers,
		actual.WriteAheadWriters,
		actual.ChunkSize,
		representativeRowBytes,
	)
	s.suggestions.MemoryEstimateOverBudget = tuning.MemoryEstimateExceedsBudget(
		ps.input.MemoryBudgetMB,
		actual.Workers,
		actual.ReadAheadBuffers,
		actual.WriteAheadWriters,
		actual.ChunkSize,
		representativeRowBytes,
	)
	if s.suggestions.MemoryEstimateOverBudget {
		logging.Warn("Post-override representative tuning memory estimate exceeds the resolved budget (estimate=%d MB budget=%d MB representative_width=%d B); shared measured-byte admission and MemoryGuard remain active, and runtime writer growth is fail-closed",
			s.suggestions.EstimatedMemMB, ps.input.MemoryBudgetMB, representativeRowBytes)
	}

	reasoning := executionPolicyReasoning(ps.reasoning, actual.ChunkSize)
	rowID, err := s.saveTuningResult(ps.input, reasoning, actual)
	if err != nil {
		logging.Debug("Failed to save tuning history: %v", err)
		return 0
	}
	return rowID
}

func executionPolicyReasoning(reasoning string, chunkSize int) string {
	projection := fmt.Sprintf(
		"execution policy: chunk_size=%d is the global policy used directly for ordinary reader chunks and writer batches; shared measured-byte admission/MemoryGuard enforce steady memory safety; target protocol limits and complete-inventory writer-transition ratchets are recorded at completion",
		chunkSize,
	)
	if reasoning == "" {
		return projection
	}
	return reasoning + "; " + projection
}

func pendingRepresentativeRowBytes(ps *pendingTuningSave) int64 {
	if ps.representativeRowBytes > 0 {
		return ps.representativeRowBytes
	}
	if ps.input.RepresentativeRowBytes > 0 {
		return ps.input.RepresentativeRowBytes
	}
	return fallbackRowBytes
}

func pendingSafetyRowBytes(ps *pendingTuningSave) int64 {
	if ps.safetyRowBytes > 0 {
		return ps.safetyRowBytes
	}
	if ps.input.SafetyRowBytes > 0 {
		return ps.input.SafetyRowBytes
	}
	return fallbackRowBytes
}

func pendingSafetyRowBytesKnown(ps *pendingTuningSave) bool {
	if ps.safetyRowBytes > 0 {
		return ps.safetyRowBytesKnown
	}
	if ps.input.SafetyRowBytes > 0 {
		return ps.input.SafetyRowBytesKnown
	}
	return false
}

// saveTuningResult saves the tuning recommendation to history.
func (s *SmartConfigAnalyzer) saveTuningResult(input AutoTuneInput, reasoning string, actual ActualParams) (int64, error) {
	if s.historyProvider == nil {
		return 0, nil
	}

	record := checkpoint.TuningRecord{
		Timestamp:                    time.Now(),
		SourceDBType:                 s.dbType,
		TargetDBType:                 s.targetDBType,
		TotalTables:                  s.suggestions.TotalTables,
		TotalRows:                    s.suggestions.TotalRows,
		AvgRowSizeBytes:              s.suggestions.AvgRowSizeBytes,
		CPUCores:                     input.CPUCores,
		MemoryGB:                     input.MemoryGB,
		Workers:                      s.suggestions.Workers,
		ChunkSize:                    s.suggestions.ChunkSizeRecommendation,
		ReadAheadBuffers:             s.suggestions.ReadAheadBuffers,
		WriteAheadWriters:            s.suggestions.WriteAheadWriters,
		ParallelReaders:              s.suggestions.ParallelReaders,
		MaxPartitions:                s.suggestions.MaxPartitions,
		LargeTableThreshold:          s.suggestions.LargeTableThreshold,
		MaxSourceConns:               actual.MaxSourceConnections,
		MaxTargetConns:               actual.MaxTargetConnections,
		EstimatedMemoryMB:            s.suggestions.EstimatedMemMB,
		Reasoning:                    reasoning, // deterministic tuner's reasoning string
		WasAIUsed:                    false,     // PR1 dropped the AI path entirely
		Platform:                     firstNonEmpty(actual.Platform, input.Platform),
		TargetSharedBuffersMB:        actual.TargetSharedBuffersMB,
		TargetSyncCommit:             actual.TargetSyncCommit,
		TargetFsync:                  actual.TargetFsync,
		TargetFullPageWrites:         actual.TargetFullPageWrites,
		TargetMaxWALSizeMB:           actual.TargetMaxWALSizeMB,
		TargetWALLevel:               actual.TargetWALLevel,
		SourceMaxServerMemoryMB:      actual.SourceMaxServerMemoryMB,
		ProjectionContextFingerprint: input.ProjectionContextFingerprint,
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
