package driver

import (
	"github.com/johndauphine/dmt/internal/tuning"
	"math"
)

func (s *SmartConfigAnalyzer) calculateAutoTuneParams(tables []tableInfo) {
	avgRowSize := s.calculateAvgRowSize(tables)
	s.suggestions.AvgRowSizeBytes = avgRowSize
	s.suggestions.RepresentativeRowBytes = s.representativeRowBytes
	s.suggestions.SafetyRowBytes = s.safetyRowBytes
	s.suggestions.SafetyRowBytesKnown = s.safetyRowBytesKnown

	input := s.buildAutoTuneInput(tables, avgRowSize)
	output := tuning.Tune(
		s.toTuningInput(input),
		s.toTuningProfile(),
		s.toTuningHistory(),
		s.currentTuning,
	)
	s.applyTuningOutput(output)

	s.pendingSave = &pendingTuningSave{
		input:                  input,
		reasoning:              output.Reasoning,
		representativeRowBytes: input.RepresentativeRowBytes,
		safetyRowBytes:         input.SafetyRowBytes,
		safetyRowBytesKnown:    input.SafetyRowBytesKnown,
	}
}

// toTuningInput maps the analyzer's AutoTuneInput onto tuning.Input,
// adding the exploration fields from the analyzer's configured state.
func (s *SmartConfigAnalyzer) toTuningInput(in AutoTuneInput) tuning.Input {
	return tuning.Input{
		CPUCores:               in.CPUCores,
		MemoryGB:               in.MemoryGB,
		MemoryBudgetMB:         in.MemoryBudgetMB,
		Platform:               in.Platform,
		SourceDBType:           in.DatabaseType,
		TargetDBType:           in.TargetType,
		TargetMode:             in.TargetMode,
		TotalTables:            in.TotalTables,
		TotalRows:              in.TotalRows,
		AvgRowBytes:            in.AvgRowBytes,
		RepresentativeRowBytes: in.RepresentativeRowBytes,
		SafetyRowBytes:         in.SafetyRowBytes,
		SafetyRowBytesKnown:    in.SafetyRowBytesKnown,
		UncappedAvgRowBytes:    in.UncappedAvgRowBytes,
		LargestTableBytes:      in.LargestTableBytes,
		// Workload identity passthrough (#215).
		SourceHost:              in.SourceHost,
		SourcePort:              in.SourcePort,
		SourceDatabase:          in.SourceDatabase,
		SourceSchema:            in.SourceSchema,
		TargetHost:              in.TargetHost,
		TargetPort:              in.TargetPort,
		TargetDatabase:          in.TargetDatabase,
		TargetSchema:            in.TargetSchema,
		ForceExplore:            s.forceExplore,
		ExplorationEpsilon:      explorationEpsilon(s.exploreMode),
		PinnedWriteAheadWriters: s.pinnedWriteAheadWriters,
		PinnedParallelReaders:   s.pinnedParallelReaders,
		PinnedReadAheadBuffers:  s.pinnedReadAheadBuffers,
	}
}

// ExplorationEpsilon is the package-public version of explorationEpsilon
// for callers outside the smartconfig analyzer (e.g. healthcheck's
// offline path). Same mapping; same fallback behavior.
func ExplorationEpsilon(mode string) float64 {
	return explorationEpsilon(mode)
}

// explorationEpsilon maps the user-facing ExploreMode string to the
// numeric Îµ used by the tuner's perturbation policy (#179). Empty
// defaults to "balanced" (0.15) â€” the documented PR2 default. Unknown
// values fall back to "balanced" rather than failing, since this is
// configuration data and a typo shouldn't break tuning.
func explorationEpsilon(mode string) float64 {
	switch mode {
	case "off":
		return 0
	case "low":
		return 0.10
	case "high":
		return 0.25
	case "", "balanced":
		return 0.15
	default:
		return 0.15
	}
}

// toTuningProfile builds the per-target profile through the shared driver
// policy used by schema-aware and offline tuning.
func (s *SmartConfigAnalyzer) toTuningProfile() tuning.DriverProfile {
	// SafetyRowBytes is the widest observed table-average width and is the
	// best available packet-sizing estimate. It is not a bound on every
	// serialized row; write-error-driven chunk reduction remains the backstop.
	packetRowBytes := s.packetSizingRowBytes()
	return BuildTuningProfile(s.targetDBType, packetRowBytes, s.targetProbe)
}

func (s *SmartConfigAnalyzer) packetSizingRowBytes() int64 {
	if s.safetyRowBytes > 0 {
		return s.safetyRowBytes
	}
	if s.uncappedAvgRowBytes > 0 {
		return s.uncappedAvgRowBytes
	}
	if s.suggestions != nil && s.suggestions.AvgRowSizeBytes > 0 {
		return s.suggestions.AvgRowSizeBytes
	}
	return fallbackRowBytes
}

// chunkLimitFromProbe picks the HardChunkLimit value used in the
// DriverProfile. When a target-side probe surfaces a protocol cap
// (today: MySQL @@max_allowed_packet), that wins â€” it's the hard
// physical limit, server-configurable, and only knowable at runtime. The
// row-count conversion is still modeled from a table-average width, so it
// does not prove that every individual serialized row fits. Otherwise the
// driver's static HardChunkLimit applies. Extracted as
// a pure function so smartconfig tests can exercise the math without
// depending on driver-registry state (#166).
func chunkLimitFromProbe(driverStaticLimit int, probe TargetProbe, estimatedRowBytes int64) int {
	if probe.MaxAllowedPacket > 0 && estimatedRowBytes > 0 {
		const safetyMarginPct = 80
		// Decompose before multiplying so a malformed near-MaxInt64 probe
		// cannot wrap the packet budget negative.
		safeBytes := (probe.MaxAllowedPacket/100)*safetyMarginPct +
			(probe.MaxAllowedPacket%100)*safetyMarginPct/100
		limit64 := safeBytes / estimatedRowBytes
		maxInt := int64(^uint(0) >> 1)
		if limit64 > maxInt {
			return int(maxInt)
		}
		limit := int(limit64)
		// Clamp to at least 1 â€” when the modeled table-average width
		// exceeds 80% of the packet, integer division returns 0 and downstream
		// code would
		// treat that as "no cap." One row is minimum progress, but may still
		// exceed the modeled packet budget; runtime structural error handling
		// remains responsible for that over-budget case.
		if limit < 1 {
			limit = 1
		}
		return limit
	}
	return driverStaticLimit
}

// toTuningHistory wraps the analyzer's TuningHistoryProvider in an
// adapter that satisfies tuning.HistoryProvider. Returns nil when no
// provider is configured so Tune treats it as cold-start.
func (s *SmartConfigAnalyzer) toTuningHistory() tuning.HistoryProvider {
	if s.historyProvider == nil {
		return nil
	}
	return &tuningHistoryAdapter{base: s.historyProvider}
}

// tuningHistoryAdapter converts the analyzer's TuningHistoryProvider into
// the tuning package's HistoryProvider â€” fetches TuningRecord rows and
// translates each into a tuning.HistoryRecord.
type tuningHistoryAdapter struct {
	base TuningHistoryProvider
}

// Records returns all history rows for the (source, target) pair. limit=0
// = no bound; the tuner does its own filtering downstream.
func (a *tuningHistoryAdapter) Records(sourceDBType, targetDBType string) ([]tuning.HistoryRecord, error) {
	rows, err := a.base.GetTuningHistory(0, sourceDBType, targetDBType)
	if err != nil {
		return nil, err
	}
	out := make([]tuning.HistoryRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, tuning.HistoryRecord{
			Timestamp:         r.Timestamp,
			SourceDBType:      r.SourceDBType,
			TargetDBType:      r.TargetDBType,
			Workers:           r.Workers,
			ChunkSize:         r.ChunkSize,
			WriteAheadWriters: r.WriteAheadWriters,
			ParallelReaders:   r.ParallelReaders,
			ReadAheadBuffers:  r.ReadAheadBuffers,
			AvgRowBytes:       r.AvgRowSizeBytes,
			TotalRows:         r.TotalRows,
			TotalTables:       r.TotalTables,
			// LargestTableBytes is not persisted yet (#214 follow-up will
			// add the column); historical rows leave it zero, which the
			// regime classifier treats as "unknown skew" â€” neutral on
			// the secondary axis rather than mismatching every input.
			FinalThroughput: r.FinalThroughput,
			// FinalThroughputBytes is the bytes/sec form the regression
			// trains on (#224). rows/sec stays the canonical persisted
			// value (existing ai_tuning_history rows + non-regression
			// consumers); the multiplication happens here, at the single
			// adapter boundary, so a pre-#215 row with AvgRowSizeBytes==0
			// gets the safeAvgRowBytes fallback consistently with the
			// rest of the tuning pipeline.
			FinalThroughputBytes:    throughputBytesForHistory(r.FinalThroughput, r.AvgRowSizeBytes),
			ChunkRetryCount:         r.ChunkRetryCount,
			AdjustedAtRuntime:       r.AdjustedAtRuntime,
			CPUCores:                r.CPUCores,
			MemoryGB:                r.MemoryGB,
			Platform:                r.Platform,
			TargetSharedBuffersMB:   r.TargetSharedBuffersMB,
			TargetSyncCommit:        r.TargetSyncCommit,
			TargetFsync:             r.TargetFsync,
			TargetFullPageWrites:    r.TargetFullPageWrites,
			TargetMaxWALSizeMB:      r.TargetMaxWALSizeMB,
			TargetWALLevel:          r.TargetWALLevel,
			SourceMaxServerMemoryMB: r.SourceMaxServerMemoryMB,
			// Workload identity passthrough (#215). Pre-#215 rows
			// have empty values for these â€” exact-identity match is
			// equality on strings/ints, so empties fail to match the
			// user's input and the row falls through to Tier 2.
			SourceHost:     r.SourceHost,
			SourcePort:     r.SourcePort,
			SourceDatabase: r.SourceDatabase,
			SourceSchema:   r.SourceSchema,
			TargetHost:     r.TargetHost,
			TargetPort:     r.TargetPort,
			TargetDatabase: r.TargetDatabase,
			TargetSchema:   r.TargetSchema,
		})
	}
	return out, nil
}

// throughputBytesForHistory converts a persisted rows/sec value into
// the bytes/sec form the regression trains on, guarding the cast
// against the pathological inputs that would otherwise produce
// undefined int64 values (Copilot review on PR #289):
//
//   - FinalThroughput â‰¤ 0 (incomplete or corrupt run): return 0 so
//     the row is dropped at the regression's FinalThroughput-positive
//     filter anyway, and the bytes field doesn't carry a meaningless
//     negative int64 in the meantime.
//   - NaN: `x > 0` is false for NaN, caught by the same branch.
//   - +Inf: caught explicitly via the IsInf check before multiply, so
//     the multiplication doesn't produce another +Inf that overflows
//     the int64 cast.
//   - Cap at math.MaxInt64 just in case the multiplied float is past
//     the int64 range (a hypothetical 10Â²â° bytes/s, which couldn't
//     happen in practice but doesn't cost us to guard).
func throughputBytesForHistory(throughputRowsPerSec float64, avgRowBytes int64) int64 {
	if !(throughputRowsPerSec > 0) || math.IsInf(throughputRowsPerSec, 0) {
		return 0
	}
	bytes := throughputRowsPerSec * float64(tuning.SafeAvgRowBytes(avgRowBytes))
	if bytes >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(bytes)
}

// applyTuningOutput copies the tuner's recommendations onto the
// analyzer's suggestions struct.
func (s *SmartConfigAnalyzer) applyTuningOutput(out tuning.Output) {
	s.suggestions.Workers = out.Workers
	s.suggestions.ChunkSizeRecommendation = out.ChunkSize
	s.suggestions.ReadAheadBuffers = out.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = out.WriteAheadWriters
	s.suggestions.ParallelReaders = out.ParallelReaders
	s.suggestions.MaxPartitions = out.MaxPartitions
	s.suggestions.LargeTableThreshold = out.LargeTableThreshold
	s.suggestions.MaxSourceConnections = out.MaxSourceConnections
	s.suggestions.MaxTargetConnections = out.MaxTargetConnections
	s.suggestions.UpsertMergeChunkSize = out.UpsertMergeChunkSize
	s.suggestions.CheckpointFrequency = out.CheckpointFrequency
	s.suggestions.MaxRetries = out.MaxRetries
	s.suggestions.EstimatedMemMB = out.EstimatedMemMB
	s.suggestions.MemoryEstimateOverBudget = out.MemoryEstimateOverBudget
	s.suggestions.Reasoning = out.Reasoning
	s.suggestions.Tier = out.Tier
	s.suggestions.PinnedAdvice = out.PinnedAdvice
}

// pendingTuningSave holds data for deferred tuning history save.
