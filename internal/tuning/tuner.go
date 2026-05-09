// Package tuning is the deterministic migration parameter tuner that
// replaces the AI-driven smartconfig path (issue #175). It computes
// workers / chunk_size / write_ahead_writers / etc. from system stats
// + per-target driver profile + (optionally) historical run data, with
// no LLM round-trip on the critical path.
//
// Public entry point: [Tune]. Inputs are pure data (no live DB or HTTP
// dependencies); the optional [HistoryProvider] supplies past-run data
// when caller has a checkpoint backend handy. Output is a complete
// parameter set ready to apply.
//
// AI is intentionally absent. PR1 (this PR) ships baseline + smoothed-
// mean history selection + RULE 1 retry filter + memory clamp. PR2
// (#179) adds quadratic regression + planned-grid exploration.
package tuning

// Input carries the system + workload inputs the tuner reasons about.
// All fields are populated from the smartconfig analyzer's existing
// data collection — no new probes here.
type Input struct {
	// System
	CPUCores          int
	MemoryGB          int
	AvailableMemoryMB int64
	MaxMemoryMB       int64
	Platform          string // "linux", "wsl2", "darwin", "windows"

	// Workload
	SourceDBType string
	TargetDBType string
	TargetMode   string // "drop_recreate" | "upsert"
	TotalTables  int
	TotalRows    int64
	AvgRowBytes  int64
}

// Output is the complete recommended parameter set. Mirrors the fields
// the smartconfig analyzer's AutoTuneOutput populates today.
type Output struct {
	Workers             int
	ChunkSize           int
	ReadAheadBuffers    int
	WriteAheadWriters   int
	ParallelReaders     int
	MaxPartitions       int
	LargeTableThreshold int64

	MaxSourceConnections int
	MaxTargetConnections int

	UpsertMergeChunkSize int
	CheckpointFrequency  int
	MaxRetries           int

	EstimatedMemMB int64

	// Reasoning is a short human-readable explanation of any non-baseline
	// choices — for logging and the run record. Empty when output is pure
	// baseline.
	Reasoning string
}

// DriverProfile is the per-target tuning profile, populated by the caller
// from internal/driver.Driver. Pure data; no driver-package dependency
// here so internal/tuning stays import-cycle-clean.
type DriverProfile struct {
	Name                  string // canonical driver name
	BaselineWAW           int    // Driver.Defaults().WriteAheadWriters
	ScaleWritersWithCores bool   // Driver.Defaults().ScaleWritersWithCores
	OptimumBulkChunkBytes int64  // #166 — 0 means unmeasured (10 MB fallback applies)
	HardChunkLimit        int    // #166 — 0 means no protocol cap
}

// HistoryProvider supplies past-run data for history-aware selection.
// PR1 calls Aggregates() only. May be nil — Tune treats it as no-history
// and returns a pure-baseline output.
type HistoryProvider interface {
	// AggregatesByWAW returns per-(write_ahead_writers) totals over the
	// full ai_tuning_history filtered to (source, target). Ordered by WAW
	// ascending. Caller in PR1 uses this for smoothed-mean selection +
	// RULE 1 retry filter.
	AggregatesByWAW(sourceDBType, targetDBType string) ([]WAWAggregate, error)

	// AggregatesByChunkSize returns per-chunk_size mean throughput over the
	// same filtered history. Ordered by chunk_size ascending.
	AggregatesByChunkSize(sourceDBType, targetDBType string) ([]ChunkSizeAggregate, error)
}

// WAWAggregate is one row of per-WAW history.
type WAWAggregate struct {
	WriteAheadWriters int
	TotalRuns         int
	RunsWithRetries   int
	MeanThroughput    float64
}

// ChunkSizeAggregate is one row of per-chunk_size history.
type ChunkSizeAggregate struct {
	ChunkSize     int
	Runs          int
	AvgThroughput float64
}

// Tune computes the recommended parameters for a migration. The history
// provider is optional — pass nil for pure-baseline (cold-start) output.
func Tune(in Input, profile DriverProfile, history HistoryProvider) Output {
	out := baseline(in, profile)
	if history != nil {
		applyHistory(&out, in, profile, history)
	}
	applyMemoryClamp(&out, in)
	return out
}
