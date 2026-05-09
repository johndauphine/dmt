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

// HistoryProvider supplies past-run rows for history-aware selection.
// PR1 reads raw rows and does its own regime/outlier filtering +
// per-WAW aggregation in-package. May be nil — Tune treats nil as
// no-history and returns a pure-baseline output.
type HistoryProvider interface {
	// Records returns the migration history for the given (source, target)
	// pair. Order is not significant — Tune sorts/filters as needed.
	// Implementations may bound the result; PR1 doesn't depend on a
	// specific bound (regime + outlier filtering shrink the effective
	// dataset before selection).
	Records(sourceDBType, targetDBType string) ([]HistoryRecord, error)
}

// HistoryRecord is one past migration's input + outcome, just the fields
// the tuner reads. Mirrors the relevant subset of the SQL ai_tuning_history
// schema; the smartconfig analyzer's adapter populates this from the
// checkpoint backend.
type HistoryRecord struct {
	SourceDBType string
	TargetDBType string

	// What ran
	Workers           int
	ChunkSize         int
	WriteAheadWriters int

	// AvgRowBytes is the avg_row_size_bytes the analyzer recorded for
	// the migration that produced this row. Used by PR2's regression to
	// derive chunk_size in bytes (chunk_rows × avg_row_bytes) for the
	// quadratic CS feature.
	AvgRowBytes int64

	// What happened
	FinalThroughput float64
	ChunkRetryCount int

	// Regime classification fields (host)
	CPUCores int
	MemoryGB int

	// DB-tuning regime snapshot (#144 — older rows leave these zero/empty
	// and classify as "unknown" regime).
	Platform                string
	TargetSharedBuffersMB   int64
	TargetSyncCommit        string
	TargetFsync             string
	TargetFullPageWrites    string
	TargetMaxWALSizeMB      int64
	TargetWALLevel          string
	SourceMaxServerMemoryMB int64
}

// DBTuning is the current run's DB-tuning snapshot used by ClassifyRegime
// to flag history rows that ran under different DB-side settings (#144).
// Lifted from internal/driver.DBTuningSnapshot.
type DBTuning struct {
	TargetSharedBuffersMB   int64
	TargetSyncCommit        string
	TargetFsync             string
	TargetFullPageWrites    string
	TargetMaxWALSizeMB      int64
	TargetWALLevel          string
	SourceMaxServerMemoryMB int64
}

// Tune computes the recommended parameters for a migration. The history
// provider and DBTuning are optional — pass nil / zero-value for pure-
// baseline (cold-start) output.
func Tune(in Input, profile DriverProfile, history HistoryProvider, currentTuning DBTuning) Output {
	out := baseline(in, profile)
	if history != nil {
		applyHistory(&out, in, history, currentTuning)
	}
	applyMemoryClamp(&out, in)
	return out
}
