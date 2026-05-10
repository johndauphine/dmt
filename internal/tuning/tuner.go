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

import (
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

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

	// Exploration policy (PR2 #179). ForceExplore mirrors the user's
	// --explore CLI flag / cfg.Migration.Explore: when true, the tuner
	// picks from the planned exploration grid this run regardless of
	// bucket state. ExplorationEpsilon is the steady-state perturbation
	// probability — 0 disables ε-perturbation, 0.15 is the documented
	// "balanced" default.
	ForceExplore       bool
	ExplorationEpsilon float64
}

// Tier names identify which selector ultimately picked the (WAW,
// ChunkSize) values on the Output. Logged by the orchestrator on every
// run so a reader can group runs by the tier that drove them without
// parsing the freeform Reasoning string. Not persisted to the
// ai_tuning_history schema yet — only Reasoning is (as ai_reasoning).
// Adding a tier column is tracked separately as out-of-scope on #202.
const (
	TierBaseline     = "baseline"
	TierExploration  = "exploration"
	TierRegression   = "regression"
	TierSmoothedBins = "smoothed-bins"
)

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

	// Tier names which selector picked the WAW/ChunkSize values. One of
	// the Tier* constants above. Always populated by Tune.
	Tier string

	// Reasoning is a short human-readable explanation of the picking
	// path — for logging and the run record. Always populated by Tune;
	// for baseline-only runs it explains *why* baseline stood (no
	// history, fetch failed, insufficient rows, etc.) rather than being
	// empty.
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
	// Timestamp is the migration completion time. Used by PR2's regime-
	// drift detector to compare median throughput in the most-recent N
	// runs against earlier runs at the same (WAW, CS) config.
	Timestamp time.Time

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

	// TotalRows is the row count of the dataset this run migrated.
	// Used by ClassifyRegime (#198) to drop rows from datasets materially
	// different in size from the current run — without this filter, a
	// 19M-row dataset and a 106M-row dataset classify as the same regime,
	// contaminating the regression's training set with rows from a
	// different operating point.
	TotalRows int64

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
//
// Decision flow (PR2 #179):
//
//	baseline → fetch+filter history rows → (
//	  shouldExplore        → planned-grid pick (override regression)
//	  steady-state         → tier dispatch (regression or smoothed bins)
//	                       → with prob ε, ε-perturbation around argmax
//	) → memory clamp
func Tune(in Input, profile DriverProfile, history HistoryProvider, currentTuning DBTuning) Output {
	out := baseline(in, profile)

	// Fetch + filter once so both exploration and history-selection see
	// the same row set. Track whether the fetch actually succeeded so a
	// failed fetch doesn't masquerade as cold-start (Copilot review on
	// PR #183).
	//
	// regimeRows holds the regime-filtered set BEFORE the outlier filter
	// runs — drift detection inspects this so a drift-induced burst of
	// low-throughput-but-clean runs doesn't get classified as "noise"
	// and removed before the detector can compare it to older runs
	// (Codex review on PR #183 — bug fix).
	var regimeRows, rows []HistoryRecord
	historyAvailable := false
	if history != nil {
		if r, err := history.Records(in.SourceDBType, in.TargetDBType); err == nil {
			historyAvailable = true
			regimeRows = filterByRegime(r, in, currentTuning)
			rows = filterOutliers(regimeRows)
		} else {
			logging.Debug("tuning: history fetch failed (%v) — using baseline", err)
		}
	}

	// Exploration enters three ways:
	//   1. The user forced it via --explore, regardless of history state.
	//   2. We have a working history backend AND the bucket is in
	//      cold-start (< explorationGridRuns runs) — the planned grid
	//      fills out the regression's training data.
	//   3. Regime-drift detector fires — throughput at a fixed (WAW, CS)
	//      config has shifted materially, so the regression's training
	//      data is stale and we re-explore to refresh.
	// Nil-history OR a failed history fetch does NOT enter exploration
	// on its own — there's nowhere to learn from / persist the probe
	// to, so the baseline output is what the user gets.
	driftDetected := historyAvailable && detectRegimeDrift(regimeRows)
	if in.ForceExplore || driftDetected || (historyAvailable && shouldExplore(in, len(rows))) {
		// Exploration paths (planned grid, ε-perturbation) intentionally
		// don't apply the historical-retry filter — that's a SELECTION
		// concern (handled inside applyHistorySelection's selectWAW /
		// argmaxRegression). See issue #186.
		applyGridExploration(&out, in, profile, len(rows))
	} else if len(rows) > 0 {
		applyHistorySelection(&out, in, profile, rows)
		if shouldEpsilonPerturb(in.ExplorationEpsilon) {
			applyEpsilonPerturbation(&out, profile)
		}
	}

	finalizeTierAndReasoning(&out, history, historyAvailable)
	applyMemoryClamp(&out, in)
	return out
}

// finalizeTierAndReasoning fills in Tier and Reasoning for the cases no
// selector path covered. Issue #202: silence is not a valid signal —
// every run must carry the provenance of the (Tier, Reasoning) pair so a
// reviewer can tell *why* the output looks the way it does. Tier
// defaults to baseline; Reasoning is filled in only when no upstream
// selector left a note (otherwise we'd clobber a more specific
// "exploration grid empty" or "smoothed-bins ineligible" message).
//
// Reachable Reasoning-empty paths from Tune are limited: when history is
// available the dispatch always routes to exploration (cold-start
// bucket) or applyHistorySelection (which now always emits), so the
// only no-Reasoning case left is when history was unavailable — either
// nil backend or a failed fetch. Distinguishing those two matters: nil
// is intentional, fetch error means the SQLite/file backend is broken
// and the user should investigate.
func finalizeTierAndReasoning(out *Output, history HistoryProvider, historyAvailable bool) {
	if out.Tier == "" {
		out.Tier = TierBaseline
	}
	if out.Reasoning != "" {
		return
	}
	switch {
	case history == nil:
		out.Reasoning = "baseline (no history backend configured)"
	case !historyAvailable:
		out.Reasoning = "baseline (history fetch failed — see prior debug log)"
	default:
		// Defensive: every other path through Tune leaves a Reasoning
		// note. If we land here, something added a new dispatch branch
		// without wiring up its reasoning — flag it loudly rather than
		// silently fall back to "baseline".
		out.Reasoning = "baseline (unexpected — selector dispatch left no reasoning note; please file a bug)"
	}
}
