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
	CPUCores int
	MemoryGB int
	// MemoryBudgetMB is the resolved process memory budget supplied by the
	// caller's shared memory envelope (#708). A positive value is enforced
	// exactly; zero or negative means no budget was supplied, so tuning still
	// computes EstimatedMemMB but does not clamp ChunkSize.
	MemoryBudgetMB int64
	Platform       string // "linux", "wsl2", "darwin", "windows"

	// Workload
	SourceDBType string
	TargetDBType string
	TargetMode   string // "drop_recreate" | "upsert"
	TotalTables  int
	TotalRows    int64

	// AvgRowBytes is the legacy, capped, unweighted top-five-table average.
	// It remains the persisted regression feature for compatibility with
	// historical models; it must not be reused for performance or safety math.
	AvgRowBytes int64

	// RepresentativeRowBytes is the row-count-weighted workload width used to
	// convert byte-shaped performance choices (baseline, exploration, and
	// regression candidates) into row counts. Non-positive values use the
	// conservative 500-byte fallback rather than aliasing the legacy average.
	RepresentativeRowBytes int64

	// SafetyRowBytes is the widest observed positive per-table average and is
	// used exclusively for memory clamps and estimates. SafetyRowBytesKnown is
	// true only when schema statistics supplied that positive width; fallback
	// estimates keep it false so callers never present an assumed width as
	// observed fact.
	SafetyRowBytes      int64
	SafetyRowBytesKnown bool

	// UncappedAvgRowBytes is the pre-cap average row size in bytes
	// (#214 Copilot fix on PR #288). ClassifyRegime reads this for
	// total-bytes band classification so wide-row workloads land in
	// the right band — using AvgRowBytes alone would mis-bucket an
	// 8KB-row workload as Small because the 2KB cap shrinks
	// `rows × avg_row_bytes` below the band boundary. Zero falls back
	// to AvgRowBytes (callers that don't compute it separately keep
	// the pre-#214 behavior).
	UncappedAvgRowBytes int64

	// LargestTableBytes is the size of the single largest table in the
	// source dataset, used by ClassifyRegime (#214) to assign a skew tier
	// (EvenlyDistributed / Skewed / Dominant). Zero short-circuits the
	// skew gate as "unknown" on this side — callers that don't populate
	// it skip the skew axis for that comparison rather than mismatching
	// every current input.
	LargestTableBytes int64

	// Workload identity (#215). Together these form the tuple the
	// Tier 1 exact-identity classifier uses to find historically-
	// comparable runs. Empty fields skip the Tier 1 lookup entirely —
	// callers who don't propagate the identity get the Tier 2 / Tier 3
	// fallback behavior (which was the only path pre-#215).
	SourceHost     string
	SourcePort     int
	SourceDatabase string
	SourceSchema   string
	TargetHost     string
	TargetPort     int
	TargetDatabase string
	TargetSchema   string

	// Exploration policy (PR2 #179). ForceExplore mirrors the user's
	// --explore CLI flag / cfg.Migration.Explore: when true, the tuner
	// picks from the planned exploration grid this run regardless of
	// bucket state. ExplorationEpsilon is the steady-state perturbation
	// probability — 0 disables ε-perturbation, 0.15 is the documented
	// "balanced" default.
	ForceExplore       bool
	ExplorationEpsilon float64

	// PinnedWriteAheadWriters is the user-pinned WAW value, when the
	// config provenance says the tuner may not adjust it (#461). Tune
	// still computes its recommendation; this field only enables the
	// measured override-cost advice in Output.PinnedAdvice. Nil when
	// WAW is tuner-managed.
	PinnedWriteAheadWriters *int

	// PinnedParallelReaders / PinnedReadAheadBuffers mirror the pin for
	// the reader knobs (#461). The override-cost advice filters history
	// to the reader settings that will ACTUALLY run — pinned values win
	// over the tuner's output, since ApplyTunerSuggestions will refuse
	// to overwrite them (codex review).
	PinnedParallelReaders  *int
	PinnedReadAheadBuffers *int
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

	// MemoryEstimateOverBudget is true when the final recommendation's modeled
	// working set exceeds MemoryBudgetMB. In particular, it remains true when
	// the tuner returns a one-row minimum-progress chunk that still cannot fit;
	// the recommendation must never imply that such a chunk is memory-safe.
	MemoryEstimateOverBudget bool

	// Tier names which selector picked the WAW/ChunkSize values. One of
	// the Tier* constants above. Always populated by Tune.
	Tier string

	// Reasoning is a short human-readable explanation of the picking
	// path — for logging and the run record. Always populated by Tune;
	// for baseline-only runs it explains *why* baseline stood (no
	// history, fetch failed, insufficient rows, etc.) rather than being
	// empty.
	Reasoning string

	// PinnedAdvice carries measured override-cost findings (#461): when
	// a user-pinned knob's history bin materially underperforms the best
	// eligible bin, one line per finding states the measured means — no
	// predictions, only bins with enough runs to trust. Empty when
	// nothing is pinned or the history can't support a comparison.
	PinnedAdvice []string
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
	// pair. Order is not significant — Tune sorts/filters as needed. The
	// result must be the unbounded pair history: planned-grid rotation uses
	// its raw length as a sequential at-least-once attempt index (#697).
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
	// ParallelReaders and ReadAheadBuffers are persisted on TuningRecord
	// but were dropped by the adapter before #219. Now carried through so
	// the regression can learn from their variance once the exploration
	// grid starts varying them.
	ParallelReaders  int
	ReadAheadBuffers int

	// AvgRowBytes is the avg_row_size_bytes the analyzer recorded for
	// the migration that produced this row. Used by PR2's regression to
	// derive chunk_size in bytes (chunk_rows × avg_row_bytes) for the
	// quadratic CS feature. Capped at 2KB by the smartconfig analyzer.
	AvgRowBytes int64

	// UncappedAvgRowBytes is the pre-cap row size (Copilot fix on PR
	// #288). Mirrors tuning.Input — ClassifyRegime prefers this for
	// band classification when set. Not persisted to ai_tuning_history
	// yet (separate schema migration); pre-#214 rows leave it zero and
	// the classifier falls back to AvgRowBytes.
	UncappedAvgRowBytes int64

	// TotalRows is the row count of the dataset this run migrated.
	// Used by ClassifyRegime (#198, #214) to assign a total-bytes band
	// (Tiny / Small / Medium / Large / Huge) together with AvgRowBytes;
	// rows in a different band are dropped from the regression's
	// training set.
	TotalRows int64

	// TotalTables is the number of tables this run migrated. Used by
	// ClassifyRegime (#214) as the tertiary axis (Few / Many / Massive).
	// Zero is treated as "unknown" — pre-#214 rows leave this empty and
	// classify as a single (current) tier so they aren't excluded on
	// the count axis alone.
	TotalTables int

	// LargestTableBytes is the size of the largest table this run
	// migrated. Used by ClassifyRegime (#214) for the secondary skew
	// axis (EvenlyDistributed / Skewed / Dominant). Zero is treated as
	// "unknown" — pre-#214 rows didn't persist this so they fall into
	// a neutral skew tier rather than mismatching every current input.
	LargestTableBytes int64

	// What happened
	FinalThroughput float64
	// FinalThroughputBytes is the same observed throughput expressed in
	// bytes/sec (#224 — FinalThroughput × AvgRowBytes at adapter-fill time,
	// with the safeAvgRowBytes fallback when AvgRowBytes is unset on pre-#215
	// rows). The regression trains on this so the dependent variable is in
	// the same unit family as the chunk_size_bytes feature — drops the
	// dual role log(avg_row_bytes) was playing as an implicit unit
	// conversion AND a row-width effect, which materially improves
	// cross-workload R².
	//
	// Stays alongside FinalThroughput (not replacing it) because the
	// smoothed-bins tier, regime-drift detector, and orchestrator
	// reasoning still operate in rows/sec — that's the unit the user's
	// log expressed historically and rows/sec is the natural unit for a
	// fixed-workload retry/throughput comparison.
	FinalThroughputBytes int64
	ChunkRetryCount      int

	// AdjustedAtRuntime is true when the runtime controller changed
	// parameters mid-run (#451). The row's throughput is a blend across
	// configs and can't be attributed to the recorded (WAW, CS, PR, RAB),
	// so Tune drops such rows before every training cohort is built —
	// regression, smoothed bins, drift detection, and the exploration
	// bucket count. Pre-#451 rows are false and stay eligible.
	AdjustedAtRuntime bool

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

	// Workload identity (#215). Pre-#215 rows have empty values for
	// these — the Tier 1 exact-identity filter compares string/int
	// equality so any empty field on either side fails to match. That
	// is correct: pre-#215 rows have unknown identity and should fall
	// through to the Tier 2 / Tier 3 fallback.
	SourceHost     string
	SourcePort     int
	SourceDatabase string
	SourceSchema   string
	TargetHost     string
	TargetPort     int
	TargetDatabase string
	TargetSchema   string
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
	var regimeRows []HistoryRecord
	var regimeFilter outlierFilterResult
	historyAvailable := false
	rawCount := 0
	if history != nil {
		if r, err := history.Records(in.SourceDBType, in.TargetDBType); err == nil {
			historyAvailable = true
			// Pair-scoped raw persisted attempts advance planned-grid rotation
			// even when runtime-adjusted, regime, or outlier filters later
			// exclude them. This is a sequential at-least-once index, not an
			// atomic reservation across concurrent Tune calls.
			rawCount = len(r)
			// #451: runtime-adjusted rows are dropped before any cohort
			// is built — regime filter, drift detector, outlier passes,
			// the exploration bucket count, and both selection tiers all
			// see only rows whose throughput is attributable to their
			// recorded parameters. Emitted unconditionally (not deferred
			// like the outlier reasoning) because the drop shapes every
			// downstream decision, including which tier runs at all.
			var adjustedDropped int
			r, adjustedDropped = dropRuntimeAdjusted(r)
			if adjustedDropped > 0 {
				out.Reasoning = appendReasoning(out.Reasoning,
					"history hygiene: %d runtime-adjusted run(s) excluded from tuning cohorts (#451)",
					adjustedDropped,
				)
			}
			regimeRows = filterByRegime(r, in, currentTuning)
			// Defer the outlier-reasoning emission until we know which
			// cohort drives the selector — Tier 1 (identity) may take
			// over and run its own filter pass below. Emitting eagerly
			// here would double-report when the identity cohort is a
			// subset of the regime cohort (codex review on the initial
			// #225 commit).
			regimeFilter = filterOutliersForRegression(regimeRows)
		} else {
			logging.Debug("tuning: history fetch failed (%v) — using baseline", err)
		}
	}
	rows := regimeFilter.kept

	// Override-cost advice (#461) is emitted per exit path below, from
	// the cohort that path's selector actually consumed (identity rows
	// for Tier 1, regime rows otherwise) and filtered to the current
	// reader settings — the same apples-to-apples rule the smoothed-bins
	// tier applies (codex review). Advising from rows the selector
	// deliberately ignored could tell users to unpin based on
	// incomparable history.
	pinnedAdvice := func(cohort []HistoryRecord) {
		if in.PinnedWriteAheadWriters == nil {
			return
		}
		// Reader settings that will actually run: pins beat tuner output.
		pr, rab := out.ParallelReaders, out.ReadAheadBuffers
		if in.PinnedParallelReaders != nil {
			pr = *in.PinnedParallelReaders
		}
		if in.PinnedReadAheadBuffers != nil {
			rab = *in.PinnedReadAheadBuffers
		}
		appendPinnedWAWAdvice(&out, *in.PinnedWriteAheadWriters,
			rowsAtReaderSettings(cohort, pr, rab))
	}

	// Forced/drift-triggered exploration takes precedence over every
	// selection tier (including #215's Tier 1). Users invoking
	// --explore explicitly want a planned-grid probe — overriding it
	// based on history depth would silently ignore the request. Drift
	// detection is similar: when throughput has materially shifted at
	// a fixed config, the existing history is stale and the right
	// answer is to re-explore, not to keep training on it (Codex
	// review on PR #218).
	//
	// Exploration paths don't use the regression's prediction surface,
	// so outlier-filter drops here would be irrelevant to the chosen
	// output — leave appendOutlierReasoning unfired.
	driftDetected := historyAvailable && detectRegimeDrift(regimeRows)
	if in.ForceExplore || driftDetected {
		applyGridExploration(&out, in, profile, len(rows), rawCount)
		// Advice runs after the selector so the reader-settings filter
		// uses the FINAL output readers, not baseline. Suppressed on
		// drift: the tuner just declared this history stale, so means
		// computed from it can't back an unpin recommendation (codex
		// review on #461).
		if !driftDetected {
			pinnedAdvice(rows)
		}
		finalizeTierAndReasoning(&out, history, historyAvailable)
		applyMemoryClamp(&out, in)
		return out
	}

	// Tier 1 (#215): exact-identity classifier. When the history holds
	// enough rows for THIS exact workload (source+target endpoints all
	// match) AND those rows are still regime-comparable to today's run,
	// train the regression on those alone. Best signal when available;
	// falls through silently to Tier 2 below when there aren't enough
	// exact matches.
	//
	// The identity filter is applied to the *regime-filtered* cohort,
	// not rawRows. Otherwise a same-endpoint run after a hardware or
	// DB-tuning change (or a workload that grew past the row-count
	// threshold) would silently keep training on incompatible old
	// data: the host hasn't physically changed identity, but the
	// regime has. Apply both filters to keep "exact same workload +
	// comparable setup" as the actual Tier 1 contract (Codex review
	// on PR #218).
	//
	// Tier 1 skips the cold-start exploration grid because the bucket
	// is already past cold-start FOR THIS WORKLOAD by definition. ε-
	// perturbation still applies — that's the steady-state exploration
	// channel for keeping the regression's training data fresh.
	if historyAvailable && hasExactIdentity(in) {
		identityRows := filterByExactIdentity(regimeRows, in)
		identityFilter := filterOutliersForRegression(identityRows)
		if len(identityFilter.kept) >= minRowsToAttemptRegression {
			// Identity cohort wins — its drops are the ones the
			// selector consumed, so emit those (not the regime pass).
			appendOutlierReasoning(&out, identityFilter)
			applyHistorySelection(&out, in, profile, identityFilter.kept)
			if shouldEpsilonPerturb(in.ExplorationEpsilon) {
				applyEpsilonPerturbation(&out, profile, in.ExplorationEpsilon)
			}
			pinnedAdvice(identityFilter.kept)
			finalizeTierAndReasoning(&out, history, historyAvailable)
			applyMemoryClamp(&out, in)
			return out
		}
	}

	// Tier 2 / Tier 3 fallback: regime-filtered selection + cold-start
	// exploration + baseline. Pre-#215 behavior, kept verbatim. Fires
	// when the exact-identity cohort is too thin (or absent — pre-#215
	// rows, nil-history, missing identity in Input).
	//
	// Cold-start exploration: when we have a working history backend
	// AND the bucket is in cold-start (< explorationGridRuns runs) —
	// the planned grid fills out the regression's training data.
	// Nil-history OR a failed history fetch does NOT enter exploration
	// on its own — there's nowhere to learn from / persist the probe
	// to, so the baseline output is what the user gets.
	if historyAvailable && shouldExplore(in, len(rows)) {
		// Exploration paths (planned grid, ε-perturbation) intentionally
		// don't apply the historical-retry filter — that's a SELECTION
		// concern (handled inside applyHistorySelection's selectWAW /
		// argmaxRegression). See issue #186.
		//
		// Same rationale as the ForceExplore/drift branch: the
		// regression's prediction surface isn't consumed, so the
		// regime cohort's outlier drops aren't part of the chosen
		// output — leave appendOutlierReasoning unfired.
		applyGridExploration(&out, in, profile, len(rows), rawCount)
	} else if len(rows) > 0 {
		// Tier 2/3 fallback consumes the regime-filtered cohort —
		// emit its drops as the audit trail for the chosen output.
		appendOutlierReasoning(&out, regimeFilter)
		applyHistorySelection(&out, in, profile, rows)
		if shouldEpsilonPerturb(in.ExplorationEpsilon) {
			applyEpsilonPerturbation(&out, profile, in.ExplorationEpsilon)
		}
	}

	pinnedAdvice(rows)
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
