package tuning

import "github.com/johndauphine/dmt/internal/logging"

// shrinkageK is the prior weight in the smoothed-mean estimator. With
// k=3 a 3-run bin's mean weighs equally with the global prior; larger N
// gradually overrides the prior. Standard James-Stein-flavored shrinkage.
//
// Smoothed mean alone is not enough to discount sparse outliers: when
// the global mean is data-derived, a 1-run high peak pulls the global
// up too, and the shrunk mean for that bin stays above stable bins.
// The minRunsPerBin floor below catches that case — sparse bins are
// excluded entirely (they're "needs more evidence" not "small loss").
const shrinkageK = 3.0

// minRunsPerBin: a bin needs at least this many measured runs to be
// eligible for selection. Below the floor the bin is excluded — selectWAW
// treats it as no-evidence rather than as low-confidence evidence.
const minRunsPerBin = 3

// outlierFloorRatio is the row-level outlier filter threshold. Rows whose
// throughput is below this fraction of the median AND ran clean (zero
// retries) are treated as measurement noise, not signal. Low throughput
// WITH retries is real load contention and stays in the pool.
const outlierFloorRatio = 0.5

// maxWAWForGrid bounds the WAW search grid used by the regression's
// argmax. Most parallel-write workloads peak well below 8; PR3+ can
// derive this dynamically from cores or driver hints.
const maxWAWForGrid = 8

// retryRateExclusionThreshold is the per-WAW retry rate above which the
// selection paths (smoothed bins, regression argmax) refuse to recommend
// the WAW. Below this rate, transient retries don't poison a WAW value.
//
// Exploration paths (planned grid, ε-perturbation) ignore this entirely
// — they keep probing so historical verdicts get re-examined as new
// data arrives. See issue #186: under the original "any retry → permanent
// exclusion" rule, AI-era runs at WAW=2 (~17% retries from AI's other
// bad choices, not WAW itself) permanently locked the deterministic
// tuner out of WAW=2.
const retryRateExclusionThreshold = 0.15

// minRunsForRetryExclusion is the floor on per-WAW sample count before
// the retry-rate filter is allowed to trigger. With <3 runs the rate is
// too noisy to act on (1/1 = 100% from a single transient retry would
// fire the filter); let exploration accumulate more data first.
const minRunsForRetryExclusion = 3

// applyHistory layers history-aware selection on top of the baseline.
// Tiered engagement (PR2 #179):
//
//	rows < minRunsPerBin (3)          → baseline stands (no history signal)
//	rows < minRowsToAttemptRegression → smoothed bins from PR1
//	rows ≥ minRowsToAttemptRegression → quadratic regression (PR2 §A);
//	  fitRegression enforces the exact dof-based floor for the model it
//	  builds (#452) and a refusal falls through to smoothed bins
//
// The regression tier picks both WAW and chunk_size from a small grid;
// the smoothed-bins tier picks WAW only and leaves chunk_size at the
// per-target byte anchor from baseline. On regression failure the
// caller falls through to the smoothed-bins path so a degenerate fit
// doesn't lose the run.
//
// Filter chain (applied before tier dispatch):
//  1. Pull raw records from the provider for the (source, target) pair.
//  2. Filter to comparable regimes (same_regime + different_tuning;
//     exclude different_hw + different_hw_and_tuning + unknown).
//  3. Drop outliers — clean runs whose throughput is below half the
//     median of the surviving rows.
//
// On any error or empty post-filter dataset, the baseline output stands.
//
// Production calls flow through Tune (which fetches + filters once and
// dispatches between exploration and history selection). This wrapper
// exists so the existing tests can exercise applyHistory's behavior
// directly without rebuilding the whole Tune pipeline.
func applyHistory(out *Output, in Input, profile DriverProfile, history HistoryProvider, currentTuning DBTuning) {
	rows, err := history.Records(in.SourceDBType, in.TargetDBType)
	if err != nil {
		logging.Debug("tuning: history fetch failed (%v) — using baseline", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	// #451: same hygiene as Tune — runtime-adjusted rows precede every
	// cohort, so this wrapper can't train on them either.
	rows, adjustedDropped := dropRuntimeAdjusted(rows)
	if adjustedDropped > 0 {
		out.Reasoning = appendReasoning(out.Reasoning,
			"history hygiene: %d runtime-adjusted run(s) excluded from tuning cohorts (#451)",
			adjustedDropped,
		)
	}

	rows = filterByRegime(rows, in, currentTuning)
	if len(rows) == 0 {
		return
	}

	res := filterOutliersForRegression(rows)
	if len(res.kept) == 0 {
		return
	}
	// applyHistory has a single cohort (no Tier 1 dispatch), so the
	// drops it makes are the ones the selector consumes — emit
	// immediately.
	appendOutlierReasoning(out, res)
	applyHistorySelection(out, in, profile, res.kept)
}

// applyHistorySelection is the tier dispatcher: regression first when
// rows ≥ minRowsToAttemptRegression, smoothed bins as fallback. Operates on
// pre-filtered rows; the caller (Tune or applyHistory) handles fetch +
// regime + outlier filtering.
func applyHistorySelection(out *Output, in Input, profile DriverProfile, rows []HistoryRecord) {
	if len(rows) == 0 {
		return
	}

	// Regression tier first — when the row count clears the floor and the
	// fit succeeds, the model picks both WAW and chunk_size from the
	// per-target grid. On any failure (fit error, empty grid) the
	// regression's skip reason is appended to Reasoning so the
	// fall-through to smoothed-bins is visible to the user — silence
	// would hide why regression was tried but didn't pick (#202).
	if len(rows) >= minRowsToAttemptRegression {
		if applyHistoryRegression(out, in, profile, rows) {
			return
		}
	}

	// Smoothed-bins tier (PR1 path). Picks WAW only; chunk_size stays at
	// the baseline anchor from chunkRowsFromProfile.
	//
	// Pre-#219 every history row used the same reader settings (PR=2,
	// RAB=4) because the exploration grid never varied them, so the WAW
	// bin was inherently apples-to-apples. After #219 the same WAW can
	// have history runs at multiple (PR, RAB) combos. Aggregating those
	// into one WAW bin contaminates retry rates and throughput means
	// (codex review on #219): a bad reader combo's retries could ban a
	// WAW that's actually clean at the current run's reader settings.
	//
	// Fix: filter rows to those matching the output's reader settings
	// (set by baseline above) before binning. The smoothed-bins tier
	// is "what's the best WAW *for this reader configuration*". Rows at
	// other PR/RAB combos are kept in the dataset for the regression
	// tier (above), which can model the reader axes; they just don't
	// belong in a comparison that ignores them.
	binsRows := rowsAtReaderSettings(rows, out.ParallelReaders, out.ReadAheadBuffers)
	if len(binsRows) == 0 {
		out.Reasoning = appendReasoning(out.Reasoning,
			"smoothed-bins skipped: no rows match current reader settings (PR=%d, RAB=%d) — exploration will gather them",
			out.ParallelReaders, out.ReadAheadBuffers,
		)
		return
	}
	bins := aggregateByWAW(binsRows)
	if len(bins) == 0 {
		out.Reasoning = appendReasoning(out.Reasoning,
			"smoothed-bins skipped: no bins after aggregation (zero throughput rows only)",
		)
		return
	}

	picked, pickedMean, ok := selectWAW(bins)
	if !ok {
		out.Reasoning = appendReasoning(out.Reasoning,
			"smoothed-bins skipped: every bin filtered (retry-rate ≥%.0f%% or below ≥%d-run floor)",
			retryRateExclusionThreshold*100, minRunsPerBin,
		)
		return
	}
	// Always emit reasoning even when the pick equals the existing WAW —
	// silence is exactly the observability gap from #202. The user needs
	// to see *which* tier picked, not just whether the value changed.
	verb := "selected"
	if picked == out.WriteAheadWriters {
		verb = "kept"
	}
	out.WriteAheadWriters = picked
	out.Tier = TierSmoothedBins
	out.Reasoning = appendReasoning(out.Reasoning,
		"smoothed-bins %s WAW=%d (shrunk mean %.0f rows/s; %d bins eligible after reader=PR%d/RAB%d, retry-rate, regime, outlier, and ≥%d-run threshold filters)",
		verb, picked, pickedMean, countEligibleBins(bins), out.ParallelReaders, out.ReadAheadBuffers, minRunsPerBin,
	)
}

// rowsAtReaderSettings filters rows to those matching the given
// (parallel_readers, read_ahead_buffers) (#219 codex review pass 3).
// Used by the smoothed-bins tier so the per-WAW comparison is
// apples-to-apples — a bad reader combo's retries don't contaminate
// the WAW bin for the current run's reader configuration.
//
// Rows with FinalThroughput <= 0 are dropped (matches the convention
// in the rest of the tuning filters — incomplete runs aren't evidence).
func rowsAtReaderSettings(rows []HistoryRecord, pr, rab int) []HistoryRecord {
	out := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput <= 0 {
			continue
		}
		if r.ParallelReaders == pr && r.ReadAheadBuffers == rab {
			out = append(out, r)
		}
	}
	return out
}
