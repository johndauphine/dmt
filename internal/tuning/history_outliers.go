package tuning

import (
	"math"
	"sort"

	"github.com/johndauphine/dmt/internal/logging"
)

// medianOfFloats returns the upper-middle element of v after sorting.
// Matches the convention in filterOutliers (`throughputs[len/2]`) so
// median calculations across the package are consistent. Returns 0 for
// empty input — callers should treat that as "no median, nothing to
// compare against" (the throughput gate effectively never fires).
func medianOfFloats(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sorted := make([]float64, len(v))
	copy(sorted, v)
	sort.Float64s(sorted)
	return sorted[len(sorted)/2]
}

// filterByRegime keeps rows that ran on comparable hardware. Same regime
// AND different-tuning are both kept (DB-tuning differences don't change
// the WAW throughput surface materially in the cases we've measured);
// different-hw and unknown are dropped (their throughput is at most weak
// evidence for the current run).
func filterByRegime(rows []HistoryRecord, in Input, currentTuning DBTuning) []HistoryRecord {
	out := rows[:0]
	for _, r := range rows {
		label, _ := ClassifyRegime(r, in, currentTuning)
		switch label {
		case RegimeSame, RegimeDifferentTuning:
			out = append(out, r)
		}
	}
	return out
}

// outlierFilterResult bundles the survivors of an outlier-filter pass
// with the drops the pass made and the count of rows the pass actually
// scored. Lets the caller defer reasoning emission until it knows
// whether THIS cohort is the one feeding the eventual selector — Tune
// filters at two cohorts (regime-wide + identity-only) and only one
// of them drives the final pick. Emitting eagerly inside the filter
// would either double-report (when the second cohort is a subset of
// the first) or misattribute drops to a cohort that wasn't used
// (codex review on the initial #225 commit).
type outlierFilterResult struct {
	// kept is the survivor list (always populated, even when no drops).
	kept []HistoryRecord
	// drops is the per-row diagnostic record. Nil/empty when the
	// marginal fallback ran or when no row cleared the |t| threshold;
	// at most residualFilterDropCap × total entries when the residual
	// filter fired.
	drops []outlierDrop
	// total is the count of rows the residual filter actually scored
	// against |t| (i.e. the input minus incomplete rows that the
	// filter strips up front). Matches the denominator used by the
	// 10% drop cap so the "dropped X of Y rows" reasoning line agrees
	// with the cap math. Set to the raw input length on the marginal
	// fallback path — that path doesn't emit reasoning, so the value
	// is effectively unused there (Copilot review on #290).
	total int
}

// filterOutliersForRegression dispatches between the residual-based
// outlier filter (#225) and the marginal-median filter based on row
// count. At or above residualFilterMinRows the residual filter runs — its
// feature-aware t-statistic catches near-threshold noisy points that
// the marginal floor misses (the original 0.5×median rule survived
// run-8-style host throttling at 575K vs a 1.06M prediction). Below
// the gate the residual filter's β estimate isn't trustworthy enough
// to drive principled drops, so the marginal filter is the safety net.
//
// Returns the survivors + drop log; callers emit reasoning via
// appendOutlierReasoning only when they commit to using the cohort,
// so cohorts that get filtered out of the selection path (e.g. an
// identity subset that fell through to Tier 2) don't leave their
// drop counts in the user-visible reasoning.
func filterOutliersForRegression(rows []HistoryRecord) outlierFilterResult {
	if len(rows) < residualFilterMinRows {
		return outlierFilterResult{kept: filterOutliers(rows), total: len(rows)}
	}
	kept, drops := filterOutliersByResiduals(rows)
	// total = scored-row count = survivors + drops. Equals the
	// completed-row count the residual filter actually evaluated
	// (FinalThroughput==0 rows are stripped before scoring), which
	// matches the 10% drop-cap denominator. Pre-fix this was the
	// raw input length, which over-reported when incompletes were
	// present and disagreed with the cap math (Copilot review on
	// #290).
	return outlierFilterResult{kept: kept, drops: drops, total: len(kept) + len(drops)}
}

// appendOutlierReasoning emits a one-line summary of the residual
// filter's drops into out.Reasoning and per-drop diagnostics at DEBUG.
// No-op when out is nil, when no drops happened, or when r is the
// zero value (the marginal-fallback path leaves drops empty).
//
// Callers MUST invoke this exactly once per run, on the cohort that
// fed the eventual selector. Tune filters at two cohorts (regime +
// identity) and picks one — only the chosen cohort's drops belong
// in the operator-facing reasoning string.
func appendOutlierReasoning(out *Output, r outlierFilterResult) {
	if out == nil || len(r.drops) == 0 {
		return
	}
	maxAbsT := 0.0
	for _, d := range r.drops {
		absT := math.Abs(d.tStat)
		if absT > maxAbsT {
			maxAbsT = absT
		}
		logging.Debug("tuning: outlier dropped row #%d actual=%.0f predicted=%.0f t=%.2f",
			d.index, d.actual, d.predicted, d.tStat,
		)
	}
	out.Reasoning = appendReasoning(out.Reasoning,
		"outlier filter dropped %d of %d rows (studentized |t|>%g, max |t|=%.2f)",
		len(r.drops), r.total, studentizedOutlierThreshold, maxAbsT,
	)
}

// filterOutliers drops two kinds of unhelpful rows:
//   - Incomplete: FinalThroughput == 0 (NULL coerced to zero by the
//     checkpoint backend before UpdateTuningResult fires for the
//     row's run). The smoothed-bins aggregator skips these too; the
//     regression in fitRegression would otherwise train on them as
//     legitimate 0 rows/s observations, biasing the fit toward zero
//     (Codex review on PR #183 — bug fix).
//   - Noise: throughput below half the median AND zero retries means
//     measurement variance, not load. Low throughput with retries is
//     a real contention signal and stays.
//
// Retained as the < residualFilterMinRows safety net for
// filterOutliersForRegression — see that function's comment for the
// dispatch rationale.
// dropRuntimeAdjusted strips rows whose run was changed mid-flight by the
// runtime controller or write-error adjuster (#451). Such rows attribute a
// blended multi-config throughput to the single recorded parameter set —
// and the bias is systematic, not noise: the controller intervenes exactly
// when the configured values underperform, so the blend flatters bad
// configs. They are removed before ANY cohort is built (regime, drift,
// outliers, exploration bucket count, selection) — unlike the outlier
// filter, there is no selector for which these rows are valid evidence.
func dropRuntimeAdjusted(rows []HistoryRecord) (kept []HistoryRecord, dropped int) {
	kept = make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.AdjustedAtRuntime {
			dropped++
			continue
		}
		kept = append(kept, r)
	}
	return kept, dropped
}

func filterOutliers(rows []HistoryRecord) []HistoryRecord {
	// First pass: drop incomplete rows so neither pipeline tier sees
	// them. Median below is computed on completed runs only.
	completed := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 {
			completed = append(completed, r)
		}
	}
	if len(completed) < 3 {
		return completed
	}
	throughputs := make([]float64, 0, len(completed))
	for _, r := range completed {
		throughputs = append(throughputs, r.FinalThroughput)
	}
	sort.Float64s(throughputs)
	median := throughputs[len(throughputs)/2]
	floor := median * outlierFloorRatio

	kept := make([]HistoryRecord, 0, len(completed))
	for _, r := range completed {
		if r.FinalThroughput < floor && r.ChunkRetryCount == 0 {
			continue // measurement noise
		}
		kept = append(kept, r)
	}
	return kept
}
