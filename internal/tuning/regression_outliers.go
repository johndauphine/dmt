package tuning

import (
	"math"
	"sort"

	"gonum.org/v1/gonum/mat"
)

// studentizedOutlierThreshold is the |t|-statistic cutoff for the
// residual-based outlier filter (#225). Under a normal-residuals
// assumption K=3 corresponds to ~0.3% expected false-positive rate —
// a probabilistic interpretation the marginal-median 0.5× floor
// doesn't have. The residual statistic is also feature-aware: a
// low-throughput row at a config the model predicts to be fast is
// flagged; a legitimately fast row at a config the model also
// predicts to be fast is not.
const studentizedOutlierThreshold = 3.0

// residualFilterMinRows is the row-count gate below which the residual
// filter is skipped and the marginal filter runs instead. Set at
// 2×minRowsForRegression so the β estimate that drives the residuals
// has enough data to be a trustworthy predictor before its predictions
// are used to flag outliers — the bootstrapping caveat from #225.
const residualFilterMinRows = 2 * minRowsForRegression

// residualFilterDropCap is the maximum fraction of rows the residual
// filter may discard in one pass. Caps the model from "defining
// reality" wholesale: even when many rows clear |t|>K, the worst N
// drop and the rest survive. Drops are ordered by |t| descending.
const residualFilterDropCap = 0.10

// outlierDrop records one row the residual filter discarded. Returned
// alongside the survivor list so the caller can surface counts and
// per-row diagnostics in reasoning + DEBUG logs.
type outlierDrop struct {
	// index is the row's position in the pre-filter (incomplete-stripped)
	// slice. Stable within one pass; not a persistent run identifier.
	index int
	// actual is the row's observed throughput in BYTES per second
	// (throughputBytes after the AvgRowBytes fallback).
	actual float64
	// predicted is the model's ŷ for the row's features, also in
	// bytes/sec. Comparing actual vs predicted is what makes a t-stat
	// interpretable: "this row's throughput is N σ away from what the
	// model expected for its configuration."
	predicted float64
	// tStat is the signed studentized residual rᵢ / (σ̂·√(1-hᵢ)).
	// Negative when the row underperformed the prediction; positive
	// when it overperformed.
	tStat float64
}

// filterOutliersByResiduals replaces the marginal-median 0.5× cutoff
// with a leverage-adjusted studentized-residual filter (#225). Fits the
// regression on the (incomplete-stripped) rows, scores each row by its
// t-statistic against the model's prediction at its own features, and
// drops rows with |t|>studentizedOutlierThreshold AND ChunkRetryCount==0.
//
// Gated above the caller — when len(rows) < residualFilterMinRows the
// caller (filterOutliersForRegression) falls back to the marginal filter
// because the regression's predictions aren't trustworthy enough below
// that floor for residual-filtering to be principled.
//
// Drops are capped at residualFilterDropCap of the row count per pass —
// even if many rows clear the threshold (mis-specified model), never
// strip more than a tenth of the training data. The cap consumes the
// worst offenders first (sorted by |t| descending).
//
// On regression-fit failure, degenerate σ̂², or missing xtxInverse the
// function returns the marginal filter's output — a fit problem at the
// outlier-detection stage must never block history-aware tuning.
//
// Rows with FinalThroughput == 0 are stripped up-front (matches the
// marginal filter's incomplete-row policy from filterOutliers; the
// regression in fitRegression would otherwise train on them as
// legitimate zeroes — Codex review on PR #183).
func filterOutliersByResiduals(rows []HistoryRecord) ([]HistoryRecord, []outlierDrop) {
	completed := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 {
			completed = append(completed, r)
		}
	}
	if len(completed) < residualFilterMinRows {
		return filterOutliers(rows), nil
	}
	model, err := fitRegression(completed)
	if err != nil || model.sigmaSq == nil || *model.sigmaSq <= 0 || model.xtxInverse == nil {
		return filterOutliers(rows), nil
	}
	sigma := math.Sqrt(*model.sigmaSq)
	betaVec := mat.NewVecDense(model.nFeat, model.beta)

	type candidate struct {
		drop outlierDrop
		abs  float64
	}
	candidates := make([]candidate, 0)
	for i, r := range completed {
		// safeAvgRowBytes for BOTH csBytes (already) and avgRowBytes (codex
		// review on this commit): fitRegression applies safeAvgRowBytes when
		// computing the log-row-width feature, so the scoring side must too.
		// Without this, legacy rows with AvgRowBytes==0 get log(1)=0 at
		// scoring vs log(500)≈6.21 at fit time → huge spurious residuals →
		// valid rows wrongly cleared up to the drop cap.
		avg := safeAvgRowBytes(r.AvgRowBytes)
		x := model.featureVector(
			r.WriteAheadWriters,
			int64(r.ChunkSize)*avg,
			r.ParallelReaders,
			r.ReadAheadBuffers,
			r.SourceDBType,
			r.TargetDBType,
			"", // mode not on HistoryRecord
			avg,
		)
		var leverage mat.VecDense
		leverage.MulVec(model.xtxInverse, x)
		h := mat.Dot(x, &leverage)
		// Hat-matrix diagonals are bounded in [0, 1] for ridge regression;
		// clamp the numerical-noise edges so √(1-h) stays real and the
		// denominator stays positive.
		switch {
		case h < 0:
			h = 0
		case h >= 1:
			h = 1 - 1e-9
		}
		pred := mat.Dot(betaVec, x)
		actual := throughputBytes(r)
		residual := actual - pred
		denom := sigma * math.Sqrt(1.0-h)
		if denom <= 0 {
			continue
		}
		tStat := residual / denom
		abs := math.Abs(tStat)
		if abs > studentizedOutlierThreshold && r.ChunkRetryCount == 0 {
			candidates = append(candidates, candidate{
				drop: outlierDrop{
					index:     i,
					actual:    actual,
					predicted: pred,
					tStat:     tStat,
				},
				abs: abs,
			})
		}
	}
	if len(candidates) == 0 {
		return completed, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].abs > candidates[j].abs })

	maxDrops := int(math.Floor(residualFilterDropCap * float64(len(completed))))
	if maxDrops < 1 {
		maxDrops = 1
	}
	if len(candidates) > maxDrops {
		candidates = candidates[:maxDrops]
	}

	dropSet := make(map[int]bool, len(candidates))
	drops := make([]outlierDrop, 0, len(candidates))
	for _, c := range candidates {
		dropSet[c.drop.index] = true
		drops = append(drops, c.drop)
	}
	kept := make([]HistoryRecord, 0, len(completed)-len(dropSet))
	for i, r := range completed {
		if dropSet[i] {
			continue
		}
		kept = append(kept, r)
	}
	return kept, drops
}
