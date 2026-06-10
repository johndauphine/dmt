package tuning

import (
	"math"

	"gonum.org/v1/gonum/mat"
)

// PredictionInterval returns the 95% prediction interval at the given
// candidate point in BYTES per second (#224 — matches the unit Predict
// returns; the regression's y was switched from rows/sec to bytes/sec
// for dimensional cleanliness). low and high are the values such that
// the true throughput in bytes/sec is expected to fall in [low, high]
// 95% of the time. Standard ridge-OLS formula:
//
//	ŷ ± t_{0.975, n-p} × σ̂ × √(1 + x*ᵀ (XᵀX + λI)⁻¹ x*)
//
// Returns ok=false when the model can't compute a valid interval —
// degenerate dof (nObs ≤ nFeat), missing residual variance, or
// singular xtxInverse. Caller should emit "N/A" in that case rather
// than treating low/high as meaningful.
//
// Codex review on PR #217 caught the prior "low == high → N/A"
// sentinel as ambiguous: a perfect fit (σ̂² == 0) can legitimately
// produce a zero-width interval, which is meaningful (high model
// confidence) — collapsing it to "N/A" would hide that signal.
//
// Complement to R²: R² says how well the model fits its training data
// (model-level signal); the prediction interval says how uncertain the
// prediction at THIS specific point is (point-level signal). Tight CI
// at a point in a sparsely-trained region is mathematically rare — the
// (XᵀX)⁻¹ leverage term grows when x* is far from training-row centers.
func (m *regressionModel) PredictionInterval(waw int, csBytes int64, parallelReaders, readAheadBuffers int, sourceDB, targetDB, mode string, avgRowBytes int64) (low, high float64, ok bool) {
	pred := m.Predict(waw, csBytes, parallelReaders, readAheadBuffers, sourceDB, targetDB, mode, avgRowBytes)
	if m.sigmaSq == nil || m.xtxInverse == nil {
		return 0, 0, false
	}
	xStar := m.featureVector(waw, csBytes, parallelReaders, readAheadBuffers, sourceDB, targetDB, mode, avgRowBytes)
	var leverage mat.VecDense
	leverage.MulVec(m.xtxInverse, xStar)
	quad := mat.Dot(xStar, &leverage)
	if quad < 0 {
		// Numerical noise; clamp at 0 so the sqrt below is real.
		quad = 0
	}
	se := math.Sqrt(*m.sigmaSq * (1 + quad))
	tCrit := tCritical(m.nObs - m.nFeat)
	return pred - tCrit*se, pred + tCrit*se, true
}

// featureVector builds the standardized feature vector x* for a
// prediction candidate, in the same column layout fitRegression used
// to build the design matrix X. Used by both Predict (which dots it
// with β to get ŷ) and PredictionInterval (which uses it for both
// ŷ and the leverage term). Single source of truth so the two paths
// can't drift out of sync if the feature layout ever changes.
func (m *regressionModel) featureVector(waw int, csBytes int64, parallelReaders, readAheadBuffers int, sourceDB, targetDB, mode string, avgRowBytes int64) *mat.VecDense {
	wz := standardize(float64(waw), m.wawMean, m.wawStd)
	cz := standardize(float64(csBytes), m.csMean, m.csStd)
	laz := standardize(math.Log(math.Max(1, float64(avgRowBytes))), m.logAvgMean, m.logAvgStd)
	prz := standardize(float64(parallelReaders), m.prMean, m.prStd)
	rabz := standardize(float64(readAheadBuffers), m.rabMean, m.rabStd)

	x := mat.NewVecDense(m.nFeat, nil)
	x.SetVec(0, 1) // intercept
	x.SetVec(1, wz)
	x.SetVec(2, wz*wz)
	x.SetVec(3, cz)
	x.SetVec(4, cz*cz)
	x.SetVec(5, laz)
	x.SetVec(6, prz)
	x.SetVec(7, rabz)

	for j, p := range m.pairs {
		if p.source == sourceDB && p.target == targetDB {
			x.SetVec(8+j, 1)
			break
		}
	}
	for j, mm := range m.modes {
		if mm == mode {
			x.SetVec(8+len(m.pairs)+j, 1)
			break
		}
	}
	return x
}

// tCritical returns the two-tailed 95% t-critical value for the given
// degrees of freedom. Small lookup table for the dofs we actually see —
// fitRegression's floor guarantees dof ≥ minRegressionDOF (=4), so the
// table now starts at the dof≤4 row (#452). Falls back to 1.96 for
// large dof (the t distribution converges to the normal).
//
// Returns 1.96 (the normal-approximation value) for dof > 100 and for
// dof < 1 (degenerate; caller should be using a fallback marker
// anyway). Conservative interpolation between table values isn't worth
// the precision cost — a 0.05 difference in t-crit shifts the CI by a
// similar fraction; users care about order-of-magnitude not exact width.
func tCritical(dof int) float64 {
	switch {
	case dof < 1:
		return 1.96 // degenerate; caller won't reach this in practice
	case dof <= 4:
		return 2.78
	case dof <= 5:
		return 2.57
	case dof <= 10:
		return 2.23
	case dof <= 20:
		return 2.09
	case dof <= 30:
		return 2.04
	case dof <= 50:
		return 2.01
	case dof <= 100:
		return 1.98
	default:
		return 1.96
	}
}

// Predict returns the model's expected throughput in BYTES per second
// for a candidate (waw, csBytes) at the given current run's (source,
// target, target_mode, avg_row_bytes). #224 switched the dependent
// variable from rows/sec to bytes/sec — callers that need rows/sec
// divide by avg_row_bytes at the boundary (argmaxRegression's
// `csRows := int(pickedCSBytes / avg)` is unaffected because it
// only consumes csBytes, not the prediction itself). Pair and mode
// the model didn't see during training contribute zero (the
// categorical features are absent); the prediction then collapses
// to the numeric-feature surface plus the intercept.
//
// Uses the same featureVector helper as PredictionInterval so the two
// paths share one feature-construction layout (Codex review on PR #217
// caught the duplication). The dot product β·x* is the prediction.
func (m *regressionModel) Predict(waw int, csBytes int64, parallelReaders, readAheadBuffers int, sourceDB, targetDB, mode string, avgRowBytes int64) float64 {
	x := m.featureVector(waw, csBytes, parallelReaders, readAheadBuffers, sourceDB, targetDB, mode, avgRowBytes)
	betaVec := mat.NewVecDense(m.nFeat, m.beta)
	return mat.Dot(betaVec, x)
}
