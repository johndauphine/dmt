package tuning

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"gonum.org/v1/gonum/mat"
)

// Quadratic OLS over the migration history.
//
// Model:
//
//	throughput = β₀
//	           + β₁·waw_z + β₂·waw_z²
//	           + β₃·cs_z  + β₄·cs_z²
//	           + β₅·logAvgRowBytes_z
//	           + β₆·pr_z  + β₇·rab_z      (#219 — reader-side linear features)
//	           + Σ γᵢ · pair_i        (one-hot for each (source,target) pair seen)
//	           + Σ δⱼ · mode_j        (one-hot for each target_mode seen)
//	           + ε
//
// The numeric features (waw, chunk_size_bytes, log(avg_row_bytes),
// parallel_readers, read_ahead_buffers) are standardized to mean=0/std=1
// from the training data so the OLS solve doesn't get ill-conditioned by
// the cs_bytes vs waw scale gap (cs_bytes ≈ 1e7, waw ≈ 1-8). PR and RAB
// enter linearly only — the planned exploration grid (#219) probes them
// at just two values each ({2,4} and {4,8}), so a quadratic term would be
// perfectly collinear with the linear one. Categorical features are
// one-hot encoded with NO baseline-dropped category — the intercept
// absorbs the baseline which the ridge solve handles.
//
// The model is trained on rows the caller filters (regime + outliers +
// RULE 1) so the regression sees only "comparable" past runs. PR2 wires
// it as the third tier in applyHistory after smoothed bins and baseline.
//
// Solver: ridge-regularized normal equations. β = (XᵀX + λI)⁻¹ Xᵀy with
// a tiny λ=1e-6. The ridge term is small enough that it barely shifts
// coefficients on well-conditioned fits but big enough to keep the solve
// non-singular when feature columns are collinear (single distinct pair
// → one-hot collinear with intercept; constant numeric feature → zero
// variance column). Pure QR was the first attempt but failed on every
// realistic single-pair fixture — ridge collapses those failures into
// degraded-but-finite predictions, which is what the caller wants
// (predict-then-argmax over a small grid; the intercept dominates and
// the result is ≈ the global mean, which is sensible).

// minRowsForRegression is the floor below which fitRegression refuses
// to solve. p features need at least p+1 observations in principle; in
// practice we want enough data that coefficients aren't pure noise. After
// #219 added pr_z + rab_z the largest model has ~7 numeric + 9 pairs +
// 2 modes = 18 features; 30 rows still gives a dof of ≥12, enough for
// the t-critical lookup to stay in well-tabled territory.
const minRowsForRegression = 30

// halfOptimumFraction and fullOptimumFraction set the chunk-byte grid
// the argmax search walks (per the issue spec for #179 — "CS_BYTES ∈
// {0.5×optimum, 1.0×optimum}"). Kept as named constants so PR3+ can
// extend the grid without hunting through magic numbers.
const (
	halfOptimumFraction = 0.5
	fullOptimumFraction = 1.0
)

// ridgeLambda is the regularization strength for the ridge-augmented
// normal equations. Big enough to handle near-singular X (single-pair
// fixtures, constant-WAW history) without producing infinities; small
// enough that coefficients on well-conditioned fits are essentially
// equivalent to ordinary OLS.
const ridgeLambda = 1e-6

// errInsufficientRowsForRegression is returned by fitRegression when the
// caller passed fewer than minRowsForRegression rows. The caller should
// fall back to the smoothed-bins tier on this error.
var errInsufficientRowsForRegression = errors.New("regression: insufficient rows to fit model")

// regressionModel holds a fitted quadratic model and the metadata
// needed to standardize a prediction's features back into the same
// space the training data used.
type regressionModel struct {
	pairs []pairKey // categorical level for (source, target); deterministic order
	modes []string  // categorical level for target_mode; deterministic order

	// Numeric standardization: subtract mean, divide by std.
	wawMean, wawStd       float64
	csMean, csStd         float64
	logAvgMean, logAvgStd float64
	prMean, prStd         float64 // parallel_readers (#219)
	rabMean, rabStd       float64 // read_ahead_buffers (#219)

	beta []float64 // one coefficient per feature column, in column order

	// Fit-quality signals (#216). Populated by fitRegression after the
	// β solve; surfaced in the regression-tier reasoning string so users
	// can tell whether to trust the prediction.
	//
	// r2 is the training-set R² (1 - SSE/SST). nil when SST==0
	// (degenerate constant-y training data).
	r2 *float64
	// sigmaSq is residual variance σ̂² = SSE / (n - p). Used by
	// PredictionInterval. nil when n ≤ p (degenerate dof).
	sigmaSq *float64
	// xtxInverse is (XᵀX + λI)⁻¹ cached from the ridge solve so
	// PredictionInterval's leverage term doesn't re-do the matrix
	// inversion. nil when the inversion failed (extremely rare with
	// ridge regularization).
	xtxInverse *mat.Dense
	// nObs / nFeat are the dimensions used for the fit. CI math needs
	// them for the t-critical lookup and dof check.
	nObs  int
	nFeat int
}

// PredictionInterval returns the 95% prediction interval at the given
// candidate point: low and high values such that the true throughput is
// expected to fall in [low, high] 95% of the time. Standard ridge-OLS
// formula:
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
// degrees of freedom. Small lookup table for the dofs we actually see
// (n - p where minRowsForRegression=30 and the largest model has
// ~16 features, so dof typically ≥ 14). Falls back to 1.96 for large
// dof (the t distribution converges to the normal).
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

type pairKey struct {
	source, target string
}

// safeAvgRowBytes guards against historical rows that recorded
// AvgRowBytes=0 (older schema, manual inserts, or corrupted writes).
// Falls back to 500 — the same heuristic the analyzer's
// calculateAvgRowSize uses when no row stats are available (#157).
func safeAvgRowBytes(v int64) int64 {
	if v <= 0 {
		return 500
	}
	return v
}

// fitRegression builds the design matrix from the supplied rows and
// solves for β via ridge-regularized normal equations (NOT pure QR;
// see the package comment for why). Standardizes numeric features for
// stability. Rows must have FinalThroughput > 0 (incomplete runs are
// excluded by the caller in PR1's pipeline).
func fitRegression(rows []HistoryRecord) (*regressionModel, error) {
	if len(rows) < minRowsForRegression {
		return nil, errInsufficientRowsForRegression
	}

	pairs := distinctPairs(rows)
	modes := distinctModes(rows)

	// First pass: numeric feature stats for standardization.
	var wawSum, csSum, logAvgSum, prSum, rabSum float64
	var wawSqSum, csSqSum, logAvgSqSum, prSqSum, rabSqSum float64
	for _, r := range rows {
		w := float64(r.WriteAheadWriters)
		c := float64(int64(r.ChunkSize) * safeAvgRowBytes(r.AvgRowBytes)) // CS in bytes
		la := math.Log(float64(safeAvgRowBytes(r.AvgRowBytes)))
		pr := float64(r.ParallelReaders)
		rab := float64(r.ReadAheadBuffers)
		wawSum += w
		csSum += c
		logAvgSum += la
		prSum += pr
		rabSum += rab
		wawSqSum += w * w
		csSqSum += c * c
		logAvgSqSum += la * la
		prSqSum += pr * pr
		rabSqSum += rab * rab
	}
	n := float64(len(rows))
	wawMean := wawSum / n
	csMean := csSum / n
	logAvgMean := logAvgSum / n
	prMean := prSum / n
	rabMean := rabSum / n
	wawStd := stddevSafe(wawSqSum, wawMean, n)
	csStd := stddevSafe(csSqSum, csMean, n)
	logAvgStd := stddevSafe(logAvgSqSum, logAvgMean, n)
	prStd := stddevSafe(prSqSum, prMean, n)
	rabStd := stddevSafe(rabSqSum, rabMean, n)

	// Build the design matrix. Column layout:
	//   [0]            intercept (always 1)
	//   [1] waw_z
	//   [2] waw_z²
	//   [3] cs_z
	//   [4] cs_z²
	//   [5] logAvg_z
	//   [6] pr_z                                 (#219)
	//   [7] rab_z                                (#219)
	//   [8 .. 8+|pairs|-1]    one-hot per pair
	//   [8+|pairs| .. ]       one-hot per mode
	nFeat := 8 + len(pairs) + len(modes)
	X := mat.NewDense(len(rows), nFeat, nil)
	y := mat.NewVecDense(len(rows), nil)

	for i, r := range rows {
		wz := standardize(float64(r.WriteAheadWriters), wawMean, wawStd)
		cz := standardize(float64(int64(r.ChunkSize)*safeAvgRowBytes(r.AvgRowBytes)), csMean, csStd)
		laz := standardize(math.Log(float64(safeAvgRowBytes(r.AvgRowBytes))), logAvgMean, logAvgStd)
		prz := standardize(float64(r.ParallelReaders), prMean, prStd)
		rabz := standardize(float64(r.ReadAheadBuffers), rabMean, rabStd)

		X.Set(i, 0, 1)
		X.Set(i, 1, wz)
		X.Set(i, 2, wz*wz)
		X.Set(i, 3, cz)
		X.Set(i, 4, cz*cz)
		X.Set(i, 5, laz)
		X.Set(i, 6, prz)
		X.Set(i, 7, rabz)

		// One-hot pair
		for j, p := range pairs {
			if p.source == r.SourceDBType && p.target == r.TargetDBType {
				X.Set(i, 8+j, 1)
				break
			}
		}
		// One-hot target_mode (mode is on the row's run; not in HistoryRecord
		// today — left empty to match the schema. PR2's exploration adds the
		// field; for now this loop is a no-op when len(modes)==0.)

		y.SetVec(i, r.FinalThroughput)
	}

	// Ridge-regularized normal equations: β = (XᵀX + λI)⁻¹ Xᵀy.
	var XtX mat.Dense
	XtX.Mul(X.T(), X)
	for i := 0; i < nFeat; i++ {
		XtX.Set(i, i, XtX.At(i, i)+ridgeLambda)
	}
	var Xty mat.VecDense
	Xty.MulVec(X.T(), y)
	var beta mat.VecDense
	if err := beta.SolveVec(&XtX, &Xty); err != nil {
		return nil, fmt.Errorf("regression: ridge solve failed: %w", err)
	}

	betaSlice := make([]float64, nFeat)
	for i := 0; i < nFeat; i++ {
		betaSlice[i] = beta.AtVec(i)
	}

	m := &regressionModel{
		pairs:      pairs,
		modes:      modes,
		wawMean:    wawMean,
		wawStd:     wawStd,
		csMean:     csMean,
		csStd:      csStd,
		logAvgMean: logAvgMean,
		logAvgStd:  logAvgStd,
		prMean:     prMean,
		prStd:      prStd,
		rabMean:    rabMean,
		rabStd:     rabStd,
		beta:       betaSlice,
		nObs:       len(rows),
		nFeat:      nFeat,
	}

	// Fit-quality signals (#216). Computed after the β solve so
	// PredictionInterval and the reasoning string both have access to
	// R² and the cached (XᵀX + λI)⁻¹ leverage matrix.
	computeFitQuality(m, X, y, &XtX)

	return m, nil
}

// computeFitQuality populates the model's R², residual variance, and
// cached (XᵀX + λI)⁻¹ leverage matrix from the freshly-solved fit
// (#216). All three are nil-able to signal "couldn't compute" cases:
// constant-y training data leaves R² nil; n ≤ p degenerate dof leaves
// sigmaSq nil; numerical inversion failure leaves xtxInverse nil. The
// caller (PredictionInterval) and reasoning emitter both handle nils.
func computeFitQuality(m *regressionModel, X *mat.Dense, y *mat.VecDense, ridgeXtX *mat.Dense) {
	n := y.Len()

	// Sum of squares: SSE = Σ(yᵢ - ŷᵢ)²; SST = Σ(yᵢ - ȳ)².
	var ySum float64
	for i := 0; i < n; i++ {
		ySum += y.AtVec(i)
	}
	yMean := ySum / float64(n)

	// Compute ŷ = Xβ via matrix multiply; faster than per-row dot.
	var yHat mat.VecDense
	betaVec := mat.NewVecDense(m.nFeat, m.beta)
	yHat.MulVec(X, betaVec)

	var ssRes, ssTot float64
	for i := 0; i < n; i++ {
		residual := y.AtVec(i) - yHat.AtVec(i)
		ssRes += residual * residual
		dev := y.AtVec(i) - yMean
		ssTot += dev * dev
	}

	// R² = 1 - SSE/SST. Defined only when SST > 0; constant-y training
	// data leaves R² unmeasurable (nothing to "explain").
	if ssTot > 0 {
		r2 := 1.0 - ssRes/ssTot
		m.r2 = &r2
	}

	// Residual variance σ̂² = SSE / (n - p). Defined only when n > p;
	// otherwise the dof is non-positive and the variance estimate is
	// undefined.
	dof := m.nObs - m.nFeat
	if dof > 0 {
		sigmaSq := ssRes / float64(dof)
		m.sigmaSq = &sigmaSq
	}

	// Cache (XᵀX + λI)⁻¹ for PredictionInterval's leverage term. The
	// ridge-augmented matrix is generally invertible (that's the point
	// of ridge); leave nil on the rare failure so PredictionInterval
	// returns the "N/A" sentinel.
	var inv mat.Dense
	if err := inv.Inverse(ridgeXtX); err == nil {
		m.xtxInverse = &inv
	}
}

// Predict returns the model's expected throughput for a candidate (waw,
// csBytes) at the given current run's (source, target, target_mode,
// avg_row_bytes). Pair and mode the model didn't see during training
// contribute zero (the categorical features are absent); the prediction
// then collapses to the numeric-feature surface plus the intercept.
//
// Uses the same featureVector helper as PredictionInterval so the two
// paths share one feature-construction layout (Codex review on PR #217
// caught the duplication). The dot product β·x* is the prediction.
func (m *regressionModel) Predict(waw int, csBytes int64, parallelReaders, readAheadBuffers int, sourceDB, targetDB, mode string, avgRowBytes int64) float64 {
	x := m.featureVector(waw, csBytes, parallelReaders, readAheadBuffers, sourceDB, targetDB, mode, avgRowBytes)
	betaVec := mat.NewVecDense(m.nFeat, m.beta)
	return mat.Dot(betaVec, x)
}

// distinctPairs returns the set of (source, target) pairs seen in rows,
// deterministically ordered (sorted) so model coefficients line up with
// the same indices across calls.
func distinctPairs(rows []HistoryRecord) []pairKey {
	seen := map[pairKey]bool{}
	for _, r := range rows {
		seen[pairKey{r.SourceDBType, r.TargetDBType}] = true
	}
	out := make([]pairKey, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].source != out[j].source {
			return out[i].source < out[j].source
		}
		return out[i].target < out[j].target
	})
	return out
}

// distinctModes is the placeholder for target_mode encoding. The model
// spec calls for target_mode as a categorical fixed effect, but
// HistoryRecord doesn't carry target_mode today (the AITuningRecord
// schema in checkpoint never persisted it). Returns an empty slice so
// the design matrix has zero mode columns — fitRegression handles this
// gracefully and Predict's mode parameter is silently ignored.
//
// A follow-up issue should add TargetMode to HistoryRecord + adapter +
// schema migration to ai_tuning_history; once done this function can
// return the distinct modes seen and the encoding loop in fitRegression
// will start producing real coefficients.
func distinctModes(rows []HistoryRecord) []string {
	_ = rows
	return nil
}

func standardize(x, mean, std float64) float64 {
	if std <= 0 {
		return 0
	}
	return (x - mean) / std
}

// stddevSafe returns sqrt(E[X²] - (E[X])²), or 1 when the variance is
// zero or the count is degenerate. Returning 1 keeps standardize() a
// no-op rather than producing NaN when a feature is constant.
func stddevSafe(sqSum, mean, n float64) float64 {
	if n <= 0 {
		return 1
	}
	variance := sqSum/n - mean*mean
	if variance <= 0 {
		return 1
	}
	return math.Sqrt(variance)
}
