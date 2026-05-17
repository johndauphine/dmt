package tuning

import (
	"fmt"
	"math"
	"sort"

	"gonum.org/v1/gonum/mat"
)

// fitRegression builds the design matrix from the supplied rows and
// solves for β via ridge-regularized normal equations (NOT pure QR;
// see the package comment for why). Standardizes numeric features for
// stability. Rows must have FinalThroughput > 0 (incomplete runs are
// excluded by the caller in PR1's pipeline).
//
// The dependent variable y is FinalThroughputBytes (#224 — bytes/sec).
// Rows where FinalThroughputBytes is zero get the safeAvgRowBytes
// fallback applied here so older fixtures that only set FinalThroughput
// (notably tests written before #224) keep training rather than
// silently collapsing to a constant-zero y.
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

		y.SetVec(i, throughputBytes(r))
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
