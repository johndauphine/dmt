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
//	           + Σ γᵢ · pair_i        (one-hot for each (source,target) pair seen)
//	           + Σ δⱼ · mode_j        (one-hot for each target_mode seen)
//	           + ε
//
// The three numeric features (waw, chunk_size_bytes, log(avg_row_bytes))
// are standardized to mean=0/std=1 from the training data so the OLS
// solve doesn't get ill-conditioned by the cs_bytes vs waw scale gap
// (cs_bytes ≈ 1e7, waw ≈ 1-8). Categorical features are one-hot encoded
// with NO baseline-dropped category — the intercept absorbs the baseline
// which the QR solve handles.
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
// practice we want enough data that coefficients aren't pure noise. The
// largest model we'd build here has ~5 numeric + 9 pairs + 2 modes = 16
// features; require 30 rows so there's headroom.
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

	beta []float64 // one coefficient per feature column, in column order
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
	var wawSum, csSum, logAvgSum float64
	var wawSqSum, csSqSum, logAvgSqSum float64
	for _, r := range rows {
		w := float64(r.WriteAheadWriters)
		c := float64(int64(r.ChunkSize) * safeAvgRowBytes(r.AvgRowBytes)) // CS in bytes
		la := math.Log(float64(safeAvgRowBytes(r.AvgRowBytes)))
		wawSum += w
		csSum += c
		logAvgSum += la
		wawSqSum += w * w
		csSqSum += c * c
		logAvgSqSum += la * la
	}
	n := float64(len(rows))
	wawMean := wawSum / n
	csMean := csSum / n
	logAvgMean := logAvgSum / n
	wawStd := stddevSafe(wawSqSum, wawMean, n)
	csStd := stddevSafe(csSqSum, csMean, n)
	logAvgStd := stddevSafe(logAvgSqSum, logAvgMean, n)

	// Build the design matrix. Column layout:
	//   [0]            intercept (always 1)
	//   [1] waw_z
	//   [2] waw_z²
	//   [3] cs_z
	//   [4] cs_z²
	//   [5] logAvg_z
	//   [6 .. 6+|pairs|-1]    one-hot per pair
	//   [6+|pairs| .. ]       one-hot per mode
	nFeat := 6 + len(pairs) + len(modes)
	X := mat.NewDense(len(rows), nFeat, nil)
	y := mat.NewVecDense(len(rows), nil)

	for i, r := range rows {
		wz := standardize(float64(r.WriteAheadWriters), wawMean, wawStd)
		cz := standardize(float64(int64(r.ChunkSize)*safeAvgRowBytes(r.AvgRowBytes)), csMean, csStd)
		laz := standardize(math.Log(float64(safeAvgRowBytes(r.AvgRowBytes))), logAvgMean, logAvgStd)

		X.Set(i, 0, 1)
		X.Set(i, 1, wz)
		X.Set(i, 2, wz*wz)
		X.Set(i, 3, cz)
		X.Set(i, 4, cz*cz)
		X.Set(i, 5, laz)

		// One-hot pair
		for j, p := range pairs {
			if p.source == r.SourceDBType && p.target == r.TargetDBType {
				X.Set(i, 6+j, 1)
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

	return &regressionModel{
		pairs:      pairs,
		modes:      modes,
		wawMean:    wawMean,
		wawStd:     wawStd,
		csMean:     csMean,
		csStd:      csStd,
		logAvgMean: logAvgMean,
		logAvgStd:  logAvgStd,
		beta:       betaSlice,
	}, nil
}

// Predict returns the model's expected throughput for a candidate (waw,
// csBytes) at the given current run's (source, target, target_mode,
// avg_row_bytes). Pair and mode the model didn't see during training
// contribute zero (the categorical features are absent); the prediction
// then collapses to the numeric-feature surface plus the intercept.
func (m *regressionModel) Predict(waw int, csBytes int64, sourceDB, targetDB, mode string, avgRowBytes int64) float64 {
	wz := standardize(float64(waw), m.wawMean, m.wawStd)
	cz := standardize(float64(csBytes), m.csMean, m.csStd)
	laz := standardize(math.Log(math.Max(1, float64(avgRowBytes))), m.logAvgMean, m.logAvgStd)

	pred := m.beta[0]           // intercept
	pred += m.beta[1] * wz      // waw
	pred += m.beta[2] * wz * wz // waw²
	pred += m.beta[3] * cz      // cs
	pred += m.beta[4] * cz * cz // cs²
	pred += m.beta[5] * laz     // log(avg)

	for j, p := range m.pairs {
		if p.source == sourceDB && p.target == targetDB {
			pred += m.beta[6+j]
			break
		}
	}
	for j, mm := range m.modes {
		if mm == mode {
			pred += m.beta[6+len(m.pairs)+j]
			break
		}
	}
	return pred
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
