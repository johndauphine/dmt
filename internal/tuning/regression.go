package tuning

import (
	"errors"

	"gonum.org/v1/gonum/mat"
)

// Quadratic OLS over the migration history.
//
// Model (#224 — dependent variable is throughput in BYTES per second):
//
//	throughput_bytes = β₀
//	           + β₁·waw_z + β₂·waw_z²
//	           + β₃·cs_z  + β₄·cs_z²
//	           + β₅·logAvgRowBytes_z
//	           + β₆·pr_z  + β₇·rab_z      (#219 — reader-side linear features)
//	           + Σ γᵢ · pair_i        (one-hot for each (source,target) pair seen)
//	           + Σ δⱼ · mode_j        (one-hot for each target_mode seen)
//	           + ε
//
// y is bytes/sec (FinalThroughputBytes — rows/sec × avg_row_bytes filled
// in by the adapter), not rows/sec. Predicting in bytes/sec makes the
// regression dimensionally clean: chunk_size_bytes (input feature) and
// the prediction target now use the same unit family. Pre-#224 the
// log(avg_row_bytes) feature did double duty — it captured genuine row-
// width effects AND served as the implicit unit conversion relating
// bytes-of-input to rows-of-output. With bytes/sec on the y axis, that
// feature is purely about row-width effects, which materially improves
// cross-workload R². Within a fixed-row-width cohort the math is
// unchanged (bytes/sec = rows/sec × constant; R² is invariant under
// linear y rescaling).
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

type pairKey struct {
	source, target string
}
