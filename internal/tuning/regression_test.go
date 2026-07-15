package tuning

import (
	"errors"
	"math"
	"testing"
)

// TestFitRegression_InsufficientData verifies the fit refuses to solve
// below the dof-based floor (#452): a single-pair cohort builds the
// 8-feature model, so 8 + minRegressionDOF = 12 rows are required.
// Caller is expected to fall back to the smoothed-bins tier on this error.
func TestFitRegression_InsufficientData(t *testing.T) {
	rows := make([]HistoryRecord, minRowsToAttemptRegression-1)
	_, err := fitRegression(rows)
	if !errors.Is(err, errInsufficientRowsForRegression) {
		t.Fatalf("got %v, want errInsufficientRowsForRegression", err)
	}
}

func TestArgmaxRegression_SplitsRepresentativeAndLegacyWidths(t *testing.T) {
	model := &regressionModel{
		nFeat:     8,
		beta:      []float64{0, 0, 0, 0, 0, 1, 0, 0},
		logAvgStd: 1,
	}
	in := Input{
		SourceDBType:           "mssql",
		TargetDBType:           "postgres",
		AvgRowBytes:            2_000,
		RepresentativeRowBytes: 8_000,
	}
	profile := DriverProfile{
		OptimumBulkChunkBytes: 16_000_000,
		HardChunkLimit:        1_500,
	}
	covered := map[coverageCellKey]bool{{WAW: 1, ParallelReaders: 2, ReadAheadBuffers: 4}: true}

	_, csBytes, _, _, predicted, ok := argmaxRegression(model, in, profile, nil, covered)
	if !ok {
		t.Fatal("argmax rejected every candidate; representative width should make the 8 MB candidate fit the 1500-row hard limit")
	}
	if csBytes != 8_000_000 {
		t.Fatalf("candidate bytes = %d, want 8000000", csBytes)
	}
	if want := math.Log(float64(in.AvgRowBytes)); math.Abs(predicted-want) > 1e-9 {
		t.Fatalf("prediction = %f, want log(legacy AvgRowBytes)=%f; representative width leaked into model feature", predicted, want)
	}
}

func TestByteTargetAndChunkScalingSaturate(t *testing.T) {
	if got := scaledByteTarget(math.MaxInt64, 1); got != math.MaxInt64 {
		t.Fatalf("full MaxInt64 byte target = %d, want saturation", got)
	}
	maxInt := int(^uint(0) >> 1)
	if got := scalePositiveIntRatio(maxInt, 3, 2); got != maxInt {
		t.Fatalf("near-MaxInt chunk growth = %d, want saturated MaxInt", got)
	}
	if got := scalePositiveIntRatio(maxInt, 67, 100); got <= 0 || got >= maxInt {
		t.Fatalf("near-MaxInt chunk reduction = %d, want a positive decrease", got)
	}
}

// TestFitRegression_RecoversLinearTrend generates rows where throughput
// is a known linear function of WAW, fits the model, and verifies the
// predictions track the data. Doesn't test for exact β recovery (the
// quadratic terms + categorical features add coefficients that interact
// with the fit) — instead checks that on the training data, residuals
// are small relative to signal.
//
// #224: y is bytes/sec. Asserts both Predict and the training-row
// FinalThroughputBytes (rows/sec × avg_row_bytes) in the same unit.
func TestFitRegression_RecoversLinearTrend(t *testing.T) {
	// Synthetic: throughput = 100 + 50·WAW (rows/sec).
	// 30 rows, WAW spread across 1..6, all same pair, fixed avg_row_bytes.
	// bytes/sec = rows/sec × 500B = (100 + 50·WAW) × 500.
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		waw := (i % 6) + 1
		throughput := 100.0 + 50.0*float64(waw)
		rows[i] = HistoryRecord{
			SourceDBType:         "mssql",
			TargetDBType:         "postgres",
			WriteAheadWriters:    waw,
			ChunkSize:            50000,
			AvgRowBytes:          500,
			FinalThroughput:      throughput,
			FinalThroughputBytes: int64(throughput * 500),
			CPUCores:             16, MemoryGB: 48,
		}
	}

	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	// Spot-check predictions across the WAW range — residuals should be
	// small (data is noiseless; only the model's polynomial-vs-linear
	// approximation introduces tiny error). Residual tolerance scaled by
	// AvgRowBytes since y is now in bytes/sec.
	maxResidual := 0.0
	for _, r := range rows {
		pred := m.Predict(r.WriteAheadWriters, int64(r.ChunkSize)*r.AvgRowBytes,
			r.ParallelReaders, r.ReadAheadBuffers,
			r.SourceDBType, r.TargetDBType, "", r.AvgRowBytes)
		resid := math.Abs(pred - float64(r.FinalThroughputBytes))
		if resid > maxResidual {
			maxResidual = resid
		}
	}
	// 1.0 rows/sec pre-#224 → 500 bytes/sec at the 500B avg_row_bytes scale.
	if maxResidual > 500.0 {
		t.Errorf("max residual on noiseless data = %.2f bytes/s, want < 500 (model under-fits)", maxResidual)
	}
}

// TestFitRegression_RecoversQuadraticPeak generates throughput shaped
// like a quadratic in WAW peaking at WAW=4. Verifies the model picks
// WAW=4 as the highest-predicted point. This is the design's whole
// purpose — handle non-monotone WAW surfaces a smoothed-mean misses.
func TestFitRegression_RecoversQuadraticPeak(t *testing.T) {
	// throughput = 1000 - 50·(WAW - 4)² → peak at WAW=4, symmetric
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1 // 1..6
		dev := waw - 4
		throughput := 1000.0 - 50.0*float64(dev*dev)
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   throughput,
			CPUCores:          16, MemoryGB: 48,
		}
	}

	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	// Predict at each candidate WAW; the peak should be at 4.
	bestWAW := 0
	bestPred := math.Inf(-1)
	for waw := 1; waw <= 6; waw++ {
		pred := m.Predict(waw, 50000*500, 0, 0, "mssql", "postgres", "", 500)
		if pred > bestPred {
			bestPred = pred
			bestWAW = waw
		}
	}
	if bestWAW != 4 {
		t.Errorf("regression's argmax WAW = %d, want 4 (quadratic peak)", bestWAW)
	}
}

// TestFitRegression_ConstantFeatureSurvives covers the std=0 edge: when
// every row has the same WAW (or same chunk_size), the variance is zero
// and the standardize() helper would divide by zero. The fit must not
// produce NaN — stddevSafe returns 1 to keep standardize a no-op.
func TestFitRegression_ConstantFeatureSurvives(t *testing.T) {
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: 2, // constant
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   600_000 + float64(i),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	pred := m.Predict(2, 50000*500, 0, 0, "mssql", "postgres", "", 500)
	if math.IsNaN(pred) || math.IsInf(pred, 0) {
		t.Errorf("constant-feature fit produced %v; expected finite", pred)
	}
}

// TestFitRegression_DistinctPairsAreOrdered verifies the categorical
// pair list is deterministically ordered so the same training data
// produces stable coefficient indices across runs (matters for any
// future serialization or coefficient-comparison test).
func TestFitRegression_DistinctPairsAreOrdered(t *testing.T) {
	rows := []HistoryRecord{
		{SourceDBType: "postgres", TargetDBType: "mysql"},
		{SourceDBType: "mssql", TargetDBType: "postgres"},
		{SourceDBType: "mysql", TargetDBType: "mssql"},
		{SourceDBType: "mssql", TargetDBType: "mysql"},
	}
	pairs := distinctPairs(rows)
	want := []pairKey{
		{"mssql", "mysql"},
		{"mssql", "postgres"},
		{"mysql", "mssql"},
		{"postgres", "mysql"},
	}
	if len(pairs) != len(want) {
		t.Fatalf("len = %d, want %d", len(pairs), len(want))
	}
	for i := range pairs {
		if pairs[i] != want[i] {
			t.Errorf("pairs[%d] = %v, want %v", i, pairs[i], want[i])
		}
	}
}

// --- Issue #216: R² + 95% prediction interval ---

// TestFitRegression_R2_PerfectFitNearOne — a noiseless quadratic
// fixture should produce R² close to 1 (the model's surface IS the
// quadratic that generated the data, modulo the standardization).
func TestFitRegression_R2_PerfectFitNearOne(t *testing.T) {
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		dev := waw - 4
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   1000.0 - 50.0*float64(dev*dev),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	if m.r2 == nil {
		t.Fatal("R² nil on a noiseless quadratic; expected near 1.0")
	}
	if *m.r2 < 0.95 {
		t.Errorf("R² = %.3f on noiseless quadratic, want > 0.95", *m.r2)
	}
}

// TestFitRegression_R2_NoiseFitNearZero — uncorrelated noise (random
// throughput, no relationship to features) should yield R² ≈ 0.
// Uses a deterministic seed-equivalent generator so the test is stable.
func TestFitRegression_R2_NoiseFitNearZero(t *testing.T) {
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		// Pseudo-random throughput uncorrelated with WAW — modular hash
		// of i gives a deterministic but non-systematic value.
		throughput := 500_000.0 + float64((i*7919)%200_000)
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: (i % 6) + 1,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   throughput,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	if m.r2 == nil {
		t.Fatal("R² nil on noise fixture; expected near 0")
	}
	// Noise → low R². Allow some slack since the WAW one-hot interaction
	// might pick up modest patterns by chance.
	if *m.r2 > 0.30 {
		t.Errorf("R² = %.3f on uncorrelated noise, want < 0.30 (model is overfitting noise)", *m.r2)
	}
}

// TestFitRegression_R2_PartialFit — a fit on dominant-feature-with-noise
// should produce R² in the (0, 1) range. Throughput is mostly explained
// by WAW (the quadratic catches it), with noise on top.
func TestFitRegression_R2_PartialFit(t *testing.T) {
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		dev := waw - 4
		signal := 1000.0 - 50.0*float64(dev*dev)
		noise := float64((i*7919)%100) - 50 // ±50 around signal
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   signal + noise,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	if m.r2 == nil {
		t.Fatal("R² nil on partial-fit fixture")
	}
	if *m.r2 <= 0.0 || *m.r2 >= 1.0 {
		t.Errorf("R² = %.3f on partial-signal fixture, want in (0, 1)", *m.r2)
	}
}

// TestPredictionInterval_BracketsPrediction — the 95% prediction
// interval must contain the point estimate and have non-zero width on
// any non-degenerate fit. The simplest correctness invariant.
func TestPredictionInterval_BracketsPrediction(t *testing.T) {
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		waw := (i % 6) + 1
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   500_000.0 + float64(waw*100_000) + float64((i*7919)%50_000),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	pred := m.Predict(2, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	low, high, _ := m.PredictionInterval(2, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	if !(low <= pred && pred <= high) {
		t.Errorf("predicted %.0f not bracketed by CI [%.0f, %.0f]", pred, low, high)
	}
	if high-low <= 0 {
		t.Errorf("CI width = %.2f, want > 0 on a non-degenerate fit", high-low)
	}
}

// TestPredictionInterval_TightFitNarrowCI — a noiseless quadratic fit
// should produce a tight CI relative to the prediction (< 10% of the
// point estimate at the well-trained center of the data). The leverage
// term is small there because the design matrix has many similar rows.
func TestPredictionInterval_TightFitNarrowCI(t *testing.T) {
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		dev := waw - 4
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   1_000_000.0 - 50_000.0*float64(dev*dev),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	pred := m.Predict(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	low, high, _ := m.PredictionInterval(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	width := high - low
	relWidth := width / pred
	if relWidth > 0.10 {
		t.Errorf("CI width / prediction = %.3f on noiseless fit, want < 0.10 (CI=[%.0f, %.0f], pred=%.0f)",
			relWidth, low, high, pred)
	}
}

// TestPredictionInterval_NoisyFitWideCI — a fit on noisy data should
// produce a wider CI than a noiseless fit with the same structure.
// Compares relative widths rather than absolute (different data scales
// would otherwise dominate).
func TestPredictionInterval_NoisyFitWideCI(t *testing.T) {
	mkRows := func(noiseAmplitude float64) []HistoryRecord {
		rows := make([]HistoryRecord, 60)
		for i := range rows {
			waw := (i % 6) + 1
			dev := waw - 4
			signal := 1_000_000.0 - 50_000.0*float64(dev*dev)
			noise := noiseAmplitude * (float64((i*7919)%100) - 50) // ±50 × amp
			rows[i] = HistoryRecord{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				WriteAheadWriters: waw,
				ChunkSize:         50000,
				AvgRowBytes:       500,
				FinalThroughput:   signal + noise,
				CPUCores:          16, MemoryGB: 48,
			}
		}
		return rows
	}

	// Noiseless: very narrow CI.
	mClean, _ := fitRegression(mkRows(0))
	predClean := mClean.Predict(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	lowC, highC, _ := mClean.PredictionInterval(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	widthClean := (highC - lowC) / predClean

	// Noisy: noise amplitude 10000× compounds to ±500K throughput jitter.
	mNoisy, _ := fitRegression(mkRows(10_000))
	predNoisy := mNoisy.Predict(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	lowN, highN, _ := mNoisy.PredictionInterval(4, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	widthNoisy := (highN - lowN) / predNoisy

	if widthNoisy <= widthClean*5 {
		t.Errorf("noisy CI relative width %.3f should be much wider than clean %.3f (noise scale 10000x)",
			widthNoisy, widthClean)
	}
}

// TestPredictionInterval_DegenerateDof_EmitsNA — when the model has no
// residual variance computed (sigmaSq nil), PredictionInterval must
// return (pred, pred) so the caller can emit "N/A" rather than NaN/Inf.
// This is a safety test for the degenerate path.
func TestPredictionInterval_DegenerateDof_EmitsNA(t *testing.T) {
	// Build a valid model first, then null out sigmaSq to simulate the
	// degenerate-dof path.
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: (i % 6) + 1,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   500_000.0 + float64(i*1000),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	m.sigmaSq = nil // simulate degenerate dof
	low, high, ok := m.PredictionInterval(2, 25_000_000, 0, 0, "mssql", "postgres", "", 500)
	if ok {
		t.Errorf("degenerate sigmaSq should return ok=false; got ok=true, low=%.0f, high=%.0f", low, high)
	}
}

// TestFitRegression_RecoversPRGradient (#219) builds a dataset where
// throughput depends linearly on parallel_readers with WAW + CS held
// constant. The model must (a) fit without errors on the new feature
// columns and (b) predict higher throughput at PR=4 than at PR=2.
//
// This is the smallest end-to-end check that the new reader-side
// features actually contribute signal — pre-#219 the model dropped
// these axes on the floor, so this test would not have been writable.
func TestFitRegression_RecoversPRGradient(t *testing.T) {
	// throughput = 200 + 100·PR (plus tiny WAW noise so the WAW columns
	// don't collapse to a zero-variance fit). 32 rows: 16 at PR=2,
	// 16 at PR=4, balanced across WAW={1,2,3,4} for stability.
	rows := make([]HistoryRecord, 0, 32)
	for _, pr := range []int{2, 4} {
		for i := 0; i < 16; i++ {
			waw := (i % 4) + 1
			rows = append(rows, HistoryRecord{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				WriteAheadWriters: waw,
				ChunkSize:         50000,
				AvgRowBytes:       500,
				ParallelReaders:   pr,
				ReadAheadBuffers:  4,
				FinalThroughput:   200.0 + 100.0*float64(pr) + 0.1*float64(waw),
				CPUCores:          16, MemoryGB: 48,
			})
		}
	}

	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	pred2 := m.Predict(2, 50000*500, 2, 4, "mssql", "postgres", "", 500)
	pred4 := m.Predict(2, 50000*500, 4, 4, "mssql", "postgres", "", 500)

	// True slope is +100/unit-PR; predicted gap should be in that
	// ballpark (ridge shrinks it slightly, quadratic WAW terms absorb
	// nothing here since WAW is balanced across both PR groups).
	gap := pred4 - pred2
	if gap < 150 {
		t.Errorf("PR gradient not recovered: pred(PR=4)-pred(PR=2)=%.1f, want >=150 (true=200)", gap)
	}
}

// TestFitRegression_RecoversRABGradient is the read_ahead_buffers
// counterpart of TestFitRegression_RecoversPRGradient (#219).
func TestFitRegression_RecoversRABGradient(t *testing.T) {
	// throughput = 200 + 25·RAB + waw noise. 32 rows balanced.
	rows := make([]HistoryRecord, 0, 32)
	for _, rab := range []int{4, 8} {
		for i := 0; i < 16; i++ {
			waw := (i % 4) + 1
			rows = append(rows, HistoryRecord{
				SourceDBType:      "mssql",
				TargetDBType:      "postgres",
				WriteAheadWriters: waw,
				ChunkSize:         50000,
				AvgRowBytes:       500,
				ParallelReaders:   2,
				ReadAheadBuffers:  rab,
				FinalThroughput:   200.0 + 25.0*float64(rab) + 0.1*float64(waw),
				CPUCores:          16, MemoryGB: 48,
			})
		}
	}

	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	pred4 := m.Predict(2, 50000*500, 2, 4, "mssql", "postgres", "", 500)
	pred8 := m.Predict(2, 50000*500, 2, 8, "mssql", "postgres", "", 500)

	// True slope is +25/unit-RAB; gap from RAB=4 to RAB=8 = 100.
	gap := pred8 - pred4
	if gap < 75 {
		t.Errorf("RAB gradient not recovered: pred(RAB=8)-pred(RAB=4)=%.1f, want >=75 (true=100)", gap)
	}
}

// TestFitRegression_ConstantPRRABSurvives covers the std=0 edge for
// the new features (#219). When every row has PR=2 RAB=4, the new
// pr_z / rab_z columns are constant and the standardize() path must
// not produce NaN — stddevSafe returns 1, keeping the column at z=0.
func TestFitRegression_ConstantPRRABSurvives(t *testing.T) {
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: (i % 6) + 1,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			ParallelReaders:   2, // constant
			ReadAheadBuffers:  4, // constant
			FinalThroughput:   600_000 + float64(i),
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}
	pred := m.Predict(2, 50000*500, 2, 4, "mssql", "postgres", "", 500)
	if math.IsNaN(pred) || math.IsInf(pred, 0) {
		t.Errorf("constant PR/RAB fit produced %v; expected finite", pred)
	}
}

// TestTCritical_TableValuesMonotone verifies the t-critical lookup is
// monotonically decreasing with dof in the documented domain (dof ≥ 5)
// and converges to the normal-approximation 1.96 for large dof.
//
// dof < 1 returns 1.96 as a degenerate-marker — caller is expected to
// have already routed through the "N/A" path before invoking. This
// test skips that edge case and validates the meaningful range.
func TestTCritical_TableValuesMonotone(t *testing.T) {
	dofs := []int{5, 10, 20, 30, 50, 100, 200}
	prev := math.Inf(1)
	for _, d := range dofs {
		got := tCritical(d)
		if got >= prev {
			t.Errorf("tCritical(%d)=%.2f not strictly less than previous %.2f (table not monotone)", d, got, prev)
		}
		prev = got
	}
	if tCritical(1000) != 1.96 {
		t.Errorf("tCritical(1000)=%.2f, want 1.96 (normal approximation for large dof)", tCritical(1000))
	}
}

// --- Issue #224: bytes/sec dependent variable ---

// TestFitRegression_LinearScalingInvariance_R2 verifies the issue's
// "Tier 1 (single-workload)" claim: within a fixed-avg_row_bytes
// cohort, scaling y by a constant (bytes/sec = rows/sec × const) is
// a linear transform, and R² is invariant under linear y rescaling.
//
// The test fits the same synthetic fixture twice with FinalThroughputBytes
// set two ways: once as rows/sec × 500 (the "post-#224" path), once as
// rows/sec × 1000 (a fixture pretending the rows were twice as wide).
// R² must be identical to numerical precision — if it isn't, the
// regression has accidentally introduced a non-linear dependence on
// the y scale (e.g. by mixing in a hard-coded threshold or a feature
// derived from y).
func TestFitRegression_LinearScalingInvariance_R2(t *testing.T) {
	mkRows := func(scale int64) []HistoryRecord {
		rows := make([]HistoryRecord, 60)
		for i := range rows {
			waw := (i % 6) + 1
			dev := waw - 4
			rowsPerSec := 1000.0 - 50.0*float64(dev*dev)
			rows[i] = HistoryRecord{
				SourceDBType:         "mssql",
				TargetDBType:         "postgres",
				WriteAheadWriters:    waw,
				ChunkSize:            50000,
				AvgRowBytes:          500,
				FinalThroughput:      rowsPerSec,
				FinalThroughputBytes: int64(rowsPerSec) * scale,
				CPUCores:             16, MemoryGB: 48,
			}
		}
		return rows
	}

	mA, errA := fitRegression(mkRows(500))
	mB, errB := fitRegression(mkRows(1000))
	if errA != nil || errB != nil {
		t.Fatalf("fitRegression errors: A=%v, B=%v", errA, errB)
	}
	if mA.r2 == nil || mB.r2 == nil {
		t.Fatal("R² unexpectedly nil on linearly-rescaled fixtures")
	}
	delta := math.Abs(*mA.r2 - *mB.r2)
	if delta > 1e-9 {
		t.Errorf("R² changed under linear y rescale: A=%.12f, B=%.12f, |Δ|=%.3e (must be invariant)",
			*mA.r2, *mB.r2, delta)
	}
}

// TestFitRegression_BytesPerSecScale verifies the regression's y is in
// bytes/sec post-#224: predictions for a fixed-row-width dataset must
// be ≈ rows/sec × avg_row_bytes, not rows/sec. Catches the "forgot
// to populate FinalThroughputBytes" regression that would otherwise
// silently collapse y to zero (the throughputBytes fallback prevents
// that today, but a future refactor could lose it).
func TestFitRegression_BytesPerSecScale(t *testing.T) {
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		waw := (i % 6) + 1
		rowsPerSec := 1_000_000.0 + 100_000.0*float64(waw) // 1.1M..1.6M rows/s
		rows[i] = HistoryRecord{
			SourceDBType:         "mssql",
			TargetDBType:         "postgres",
			WriteAheadWriters:    waw,
			ChunkSize:            50000,
			AvgRowBytes:          1000, // 1KB rows
			FinalThroughput:      rowsPerSec,
			FinalThroughputBytes: int64(rowsPerSec * 1000), // 1.1GB..1.6GB/s
			CPUCores:             16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	// Predict at WAW=4 should be ≈ (1_000_000 + 100_000·4) × 1000 = 1.4 GB/s,
	// not 1.4M (the rows/sec value pre-#224).
	pred := m.Predict(4, 50_000*1000, 0, 0, "mssql", "postgres", "", 1000)
	if pred < 1.0e9 {
		t.Errorf("Predict at the bytes/sec scale: got %.0f, want ≈1.4e9 (bytes/sec)", pred)
	}
}

// TestThroughputBytes_FallbackWhenBytesUnset covers the back-compat path
// for fixtures and adapters that only set FinalThroughput (rows/sec).
// Tests and old persisted rows shouldn't silently train against y=0.
func TestThroughputBytes_FallbackWhenBytesUnset(t *testing.T) {
	r := HistoryRecord{FinalThroughput: 500_000, AvgRowBytes: 800}
	got := throughputBytes(r)
	want := 500_000.0 * 800
	if got != want {
		t.Errorf("throughputBytes fallback = %.0f, want %.0f", got, want)
	}

	// AvgRowBytes==0 → safeAvgRowBytes(0)==500 fallback applies.
	r2 := HistoryRecord{FinalThroughput: 500_000}
	got2 := throughputBytes(r2)
	want2 := 500_000.0 * 500
	if got2 != want2 {
		t.Errorf("throughputBytes default-row-size fallback = %.0f, want %.0f", got2, want2)
	}

	// Explicit FinalThroughputBytes wins.
	r3 := HistoryRecord{FinalThroughput: 1, AvgRowBytes: 1, FinalThroughputBytes: 42}
	if throughputBytes(r3) != 42 {
		t.Errorf("explicit FinalThroughputBytes should win, got %.0f", throughputBytes(r3))
	}

	if got := throughputBytes(HistoryRecord{FinalThroughput: math.Inf(1), AvgRowBytes: math.MaxInt64}); got != 0 {
		t.Errorf("infinite throughput fallback = %.0f, want rejected zero", got)
	}
	if got := throughputBytes(HistoryRecord{FinalThroughput: float64(math.MaxInt64), AvgRowBytes: math.MaxInt64}); got != float64(math.MaxInt64) {
		t.Errorf("overflowing throughput bytes = %.0f, want MaxInt64 saturation", got)
	}
}

// TestFormatBytesPerSec_PicksUnit pins the unit-selection thresholds for
// the new helper (#224). MB/s below 1 GB/s, GB/s at-or-above. The
// boundary value 999_999_999 stays in MB/s (one byte/s short of the
// switchover) so the comparison is "≥" and stable against off-by-one.
func TestFormatBytesPerSec_PicksUnit(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{500_000_000, "500 MB/s"},    // typical disk-bound
		{999_999_999, "1000 MB/s"},   // boundary; %.0f rounds, but stays MB/s
		{1_000_000_000, "1.00 GB/s"}, // exactly at switchover
		{2_500_000_000, "2.50 GB/s"}, // fast NVMe
		{0, "0 MB/s"},                // zero — render, don't blank
		{-50_000_000, "0 MB/s"},      // Copilot review on PR #289 — negative clamps to 0
		{-1_500_000_000, "0 MB/s"},   // negative magnitude doesn't matter; still 0
	}
	for _, tc := range cases {
		got := formatBytesPerSec(tc.in)
		if got != tc.want {
			t.Errorf("formatBytesPerSec(%.0f) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// residualFilterFixture builds a fixture of mostly-clustered rows + one
// underperforming "run 8" outlier + one matching-throughput survivor, all
// at the same (WAW, PR, RAB, ChunkSize, AvgRowBytes). The majority cluster
// trains the regression to predict ~1.0M rows/sec; the outlier at the
// same config but 575K rows/sec is the run-8 repro from the issue and
// must be flagged; the matching row at 1.028M sits within sigma of the
// prediction and must survive. Returns the rows, the outlier's index in
// the slice, and the matching survivor's index.
func residualFilterFixture() ([]HistoryRecord, int, int) {
	rows := make([]HistoryRecord, 0, 60)
	for i := 0; i < 58; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: 1,
			ParallelReaders:   4,
			ReadAheadBuffers:  4,
			ChunkSize:         1000,
			AvgRowBytes:       500,
			// Small spread around 1.03M rows/sec so the model fits tightly
			// and sigma stays small relative to the outlier's deviation.
			FinalThroughput: 1_000_000 + float64(i*1000),
			ChunkRetryCount: 0,
		})
	}
	outlier := HistoryRecord{
		SourceDBType:      "mssql",
		TargetDBType:      "postgres",
		WriteAheadWriters: 1,
		ParallelReaders:   4,
		ReadAheadBuffers:  4,
		ChunkSize:         1000,
		AvgRowBytes:       500,
		FinalThroughput:   575_000, // host throttling — far below the cluster
		ChunkRetryCount:   0,       // clean run; matches the run-8 case
	}
	matching := HistoryRecord{
		SourceDBType:      "mssql",
		TargetDBType:      "postgres",
		WriteAheadWriters: 1,
		ParallelReaders:   4,
		ReadAheadBuffers:  4,
		ChunkSize:         1000,
		AvgRowBytes:       500,
		FinalThroughput:   1_028_000, // within sigma of the cluster mean
		ChunkRetryCount:   0,
	}
	rows = append(rows, outlier, matching)
	return rows, 58, 59
}

// TestFilterOutliersByResiduals_Run8Repro covers the headline case from
// issue #225: the marginal-median 0.5× rule kept a 575K rows/sec run at
// a config the regression predicted to be ~1.06M because 575K was just
// above the 0.5×median floor. The residual filter must catch it.
func TestFilterOutliersByResiduals_Run8Repro(t *testing.T) {
	rows, outlierIdx, matchingIdx := residualFilterFixture()

	kept, drops := filterOutliersByResiduals(rows)

	if len(drops) != 1 {
		t.Fatalf("expected exactly 1 drop, got %d (drops=%+v)", len(drops), drops)
	}
	if drops[0].index != outlierIdx {
		t.Errorf("expected the 575K rows/sec row (index %d) to be the drop, got index %d", outlierIdx, drops[0].index)
	}
	if drops[0].tStat >= -studentizedOutlierThreshold {
		t.Errorf("outlier t-stat should be more negative than -%g, got %.2f", studentizedOutlierThreshold, drops[0].tStat)
	}

	// Matching row must survive — same features, throughput near the
	// prediction surface, no reason to drop it.
	matchingY := rows[matchingIdx].FinalThroughput
	foundMatching := false
	for _, r := range kept {
		if r.FinalThroughput == matchingY {
			foundMatching = true
			break
		}
	}
	if !foundMatching {
		t.Errorf("matching-throughput row (%.0f rows/s) was wrongly dropped", matchingY)
	}
	if len(kept) != len(rows)-1 {
		t.Errorf("expected %d survivors, got %d", len(rows)-1, len(kept))
	}
}

// TestFilterOutliersByResiduals_BelowGate_FallsBack verifies the
// dispatcher uses the marginal filter when the row count is below
// residualFilterMinRows. Below the gate the regression's β isn't
// trustworthy enough to drive principled drops — falling back to the
// marginal-median rule preserves the pre-#225 safety net.
func TestFilterOutliersByResiduals_BelowGate_FallsBack(t *testing.T) {
	// Build a row count below the gate but above filterOutliers'
	// 3-row median-needed floor.
	rows := []HistoryRecord{
		{FinalThroughput: 600_000, ChunkRetryCount: 0},
		{FinalThroughput: 700_000, ChunkRetryCount: 0}, // median
		{FinalThroughput: 800_000, ChunkRetryCount: 0},
		{FinalThroughput: 100_000, ChunkRetryCount: 0}, // <0.5×median, clean → drop
		{FinalThroughput: 100_000, ChunkRetryCount: 1}, // <0.5×median, retried → keep
	}
	if len(rows) >= residualFilterMinRows {
		t.Fatalf("test fixture broken: row count %d should be below %d gate", len(rows), residualFilterMinRows)
	}
	// filterOutliersByResiduals delegates to filterOutliers below the gate.
	kept, drops := filterOutliersByResiduals(rows)
	if drops != nil {
		t.Errorf("expected nil drops when falling back to marginal filter, got %+v", drops)
	}
	if len(kept) != 4 {
		t.Errorf("expected 4 rows kept by marginal fallback, got %d", len(kept))
	}

	// And via the public dispatcher (filterOutliersForRegression) —
	// should also produce the marginal-filter result with nil drops
	// (the dispatcher leaves drops empty on the marginal path, so
	// appendOutlierReasoning is a no-op).
	res := filterOutliersForRegression(rows)
	if len(res.kept) != 4 {
		t.Errorf("dispatcher fallback: expected 4 rows kept, got %d", len(res.kept))
	}
	if res.drops != nil {
		t.Errorf("dispatcher fallback should leave drops nil; got %+v", res.drops)
	}
	if res.total != len(rows) {
		t.Errorf("dispatcher fallback total: got %d, want %d", res.total, len(rows))
	}
}

// TestFilterOutliersByResiduals_DropCapEnforced verifies the
// residualFilterDropCap (10%) ceiling — when far more than 10% of rows
// clear |t|>3 (a sign the model is mis-specified or noise is structured),
// the filter drops at most cap×N and orders them by |t| descending so
// the worst offenders go first. Prevents the model from "defining
// reality" wholesale.
func TestFilterOutliersByResiduals_DropCapEnforced(t *testing.T) {
	// 60 rows total. Half are tightly clustered at ~1.0M rows/sec; the
	// other half are spread at ~200K. The regression splits the
	// difference, residuals on BOTH sides clear |t|>3. With cap=10% the
	// filter must drop at most 6 rows even though 30 candidates exist.
	rows := make([]HistoryRecord, 0, 60)
	for i := 0; i < 30; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 1, ParallelReaders: 4, ReadAheadBuffers: 4,
			ChunkSize: 1000, AvgRowBytes: 500,
			FinalThroughput: 1_000_000 + float64(i*100), // tight cluster
			ChunkRetryCount: 0,
		})
	}
	for i := 0; i < 30; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			WriteAheadWriters: 1, ParallelReaders: 4, ReadAheadBuffers: 4,
			ChunkSize: 1000, AvgRowBytes: 500,
			FinalThroughput: 200_000 + float64(i*100), // far-below cluster
			ChunkRetryCount: 0,
		})
	}

	_, drops := filterOutliersByResiduals(rows)
	maxAllowed := int(math.Floor(residualFilterDropCap * float64(len(rows))))
	if len(drops) > maxAllowed {
		t.Errorf("drop cap violated: %d drops > %d max (%.0f%% of %d rows)",
			len(drops), maxAllowed, residualFilterDropCap*100, len(rows))
	}

	// Verify drops are ordered worst-first.
	for i := 1; i < len(drops); i++ {
		if math.Abs(drops[i-1].tStat) < math.Abs(drops[i].tStat) {
			t.Errorf("drops not ordered by |t| descending at i=%d: |%.2f| < |%.2f|",
				i, drops[i-1].tStat, drops[i].tStat)
		}
	}
}

// TestFilterOutliersByResiduals_WellFitNoNoise verifies no drops on a
// clean fit: when residuals are uniformly small relative to sigma, every
// row's |t| stays well below the threshold and none are flagged. Guards
// against the filter accidentally peeling perfectly-valid runs.
func TestFilterOutliersByResiduals_WellFitNoNoise(t *testing.T) {
	// 60 rows, throughput = 100 + 50·WAW (deterministic linear). Noise
	// is zero; residuals are floating-point rounding errors only.
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		throughput := 100.0 + 50.0*float64(waw)
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ParallelReaders:   4,
			ReadAheadBuffers:  4,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   throughput,
			ChunkRetryCount:   0,
		}
	}

	kept, drops := filterOutliersByResiduals(rows)
	if len(drops) != 0 {
		t.Errorf("expected no drops on noiseless data, got %d: %+v", len(drops), drops)
	}
	if len(kept) != len(rows) {
		t.Errorf("expected all %d rows kept, got %d", len(rows), len(kept))
	}
}

// TestFilterOutliersByResiduals_LegacyZeroAvgRowBytes guards the codex-
// reviewed P2 bug: rows with AvgRowBytes=0 (older schema, manual inserts)
// score against the SAME log-row-width feature fitRegression used during
// training. Without the safeAvgRowBytes fallback on the scoring side, the
// logAvg column has zero variance at scoring → ridge inverse blows up →
// leverage clamps near 1 → valid well-fit rows exceed |t|>3 and get
// cleared up to the 10% cap. Reproduced by codex on the first commit.
func TestFilterOutliersByResiduals_LegacyZeroAvgRowBytes(t *testing.T) {
	rows := make([]HistoryRecord, 60)
	for i := range rows {
		waw := (i % 6) + 1
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ParallelReaders:   4,
			ReadAheadBuffers:  4,
			ChunkSize:         50000,
			AvgRowBytes:       0, // legacy — adapter should have filled it
			FinalThroughput:   100.0 + 50.0*float64(waw),
			ChunkRetryCount:   0,
		}
	}
	_, drops := filterOutliersByResiduals(rows)
	if len(drops) != 0 {
		t.Errorf("legacy zero-AvgRowBytes rows wrongly dropped: %d drops, first=%+v", len(drops), drops[0])
	}
}

// TestFilterOutliersForRegression_TotalExcludesIncompletes guards the
// Copilot review on PR #290: outlierFilterResult.total must reflect
// the count of rows actually scored against |t| (i.e. with incomplete
// FinalThroughput==0 rows stripped), not the raw input length. The
// reasoning line "dropped X of Y rows" uses total as the denominator,
// and that denominator must agree with the 10% drop cap (which is
// computed against the scored set, not the input).
func TestFilterOutliersForRegression_TotalExcludesIncompletes(t *testing.T) {
	rows, _, _ := residualFilterFixture()
	// Pad with 5 incomplete rows the residual filter will strip before
	// scoring. Pre-fix this would have pushed total to 65; post-fix it
	// stays at 60 (the scored set).
	for i := 0; i < 5; i++ {
		rows = append(rows, HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: 1,
			ParallelReaders:   4,
			ReadAheadBuffers:  4,
			ChunkSize:         1000,
			AvgRowBytes:       500,
			FinalThroughput:   0, // incomplete — gets stripped
		})
	}
	res := filterOutliersForRegression(rows)
	scored := len(res.kept) + len(res.drops)
	if res.total != scored {
		t.Errorf("total=%d should equal scored count (kept %d + drops %d = %d); raw input was %d",
			res.total, len(res.kept), len(res.drops), scored, len(rows))
	}
	// And explicitly: total must NOT count the 5 incompletes.
	if res.total == len(rows) {
		t.Errorf("total=%d equals raw input length, but it should exclude incomplete rows", res.total)
	}
}

// TestFilterOutliersByResiduals_RetryRowsExempt verifies a low-throughput
// run with ChunkRetryCount > 0 is NOT dropped even when |t|>3 — low
// throughput WITH retries is real load contention, not measurement
// noise, and the regression should learn from it. Matches the existing
// filterOutliers policy preserving retry signal.
func TestFilterOutliersByResiduals_RetryRowsExempt(t *testing.T) {
	rows, outlierIdx, _ := residualFilterFixture()
	rows[outlierIdx].ChunkRetryCount = 5 // same features + low throughput, but retried

	_, drops := filterOutliersByResiduals(rows)
	for _, d := range drops {
		if d.index == outlierIdx {
			t.Errorf("retry-row (index %d, %d retries) was wrongly flagged as outlier: t=%.2f",
				outlierIdx, rows[outlierIdx].ChunkRetryCount, d.tStat)
		}
	}
}

// TestFitRegression_EngagesAtDOFFloor verifies the single-pair model
// fits at exactly nFeat + minRegressionDOF = 12 rows (#452) — the row
// count where the regression tier starts engaging for the cohort shape
// production always produces (per-pair fetch → pair one-hot dropped,
// modes never persisted).
func TestFitRegression_EngagesAtDOFFloor(t *testing.T) {
	rows := make([]HistoryRecord, minRowsToAttemptRegression)
	for i := range rows {
		waw := (i % 6) + 1
		throughput := 100.0 + 50.0*float64(waw)
		rows[i] = HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: waw,
			ChunkSize:         50000,
			AvgRowBytes:       500,
			FinalThroughput:   throughput,
			CPUCores:          16, MemoryGB: 48,
		}
	}
	m, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression at the 12-row floor: %v", err)
	}
	if m.nFeat != 8 {
		t.Errorf("nFeat = %d, want 8 (single-level pair one-hot must be dropped)", m.nFeat)
	}
	if len(m.pairs) != 0 {
		t.Errorf("pairs = %v, want none encoded for a single-pair cohort", m.pairs)
	}
	if m.sigmaSq == nil {
		t.Error("sigmaSq nil — dof floor should leave residual variance defined")
	}
}

// TestFitRegression_MultiPairFloorScales verifies the dof-based floor
// grows with the encoded feature count (#452): a two-pair cohort keeps
// its one-hot columns (nFeat=10), so 13 rows refuse and 14 fit.
func TestFitRegression_MultiPairFloorScales(t *testing.T) {
	mkRows := func(n int) []HistoryRecord {
		rows := make([]HistoryRecord, n)
		for i := range rows {
			src := "mssql"
			if i%2 == 0 {
				src = "mysql"
			}
			waw := (i % 6) + 1
			rows[i] = HistoryRecord{
				SourceDBType:      src,
				TargetDBType:      "postgres",
				WriteAheadWriters: waw,
				ChunkSize:         50000,
				AvgRowBytes:       500,
				FinalThroughput:   100.0 + 50.0*float64(waw),
				CPUCores:          16, MemoryGB: 48,
			}
		}
		return rows
	}

	if _, err := fitRegression(mkRows(13)); !errors.Is(err, errInsufficientRowsForRegression) {
		t.Fatalf("13 rows / 10 features: got %v, want errInsufficientRowsForRegression", err)
	}
	m, err := fitRegression(mkRows(14))
	if err != nil {
		t.Fatalf("14 rows / 10 features: %v", err)
	}
	if m.nFeat != 10 || len(m.pairs) != 2 {
		t.Errorf("nFeat=%d pairs=%d, want 10 and 2 (multi-level pair one-hot kept)", m.nFeat, len(m.pairs))
	}
}
