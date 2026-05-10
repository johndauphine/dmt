package tuning

import (
	"errors"
	"math"
	"testing"
)

// TestFitRegression_InsufficientData verifies the fit refuses to solve
// below the minRowsForRegression floor. Caller is expected to fall back
// to the smoothed-bins tier on this error.
func TestFitRegression_InsufficientData(t *testing.T) {
	rows := make([]HistoryRecord, minRowsForRegression-1)
	_, err := fitRegression(rows)
	if !errors.Is(err, errInsufficientRowsForRegression) {
		t.Fatalf("got %v, want errInsufficientRowsForRegression", err)
	}
}

// TestFitRegression_RecoversLinearTrend generates rows where throughput
// is a known linear function of WAW, fits the model, and verifies the
// predictions track the data. Doesn't test for exact β recovery (the
// quadratic terms + categorical features add coefficients that interact
// with the fit) — instead checks that on the training data, residuals
// are small relative to signal.
func TestFitRegression_RecoversLinearTrend(t *testing.T) {
	// Synthetic: throughput = 100 + 50·WAW + noise.
	// 30 rows, WAW spread across 1..6, all same pair, fixed avg_row_bytes.
	rows := make([]HistoryRecord, 30)
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
		t.Fatalf("fitRegression: %v", err)
	}

	// Spot-check predictions across the WAW range — residuals should be
	// small (data is noiseless; only the model's polynomial-vs-linear
	// approximation introduces tiny error).
	maxResidual := 0.0
	for _, r := range rows {
		pred := m.Predict(r.WriteAheadWriters, int64(r.ChunkSize)*r.AvgRowBytes,
			r.ParallelReaders, r.ReadAheadBuffers,
			r.SourceDBType, r.TargetDBType, "", r.AvgRowBytes)
		resid := math.Abs(pred - r.FinalThroughput)
		if resid > maxResidual {
			maxResidual = resid
		}
	}
	if maxResidual > 1.0 {
		t.Errorf("max residual on noiseless data = %.2f, want < 1.0 (model under-fits)", maxResidual)
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
