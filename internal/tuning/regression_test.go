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
		pred := m.Predict(waw, 50000*500, "mssql", "postgres", "", 500)
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
	pred := m.Predict(2, 50000*500, "mssql", "postgres", "", 500)
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
