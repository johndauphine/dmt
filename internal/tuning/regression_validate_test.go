package tuning

import (
	"math"
	"strings"
	"testing"
)

func TestValidateRegressionPrediction_RejectsInvalidSignals(t *testing.T) {
	observed := []HistoryRecord{
		{FinalThroughputBytes: 90},
		{FinalThroughputBytes: 100},
		{FinalThroughputBytes: 110},
	}
	tests := []struct {
		name       string
		predicted  float64
		low        float64
		high       float64
		intervalOK bool
		wantReason string
	}{
		{name: "nan point", predicted: math.NaN(), low: 1, high: 2, intervalOK: true, wantReason: "non-finite point"},
		{name: "infinite point", predicted: math.Inf(1), low: 1, high: 2, intervalOK: true, wantReason: "non-finite point"},
		{name: "zero point", predicted: 0, low: 0, high: 1, intervalOK: true, wantReason: "nonpositive point"},
		{name: "negative point", predicted: -1, low: -2, high: 0, intervalOK: true, wantReason: "nonpositive point"},
		{name: "missing interval", predicted: 100, intervalOK: false, wantReason: "interval unavailable"},
		{name: "nan interval", predicted: 100, low: math.NaN(), high: 120, intervalOK: true, wantReason: "non-finite 95%"},
		{name: "infinite interval", predicted: 100, low: 80, high: math.Inf(1), intervalOK: true, wantReason: "non-finite 95%"},
		{name: "reversed interval", predicted: 100, low: 120, high: 80, intervalOK: true, wantReason: "malformed"},
		{name: "interval misses point", predicted: 100, low: 110, high: 120, intervalOK: true, wantReason: "malformed"},
		{name: "crosses zero", predicted: 100, low: 0, high: 200, intervalOK: true, wantReason: "includes nonpositive"},
		{name: "collapsed against observations", predicted: 0.5, low: 0.4, high: 0.6, intervalOK: true, wantReason: "effectively zero"},
		{name: "wide against point", predicted: 100, low: 99, high: 1_200, intervalOK: true, wantReason: "materially uninformative"},
		{name: "wide against observations", predicted: 1_000, low: 1, high: 2_001, intervalOK: true, wantReason: "materially uninformative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, ok := validateRegressionPrediction(tt.predicted, tt.low, tt.high, tt.intervalOK, observed)
			if ok {
				t.Fatalf("validation accepted invalid prediction; reason=%q", reason)
			}
			if !strings.Contains(reason, tt.wantReason) {
				t.Fatalf("reason = %q, want substring %q", reason, tt.wantReason)
			}
		})
	}
}

func TestApplyHistorySelection_IllConditionedPredictionFallsBackBeforeMutation(t *testing.T) {
	in := Input{
		SourceDBType: "mssql", TargetDBType: "postgres",
		AvgRowBytes:            100_000,
		RepresentativeRowBytes: 100_000,
	}
	profile := DriverProfile{
		Name: "postgres", BaselineWAW: 2,
		OptimumBulkChunkBytes: 25_000_000,
	}
	rows := illConditionedSyntheticHistory()
	model, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fit synthetic regression: %v", err)
	}
	waw, chunkBytes, pr, rab, predicted, ok := argmaxRegression(
		model, in, profile, nil, cellsWithCoverage(rows),
	)
	if !ok {
		t.Fatal("synthetic fixture did not produce an argmax candidate")
	}
	low, high, intervalOK := model.PredictionInterval(
		waw, chunkBytes, pr, rab,
		in.SourceDBType, in.TargetDBType, in.TargetMode, in.AvgRowBytes,
	)
	if !intervalOK || !isFinite(predicted) || predicted <= 0 || low > 0 || high <= piRelativeRatio*predicted {
		t.Fatalf("fixture lost near-zero/wide-uncertainty shape: pred=%v interval=[%v,%v] ok=%v",
			predicted, low, high, intervalOK)
	}

	// The regression-only call proves rejection happens before any selected
	// WAW/chunk/reader values reach Output. Reasoning is expected to change.
	regressionOnly := baseline(in, profile)
	wantWAW, wantChunk := regressionOnly.WriteAheadWriters, regressionOnly.ChunkSize
	wantPR, wantRAB, wantTier := regressionOnly.ParallelReaders, regressionOnly.ReadAheadBuffers, regressionOnly.Tier
	if applyHistoryRegression(&regressionOnly, in, profile, rows) {
		t.Fatalf("ill-conditioned synthetic fit was selected: %s", regressionOnly.Reasoning)
	}
	if regressionOnly.WriteAheadWriters != wantWAW || regressionOnly.ChunkSize != wantChunk ||
		regressionOnly.ParallelReaders != wantPR || regressionOnly.ReadAheadBuffers != wantRAB ||
		regressionOnly.Tier != wantTier {
		t.Fatalf("rejected regression mutated Output: got WAW=%d chunk=%d PR=%d RAB=%d tier=%q; want WAW=%d chunk=%d PR=%d RAB=%d tier=%q",
			regressionOnly.WriteAheadWriters, regressionOnly.ChunkSize,
			regressionOnly.ParallelReaders, regressionOnly.ReadAheadBuffers, regressionOnly.Tier,
			wantWAW, wantChunk, wantPR, wantRAB, wantTier)
	}
	if !strings.Contains(regressionOnly.Reasoning, "regression skipped: invalid prediction") ||
		!strings.Contains(regressionOnly.Reasoning, "includes nonpositive throughput") {
		t.Fatalf("rejection reasoning does not name the invalid interval: %q", regressionOnly.Reasoning)
	}

	// The normal dispatcher must then use measured smoothed bins. Its WAW=4
	// result is the golden value from the deterministic completed-run rows;
	// chunk and reader settings remain at the baseline instead of leaking the
	// rejected regression candidate.
	out := baseline(in, profile)
	baselineChunk := out.ChunkSize
	applyHistorySelection(&out, in, profile, rows)
	if out.Tier != TierSmoothedBins || out.WriteAheadWriters != 4 {
		t.Fatalf("fallback = tier %q WAW=%d, want %q WAW=4; reasoning: %s",
			out.Tier, out.WriteAheadWriters, TierSmoothedBins, out.Reasoning)
	}
	if out.ChunkSize != baselineChunk || out.ParallelReaders != 2 || out.ReadAheadBuffers != 4 {
		t.Fatalf("fallback leaked rejected regression settings: chunk=%d PR=%d RAB=%d; want chunk=%d PR=2 RAB=4",
			out.ChunkSize, out.ParallelReaders, out.ReadAheadBuffers, baselineChunk)
	}
	if !strings.Contains(out.Reasoning, "regression skipped: invalid prediction") ||
		!strings.Contains(out.Reasoning, "smoothed-bins selected WAW=4") {
		t.Fatalf("reasoning does not show rejection followed by fallback: %q", out.Reasoning)
	}
}

// illConditionedSyntheticHistory is intentionally fabricated and scale-free.
// Row width is confounded with reader cells, then the current input asks the
// model to score a much wider (but still synthetic) row. The fit returns a
// finite positive point near zero with an enormous interval spanning zero —
// the exact mathematical failure shape the validity guard must reject.
func illConditionedSyntheticHistory() []HistoryRecord {
	widths := []int64{100, 500, 2_000, 10_000}
	rows := make([]HistoryRecord, 0, 48)
	for i := 0; i < 48; i++ {
		reader := readerGrid[(i/4)%len(readerGrid)]
		rows = append(rows, HistoryRecord{
			SourceDBType:      "mssql",
			TargetDBType:      "postgres",
			WriteAheadWriters: (i % 4) + 1,
			ChunkSize:         []int{25_000, 50_000}[i%2],
			AvgRowBytes:       widths[(i/4)%len(widths)],
			ParallelReaders:   reader.ParallelReaders,
			ReadAheadBuffers:  reader.ReadAheadBuffers,
			FinalThroughput:   float64(10_000 + (i*101)%990_000),
		})
	}
	return rows
}

func TestApplyHistoryRegression_NoisyInformativePredictionStillSelected(t *testing.T) {
	in := Input{
		SourceDBType: "mssql", TargetDBType: "postgres",
		AvgRowBytes: 500,
	}
	profile := DriverProfile{
		Name: "postgres", BaselineWAW: 2,
		OptimumBulkChunkBytes: 25_000_000,
	}
	rows := make([]HistoryRecord, 0, 160)
	noise := []float64{-24_000, 13_000, -7_000, 21_000, -3_000}
	for repeat := range noise {
		for _, reader := range readerGrid {
			for waw := 1; waw <= 4; waw++ {
				for _, chunk := range []int{25_000, 50_000} {
					throughput := 600_000.0 + float64(waw*50_000) +
						float64((reader.ParallelReaders-2)*30_000) +
						float64((reader.ReadAheadBuffers-4)*10_000) + noise[repeat]
					if chunk == 50_000 {
						throughput += 40_000
					}
					rows = append(rows, HistoryRecord{
						SourceDBType:      "mssql",
						TargetDBType:      "postgres",
						WriteAheadWriters: waw,
						ChunkSize:         chunk,
						AvgRowBytes:       500,
						ParallelReaders:   reader.ParallelReaders,
						ReadAheadBuffers:  reader.ReadAheadBuffers,
						FinalThroughput:   throughput,
					})
				}
			}
		}
	}

	out := baseline(in, profile)
	if !applyHistoryRegression(&out, in, profile, rows) {
		t.Fatalf("informative noisy regression was rejected: %s", out.Reasoning)
	}
	if out.Tier != TierRegression || !strings.Contains(out.Reasoning, "regression-selected") {
		t.Fatalf("informative fit did not retain regression selection: tier=%q reasoning=%q", out.Tier, out.Reasoning)
	}
	if strings.Contains(out.Reasoning, "regression skipped") || strings.Contains(out.Reasoning, "wide —") {
		t.Fatalf("informative fit was labeled invalid/uncertain: %q", out.Reasoning)
	}
}

func TestApplyHistoryRegression_DivergentRowWidthsScoresPersistedAction(t *testing.T) {
	const (
		representativeWidth = int64(500)
		legacyModelWidth    = int64(550)
	)
	in := Input{
		SourceDBType:           "mssql",
		TargetDBType:           "postgres",
		AvgRowBytes:            legacyModelWidth,
		RepresentativeRowBytes: representativeWidth,
	}
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
	}

	// A synthetic six-probe cold start followed by six stable observations.
	// The moderate deterministic noise leaves a useful interval at the
	// observed action, while an off-coordinate quadratic extrapolation is
	// intentionally too uncertain for the validity guard.
	cells := append([]explorationCell(nil), explorationCells[:explorationGridRuns]...)
	for range 6 {
		cells = append(cells, explorationCell{
			WAW: 4, CSFraction: fullOptimumFraction,
			ParallelReaders: 2, ReadAheadBuffers: 4,
		})
	}
	noise := [...]float64{-45_000, 30_000, -20_000, 40_000, -10_000, 25_000, 15_000, -35_000, 45_000, -25_000, 20_000, -40_000}
	rows := make([]HistoryRecord, len(cells))
	for i, cell := range cells {
		policyBytes := scaledByteTarget(profile.OptimumBulkChunkBytes, cell.CSFraction)
		throughput := 1_250_000.0 - 18_000*float64((cell.WAW-2)*(cell.WAW-2)) + noise[i]
		if cell.CSFraction == fullOptimumFraction {
			throughput += 35_000
		}
		if cell.ParallelReaders == 4 {
			throughput -= 30_000
		}
		if cell.ReadAheadBuffers == 8 {
			throughput += 25_000
		}
		rows[i] = HistoryRecord{
			SourceDBType:         in.SourceDBType,
			TargetDBType:         in.TargetDBType,
			WriteAheadWriters:    cell.WAW,
			ChunkSize:            rowsFromBytes(policyBytes, representativeWidth),
			AvgRowBytes:          legacyModelWidth,
			ParallelReaders:      cell.ParallelReaders,
			ReadAheadBuffers:     cell.ReadAheadBuffers,
			FinalThroughput:      throughput,
			FinalThroughputBytes: int64(throughput * float64(legacyModelWidth)),
		}
	}

	out := baseline(in, profile)
	if !applyHistoryRegression(&out, in, profile, rows) {
		t.Fatalf("regression rejected an observed policy action after row-width mapping: %s", out.Reasoning)
	}
	if out.Tier != TierRegression || strings.Contains(out.Reasoning, "regression skipped") {
		t.Fatalf("selection = tier %q reasoning %q, want a valid regression", out.Tier, out.Reasoning)
	}
	wantHalf := rowsFromBytes(scaledByteTarget(profile.OptimumBulkChunkBytes, halfOptimumFraction), representativeWidth)
	wantFull := rowsFromBytes(scaledByteTarget(profile.OptimumBulkChunkBytes, fullOptimumFraction), representativeWidth)
	if out.ChunkSize != wantHalf && out.ChunkSize != wantFull {
		t.Fatalf("selected chunk rows = %d, want mapped policy candidate %d or %d", out.ChunkSize, wantHalf, wantFull)
	}
}
