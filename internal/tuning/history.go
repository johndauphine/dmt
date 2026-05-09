package tuning

import (
	"fmt"
	"math"
	"sort"

	"github.com/johndauphine/dmt/internal/logging"
)

// shrinkageK is the prior weight in the smoothed-mean estimator. With
// k=3 a 3-run bin's mean weighs equally with the global prior; larger N
// gradually overrides the prior. Standard James-Stein-flavored shrinkage.
//
// Smoothed mean alone is not enough to discount sparse outliers: when
// the global mean is data-derived, a 1-run high peak pulls the global
// up too, and the shrunk mean for that bin stays above stable bins.
// The minRunsPerBin floor below catches that case — sparse bins are
// excluded entirely (they're "needs more evidence" not "small loss").
const shrinkageK = 3.0

// minRunsPerBin: a bin needs at least this many measured runs to be
// eligible for selection. Below the floor the bin is excluded — selectWAW
// treats it as no-evidence rather than as low-confidence evidence.
const minRunsPerBin = 3

// outlierFloorRatio is the row-level outlier filter threshold. Rows whose
// throughput is below this fraction of the median AND ran clean (zero
// retries) are treated as measurement noise, not signal. Low throughput
// WITH retries is real load contention and stays in the pool.
const outlierFloorRatio = 0.5

// maxWAWForGrid bounds the WAW search grid used by the regression's
// argmax. Most parallel-write workloads peak well below 8; PR3+ can
// derive this dynamically from cores or driver hints.
const maxWAWForGrid = 8

// applyHistory layers history-aware selection on top of the baseline.
// Tiered engagement (PR2 #179):
//
//	rows < minRunsPerBin (3)   → baseline stands (no history signal)
//	rows < minRowsForRegression → smoothed bins from PR1
//	rows ≥ minRowsForRegression → quadratic regression (PR2 §A)
//
// The regression tier picks both WAW and chunk_size from a small grid;
// the smoothed-bins tier picks WAW only and leaves chunk_size at the
// per-target byte anchor from baseline. On regression failure the
// caller falls through to the smoothed-bins path so a degenerate fit
// doesn't lose the run.
//
// Filter chain (applied before tier dispatch):
//  1. Pull raw records from the provider for the (source, target) pair.
//  2. Filter to comparable regimes (same_regime + different_tuning;
//     exclude different_hw + different_hw_and_tuning + unknown).
//  3. Drop outliers — clean runs whose throughput is below half the
//     median of the surviving rows.
//
// On any error or empty post-filter dataset, the baseline output stands.
func applyHistory(out *Output, in Input, profile DriverProfile, history HistoryProvider, currentTuning DBTuning) {
	rows, err := history.Records(in.SourceDBType, in.TargetDBType)
	if err != nil {
		logging.Debug("tuning: history fetch failed (%v) — using baseline", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	rows = filterByRegime(rows, in, currentTuning)
	if len(rows) == 0 {
		return
	}

	rows = filterOutliers(rows)
	if len(rows) == 0 {
		return
	}

	// Regression tier first — when the row count clears the floor and the
	// fit succeeds, the model picks both WAW and chunk_size from the
	// per-target grid.
	if len(rows) >= minRowsForRegression {
		if applyHistoryRegression(out, in, profile, rows) {
			return
		}
		// Fit failed (singular even with ridge, or some other numerical
		// edge) — fall through to smoothed bins. Reasoning will reflect
		// whichever tier actually picked.
	}

	// Smoothed-bins tier (PR1 path). Picks WAW only; chunk_size stays at
	// the baseline anchor from chunkRowsFromProfile.
	bins := aggregateByWAW(rows)
	if len(bins) == 0 {
		return
	}

	picked, pickedMean, ok := selectWAW(bins)
	if !ok {
		return
	}
	if picked != out.WriteAheadWriters {
		out.WriteAheadWriters = picked
		out.Reasoning = appendReasoning(out.Reasoning,
			"history-selected WAW=%d (shrunk mean %.0f rows/s; %d bins eligible after RULE 1, regime, outlier, and ≥%d-run threshold filters)",
			picked, pickedMean, countEligibleBins(bins), minRunsPerBin,
		)
	}
}

// applyHistoryRegression fits the quadratic model to the filtered rows
// and walks the (WAW × CS_BYTES) grid to pick the argmax under RULE 1
// + HardChunkLimit. Returns true when it picks (and applies) a
// recommendation; false on fit failure or empty grid (caller falls
// through to smoothed bins).
func applyHistoryRegression(out *Output, in Input, profile DriverProfile, rows []HistoryRecord) bool {
	model, err := fitRegression(rows)
	if err != nil {
		logging.Debug("tuning: regression fit failed (%v) — falling through to smoothed bins", err)
		return false
	}

	skip := wawsWithRetries(rows)
	pickedWAW, pickedCSBytes, predicted, ok := argmaxRegression(model, in, profile, skip)
	if !ok {
		return false
	}

	out.WriteAheadWriters = pickedWAW
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	out.ChunkSize = int(pickedCSBytes / avg)
	out.Reasoning = appendReasoning(out.Reasoning,
		"regression-selected WAW=%d, chunk_size=%d rows (%.1f MB) over %d filtered rows; predicted %.0f rows/s",
		pickedWAW, out.ChunkSize, float64(pickedCSBytes)/1024/1024, len(rows), predicted,
	)
	return true
}

// argmaxRegression walks the small (WAW, CS_BYTES) grid and returns the
// candidate with the highest predicted throughput, subject to RULE 1
// (skip WAWs that have any historical retries) and HardChunkLimit
// (skip CS values above the protocol cap). Returns ok=false when every
// grid point is filtered out.
func argmaxRegression(model *regressionModel, in Input, profile DriverProfile, skip map[int]bool) (waw int, csBytes int64, predicted float64, ok bool) {
	const fallbackBytes int64 = 10_000_000
	optimumBytes := profile.OptimumBulkChunkBytes
	if optimumBytes <= 0 {
		optimumBytes = fallbackBytes
	}
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	csCandidates := []int64{
		int64(halfOptimumFraction * float64(optimumBytes)),
		int64(fullOptimumFraction * float64(optimumBytes)),
	}

	bestWAW := -1
	var bestCSBytes int64
	bestPred := math.Inf(-1)

	for w := 1; w <= maxWAWForGrid; w++ {
		if skip[w] {
			continue
		}
		for _, cs := range csCandidates {
			csRows := int(cs / avg)
			if profile.HardChunkLimit > 0 && csRows > profile.HardChunkLimit {
				continue
			}
			pred := model.Predict(w, cs, in.SourceDBType, in.TargetDBType, in.TargetMode, avg)
			if pred > bestPred {
				bestPred = pred
				bestWAW = w
				bestCSBytes = cs
			}
		}
	}
	if bestWAW < 0 {
		return 0, 0, 0, false
	}
	return bestWAW, bestCSBytes, bestPred, true
}

// wawsWithRetries collects the set of WAW values that had at least one
// historical retry — RULE 1 hard exclusion for the regression argmax.
// Mirrors the bin-level filter in selectWAW for the smoothed-bins path.
func wawsWithRetries(rows []HistoryRecord) map[int]bool {
	m := map[int]bool{}
	for _, r := range rows {
		if r.ChunkRetryCount > 0 {
			m[r.WriteAheadWriters] = true
		}
	}
	return m
}

// filterByRegime keeps rows that ran on comparable hardware. Same regime
// AND different-tuning are both kept (DB-tuning differences don't change
// the WAW throughput surface materially in the cases we've measured);
// different-hw and unknown are dropped (their throughput is at most weak
// evidence for the current run).
func filterByRegime(rows []HistoryRecord, in Input, currentTuning DBTuning) []HistoryRecord {
	out := rows[:0]
	for _, r := range rows {
		label, _ := ClassifyRegime(r, in, currentTuning)
		switch label {
		case RegimeSame, RegimeDifferentTuning:
			out = append(out, r)
		}
	}
	return out
}

// filterOutliers drops noise-shaped rows: throughput below half the
// median AND zero retries means measurement variance, not load. Low
// throughput with retries is a real contention signal and stays.
func filterOutliers(rows []HistoryRecord) []HistoryRecord {
	if len(rows) < 3 {
		return rows
	}
	throughputs := make([]float64, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 {
			throughputs = append(throughputs, r.FinalThroughput)
		}
	}
	if len(throughputs) == 0 {
		return rows
	}
	sort.Float64s(throughputs)
	median := throughputs[len(throughputs)/2]
	floor := median * outlierFloorRatio

	kept := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 && r.FinalThroughput < floor && r.ChunkRetryCount == 0 {
			continue // measurement noise
		}
		kept = append(kept, r)
	}
	return kept
}

// wawBin aggregates the rows at one WriteAheadWriters value.
type wawBin struct {
	WAW             int
	TotalRuns       int
	RunsWithRetries int
	MeanThroughput  float64
}

// aggregateByWAW groups records by WriteAheadWriters and computes the
// per-bin mean throughput + retry count. Ignores rows without a recorded
// throughput (incomplete runs).
func aggregateByWAW(rows []HistoryRecord) []wawBin {
	totals := make(map[int]*struct {
		sum     float64
		count   int
		retries int
	})
	for _, r := range rows {
		if r.FinalThroughput <= 0 {
			continue
		}
		t := totals[r.WriteAheadWriters]
		if t == nil {
			t = &struct {
				sum     float64
				count   int
				retries int
			}{}
			totals[r.WriteAheadWriters] = t
		}
		t.sum += r.FinalThroughput
		t.count++
		if r.ChunkRetryCount > 0 {
			t.retries++
		}
	}
	bins := make([]wawBin, 0, len(totals))
	for waw, t := range totals {
		bins = append(bins, wawBin{
			WAW:             waw,
			TotalRuns:       t.count,
			RunsWithRetries: t.retries,
			MeanThroughput:  t.sum / float64(t.count),
		})
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i].WAW < bins[j].WAW })
	return bins
}

// selectWAW picks the WriteAheadWriters value with the highest shrunk
// mean throughput among bins with zero retries (RULE 1).
//
// Smoothed mean: μ̂_bin = (n·mean + k·global) / (n + k). With small n the
// estimate collapses toward the global mean (don't trust noisy bins);
// with large n it converges to the bin's measured mean.
//
// Returns the picked WAW, its shrunk mean, and ok=true if a clean bin
// was found. ok=false when every bin has retries or no bins exist.
func selectWAW(bins []wawBin) (waw int, shrunkMean float64, ok bool) {
	// Global mean across all bins (including ones with retries — they're
	// real measurements; just not eligible for selection).
	var total float64
	var totalN int
	for _, b := range bins {
		total += b.MeanThroughput * float64(b.TotalRuns)
		totalN += b.TotalRuns
	}
	if totalN == 0 {
		return 0, 0, false
	}
	globalMean := total / float64(totalN)

	bestWAW := -1
	bestShrunk := -1.0
	for _, b := range bins {
		if b.RunsWithRetries > 0 {
			continue // RULE 1: any history of retries → exclude
		}
		if b.TotalRuns < minRunsPerBin {
			continue // not enough evidence to override baseline
		}
		n := float64(b.TotalRuns)
		shrunk := (n*b.MeanThroughput + shrinkageK*globalMean) / (n + shrinkageK)
		if shrunk > bestShrunk {
			bestShrunk = shrunk
			bestWAW = b.WAW
		}
	}
	if bestWAW < 0 {
		return 0, 0, false
	}
	return bestWAW, bestShrunk, true
}

// countEligibleBins returns the count of bins selectWAW would actually
// consider — clean of retries (RULE 1) AND with enough runs to clear
// the minRunsPerBin floor. Used in the reasoning string so a reviewer
// can see how thin the basis was.
func countEligibleBins(bins []wawBin) int {
	n := 0
	for _, b := range bins {
		if b.RunsWithRetries == 0 && b.TotalRuns >= minRunsPerBin {
			n++
		}
	}
	return n
}

// appendReasoning concatenates a new structured reason onto the existing
// Output.Reasoning string, semicolon-separated.
func appendReasoning(existing, format string, args ...any) string {
	added := fmt.Sprintf(format, args...)
	if existing == "" {
		return added
	}
	return existing + "; " + added
}
