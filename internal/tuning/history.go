package tuning

import (
	"fmt"
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

// applyHistory layers history-aware selection on top of the baseline.
// Steps:
//  1. Pull raw records from the provider for the (source, target) pair.
//  2. Filter to comparable regimes (same_regime + different_tuning;
//     exclude different_hw + different_hw_and_tuning + unknown).
//  3. Drop outliers — clean runs whose throughput is below half the
//     median of the surviving rows.
//  4. Aggregate per-WAW: TotalRuns, RunsWithRetries, MeanThroughput.
//  5. Pick the WAW with the highest shrunk mean among bins with zero
//     retries (RULE 1 hard exclusion). Smoothed-mean shrinks each bin's
//     measured mean toward the global mean by run count.
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

	bins := aggregateByWAW(rows)
	if len(bins) == 0 {
		return
	}

	picked, picked_mean, ok := selectWAW(bins, profile.BaselineWAW)
	if !ok {
		return
	}
	if picked != out.WriteAheadWriters {
		out.WriteAheadWriters = picked
		out.Reasoning = appendReasoning(out.Reasoning,
			"history-selected WAW=%d (shrunk mean %.0f rows/s; %d bins survived RULE 1 + regime + outlier filters)",
			picked, picked_mean, countCleanBins(bins),
		)
	}
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
func selectWAW(bins []wawBin, baselineWAW int) (waw int, shrunkMean float64, ok bool) {
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

// countCleanBins returns the count of bins surviving RULE 1, used for the
// reasoning string so a reviewer can see how thin the basis was.
func countCleanBins(bins []wawBin) int {
	n := 0
	for _, b := range bins {
		if b.RunsWithRetries == 0 {
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
