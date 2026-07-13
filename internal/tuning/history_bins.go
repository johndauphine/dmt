package tuning

import (
	"fmt"
	"sort"
)

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
// mean throughput among bins below the retry-rate exclusion threshold
// (#186 — see isHighRetryRateBin / retryRateExclusionThreshold).
//
// Smoothed mean: μ̂_bin = (n·mean + k·global) / (n + k). With small n the
// estimate collapses toward the global mean (don't trust noisy bins);
// with large n it converges to the bin's measured mean.
//
// Returns the picked WAW, its shrunk mean, and ok=true if at least one
// eligible bin was found. ok=false when every bin clears the retry-rate
// threshold (or no bins exist).
func selectWAW(bins []wawBin) (waw int, shrunkMean float64, ok bool) {
	// Legacy history can contain values outside the current learnable domain.
	// Keep those rows available to pinned-override advice, but do not let them
	// become selection candidates or distort the selector's global prior.
	learnable := learnableWAWBins(bins)

	// Global mean across learnable bins (including ones with retries — they're
	// real measurements; just not eligible for selection).
	var total float64
	var totalN int
	for _, b := range learnable {
		total += b.MeanThroughput * float64(b.TotalRuns)
		totalN += b.TotalRuns
	}
	if totalN == 0 {
		return 0, 0, false
	}
	globalMean := total / float64(totalN)

	// Median throughput across eligible bins (#204 — gates the retry-rate
	// filter so high-retry-but-also-high-throughput WAWs aren't excluded).
	median := binMedianThroughput(learnable)

	bestWAW := -1
	bestShrunk := -1.0
	for _, b := range learnable {
		if isHighRetryRateBin(b, median) {
			continue // retry-rate AND below-median throughput (issue #204)
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
// consider — passes both gates of isHighRetryRateBin AND has enough
// runs to clear the minRunsPerBin floor. Used in the reasoning string
// so a reviewer can see how thin the basis was.
func countEligibleBins(bins []wawBin) int {
	learnable := learnableWAWBins(bins)
	median := binMedianThroughput(learnable)
	n := 0
	for _, b := range learnable {
		if !isHighRetryRateBin(b, median) && b.TotalRuns >= minRunsPerBin {
			n++
		}
	}
	return n
}

func learnableWAWBins(bins []wawBin) []wawBin {
	learnable := make([]wawBin, 0, len(bins))
	for _, b := range bins {
		if b.WAW >= 1 && b.WAW <= maxLearnableWAW {
			learnable = append(learnable, b)
		}
	}
	return learnable
}

// isHighRetryRateBin is the bin-level mirror of wawsWithHighRetryRate.
// Returns true when the bin has enough samples AND its retry rate
// exceeds the threshold AND its mean throughput is below medianThr.
// The throughput gate (#204) keeps the filter from punishing
// retry-tolerant workloads where retries coexist with high throughput.
//
// Below the sample-count floor returns false (insufficient evidence to
// exclude — let exploration probe more). Strict less-than on
// throughput is intentional: a bin exactly at the median stays
// eligible (preservation bias for the single-eligible-bin case where
// median equals the bin's own mean).
func isHighRetryRateBin(b wawBin, medianThr float64) bool {
	if b.TotalRuns < minRunsForRetryExclusion {
		return false
	}
	rate := float64(b.RunsWithRetries) / float64(b.TotalRuns)
	if rate <= retryRateExclusionThreshold {
		return false
	}
	return b.MeanThroughput < medianThr
}

// binMedianThroughput returns the median MeanThroughput across bins
// with at least minRunsForRetryExclusion samples. Mirrors the eligibility
// floor wawsWithHighRetryRate uses on raw rows so both paths compute
// median over the same cohort. Returns 0 when no bin clears the floor
// (in that case the throughput gate effectively never fires — but
// neither does the rate gate, so isHighRetryRateBin returns false
// uniformly).
func binMedianThroughput(bins []wawBin) float64 {
	eligible := make([]float64, 0, len(bins))
	for _, b := range bins {
		if b.TotalRuns >= minRunsForRetryExclusion {
			eligible = append(eligible, b.MeanThroughput)
		}
	}
	return medianOfFloats(eligible)
}

// pinnedAdviceMinGain is the relative throughput gain the best eligible
// WAW bin must show over the pinned value's bin before override-cost
// advice fires (#461). Below this, bin noise could be doing the talking
// and the advice would nag users over nothing.
const pinnedAdviceMinGain = 0.10

// appendPinnedWAWAdvice compares the user-pinned write_ahead_writers
// value against the best eligible WAW in the filtered history (#461).
// Both sides must be measured: the pinned value's own bin needs
// minRunsPerBin runs, and the recommendation comes from selectWAW's
// retry-filtered shrunk means. Says nothing when the data can't carry
// the claim — silence here means "no measured evidence", not "the pin
// is fine".
func appendPinnedWAWAdvice(out *Output, pinnedWAW int, rows []HistoryRecord) {
	if len(rows) == 0 {
		return
	}
	bins := aggregateByWAW(rows)
	var pinnedBin *wawBin
	for i := range bins {
		if bins[i].WAW == pinnedWAW {
			pinnedBin = &bins[i]
			break
		}
	}
	if pinnedBin == nil || pinnedBin.TotalRuns < minRunsPerBin {
		return
	}
	bestWAW, _, ok := selectWAW(bins)
	if !ok || bestWAW == pinnedWAW || pinnedBin.MeanThroughput <= 0 {
		return
	}
	// selectWAW chooses on shrunk means (selection needs the shrinkage);
	// the advice reports the chosen bin's MEASURED mean — quoting the
	// shrunk estimate as an average would misstate the evidence (codex
	// review). The bin exists and clears minRunsPerBin or selectWAW
	// wouldn't have picked it.
	var bestMean float64
	for i := range bins {
		if bins[i].WAW == bestWAW {
			bestMean = bins[i].MeanThroughput
			break
		}
	}
	gain := bestMean/pinnedBin.MeanThroughput - 1
	if gain < pinnedAdviceMinGain {
		return
	}
	out.PinnedAdvice = append(out.PinnedAdvice, fmt.Sprintf(
		"write_ahead_writers is pinned at %d (mean %.0f rows/s over %d comparable runs); history shows WAW=%d averaging %.0f rows/s (+%.0f%%) — remove the override to let the tuner manage it",
		pinnedWAW, pinnedBin.MeanThroughput, pinnedBin.TotalRuns, bestWAW, bestMean, gain*100,
	))
}
