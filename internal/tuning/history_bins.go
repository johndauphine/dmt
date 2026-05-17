package tuning

import "sort"

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

	// Median throughput across eligible bins (#204 — gates the retry-rate
	// filter so high-retry-but-also-high-throughput WAWs aren't excluded).
	median := binMedianThroughput(bins)

	bestWAW := -1
	bestShrunk := -1.0
	for _, b := range bins {
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
	median := binMedianThroughput(bins)
	n := 0
	for _, b := range bins {
		if !isHighRetryRateBin(b, median) && b.TotalRuns >= minRunsPerBin {
			n++
		}
	}
	return n
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
