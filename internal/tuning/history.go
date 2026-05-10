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

// retryRateExclusionThreshold is the per-WAW retry rate above which the
// selection paths (smoothed bins, regression argmax) refuse to recommend
// the WAW. Below this rate, transient retries don't poison a WAW value.
//
// Exploration paths (planned grid, ε-perturbation) ignore this entirely
// — they keep probing so historical verdicts get re-examined as new
// data arrives. See issue #186: under the original "any retry → permanent
// exclusion" rule, AI-era runs at WAW=2 (~17% retries from AI's other
// bad choices, not WAW itself) permanently locked the deterministic
// tuner out of WAW=2.
const retryRateExclusionThreshold = 0.15

// minRunsForRetryExclusion is the floor on per-WAW sample count before
// the retry-rate filter is allowed to trigger. With <3 runs the rate is
// too noisy to act on (1/1 = 100% from a single transient retry would
// fire the filter); let exploration accumulate more data first.
const minRunsForRetryExclusion = 3

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
//
// Production calls flow through Tune (which fetches + filters once and
// dispatches between exploration and history selection). This wrapper
// exists so the existing tests can exercise applyHistory's behavior
// directly without rebuilding the whole Tune pipeline.
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

	applyHistorySelection(out, in, profile, rows)
}

// applyHistorySelection is the tier dispatcher: regression first when
// rows ≥ minRowsForRegression, smoothed bins as fallback. Operates on
// pre-filtered rows; the caller (Tune or applyHistory) handles fetch +
// regime + outlier filtering.
func applyHistorySelection(out *Output, in Input, profile DriverProfile, rows []HistoryRecord) {
	if len(rows) == 0 {
		return
	}

	// Regression tier first — when the row count clears the floor and the
	// fit succeeds, the model picks both WAW and chunk_size from the
	// per-target grid. On any failure (fit error, empty grid) the
	// regression's skip reason is appended to Reasoning so the
	// fall-through to smoothed-bins is visible to the user — silence
	// would hide why regression was tried but didn't pick (#202).
	if len(rows) >= minRowsForRegression {
		if applyHistoryRegression(out, in, profile, rows) {
			return
		}
	}

	// Smoothed-bins tier (PR1 path). Picks WAW only; chunk_size stays at
	// the baseline anchor from chunkRowsFromProfile.
	bins := aggregateByWAW(rows)
	if len(bins) == 0 {
		out.Reasoning = appendReasoning(out.Reasoning,
			"smoothed-bins skipped: no bins after aggregation (zero throughput rows only)",
		)
		return
	}

	picked, pickedMean, ok := selectWAW(bins)
	if !ok {
		out.Reasoning = appendReasoning(out.Reasoning,
			"smoothed-bins skipped: every bin filtered (retry-rate ≥%.0f%% or below ≥%d-run floor)",
			retryRateExclusionThreshold*100, minRunsPerBin,
		)
		return
	}
	// Always emit reasoning even when the pick equals the existing WAW —
	// silence is exactly the observability gap from #202. The user needs
	// to see *which* tier picked, not just whether the value changed.
	verb := "selected"
	if picked == out.WriteAheadWriters {
		verb = "kept"
	}
	out.WriteAheadWriters = picked
	out.Tier = TierSmoothedBins
	out.Reasoning = appendReasoning(out.Reasoning,
		"smoothed-bins %s WAW=%d (shrunk mean %.0f rows/s; %d bins eligible after retry-rate, regime, outlier, and ≥%d-run threshold filters)",
		verb, picked, pickedMean, countEligibleBins(bins), minRunsPerBin,
	)
}

// applyHistoryRegression fits the quadratic model to the filtered rows
// and walks the (WAW × CS_BYTES) grid to pick the argmax. The grid is
// filtered by wawsWithHighRetryRate (#186 — exclude WAWs whose retry
// rate clears the threshold over enough samples) and HardChunkLimit
// (skip CS values above the protocol cap). Returns true when it picks
// (and applies) a recommendation; false on fit failure or empty grid
// (caller falls through to smoothed bins).
func applyHistoryRegression(out *Output, in Input, profile DriverProfile, rows []HistoryRecord) bool {
	model, err := fitRegression(rows)
	if err != nil {
		// Promote from DEBUG to a Reasoning append so the user can see
		// regression was tried and skipped, not just silently bypassed
		// (#202). DEBUG log retained for the underlying error detail.
		logging.Debug("tuning: regression fit failed (%v) — falling through to smoothed bins", err)
		out.Reasoning = appendReasoning(out.Reasoning,
			"regression skipped: fit failed (%v)", err,
		)
		return false
	}

	skip := wawsWithHighRetryRate(rows)
	pickedWAW, pickedCSBytes, predicted, ok := argmaxRegression(model, in, profile, skip)
	if !ok {
		out.Reasoning = appendReasoning(out.Reasoning,
			"regression skipped: every grid candidate filtered (retry-rate exclusions: %v, HardChunkLimit=%d)",
			sortedKeys(skip), profile.HardChunkLimit,
		)
		return false
	}

	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	csRows := int(pickedCSBytes / avg)
	if csRows < 1 {
		csRows = 1 // mirror chunkRowsFromProfile's floor (Copilot PR #183 review)
	}
	out.WriteAheadWriters = pickedWAW
	out.ChunkSize = csRows
	out.Tier = TierRegression

	// Fit-quality signals (#216): R² (model-level) + 95% prediction
	// interval at the picked point (point-level). Both nil when the
	// model couldn't compute them — emit "N/A" to keep the format
	// consistent without lying about confidence.
	r2Str := "N/A"
	if model.r2 != nil {
		r2Str = fmt.Sprintf("%.2f", *model.r2)
	}
	low, high := model.PredictionInterval(pickedWAW, pickedCSBytes, in.SourceDBType, in.TargetDBType, in.TargetMode, avg)
	ciStr := "N/A"
	if low != high {
		ciStr = formatThroughputRange(low, high)
	}

	out.Reasoning = appendReasoning(out.Reasoning,
		"regression-selected WAW=%d, chunk_size=%d rows (%.1f MB) over %d filtered rows; predicted %.0f rows/s [95%% CI: %s]; R²=%s",
		pickedWAW, csRows, float64(pickedCSBytes)/1024/1024, len(rows), predicted, ciStr, r2Str,
	)
	return true
}

// formatThroughputRange formats a (low, high) throughput pair as a
// short human-readable range. Picks units (K vs M rows/s) based on
// magnitude so the reasoning string stays readable. Always emits both
// endpoints in the same unit for easy visual comparison.
//
// Negative low values (the prediction interval can extend below zero
// when uncertainty is large) are clamped to 0 in the display — a
// "throughput could be negative" string would mislead users; the
// information that matters is "the lower bound is essentially zero,
// model has very low confidence."
func formatThroughputRange(low, high float64) string {
	if low < 0 {
		low = 0
	}
	// Pick the larger endpoint's natural unit; small numbers in K, large in M.
	if high >= 1_000_000 {
		return fmt.Sprintf("%.2fM–%.2fM rows/s", low/1_000_000, high/1_000_000)
	}
	return fmt.Sprintf("%.0fK–%.0fK rows/s", low/1000, high/1000)
}

// sortedKeys returns the int keys of m sorted ascending. Used by the
// regression-skipped reasoning so the retry-rate exclusion list logs
// deterministically (map iteration order would otherwise vary across
// runs and make log diffs noisy).
func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// argmaxRegression walks the small (WAW, CS_BYTES) grid and returns the
// candidate with the highest predicted throughput, subject to the
// retry-rate exclusion (skip WAWs whose historical retry rate clears
// retryRateExclusionThreshold over ≥minRunsForRetryExclusion runs) and
// HardChunkLimit (skip CS values above the protocol cap). Returns
// ok=false when every grid point is filtered out.
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

// wawsWithHighRetryRate returns the set of WAW values that the SELECTION
// paths (smoothed bins via selectWAW, regression via argmaxRegression)
// should refuse to recommend. A WAW is excluded when BOTH gates fire:
//
//  1. Retry rate > retryRateExclusionThreshold (with ≥ minRunsForRetryExclusion samples), AND
//  2. Mean throughput < median throughput across all eligible WAWs.
//
// The throughput gate (#204) prevents the filter from punishing
// retry-tolerant workloads where high retry rates coexist with
// best-in-class throughput (e.g. PG-bottlenecked SO2013 workloads
// where lock contention drives retries upward without hurting
// throughput). Without it, a 55%-retry WAW=1 delivering 1M rows/s
// gets excluded the same as a 55%-retry WAW=2 delivering 200K — the
// signal "retries hurt throughput" was conflated with "retries
// happened at all."
//
// Replaces the original "any retry → permanent exclusion" filter
// (issue #186): a single transient retry no longer poisons a WAW
// value forever, and AI-era retries no longer prevent the deterministic
// tuner from re-examining historically-flagged WAWs once exploration
// has gathered fresh data.
func wawsWithHighRetryRate(rows []HistoryRecord) map[int]bool {
	type stats struct {
		runs    int
		retried int
		thrSum  float64
	}
	c := map[int]*stats{}
	for _, r := range rows {
		s := c[r.WriteAheadWriters]
		if s == nil {
			s = &stats{}
			c[r.WriteAheadWriters] = s
		}
		s.runs++
		s.thrSum += r.FinalThroughput
		if r.ChunkRetryCount > 0 {
			s.retried++
		}
	}

	// Pre-compute median across WAWs with enough samples — sparse WAWs
	// (below the floor) shouldn't drag the median around.
	eligibleMeans := make([]float64, 0, len(c))
	for _, s := range c {
		if s.runs < minRunsForRetryExclusion {
			continue
		}
		eligibleMeans = append(eligibleMeans, s.thrSum/float64(s.runs))
	}
	median := medianOfFloats(eligibleMeans)

	out := map[int]bool{}
	for waw, s := range c {
		if s.runs < minRunsForRetryExclusion {
			continue
		}
		mean := s.thrSum / float64(s.runs)
		rate := float64(s.retried) / float64(s.runs)
		// Strict less-than is intentional: a WAW exactly at the median
		// stays eligible (preservation-over-exclusion bias, matters
		// when there's only one eligible WAW so median == its own mean).
		if rate > retryRateExclusionThreshold && mean < median {
			out[waw] = true
		}
	}
	return out
}

// medianOfFloats returns the upper-middle element of v after sorting.
// Matches the convention in filterOutliers (`throughputs[len/2]`) so
// median calculations across the package are consistent. Returns 0 for
// empty input — callers should treat that as "no median, nothing to
// compare against" (the throughput gate effectively never fires).
func medianOfFloats(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sorted := make([]float64, len(v))
	copy(sorted, v)
	sort.Float64s(sorted)
	return sorted[len(sorted)/2]
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

// filterOutliers drops two kinds of unhelpful rows:
//   - Incomplete: FinalThroughput == 0 (NULL coerced to zero by the
//     checkpoint backend before UpdateAITuningResult fires for the
//     row's run). The smoothed-bins aggregator skips these too; the
//     regression in fitRegression would otherwise train on them as
//     legitimate 0 rows/s observations, biasing the fit toward zero
//     (Codex review on PR #183 — bug fix).
//   - Noise: throughput below half the median AND zero retries means
//     measurement variance, not load. Low throughput with retries is
//     a real contention signal and stays.
func filterOutliers(rows []HistoryRecord) []HistoryRecord {
	// First pass: drop incomplete rows so neither pipeline tier sees
	// them. Median below is computed on completed runs only.
	completed := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 {
			completed = append(completed, r)
		}
	}
	if len(completed) < 3 {
		return completed
	}
	throughputs := make([]float64, 0, len(completed))
	for _, r := range completed {
		throughputs = append(throughputs, r.FinalThroughput)
	}
	sort.Float64s(throughputs)
	median := throughputs[len(throughputs)/2]
	floor := median * outlierFloorRatio

	kept := make([]HistoryRecord, 0, len(completed))
	for _, r := range completed {
		if r.FinalThroughput < floor && r.ChunkRetryCount == 0 {
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

// appendReasoning concatenates a new structured reason onto the existing
// Output.Reasoning string, semicolon-separated.
func appendReasoning(existing, format string, args ...any) string {
	added := fmt.Sprintf(format, args...)
	if existing == "" {
		return added
	}
	return existing + "; " + added
}
