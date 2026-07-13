package tuning

import (
	"fmt"
	"sort"
	"strings"
)

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

// cellSkipsInCandidates filters retry exclusions to the effective projected
// candidate domain. Unlike the pre-#728 static-grid check, this includes
// authoritative pinned values outside the ordinary exploration grid.
func cellSkipsInCandidates(cellSkip, candidateCells map[retryCellKey]bool) map[retryCellKey]bool {
	if len(cellSkip) == 0 {
		return cellSkip
	}
	out := map[retryCellKey]bool{}
	for k, v := range cellSkip {
		if !v || !candidateCells[k] {
			continue
		}
		out[k] = true
	}
	return out
}

// cellSkipsInGrid retains the package-test helper for the ordinary unpinned
// grid. Production diagnostics use cellSkipsInCandidates so projected pins
// outside this static domain remain visible.
func cellSkipsInGrid(cellSkip map[retryCellKey]bool) map[retryCellKey]bool {
	candidates := map[retryCellKey]bool{}
	for waw := 1; waw <= maxLearnableWAW; waw++ {
		for _, reader := range readerGrid {
			candidates[retryCellKey{
				WAW:              waw,
				ParallelReaders:  reader.ParallelReaders,
				ReadAheadBuffers: reader.ReadAheadBuffers,
			}] = true
		}
	}
	return cellSkipsInCandidates(cellSkip, candidates)
}

// retryCellKey identifies a (WriteAheadWriters, ParallelReaders,
// ReadAheadBuffers) cell for the regression-tier retry-rate exclusion
// (#219 Codex review). Chunk_size is intentionally NOT part of the key
// — retry counts are driven by the writer/reader concurrency interplay
// (waw, pr, rab) far more than by the per-batch row count, and the
// existing wawsWithHighRetryRate aggregator already disregarded CS for
// the same reason. Keeping CS out of the key also keeps the per-cell
// sample count high enough that minRunsForRetryExclusion fires.
type retryCellKey struct {
	WAW              int
	ParallelReaders  int
	ReadAheadBuffers int
}

// cellsWithHighRetryRate is the regression-tier counterpart of
// wawsWithHighRetryRate, aggregating retry stats by the full (WAW, PR,
// RAB) cell instead of WAW alone. argmaxRegression uses this so that a
// retry pattern at one reader combo (e.g. PR=4/RAB=8) doesn't ban the
// other reader combos at the same WAW from selection.
//
// Same gates as wawsWithHighRetryRate: (1) retry rate above
// retryRateExclusionThreshold over ≥minRunsForRetryExclusion runs at
// that cell, AND (2) mean throughput below the median across all
// eligible cells. Below-floor cells (insufficient samples) are
// neither excluded nor used in the median — they remain selectable
// because there's not enough evidence yet to flag them.
func cellsWithHighRetryRate(rows []HistoryRecord) map[retryCellKey]bool {
	type stats struct {
		runs    int
		retried int
		thrSum  float64
	}
	c := map[retryCellKey]*stats{}
	for _, r := range rows {
		k := retryCellKey{
			WAW:              r.WriteAheadWriters,
			ParallelReaders:  r.ParallelReaders,
			ReadAheadBuffers: r.ReadAheadBuffers,
		}
		s := c[k]
		if s == nil {
			s = &stats{}
			c[k] = s
		}
		s.runs++
		s.thrSum += r.FinalThroughput
		if r.ChunkRetryCount > 0 {
			s.retried++
		}
	}

	eligibleMeans := make([]float64, 0, len(c))
	for _, s := range c {
		if s.runs < minRunsForRetryExclusion {
			continue
		}
		eligibleMeans = append(eligibleMeans, s.thrSum/float64(s.runs))
	}
	median := medianOfFloats(eligibleMeans)

	out := map[retryCellKey]bool{}
	for k, s := range c {
		if s.runs < minRunsForRetryExclusion {
			continue
		}
		mean := s.thrSum / float64(s.runs)
		rate := float64(s.retried) / float64(s.runs)
		if rate > retryRateExclusionThreshold && mean < median {
			out[k] = true
		}
	}
	return out
}

// formatRetryCellSkips renders a retryCellKey set deterministically for
// the regression-skipped reasoning message. Keys sort first by WAW,
// then PR, then RAB so log diffs stay stable across runs (map iteration
// order would otherwise vary).
func formatRetryCellSkips(m map[retryCellKey]bool) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]retryCellKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].WAW != keys[j].WAW {
			return keys[i].WAW < keys[j].WAW
		}
		if keys[i].ParallelReaders != keys[j].ParallelReaders {
			return keys[i].ParallelReaders < keys[j].ParallelReaders
		}
		return keys[i].ReadAheadBuffers < keys[j].ReadAheadBuffers
	})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("WAW=%d/PR=%d/RAB=%d", k.WAW, k.ParallelReaders, k.ReadAheadBuffers))
	}
	return "[" + strings.Join(parts, ", ") + "]"
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
