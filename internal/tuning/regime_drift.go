package tuning

import (
	"sort"

	"github.com/johndauphine/dmt/internal/logging"
)

// PR2 regime-drift detector (#179).
//
// The PR1 detectParameterTrend was an LLM-feedback-loop guard ("are we
// converging on monotonically smaller chunk_size or workers?") that
// PR1 deleted along with the rest of the AI prompt machinery. This is
// its successor with completely different semantics:
//
//   For each (WAW, ChunkSize) config that's been run ≥3 times, compare
//   median throughput in the last 3 runs vs. median in the older runs
//   at the same config. If the throughput surface at a configuration
//   has materially shifted (recent < 0.7×older OR > 1.3×older), the
//   regression's training data is stale — we kick the bucket back to
//   exploration mode to refresh.
//
// Catches hardware/network/DB-tuning regime changes the regime
// classifier (host fields + DB tuning snapshot) can miss because they
// only compare a single point in time, not the trajectory.

// driftRecentN is the size of the "recent runs" window used to compare
// against earlier runs at the same config.
const driftRecentN = 3

// driftMinAtConfig is the minimum runs at a specific (WAW, CS) config
// before drift comparison fires. Below this we don't have enough
// per-config data to distinguish drift from noise.
const driftMinAtConfig = 6

// driftDownThreshold and driftUpThreshold are the asymmetric bounds.
// Recent throughput dropping to <70% of the older median triggers
// drift; rising to >130% also triggers (could mean upstream got faster
// — the regression's old fits are too pessimistic).
const (
	driftDownThreshold = 0.70
	driftUpThreshold   = 1.30
)

// detectRegimeDrift returns true when at least one (WAW, CS) config in
// the rows shows a recent-vs-older throughput median ratio outside
// [driftDownThreshold, driftUpThreshold]. Logs a warning when drift
// fires so the user sees why the next run went into exploration mode.
//
// rows must be the post-regime, post-outlier filtered set the rest of
// applyHistory operates on. Order doesn't matter — this function sorts
// by Timestamp internally.
func detectRegimeDrift(rows []HistoryRecord) bool {
	if len(rows) < driftMinAtConfig {
		return false
	}

	type configKey struct {
		WAW, ChunkSize int
	}
	groups := map[configKey][]HistoryRecord{}
	for _, r := range rows {
		if r.FinalThroughput <= 0 {
			continue
		}
		k := configKey{r.WriteAheadWriters, r.ChunkSize}
		groups[k] = append(groups[k], r)
	}

	for k, g := range groups {
		if len(g) < driftMinAtConfig {
			continue
		}
		// Sort by timestamp ascending so the last driftRecentN entries
		// are the chronologically most recent.
		sort.Slice(g, func(i, j int) bool {
			return g[i].Timestamp.Before(g[j].Timestamp)
		})
		recent := g[len(g)-driftRecentN:]
		older := g[:len(g)-driftRecentN]

		recentMed := medianThroughput(recent)
		olderMed := medianThroughput(older)
		if olderMed <= 0 {
			continue
		}
		ratio := recentMed / olderMed

		if ratio < driftDownThreshold || ratio > driftUpThreshold {
			logging.Warn(
				"tuning: regime drift at (WAW=%d, chunk_size=%d) — recent median %.0f rows/s vs older %.0f rows/s (ratio %.2f outside [%.2f, %.2f]); forcing exploration",
				k.WAW, k.ChunkSize, recentMed, olderMed, ratio, driftDownThreshold, driftUpThreshold,
			)
			return true
		}
	}
	return false
}

func medianThroughput(rows []HistoryRecord) float64 {
	if len(rows) == 0 {
		return 0
	}
	xs := make([]float64, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput > 0 {
			xs = append(xs, r.FinalThroughput)
		}
	}
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	return xs[len(xs)/2]
}
