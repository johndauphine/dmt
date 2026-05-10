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

// driftDownThreshold and driftUpThreshold bound the recent-vs-older
// median ratio considered "stable." A ratio strictly outside this
// band (ratio < driftDownThreshold OR ratio > driftUpThreshold)
// triggers drift and forces exploration on the next run. The
// boundary values themselves are stable; only ratios past them fire.
//
// The bounds were originally [0.70, 1.30] but #184 measured ~2.5×
// within-bin variance on identical configurations (PG cache state,
// host scheduling, Docker fsync cadence as the dominant noise sources)
// — far wider than ±30%. The original threshold fired on essentially
// every run, starving the regression tier. Widening to [0.50, 1.50]
// keeps real regime changes (more than 50% throughput drops, more
// than ~2× speedups) in the firing zone while letting natural
// measurement noise pass through.
const (
	driftDownThreshold = 0.50
	driftUpThreshold   = 1.50
)

// detectRegimeDrift returns true when the (WAW, CS) config of the
// MOST RECENTLY USED run shows a recent-vs-older throughput median
// ratio outside [driftDownThreshold, driftUpThreshold]. Logs a warning
// when drift fires so the user sees why the next run went into
// exploration mode.
//
// History scope: only the cell of the most recent run is checked, not
// every cell with enough samples (#184 fix). Without this restriction,
// any cell that drifted in the past would re-fire drift on every
// subsequent run forever — even when the system has moved on to a
// different (more stable) cell. By scoping drift detection to "the
// cell that's actually in use right now," a single drift firing
// produces one exploration probe and the regression tier resumes on
// the next run unless the new cell ALSO drifts.
//
// rows must be the post-regime, post-outlier filtered set the rest of
// applyHistory operates on. Order doesn't matter — this function sorts
// by Timestamp internally.
func detectRegimeDrift(rows []HistoryRecord) bool {
	if len(rows) < driftMinAtConfig {
		return false
	}

	currentCell, ok := mostRecentCell(rows)
	if !ok {
		return false
	}

	atCell := rowsAtCell(rows, currentCell)
	if len(atCell) < driftMinAtConfig {
		// Current cell hasn't accumulated enough runs to distinguish
		// drift from noise.
		return false
	}

	// Sort by timestamp ascending so the last driftRecentN entries
	// are the chronologically most recent.
	sort.Slice(atCell, func(i, j int) bool {
		return atCell[i].Timestamp.Before(atCell[j].Timestamp)
	})
	recent := atCell[len(atCell)-driftRecentN:]
	older := atCell[:len(atCell)-driftRecentN]

	recentMed := medianThroughput(recent)
	olderMed := medianThroughput(older)
	if olderMed <= 0 {
		return false
	}
	ratio := recentMed / olderMed

	if ratio < driftDownThreshold || ratio > driftUpThreshold {
		logging.Warn(
			"tuning: regime drift at (WAW=%d, chunk_size=%d, PR=%d, RAB=%d) — recent median %.0f rows/s vs older %.0f rows/s (ratio %.2f outside [%.2f, %.2f]); forcing exploration",
			currentCell.WAW, currentCell.ChunkSize, currentCell.ParallelReaders, currentCell.ReadAheadBuffers,
			recentMed, olderMed, ratio,
			driftDownThreshold, driftUpThreshold,
		)
		return true
	}
	return false
}

// configKey identifies a (WAW, ChunkSize, ParallelReaders,
// ReadAheadBuffers) cell. Used both for drift grouping and for "which
// cell did we last run at?" lookups.
//
// The reader-side axes are part of the key (#219 Codex review): once
// the tuner started varying ParallelReaders/ReadAheadBuffers, two runs
// at the same (WAW, ChunkSize) with different reader settings could
// produce materially different throughput, and a per-pair drift cell
// would misread the parameter effect as a regime shift. Keying by the
// full quadruple keeps drift detection comparing like-for-like runs.
type configKey struct {
	WAW              int
	ChunkSize        int
	ParallelReaders  int
	ReadAheadBuffers int
}

// mostRecentCell returns the cell of the chronologically latest
// COMPLETED run in rows (one with positive FinalThroughput). Skips
// incomplete rows — a crashed mid-run or an in-progress migration
// surfaces as FinalThroughput <= 0 from the checkpoint backend, and
// using it as the current cell would route drift detection to a
// "phantom" cell (rowsAtCell drops the incomplete row, often leaving
// too few samples for drift to fire on the genuinely-current cell).
//
// Returns ok=false when no completed rows exist (Codex review on
// PR #184 — match the rest of the tuning filters which all skip
// incomplete rows).
func mostRecentCell(rows []HistoryRecord) (configKey, bool) {
	var latest *HistoryRecord
	for i := range rows {
		if rows[i].FinalThroughput <= 0 {
			continue
		}
		if latest == nil || rows[i].Timestamp.After(latest.Timestamp) {
			latest = &rows[i]
		}
	}
	if latest == nil {
		return configKey{}, false
	}
	return configKey{
		WAW:              latest.WriteAheadWriters,
		ChunkSize:        latest.ChunkSize,
		ParallelReaders:  latest.ParallelReaders,
		ReadAheadBuffers: latest.ReadAheadBuffers,
	}, true
}

// rowsAtCell filters rows down to those at the given (WAW, ChunkSize,
// ParallelReaders, ReadAheadBuffers) cell. Returns a fresh slice;
// doesn't mutate the input.
func rowsAtCell(rows []HistoryRecord, cell configKey) []HistoryRecord {
	out := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.FinalThroughput <= 0 {
			continue
		}
		if r.WriteAheadWriters == cell.WAW &&
			r.ChunkSize == cell.ChunkSize &&
			r.ParallelReaders == cell.ParallelReaders &&
			r.ReadAheadBuffers == cell.ReadAheadBuffers {
			out = append(out, r)
		}
	}
	return out
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
