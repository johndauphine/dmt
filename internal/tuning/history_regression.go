package tuning

import (
	"math"

	"github.com/johndauphine/dmt/internal/logging"
)

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

	cellSkip := cellsWithHighRetryRate(rows)
	covered := cellsWithCoverage(rows)
	pickedWAW, pickedCSBytes, pickedPR, pickedRAB, predicted, ok := argmaxRegression(model, in, profile, cellSkip, covered)
	if !ok {
		// Intersect cellSkip with the argmax grid before logging — the
		// raw map can include cells outside readerGrid (e.g., historical
		// PR=3/RAB=5 rows). Those never participated in argmax filtering,
		// so quoting them in the skip reason is misleading (Copilot
		// review on #221). The covered-cells count is left as the full
		// historical figure since it's diagnostic of "did any history
		// land in the grid at all" — orthogonal concern.
		out.Reasoning = appendReasoning(out.Reasoning,
			"regression skipped: every grid candidate filtered (retry-rate exclusions: %s, HardChunkLimit=%d, covered cells: %d)",
			formatRetryCellSkips(cellSkipsInGrid(cellSkip)), profile.HardChunkLimit, len(covered),
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
	out.ParallelReaders = pickedPR
	out.ReadAheadBuffers = pickedRAB
	out.Tier = TierRegression

	// Point-level fit signal: 95% prediction interval at the picked
	// point. !ok when the model couldn't compute it — emit "N/A" to keep
	// the format consistent without lying about confidence.
	// PredictionInterval's explicit ok return distinguishes "couldn't
	// compute" from "computed a legitimately zero-width interval"
	// (Codex review on PR #217 — the prior `low != high` sentinel was
	// ambiguous).
	//
	// R² used to be emitted here too but was removed: on noisy real
	// workloads it's structurally capped low (within-cell variance
	// dominates between-cell signal), so the reasoning line read
	// "R²=0.13" while the regression's actual decisions were near-
	// optimal (1.4% top-1 regret on the SO2010 sweep). Operators
	// reading the log thought the tuner was broken when it wasn't.
	// model.r2 is still computed and remains available to debug logs
	// and unit tests.
	ciStr := "N/A"
	if low, high, ok := model.PredictionInterval(pickedWAW, pickedCSBytes, pickedPR, pickedRAB, in.SourceDBType, in.TargetDBType, in.TargetMode, avg); ok {
		ciStr = formatPredictionInterval(predicted, low, high)
	}

	out.Reasoning = appendReasoning(out.Reasoning,
		"regression-selected WAW=%d, chunk_size=%d rows (%.1f MB), PR=%d, RAB=%d over %d filtered rows (%d covered cells); predicted %s [95%% CI: %s]",
		pickedWAW, csRows, float64(pickedCSBytes)/1024/1024, pickedPR, pickedRAB, len(rows), len(covered), formatBytesPerSec(predicted), ciStr,
	)
	return true
}

// argmaxRegression walks the small (WAW × CS_BYTES × PR × RAB) grid and
// returns the candidate with the highest predicted throughput, subject to
// the retry-rate exclusion (skip cells whose historical retry rate clears
// retryRateExclusionThreshold over ≥minRunsForRetryExclusion runs) and
// HardChunkLimit (skip CS values above the protocol cap). Returns
// ok=false when every grid point is filtered out.
//
// The PR and RAB grids match the exploration inner grid ({2,4} × {4,8})
// so argmax only picks values the planned grid actually probes — avoids
// the extrapolation that drove #218's astronomical CI.
//
// cellSkip is keyed by (WAW, ParallelReaders, ReadAheadBuffers) rather
// than WAW alone (Codex review on #219): a reader-induced retry pattern
// at one (PR, RAB) combo no longer bans the whole WAW — other reader
// combos at the same WAW remain selectable if their historical retry
// record is clean.
//
// covered is the cube-corner extrapolation gate (#221): the argmax
// refuses to pick a (WAW, PR, RAB) cell the training data has never
// visited, because the linear model adds PR and WAW effects
// independently and can extrapolate catastrophically across the empty
// corners. Measured failure: 124-row history with WAW=1 only at PR=2
// and PR=4 only at WAW≥2 → argmax picked WAW=1/PR=4, throughput
// collapsed 14×. When all candidates are uncovered (rare; only on
// fresh history before exploration), the caller falls through to the
// smoothed-bins tier — simpler model, fewer assumptions, no
// extrapolation.
func argmaxRegression(model *regressionModel, in Input, profile DriverProfile, cellSkip map[retryCellKey]bool, covered map[coverageCellKey]bool) (waw int, csBytes int64, parallelReaders, readAheadBuffers int, predicted float64, ok bool) {
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
	var bestPR, bestRAB int
	bestPred := math.Inf(-1)

	for w := 1; w <= maxWAWForGrid; w++ {
		for _, cs := range csCandidates {
			csRows := int(cs / avg)
			if profile.HardChunkLimit > 0 && csRows > profile.HardChunkLimit {
				continue
			}
			for _, reader := range readerGrid {
				if cellSkip[retryCellKey{WAW: w, ParallelReaders: reader.ParallelReaders, ReadAheadBuffers: reader.ReadAheadBuffers}] {
					continue
				}
				if !covered[coverageCellKey{WAW: w, ParallelReaders: reader.ParallelReaders, ReadAheadBuffers: reader.ReadAheadBuffers}] {
					continue
				}
				pred := model.Predict(w, cs, reader.ParallelReaders, reader.ReadAheadBuffers, in.SourceDBType, in.TargetDBType, in.TargetMode, avg)
				if pred > bestPred {
					bestPred = pred
					bestWAW = w
					bestCSBytes = cs
					bestPR = reader.ParallelReaders
					bestRAB = reader.ReadAheadBuffers
				}
			}
		}
	}
	if bestWAW < 0 {
		return 0, 0, 0, 0, 0, false
	}
	return bestWAW, bestCSBytes, bestPR, bestRAB, bestPred, true
}

// coverageCellKey identifies a (WriteAheadWriters, ParallelReaders,
// ReadAheadBuffers) cell for the argmax cube-corner extrapolation gate
// (#221). Structurally identical to retryCellKey but kept as a distinct
// type so callers can't accidentally pass one map where the other is
// expected — the two filters have different semantics (one bans cells
// because they performed badly, the other bans cells because they
// haven't been measured at all).
type coverageCellKey struct {
	WAW              int
	ParallelReaders  int
	ReadAheadBuffers int
}

// cellsWithCoverage returns the set of (WAW, PR, RAB) cells the
// training data has at least one completed run at. Used by
// argmaxRegression to refuse to extrapolate to cells the model has
// never seen. Same row-filtering convention as the other tuning
// helpers — incomplete runs (FinalThroughput <= 0) are skipped because
// they aren't evidence the cell has been "visited" in a meaningful
// sense; the chunk-level checkpoint backend may surface them.
//
// ChunkSize is intentionally not in the key. The regression treats CS
// as a continuous numeric feature (cs_z, cs_z²) so it can interpolate
// between observed CS values; (WAW, PR, RAB) are effectively
// categorical with a small fixed set of grid values, so an exact-match
// coverage check is the right granularity.
func cellsWithCoverage(rows []HistoryRecord) map[coverageCellKey]bool {
	out := map[coverageCellKey]bool{}
	for _, r := range rows {
		if r.FinalThroughput <= 0 {
			continue
		}
		out[coverageCellKey{
			WAW:              r.WriteAheadWriters,
			ParallelReaders:  r.ParallelReaders,
			ReadAheadBuffers: r.ReadAheadBuffers,
		}] = true
	}
	return out
}
