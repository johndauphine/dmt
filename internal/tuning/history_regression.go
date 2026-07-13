package tuning

import (
	"math"

	"github.com/johndauphine/dmt/internal/logging"
)

// applyHistoryRegression fits the quadratic model to the filtered rows
// and walks the projected (WAW × CS_BYTES × PR × RAB) grid to pick the
// reachable argmax. Retry-rate and coverage gates filter effective cells;
// memory and protocol limits clamp candidates before prediction. Returns true
// when it picks (and applies) a recommendation; false on fit failure or an
// empty effective domain (caller falls through to smoothed bins).
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
	search := argmaxRegression(model, in, profile, cellSkip, covered)
	if !search.ok {
		// Intersect cellSkip with the argmax grid before logging — the
		// raw map can include cells outside readerGrid (e.g., historical
		// PR=3/RAB=5 rows). Those never participated in argmax filtering,
		// so quoting them in the skip reason is misleading (Copilot
		// review on #221). The covered-cells count is left as the full
		// historical figure since it's diagnostic of "did any history
		// land in the grid at all" — orthogonal concern.
		out.Reasoning = appendReasoning(out.Reasoning,
			"regression skipped: every projected grid candidate filtered (retry-rate exclusions: %s, covered cells: %d, raw=%d, eligible_raw=%d, unique_effective=%d)",
			formatRetryCellSkips(cellSkipsInCandidates(cellSkip, search.domainCells)),
			len(covered),
			search.rawCount,
			search.eligibleRawCount,
			search.uniqueCount,
		)
		return false
	}

	legacyModelWidth := safeAvgRowBytes(in.AvgRowBytes)
	applyProjectedCandidate(out, search.projection)
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
	if low, high, ok := projectedPredictionInterval(model, search.projection, in, legacyModelWidth); ok {
		ciStr = formatPredictionInterval(search.predicted, low, high)
	}

	out.Reasoning = appendReasoning(out.Reasoning,
		"regression-selected requested WAW=%d, chunk=%d rows/%.1f MB, PR=%d, RAB=%d; effective WAW=%d, chunk=%d rows/model=%.1f MB, PR=%d, RAB=%d (%s) over %d filtered rows (%d covered cells; raw=%d, eligible_raw=%d, unique_effective=%d); predicted %s [95%% CI: %s]",
		search.projection.Requested.WriteAheadWriters,
		search.projection.Requested.ChunkSize,
		candidateMB(search.projection.Requested.RequestedChunkBytes),
		search.projection.Requested.ParallelReaders,
		search.projection.Requested.ReadAheadBuffers,
		search.projection.Effective.WriteAheadWriters,
		search.projection.Effective.ChunkSize,
		candidateMB(search.projection.ModelChunkBytes),
		search.projection.Effective.ParallelReaders,
		search.projection.Effective.ReadAheadBuffers,
		search.projection.reason(),
		len(rows),
		len(covered),
		search.rawCount,
		search.eligibleRawCount,
		search.uniqueCount,
		formatBytesPerSec(search.predicted),
		ciStr,
	)
	return true
}

type predictionIntervalModel interface {
	PredictionInterval(
		waw int,
		csBytes int64,
		parallelReaders, readAheadBuffers int,
		sourceDB, targetDB, mode string,
		avgRowBytes int64,
	) (low, high float64, ok bool)
}

// projectedPredictionInterval is the single boundary between a selected
// effective candidate and uncertainty estimation. Keeping the projection as
// the input prevents the PI path from drifting back to a raw byte anchor while
// point prediction uses ModelChunkBytes.
func projectedPredictionInterval(model predictionIntervalModel, projection candidateProjection, in Input, legacyModelWidth int64) (low, high float64, ok bool) {
	return model.PredictionInterval(
		projection.Effective.WriteAheadWriters,
		projection.ModelChunkBytes,
		projection.Effective.ParallelReaders,
		projection.Effective.ReadAheadBuffers,
		in.SourceDBType,
		in.TargetDBType,
		in.TargetMode,
		legacyModelWidth,
	)
}

type regressionSearchResult struct {
	projection       candidateProjection
	predicted        float64
	rawCount         int
	eligibleRawCount int
	uniqueCount      int
	domainCells      map[retryCellKey]bool
	ok               bool
}

// argmaxRegression walks the small (WAW × CS_BYTES × PR × RAB) grid and
// returns the candidate with the highest predicted throughput, subject to
// the retry-rate exclusion (skip cells whose historical retry rate clears
// retryRateExclusionThreshold over ≥minRunsForRetryExclusion runs) and the
// measured-cell coverage gate. Memory and protocol caps are projections, not
// filters. Returns ok=false when every projected grid point is filtered out.
//
// The PR and RAB grids match the reader domain in the exploration design
// ({2,4} × {4,8}) so argmax only picks values the planned cells actually
// probe — avoids the extrapolation that drove #218's astronomical CI.
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
func argmaxRegression(model *regressionModel, in Input, profile DriverProfile, cellSkip map[retryCellKey]bool, covered map[coverageCellKey]bool) regressionSearchResult {
	const fallbackBytes int64 = 10_000_000
	optimumBytes := profile.OptimumBulkChunkBytes
	if optimumBytes <= 0 {
		optimumBytes = fallbackBytes
	}
	representativeWidth := in.representativeRowBytes()
	legacyModelWidth := safeAvgRowBytes(in.AvgRowBytes)
	csCandidates := []int64{
		scaledByteTarget(optimumBytes, halfOptimumFraction),
		scaledByteTarget(optimumBytes, fullOptimumFraction),
	}
	if in.PinnedChunkSize != nil && *in.PinnedChunkSize > 0 {
		csCandidates = []int64{chunkBytesForModel(*in.PinnedChunkSize, representativeWidth)}
	}
	wawCandidates := make([]int, 0, maxLearnableWAW)
	if in.PinnedWriteAheadWriters != nil && *in.PinnedWriteAheadWriters > 0 {
		wawCandidates = append(wawCandidates, *in.PinnedWriteAheadWriters)
	} else {
		for waw := 1; waw <= maxLearnableWAW; waw++ {
			wawCandidates = append(wawCandidates, waw)
		}
	}
	parallelReaderCandidates := []int{2, 4}
	if in.PinnedParallelReaders != nil && *in.PinnedParallelReaders > 0 {
		parallelReaderCandidates = []int{*in.PinnedParallelReaders}
	}
	readAheadCandidates := []int{4, 8}
	if in.PinnedReadAheadBuffers != nil && *in.PinnedReadAheadBuffers > 0 {
		readAheadCandidates = []int{*in.PinnedReadAheadBuffers}
	}

	result := regressionSearchResult{domainCells: map[retryCellKey]bool{}}
	bestPred := math.Inf(-1)
	seen := make(map[effectiveCandidateKey]bool)

	for _, w := range wawCandidates {
		for _, cs := range csCandidates {
			for _, pr := range parallelReaderCandidates {
				for _, rab := range readAheadCandidates {
					result.rawCount++
					projection := projectCandidate(in, profile, candidateFromBytes(
						baselineWorkers(in.CPUCores),
						w,
						cs,
						pr,
						rab,
						representativeWidth,
					))
					cell := retryCellKey{
						WAW:              projection.Effective.WriteAheadWriters,
						ParallelReaders:  projection.Effective.ParallelReaders,
						ReadAheadBuffers: projection.Effective.ReadAheadBuffers,
					}
					result.domainCells[cell] = true
					if cellSkip[cell] {
						continue
					}
					if !covered[coverageCellKey(cell)] {
						continue
					}
					result.eligibleRawCount++
					key := projection.key()
					if seen[key] {
						continue
					}
					seen[key] = true
					result.uniqueCount++
					pred := model.Predict(
						projection.Effective.WriteAheadWriters,
						projection.ModelChunkBytes,
						projection.Effective.ParallelReaders,
						projection.Effective.ReadAheadBuffers,
						in.SourceDBType,
						in.TargetDBType,
						in.TargetMode,
						legacyModelWidth,
					)
					if math.IsNaN(pred) || math.IsInf(pred, 0) {
						continue
					}
					if pred > bestPred {
						bestPred = pred
						result.projection = projection
						result.predicted = pred
						result.ok = true
					}
				}
			}
		}
	}
	return result
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
