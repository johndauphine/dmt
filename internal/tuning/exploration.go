package tuning

import (
	"math/rand/v2"
)

// PR2 exploration (#179). Two complementary modes layered on top of the
// regression / smoothed-bins selection from history.go:
//
//  1. Planned grid (cold start). For the first explorationGridRuns runs
//     in a (source, target) bucket, deliberately rotate through a small
//     set of (WAW, CS) candidates regardless of what the regression
//     would predict. This guarantees the regression has variance to fit
//     on instead of locking at the cold-start choice.
//
//  2. ε-perturbation (steady state). After the planned phase, the
//     regression's argmax pick gets a small random nudge with
//     probability ε. Keeps the training data refreshing instead of
//     converging on a single (WAW, CS) cell forever.
//
// A `--explore` flag forcibly drops the run into the planned-grid
// branch even if the bucket is past the cold-start window — used as an
// on-demand probe (e.g. the user just resized their host or wants to
// re-validate a parameter set after a regime change).

// explorationGridRuns is the K from the issue spec — the number of
// initial runs in a bucket that go through the planned grid before the
// regression takes over.
const explorationGridRuns = 6

// gridCandidate is one (WAW × CS_BYTES_FRACTION) probe point on the
// planned grid. The fraction is multiplied by the per-target
// OptimumBulkChunkBytes (with the conservative 10 MB fallback for
// unmeasured targets) to derive the actual chunk-byte target.
type gridCandidate struct {
	WAW        int
	CSFraction float64
}

// explorationGrid is the planned-grid menu. WAW values from the issue
// spec ({1, 2, 3, drv.MaxWAW}); the drv.MaxWAW slot uses maxWAWForGrid
// since profile doesn't yet expose a per-driver max. CS fractions are
// the documented {0.5×, 1.0×} of the per-target optimum.
var explorationGrid = []gridCandidate{
	{WAW: 1, CSFraction: halfOptimumFraction},
	{WAW: 2, CSFraction: halfOptimumFraction},
	{WAW: 3, CSFraction: halfOptimumFraction},
	{WAW: maxWAWForGrid, CSFraction: halfOptimumFraction},
	{WAW: 1, CSFraction: fullOptimumFraction},
	{WAW: 2, CSFraction: fullOptimumFraction},
	{WAW: 3, CSFraction: fullOptimumFraction},
	{WAW: maxWAWForGrid, CSFraction: fullOptimumFraction},
}

// shouldExplore returns true when this run should pick from the planned
// grid instead of the regression's argmax: either the user forced it
// via --explore, or the bucket hasn't accumulated enough runs to make
// the regression meaningful.
func shouldExplore(in Input, bucketCount int) bool {
	return in.ForceExplore || bucketCount < explorationGridRuns
}

// applyGridExploration picks the next probe from the planned grid based
// on the bucket's run count, intersected with the same feasibility
// filters the regression argmax uses (RULE 1, HardChunkLimit). On a
// fully-filtered grid (every candidate skipped), the baseline output
// stands.
func applyGridExploration(out *Output, in Input, profile DriverProfile, bucketCount int, skipWAWs map[int]bool) {
	const fallbackBytes int64 = 10_000_000
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	optimumBytes := profile.OptimumBulkChunkBytes
	if optimumBytes <= 0 {
		optimumBytes = fallbackBytes
	}

	// Walk the grid starting at the bucket's run count modulo the grid
	// length. Scan forward through every candidate; pick the first that
	// passes the filters. This way a forced --explore on a bucket past
	// the cold-start window still gets a deterministic next-probe.
	n := len(explorationGrid)
	for i := 0; i < n; i++ {
		idx := (bucketCount + i) % n
		cand := explorationGrid[idx]
		if skipWAWs[cand.WAW] {
			continue
		}
		csBytes := int64(cand.CSFraction * float64(optimumBytes))
		csRows := int(csBytes / avg)
		if csRows < 1 {
			csRows = 1
		}
		if profile.HardChunkLimit > 0 && csRows > profile.HardChunkLimit {
			continue
		}
		out.WriteAheadWriters = cand.WAW
		out.ChunkSize = csRows
		out.Reasoning = appendReasoning(out.Reasoning,
			"exploration: planned grid run %d/%d (WAW=%d, CS=%.1f×optimum=%.1fMB)",
			bucketCount+1, explorationGridRuns, cand.WAW, cand.CSFraction, float64(csBytes)/1024/1024,
		)
		return
	}
	// Every grid candidate was filtered out — leave baseline alone. The
	// caller's memory clamp still runs; this is the safest fallback when
	// HardChunkLimit + RULE 1 between them eliminate every probe.
}

// shouldEpsilonPerturb returns true with probability ε. Uses the global
// math/rand/v2 source — sufficient for picking probe vs. exploit
// (deterministic seeding isn't useful here because the *decision* is
// what's random; the perturbation itself is deterministic given the
// decision).
func shouldEpsilonPerturb(epsilon float64) bool {
	if epsilon <= 0 {
		return false
	}
	if epsilon >= 1 {
		return true
	}
	return rand.Float64() < epsilon
}

// applyEpsilonPerturbation nudges the picked (WAW, ChunkSize) by one
// step in a randomly-chosen direction. The nudge is bounded by the WAW
// grid and respects HardChunkLimit; if the chosen perturbation would
// violate a filter, falls through to the next direction so we always
// generate *some* variance.
//
// Picks one of four directions:
//
//	WAW + 1   (capped at maxWAWForGrid)
//	WAW − 1   (floored at 1)
//	CS × 1.5  (capped at HardChunkLimit if set)
//	CS × 0.67 (floored at 1 row)
func applyEpsilonPerturbation(out *Output, in Input, profile DriverProfile) {
	type direction struct {
		name  string
		apply func(*Output) bool // returns true when the nudge was actually applied
	}
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	dirs := []direction{
		{"WAW+1", func(o *Output) bool {
			if o.WriteAheadWriters >= maxWAWForGrid {
				return false
			}
			o.WriteAheadWriters++
			return true
		}},
		{"WAW-1", func(o *Output) bool {
			if o.WriteAheadWriters <= 1 {
				return false
			}
			o.WriteAheadWriters--
			return true
		}},
		{"CS×1.5", func(o *Output) bool {
			next := int(float64(o.ChunkSize) * 1.5)
			if profile.HardChunkLimit > 0 && next > profile.HardChunkLimit {
				return false
			}
			if next == o.ChunkSize {
				return false
			}
			o.ChunkSize = next
			return true
		}},
		{"CS×0.67", func(o *Output) bool {
			next := int(float64(o.ChunkSize) * 0.67)
			if next < 1 {
				next = 1
			}
			if next == o.ChunkSize {
				return false
			}
			o.ChunkSize = next
			return true
		}},
	}
	// Try directions in random order until one applies.
	order := rand.Perm(len(dirs))
	for _, idx := range order {
		if dirs[idx].apply(out) {
			out.Reasoning = appendReasoning(out.Reasoning, "ε-perturbation %s applied", dirs[idx].name)
			return
		}
	}
	// All directions blocked by filters (rare) — leave argmax untouched.
}
