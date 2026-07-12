package tuning

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// PR2 exploration (#179). Two complementary modes layered on top of the
// regression / smoothed-bins selection from history.go:
//
//  1. Planned grid (cold start). For the first explorationGridRuns usable
//     runs in a (source, target) bucket, deliberately rotate through a
//     balanced set of (WAW, CS, PR, RAB) candidates regardless of what the
//     regression would predict. This guarantees the regression has variance
//     to fit on instead of locking at the cold-start choice.
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
const explorationGridRuns = 12

// perturbPRMax / perturbRABMax / perturbRABMin bound the ε-perturbation
// directions for parallel_readers and read_ahead_buffers (#219). The
// caps are chosen wide enough that the perturbation actually leaves the
// reader-grid envelope ({2,4} × {4,8}) occasionally — that's the point;
// ε-perturbation should be willing to probe further than the planned
// grid did — but tight enough that a runaway scale-up doesn't pile
// reader-side connections onto a host whose connection budget can't
// absorb them.
const (
	perturbPRMax  = 8
	perturbRABMax = 16
	perturbRABMin = 2
)

// readerCandidate is one (parallel_readers, read_ahead_buffers) point in
// the reader domain used by regression argmax and epsilon perturbation.
type readerCandidate struct {
	ParallelReaders  int
	ReadAheadBuffers int
}

// explorationCell is one complete planned probe. Keeping all four knobs in
// one cell prevents the WAW/CS and reader domains from advancing in lockstep
// and deterministically confounding WAW with a single reader combination.
// CSFraction is multiplied by OptimumBulkChunkBytes (or the conservative
// 10 MB fallback) to derive the actual chunk-byte target.
type explorationCell struct {
	WAW              int
	CSFraction       float64
	ParallelReaders  int
	ReadAheadBuffers int
}

// explorationCells is the ordered twelve-run cold-start design (#698).
// The first eight entries are unique and balanced across the four knobs. The
// last four deliberately replicate cells 0, 1, 6, and 7 so the production
// regression reaches its twelve-row gate while every WAW and reader
// combination appears three times and both CS fractions appear six times.
var explorationCells = [...]explorationCell{
	{WAW: 1, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
	{WAW: 2, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	{WAW: 3, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},
	{WAW: maxWAWForGrid, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: 1, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	{WAW: 2, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
	{WAW: 3, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: maxWAWForGrid, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},

	// Deliberate replicates of 0, 1, 6, and 7.
	{WAW: 1, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
	{WAW: 2, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	{WAW: 3, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: maxWAWForGrid, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},
}

// Compile-time equality check: either expression becomes a negative array
// length if the table and the cold-start threshold diverge.
var (
	_ [explorationGridRuns - len(explorationCells)]struct{}
	_ [len(explorationCells) - explorationGridRuns]struct{}
)

// readerGrid is the 2×2 reader candidate domain used by regression argmax.
// Values bracket the baseline (PR=2, RAB=4): small enough that doubling
// stays within a typical 8-16 GB host's connection budget, but distinct
// enough for the regression to learn both reader axes.
var readerGrid = []readerCandidate{
	{ParallelReaders: 2, ReadAheadBuffers: 4},
	{ParallelReaders: 2, ReadAheadBuffers: 8},
	{ParallelReaders: 4, ReadAheadBuffers: 4},
	{ParallelReaders: 4, ReadAheadBuffers: 8},
}

// shouldExplore returns true when this run should pick from the planned
// grid instead of the regression's argmax: either the user forced it
// via --explore, or the bucket hasn't accumulated enough runs to make
// the regression meaningful.
func shouldExplore(in Input, usableCount int) bool {
	return in.ForceExplore || usableCount < explorationGridRuns
}

// applyGridExploration picks the next probe from the planned grid based
// on raw persisted attempts, intersected with HardChunkLimit only. The usable
// post-filter count separately controls cold-start labels and completion
// (#697, #698). The raw count is a sequential rotation proxy; concurrent
// tuners can observe the same value and this function does not claim to
// reserve cells globally.
// Historical-retry exclusions are intentionally NOT applied here: the
// planned grid's job is to gather data so historical verdicts can be
// re-examined as new runs accrue (issue #186 — under the original
// "any retry → permanent exclusion" rule, AI-era retries at WAW=2
// permanently locked the deterministic tuner out of probing it). On
// a fully-filtered grid (every candidate skipped by HardChunkLimit),
// the baseline output stands.
func applyGridExploration(out *Output, in Input, profile DriverProfile, usableCount, rawIndex int) {
	const fallbackBytes int64 = 10_000_000
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}
	optimumBytes := profile.OptimumBulkChunkBytes
	if optimumBytes <= 0 {
		optimumBytes = fallbackBytes
	}

	// Filter first, then index the eligible ring with the raw persisted-attempt
	// count. A scan-forward implementation makes several consecutive raw
	// indexes collapse onto the same later cell when HardChunkLimit removes a
	// cluster of candidates. Modulo over the eligible ring preserves a complete,
	// deterministic rotation. Keep the original table index for audit labels.
	type indexedCell struct {
		index   int
		cell    explorationCell
		csBytes int64
		csRows  int
	}
	eligible := make([]indexedCell, 0, len(explorationCells))
	for idx, cell := range explorationCells {
		csBytes := int64(cell.CSFraction * float64(optimumBytes))
		csRows := int(csBytes / avg)
		if csRows < 1 {
			csRows = 1
		}
		if profile.HardChunkLimit > 0 && csRows > profile.HardChunkLimit {
			continue
		}
		eligible = append(eligible, indexedCell{index: idx, cell: cell, csBytes: csBytes, csRows: csRows})
	}
	if len(eligible) > 0 {
		selected := eligible[rawIndex%len(eligible)]
		cell := selected.cell

		out.WriteAheadWriters = cell.WAW
		out.ChunkSize = selected.csRows
		out.ParallelReaders = cell.ParallelReaders
		out.ReadAheadBuffers = cell.ReadAheadBuffers
		out.Tier = TierExploration
		// Label the run within the cold-start window when we're in it; for
		// forced exploration after the usable-row threshold, report the
		// original table index instead of an impossible "run 42/12".
		runLabel := fmt.Sprintf("probe idx %d/%d", selected.index+1, len(explorationCells))
		if usableCount < explorationGridRuns {
			runLabel = fmt.Sprintf("run %d/%d", usableCount+1, explorationGridRuns)
		}
		out.Reasoning = appendReasoning(out.Reasoning,
			"exploration: planned grid %s (WAW=%d, CS=%.1f×optimum=%.1fMB, PR=%d, RAB=%d)",
			runLabel, cell.WAW, cell.CSFraction, float64(selected.csBytes)/1024/1024, cell.ParallelReaders, cell.ReadAheadBuffers,
		)
		return
	}
	// Every grid candidate was filtered out — leave baseline alone. The
	// caller's memory clamp still runs; this is the safest fallback when
	// HardChunkLimit eliminates every probe (the historical-retry skip
	// was removed for #186, so HardChunkLimit is now the only way the
	// grid empties). Note the fallthrough so finalizeTierAndReasoning
	// doesn't have to invent a reason for this specific case.
	out.Reasoning = appendReasoning(out.Reasoning,
		"exploration: every planned-grid candidate filtered (HardChunkLimit=%d) — baseline kept",
		profile.HardChunkLimit,
	)
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

// applyEpsilonPerturbation nudges the picked (WAW, ChunkSize, PR, RAB)
// by one or two randomly-chosen directions. The first nudge is always
// attempted because the caller already made the ε decision. A second
// direction on a different knob is attempted with probability ε, so
// composite probes occur with total probability ε² (#295).
//
// Each nudge is bounded by the WAW grid, HardChunkLimit, and the
// reader-grid caps; if the chosen direction would violate a bound, falls
// through to the next direction so we always generate *some* variance.
//
// Historical-retry exclusions are intentionally NOT applied (same
// reasoning as applyGridExploration — #186). ε-perturbation is an
// exploration mechanism; its job is to occasionally probe non-argmax
// points so the regression's training data stays representative.
//
// Picks from these eight directions:
//
//	WAW + 1   (capped at maxWAWForGrid)
//	WAW − 1   (floored at 1)
//	CS × 1.5  (capped at HardChunkLimit if set)
//	CS × 0.67 (floored at 1 row)
//	PR × 2    (capped at perturbPRMax)              #219
//	PR ÷ 2    (floored at 1)                        #219
//	RAB × 2   (capped at perturbRABMax)             #219
//	RAB ÷ 2   (floored at perturbRABMin)            #219
func applyEpsilonPerturbation(out *Output, profile DriverProfile, epsilon float64) {
	type direction struct {
		knob  string
		name  string
		apply func(*Output) bool // returns true when the nudge was actually applied
	}
	dirs := []direction{
		{"waw", "WAW+1", func(o *Output) bool {
			next := o.WriteAheadWriters + 1
			if next > maxWAWForGrid {
				return false
			}
			o.WriteAheadWriters = next
			return true
		}},
		{"waw", "WAW-1", func(o *Output) bool {
			next := o.WriteAheadWriters - 1
			if next < 1 {
				return false
			}
			o.WriteAheadWriters = next
			return true
		}},
		{"cs", "CS×1.5", func(o *Output) bool {
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
		{"cs", "CS×0.67", func(o *Output) bool {
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
		{"pr", "PR×2", func(o *Output) bool {
			next := o.ParallelReaders * 2
			if next > perturbPRMax || next == o.ParallelReaders {
				return false
			}
			o.ParallelReaders = next
			return true
		}},
		{"pr", "PR÷2", func(o *Output) bool {
			next := o.ParallelReaders / 2
			if next < 1 || next == o.ParallelReaders {
				return false
			}
			o.ParallelReaders = next
			return true
		}},
		{"rab", "RAB×2", func(o *Output) bool {
			next := o.ReadAheadBuffers * 2
			if next > perturbRABMax || next == o.ReadAheadBuffers {
				return false
			}
			o.ReadAheadBuffers = next
			return true
		}},
		{"rab", "RAB÷2", func(o *Output) bool {
			next := o.ReadAheadBuffers / 2
			if next < perturbRABMin || next == o.ReadAheadBuffers {
				return false
			}
			o.ReadAheadBuffers = next
			return true
		}},
	}
	applied := make([]string, 0, 2)
	usedKnobs := map[string]bool{}
	applyOne := func() bool {
		order := rand.Perm(len(dirs))
		for _, idx := range order {
			if usedKnobs[dirs[idx].knob] {
				continue
			}
			if dirs[idx].apply(out) {
				usedKnobs[dirs[idx].knob] = true
				applied = append(applied, dirs[idx].name)
				return true
			}
		}
		return false
	}

	if !applyOne() {
		return
	}
	if shouldEpsilonPerturb(epsilon) {
		applyOne()
	}

	// A successful nudge means the *final* values came from exploration,
	// not from the upstream selector — overwrite Tier so the tier tag
	// matches what actually shipped to the migration (Codex review on
	// #202). The chained Reasoning preserves the upstream tier's note
	// ("regression-selected ... ; ε-perturbation X applied") so no
	// provenance is lost.
	out.Tier = TierExploration
	out.Reasoning = appendReasoning(out.Reasoning, "ε-perturbation %s applied", strings.Join(applied, ", "))
}
