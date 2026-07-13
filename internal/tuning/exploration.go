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
	{WAW: maxLearnableWAW, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: 1, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	{WAW: 2, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
	{WAW: 3, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: maxLearnableWAW, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},

	// Deliberate replicates of 0, 1, 6, and 7.
	{WAW: 1, CSFraction: halfOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 4},
	{WAW: 2, CSFraction: halfOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 8},
	{WAW: 3, CSFraction: fullOptimumFraction, ParallelReaders: 4, ReadAheadBuffers: 4},
	{WAW: maxLearnableWAW, CSFraction: fullOptimumFraction, ParallelReaders: 2, ReadAheadBuffers: 8},
}

// explorationReplicateOf makes the four declared variance observations
// explicit. A negative value means the entry is a unique design cell; a
// non-negative value is the raw index whose observation this entry repeats.
// Projection-created aliases are deduplicated separately and never inferred
// to be intentional merely because their effective tuples are equal.
var explorationReplicateOf = [...]int{-1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 6, 7}

// Compile-time equality check: either expression becomes a negative array
// length if the table and the cold-start threshold diverge.
var (
	_ [explorationGridRuns - len(explorationCells)]struct{}
	_ [len(explorationCells) - explorationGridRuns]struct{}
	_ [explorationGridRuns - len(explorationReplicateOf)]struct{}
	_ [len(explorationReplicateOf) - explorationGridRuns]struct{}
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

// applyGridExploration picks the next reachable probe from the planned grid
// based on raw persisted attempts. The usable post-filter count separately
// controls cold-start labels and completion (#697, #698). The raw count is a
// sequential rotation proxy; concurrent tuners can observe the same value and
// this function does not claim to reserve cells globally.
// Historical-retry exclusions are intentionally NOT applied here: the
// planned grid's job is to gather data so historical verdicts can be
// re-examined as new runs accrue (issue #186 — under the original
// "any retry → permanent exclusion" rule, AI-era retries at WAW=2
// permanently locked the deterministic tuner out of probing it).
func applyGridExploration(out *Output, in Input, profile DriverProfile, usableCount, rawIndex int) {
	const fallbackBytes int64 = 10_000_000
	representativeWidth := in.representativeRowBytes()
	optimumBytes := profile.OptimumBulkChunkBytes
	if optimumBytes <= 0 {
		optimumBytes = fallbackBytes
	}

	type scheduledCell struct {
		index       int
		replicateOf int
		cell        explorationCell
		projection  candidateProjection
	}
	schedule := make([]scheduledCell, 0, len(explorationCells))
	seenBase := make(map[effectiveCandidateKey]bool, len(explorationCells))
	uniqueEffective := make(map[effectiveCandidateKey]bool, len(explorationCells))
	declaredReplicates := 0
	for idx, cell := range explorationCells {
		csBytes := scaledByteTarget(optimumBytes, cell.CSFraction)
		projection := projectCandidate(in, profile, candidateFromBytes(
			out.Workers,
			cell.WAW,
			csBytes,
			cell.ParallelReaders,
			cell.ReadAheadBuffers,
			representativeWidth,
		))
		key := projection.key()
		uniqueEffective[key] = true
		replicateOf := explorationReplicateOf[idx]
		if replicateOf < 0 && seenBase[key] {
			continue
		}
		if replicateOf >= 0 {
			declaredReplicates++
		} else {
			seenBase[key] = true
		}
		schedule = append(schedule, scheduledCell{
			index:       idx,
			replicateOf: replicateOf,
			cell:        cell,
			projection:  projection,
		})
	}
	if len(schedule) > 0 {
		selected := schedule[rawIndex%len(schedule)]
		cell := selected.cell
		applyProjectedCandidate(out, selected.projection)
		out.Tier = TierExploration
		// Label the run within the cold-start window when we're in it; for
		// forced exploration after the usable-row threshold, report the
		// original table index instead of an impossible "run 42/12".
		runLabel := fmt.Sprintf("probe idx %d/%d", selected.index+1, len(explorationCells))
		if usableCount < explorationGridRuns {
			runLabel = fmt.Sprintf("run %d/%d", usableCount+1, explorationGridRuns)
		}
		replicateLabel := ""
		if selected.replicateOf >= 0 {
			replicateLabel = fmt.Sprintf(", declared replicate of idx %d", selected.replicateOf+1)
		}
		out.Reasoning = appendReasoning(out.Reasoning,
			"exploration: planned grid %s%s (requested WAW=%d, CS=%.1f×optimum=%d rows/%.1f MB, PR=%d, RAB=%d; effective WAW=%d, chunk=%d rows/model=%.1f MB, PR=%d, RAB=%d; %s; raw=%d, unique_effective=%d, scheduled=%d, declared_replicates=%d)",
			runLabel,
			replicateLabel,
			cell.WAW,
			cell.CSFraction,
			selected.projection.Requested.ChunkSize,
			candidateMB(selected.projection.Requested.RequestedChunkBytes),
			cell.ParallelReaders,
			cell.ReadAheadBuffers,
			selected.projection.Effective.WriteAheadWriters,
			selected.projection.Effective.ChunkSize,
			candidateMB(selected.projection.ModelChunkBytes),
			selected.projection.Effective.ParallelReaders,
			selected.projection.Effective.ReadAheadBuffers,
			selected.projection.reason(),
			len(explorationCells),
			len(uniqueEffective),
			len(schedule),
			declaredReplicates,
		)
		return
	}
	// Defensive only: the fixed design is non-empty and projection floors every
	// candidate to one row, so production cannot exhaust the schedule.
	out.Reasoning = appendReasoning(out.Reasoning,
		"exploration: projected schedule unexpectedly empty — baseline kept",
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
// Each nudge is bounded by the WAW and reader-grid caps, then passed through
// the same pin/memory/protocol projection as every other candidate. Directions
// that collapse to the current effective tuple are skipped so exploration
// never claims variance that cannot execute.
//
// Historical-retry exclusions are intentionally NOT applied (same
// reasoning as applyGridExploration — #186). ε-perturbation is an
// exploration mechanism; its job is to occasionally probe non-argmax
// points so the regression's training data stays representative.
//
// Picks from these eight directions:
//
//	WAW + 1   (capped at maxLearnableWAW)
//	WAW − 1   (floored at 1)
//	CS × 1.5  (then projected through memory/protocol caps)
//	CS × 0.67 (floored at 1 row)
//	PR × 2    (capped at perturbPRMax)              #219
//	PR ÷ 2    (floored at 1)                        #219
//	RAB × 2   (capped at perturbRABMax)             #219
//	RAB ÷ 2   (floored at perturbRABMin)            #219
func applyEpsilonPerturbation(out *Output, in Input, profile DriverProfile, epsilon float64) {
	type direction struct {
		knob  string
		name  string
		apply func(*Output) bool // returns true when the nudge was actually applied
	}
	dirs := []direction{
		{"waw", "WAW+1", func(o *Output) bool {
			if in.PinnedWriteAheadWriters != nil {
				return false
			}
			if o.WriteAheadWriters < 1 || o.WriteAheadWriters >= maxLearnableWAW {
				return false
			}
			next := o.WriteAheadWriters + 1
			o.WriteAheadWriters = next
			return true
		}},
		{"waw", "WAW-1", func(o *Output) bool {
			if in.PinnedWriteAheadWriters != nil {
				return false
			}
			if o.WriteAheadWriters <= 1 {
				return false
			}
			next := o.WriteAheadWriters - 1
			o.WriteAheadWriters = next
			return true
		}},
		{"cs", "CS×1.5", func(o *Output) bool {
			if in.PinnedChunkSize != nil {
				return false
			}
			next := scalePositiveIntRatio(o.ChunkSize, 3, 2)
			if next == o.ChunkSize {
				return false
			}
			o.ChunkSize = next
			return true
		}},
		{"cs", "CS×0.67", func(o *Output) bool {
			if in.PinnedChunkSize != nil {
				return false
			}
			next := scalePositiveIntRatio(o.ChunkSize, 67, 100)
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
			if in.PinnedParallelReaders != nil {
				return false
			}
			if o.ParallelReaders <= 0 || o.ParallelReaders > perturbPRMax/2 {
				return false
			}
			next := o.ParallelReaders * 2
			o.ParallelReaders = next
			return true
		}},
		{"pr", "PR÷2", func(o *Output) bool {
			if in.PinnedParallelReaders != nil {
				return false
			}
			next := o.ParallelReaders / 2
			if next < 1 || next == o.ParallelReaders {
				return false
			}
			o.ParallelReaders = next
			return true
		}},
		{"rab", "RAB×2", func(o *Output) bool {
			if in.PinnedReadAheadBuffers != nil {
				return false
			}
			if o.ReadAheadBuffers <= 0 || o.ReadAheadBuffers > perturbRABMax/2 {
				return false
			}
			next := o.ReadAheadBuffers * 2
			o.ReadAheadBuffers = next
			return true
		}},
		{"rab", "RAB÷2", func(o *Output) bool {
			if in.PinnedReadAheadBuffers != nil {
				return false
			}
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
	lastProjection := applyEffectiveProjection(out, in, profile)
	applyOne := func() bool {
		order := rand.Perm(len(dirs))
		for _, idx := range order {
			if usedKnobs[dirs[idx].knob] {
				continue
			}
			trial := *out
			if !dirs[idx].apply(&trial) {
				continue
			}
			projection := projectCandidate(in, profile, candidateFromOutput(trial, in))
			if projection.key() == lastProjection.key() {
				continue
			}
			applyProjectedCandidate(out, projection)
			lastProjection = projection
			usedKnobs[dirs[idx].knob] = true
			applied = append(applied, dirs[idx].name)
			return true
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
	out.Reasoning = appendReasoning(out.Reasoning,
		"ε-perturbation %s applied; effective WAW=%d, chunk=%d rows/model=%.1f MB, PR=%d, RAB=%d (%s)",
		strings.Join(applied, ", "),
		lastProjection.Effective.WriteAheadWriters,
		lastProjection.Effective.ChunkSize,
		candidateMB(lastProjection.ModelChunkBytes),
		lastProjection.Effective.ParallelReaders,
		lastProjection.Effective.ReadAheadBuffers,
		lastProjection.reason(),
	)
}
