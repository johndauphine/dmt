package tuning

import (
	"fmt"
	"math"
	"strings"
)

// tuningCandidate is one complete performance candidate. RequestedChunkBytes
// preserves the byte-shaped target used to construct exploration/regression
// candidates; ChunkSize is always the corresponding requested row count.
type tuningCandidate struct {
	Workers             int
	WriteAheadWriters   int
	ChunkSize           int
	RequestedChunkBytes int64
	ParallelReaders     int
	ReadAheadBuffers    int
}

// candidateProjection is the deterministic boundary between the tuner's raw
// candidate domain and the tuple that can actually run. ModelChunkBytes uses
// the legacy average-width encoding used by persisted training rows.
type candidateProjection struct {
	Requested            tuningCandidate
	Effective            tuningCandidate
	ModelChunkBytes      int64
	MemoryChunkLimit     int
	ProtocolChunkLimit   int
	MemoryClamped        bool
	ProtocolClamped      bool
	MinimumExceedsBudget bool
	pinned               []string
	clamps               []string
}

type effectiveCandidateKey struct {
	WriteAheadWriters int
	ChunkSize         int
	ParallelReaders   int
	ReadAheadBuffers  int
}

func candidateFromBytes(workers, waw int, chunkBytes int64, parallelReaders, readAheadBuffers int, representativeRowBytes int64) tuningCandidate {
	return tuningCandidate{
		Workers:             workers,
		WriteAheadWriters:   waw,
		ChunkSize:           rowsFromBytes(chunkBytes, representativeRowBytes),
		RequestedChunkBytes: chunkBytes,
		ParallelReaders:     parallelReaders,
		ReadAheadBuffers:    readAheadBuffers,
	}
}

func candidateFromOutput(out Output, in Input) tuningCandidate {
	return tuningCandidate{
		Workers:             out.Workers,
		WriteAheadWriters:   out.WriteAheadWriters,
		ChunkSize:           out.ChunkSize,
		RequestedChunkBytes: chunkBytesForModel(out.ChunkSize, in.representativeRowBytes()),
		ParallelReaders:     out.ParallelReaders,
		ReadAheadBuffers:    out.ReadAheadBuffers,
	}
}

// projectCandidate applies pins first, then the shared cardinality-aware
// memory limit and target protocol limit. It is pure and idempotent over its
// effective tuple: projecting Effective again cannot change the result.
func projectCandidate(in Input, profile DriverProfile, requested tuningCandidate) candidateProjection {
	representativeWidth := in.representativeRowBytes()
	if requested.ChunkSize <= 0 {
		requested.ChunkSize = rowsFromBytes(requested.RequestedChunkBytes, representativeWidth)
	}
	if requested.RequestedChunkBytes <= 0 {
		requested.RequestedChunkBytes = chunkBytesForModel(requested.ChunkSize, representativeWidth)
	}

	projection := candidateProjection{
		Requested: requested,
		Effective: requested,
	}
	applyPinnedInt := func(label string, pin *int, current *int) {
		if pin == nil || *pin <= 0 {
			return
		}
		*current = *pin
		projection.pinned = append(projection.pinned, fmt.Sprintf("%s=%d", label, *pin))
	}
	applyPinnedInt("workers", in.PinnedWorkers, &projection.Effective.Workers)
	applyPinnedInt("chunk", in.PinnedChunkSize, &projection.Effective.ChunkSize)
	applyPinnedInt("WAW", in.PinnedWriteAheadWriters, &projection.Effective.WriteAheadWriters)
	applyPinnedInt("PR", in.PinnedParallelReaders, &projection.Effective.ParallelReaders)
	applyPinnedInt("RAB", in.PinnedReadAheadBuffers, &projection.Effective.ReadAheadBuffers)

	preClampRows := projection.Effective.ChunkSize
	rowBytes, _ := in.safetyRowBytes()
	memoryModel := NewMemoryModel(in.MemoryProfile, rowBytes)
	if rows, minimumExceeds := memoryModel.safeChunkSizeDetail(
		in.MemoryBudgetMB,
		projection.Effective.Workers,
		projection.Effective.ReadAheadBuffers,
		projection.Effective.WriteAheadWriters,
	); rows > 0 {
		projection.MemoryChunkLimit = positiveRowsToInt(rows)
		projection.MinimumExceedsBudget = minimumExceeds
	}
	if profile.HardChunkLimit > 0 {
		projection.ProtocolChunkLimit = profile.HardChunkLimit
	}

	effectiveLimit := minPositiveInt(projection.MemoryChunkLimit, projection.ProtocolChunkLimit)
	if effectiveLimit > 0 && projection.Effective.ChunkSize > effectiveLimit {
		projection.Effective.ChunkSize = effectiveLimit
		if projection.MemoryChunkLimit == effectiveLimit {
			projection.MemoryClamped = true
			projection.clamps = append(projection.clamps, fmt.Sprintf("memory=%d", effectiveLimit))
		}
		if projection.ProtocolChunkLimit == effectiveLimit {
			projection.ProtocolClamped = true
			projection.clamps = append(projection.clamps, fmt.Sprintf("protocol=%d", effectiveLimit))
		}
	}
	if projection.Effective.ChunkSize <= 0 {
		projection.Effective.ChunkSize = 1
		projection.clamps = append(projection.clamps, "minimum-progress=1")
	}
	if preClampRows <= 0 && projection.Effective.ChunkSize == 1 && len(projection.clamps) == 0 {
		projection.clamps = append(projection.clamps, "minimum-progress=1")
	}
	if projection.MinimumExceedsBudget {
		projection.clamps = append(projection.clamps, "one-row-over-budget")
	}

	projection.ModelChunkBytes = chunkBytesForModel(
		projection.Effective.ChunkSize,
		safeAvgRowBytes(in.AvgRowBytes),
	)
	return projection
}

func (p candidateProjection) key() effectiveCandidateKey {
	return effectiveCandidateKey{
		WriteAheadWriters: p.Effective.WriteAheadWriters,
		ChunkSize:         p.Effective.ChunkSize,
		ParallelReaders:   p.Effective.ParallelReaders,
		ReadAheadBuffers:  p.Effective.ReadAheadBuffers,
	}
}

func (p candidateProjection) changed() bool {
	return p.Requested.Workers != p.Effective.Workers ||
		p.Requested.WriteAheadWriters != p.Effective.WriteAheadWriters ||
		p.Requested.ChunkSize != p.Effective.ChunkSize ||
		p.Requested.ParallelReaders != p.Effective.ParallelReaders ||
		p.Requested.ReadAheadBuffers != p.Effective.ReadAheadBuffers
}

func (p candidateProjection) reason() string {
	parts := make([]string, 0, 2)
	if len(p.pinned) > 0 {
		parts = append(parts, "pins="+strings.Join(p.pinned, "/"))
	}
	if len(p.clamps) > 0 {
		parts = append(parts, "clamp="+strings.Join(p.clamps, "+"))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

func applyProjectedCandidate(out *Output, projection candidateProjection) {
	if out == nil {
		return
	}
	out.Workers = projection.Effective.Workers
	out.WriteAheadWriters = projection.Effective.WriteAheadWriters
	out.ChunkSize = projection.Effective.ChunkSize
	out.ParallelReaders = projection.Effective.ParallelReaders
	out.ReadAheadBuffers = projection.Effective.ReadAheadBuffers
}

// applyPinnedAxes makes non-candidate selectors such as smoothed bins operate
// in the tuple that will run. Candidate-producing paths still call
// projectCandidate before rotation or scoring.
func applyPinnedAxes(out *Output, in Input) {
	if out == nil {
		return
	}
	if in.PinnedWorkers != nil && *in.PinnedWorkers > 0 {
		out.Workers = *in.PinnedWorkers
		out.MaxPartitions = *in.PinnedWorkers
	}
	if in.PinnedChunkSize != nil && *in.PinnedChunkSize > 0 {
		out.ChunkSize = *in.PinnedChunkSize
	}
	if in.PinnedWriteAheadWriters != nil && *in.PinnedWriteAheadWriters > 0 {
		out.WriteAheadWriters = *in.PinnedWriteAheadWriters
	}
	if in.PinnedParallelReaders != nil && *in.PinnedParallelReaders > 0 {
		out.ParallelReaders = *in.PinnedParallelReaders
	}
	if in.PinnedReadAheadBuffers != nil && *in.PinnedReadAheadBuffers > 0 {
		out.ReadAheadBuffers = *in.PinnedReadAheadBuffers
	}
}

func positiveRowsToInt(rows int64) int {
	if rows <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if rows > int64(maxInt) {
		return maxInt
	}
	return int(rows)
}

func minPositiveInt(values ...int) int {
	minimum := 0
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if minimum == 0 || value < minimum {
			minimum = value
		}
	}
	return minimum
}

func candidateMB(bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}
	if bytes == math.MaxInt64 {
		return float64(math.MaxInt64) / 1024 / 1024
	}
	return float64(bytes) / 1024 / 1024
}
