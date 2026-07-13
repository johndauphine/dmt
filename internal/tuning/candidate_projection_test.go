package tuning

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestProjectCandidateAppliesPinsBeforeCombinedCaps(t *testing.T) {
	workers, chunk, waw, pr, rab := 2, 20_000, 2, 3, 2
	in := Input{
		MemoryBudgetMB:          1,
		AvgRowBytes:             200,
		RepresentativeRowBytes:  100,
		SafetyRowBytes:          100,
		SafetyRowBytesKnown:     true,
		PinnedWorkers:           &workers,
		PinnedChunkSize:         &chunk,
		PinnedWriteAheadWriters: &waw,
		PinnedParallelReaders:   &pr,
		PinnedReadAheadBuffers:  &rab,
	}
	profile := DriverProfile{HardChunkLimit: 1_500}
	requested := candidateFromBytes(1, 1, 5_000_000, 2, 4, in.representativeRowBytes())

	got := projectCandidate(in, profile, requested)
	if got.Effective.Workers != workers || got.Effective.WriteAheadWriters != waw ||
		got.Effective.ParallelReaders != pr || got.Effective.ReadAheadBuffers != rab {
		t.Fatalf("effective pinned axes = %+v", got.Effective)
	}
	// 1 MiB / (2 workers * (2 RAB + 2 WAW) * 100 B) = 1310 rows.
	if got.MemoryChunkLimit != 1_310 || got.ProtocolChunkLimit != 1_500 || got.Effective.ChunkSize != 1_310 {
		t.Fatalf("combined projection = memory %d/protocol %d/effective %d, want 1310/1500/1310",
			got.MemoryChunkLimit, got.ProtocolChunkLimit, got.Effective.ChunkSize)
	}
	if !got.MemoryClamped || got.ProtocolClamped {
		t.Fatalf("clamp flags = memory:%v protocol:%v, want true/false", got.MemoryClamped, got.ProtocolClamped)
	}
	if got.ModelChunkBytes != 1_310*200 {
		t.Fatalf("model bytes = %d, want %d", got.ModelChunkBytes, 1_310*200)
	}
	if !strings.Contains(got.reason(), "pins=") || !strings.Contains(got.reason(), "memory=1310") {
		t.Fatalf("projection reason = %q, want pins and memory clamp", got.reason())
	}
}

func TestProjectCandidateUsesCardinalityAwareProfile(t *testing.T) {
	in := Input{
		MemoryBudgetMB:         1,
		AvgRowBytes:            100,
		RepresentativeRowBytes: 100,
		SafetyRowBytes:         36_864,
		MemoryProfile: NewMemoryProfile([]TableMemoryStat{
			{Name: "tiny_wide", RowCount: 2, AvgRowBytes: 36_864},
			{Name: "large", RowCount: 1_000_000, AvgRowBytes: 100},
		}),
	}
	got := projectCandidate(in, DriverProfile{}, tuningCandidate{
		Workers: 1, WriteAheadWriters: 0, ReadAheadBuffers: 1,
		ChunkSize: 20_000, ParallelReaders: 1,
	})
	if got.MemoryChunkLimit != 10_485 || got.Effective.ChunkSize != 10_485 {
		t.Fatalf("table-aware projection = cap %d/effective %d, want 10485",
			got.MemoryChunkLimit, got.Effective.ChunkSize)
	}
	if scalar := SafeChunkSize(1, 1, 1, 0, 36_864); scalar != 28 {
		t.Fatalf("scalar control cap = %d, want 28", scalar)
	}
}

func TestProjectCandidatePreservesOneRowOverBudgetSignal(t *testing.T) {
	in := Input{MemoryBudgetMB: 1, AvgRowBytes: 2 << 20, SafetyRowBytes: 2 << 20}
	got := projectCandidate(in, DriverProfile{}, tuningCandidate{
		Workers: 1, WriteAheadWriters: 0, ReadAheadBuffers: 1,
		ChunkSize: 100, ParallelReaders: 1,
	})
	if got.Effective.ChunkSize != 1 || !got.MinimumExceedsBudget {
		t.Fatalf("one-row projection = effective %d/minimum-over %v, want 1/true",
			got.Effective.ChunkSize, got.MinimumExceedsBudget)
	}
	if !strings.Contains(got.reason(), "one-row-over-budget") {
		t.Fatalf("reason = %q, want explicit one-row exception", got.reason())
	}
}

func TestProjectCandidateDeterministicIdempotentAndSaturating(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	in := Input{
		MemoryBudgetMB:         math.MaxInt64,
		AvgRowBytes:            math.MaxInt64,
		RepresentativeRowBytes: 1,
		SafetyRowBytes:         1,
	}
	request := tuningCandidate{
		Workers: maxInt, WriteAheadWriters: maxInt, ReadAheadBuffers: maxInt,
		ChunkSize: maxInt, ParallelReaders: maxInt,
	}
	first := projectCandidate(in, DriverProfile{}, request)
	second := projectCandidate(in, DriverProfile{}, request)
	if first.key() != second.key() || first.ModelChunkBytes != second.ModelChunkBytes || first.reason() != second.reason() {
		t.Fatalf("deterministic replay diverged: first=%+v second=%+v", first, second)
	}
	if first.ModelChunkBytes != math.MaxInt64 {
		t.Fatalf("overflowing model bytes = %d, want saturation", first.ModelChunkBytes)
	}
	reproject := projectCandidate(in, DriverProfile{}, tuningCandidate{
		Workers: first.Effective.Workers, WriteAheadWriters: first.Effective.WriteAheadWriters,
		ChunkSize: first.Effective.ChunkSize, ParallelReaders: first.Effective.ParallelReaders,
		ReadAheadBuffers: first.Effective.ReadAheadBuffers,
	})
	if reproject.key() != first.key() {
		t.Fatalf("effective tuple not idempotent: first=%+v reprojection=%+v", first.key(), reproject.key())
	}
}

func FuzzProjectCandidate(f *testing.F) {
	f.Add(uint16(12), uint16(1), uint16(4), uint32(53_648), uint32(8_556), uint32(36_864), uint32(78_000), false)
	f.Add(uint16(1), uint16(1), uint16(1), uint32(1), uint32(1), uint32(2<<20), uint32(1), true)
	f.Fuzz(func(t *testing.T, workersRaw, wawRaw, rabRaw uint16, chunkRaw, budgetRaw, rowBytesRaw, hardRaw uint32, pin bool) {
		workers := int(workersRaw%64) + 1
		waw := int(wawRaw%32) + 1
		rab := int(rabRaw%32) + 1
		chunk := int(chunkRaw%2_000_000) + 1
		budget := int64(budgetRaw%65_536) + 1
		rowBytes := int64(rowBytesRaw%4_194_304) + 1
		hard := int(hardRaw % 2_000_000)
		pr := 2
		in := Input{
			MemoryBudgetMB:         budget,
			AvgRowBytes:            rowBytes,
			RepresentativeRowBytes: rowBytes,
			SafetyRowBytes:         rowBytes,
			SafetyRowBytesKnown:    true,
		}
		if pin {
			in.PinnedWorkers = &workers
			in.PinnedChunkSize = &chunk
			in.PinnedWriteAheadWriters = &waw
			in.PinnedParallelReaders = &pr
			in.PinnedReadAheadBuffers = &rab
		}
		profile := DriverProfile{HardChunkLimit: hard}
		request := tuningCandidate{
			Workers: workers, WriteAheadWriters: waw, ChunkSize: chunk,
			ParallelReaders: pr, ReadAheadBuffers: rab,
		}
		first := projectCandidate(in, profile, request)
		second := projectCandidate(in, profile, request)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("projection is nondeterministic: first=%+v second=%+v", first, second)
		}
		if first.Effective.ChunkSize < 1 {
			t.Fatalf("effective chunk = %d, want positive", first.Effective.ChunkSize)
		}
		if hard > 0 && first.Effective.ChunkSize > hard {
			t.Fatalf("effective chunk %d exceeds protocol cap %d", first.Effective.ChunkSize, hard)
		}
		model := NewMemoryModel(in.MemoryProfile, rowBytes)
		if model.ExceedsBudget(budget, first.Effective.Workers, first.Effective.ReadAheadBuffers,
			first.Effective.WriteAheadWriters, first.Effective.ChunkSize) && !first.MinimumExceedsBudget {
			t.Fatalf("effective tuple exceeds memory without one-row signal: %+v", first)
		}
		if want := chunkBytesForModel(first.Effective.ChunkSize, safeAvgRowBytes(in.AvgRowBytes)); first.ModelChunkBytes != want {
			t.Fatalf("model bytes = %d, want %d", first.ModelChunkBytes, want)
		}
		reproject := projectCandidate(in, profile, tuningCandidate{
			Workers: first.Effective.Workers, WriteAheadWriters: first.Effective.WriteAheadWriters,
			ChunkSize: first.Effective.ChunkSize, ParallelReaders: first.Effective.ParallelReaders,
			ReadAheadBuffers: first.Effective.ReadAheadBuffers,
		})
		if reproject.key() != first.key() {
			t.Fatalf("projection is not idempotent: first=%+v reproject=%+v", first.key(), reproject.key())
		}
	})
}
