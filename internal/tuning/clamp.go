package tuning

import "fmt"

// fallbackBudgetMB is the floor for memoryBudgetMB so an unprobed system
// (AvailableMemoryMB == 0, MaxMemoryMB == 0) doesn't render a 0 MB budget.
// Lifted from internal/driver/ai_smartconfig.go (#158).
const fallbackBudgetMB = 1024

// EstimatedMemMB is the approximate working-set footprint in MiB for the
// chosen knobs. workers × (read_ahead + write_ahead) × chunk_size × avg
// is the prevailing model — each writer holds in-flight buffers up to
// chunk_size rows of avg-byte payload. Floor-MB result.
func EstimatedMemMB(workers, readAheadBuffers, writeAheadWriters, chunkSize int, avgRowBytes int64) int64 {
	return int64(workers) * int64(readAheadBuffers+writeAheadWriters) * int64(chunkSize) * avgRowBytes / 1024 / 1024
}

// safeChunkSize returns the largest chunk_size in rows that fits inside
// budgetMB at the given workers / buffers / avg_row_bytes — the inverse
// of EstimatedMemMB. Returns 0 when any input is non-positive.
func safeChunkSize(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int, avgRowBytes int64) int64 {
	denom := int64(workers) * int64(readAheadBuffers+writeAheadWriters) * avgRowBytes
	if denom <= 0 {
		return 0
	}
	return budgetMB * 1024 * 1024 / denom
}

// memoryBudgetMB returns the effective budget the clamp enforces, plus a
// short source description for diagnostic output. Selection rules (lifted
// verbatim from internal/driver/ai_smartconfig.go's effectiveMemoryBudgetMB):
//
//   - If MaxMemoryMB and AvailableMemoryMB are both > 0, take the more
//     restrictive of (cap, available − 2 GB headroom floored at fallback).
//   - Otherwise return whichever single source is set.
//   - Both unset → fallbackBudgetMB so the budget is never 0.
//
// The 2 GB headroom prevents OS / dmt-runtime overhead from being claimed
// by chunk buffers; the fallback floor prevents a non-monotone cliff
// where less-RAM-than-fallback would otherwise yield a smaller budget
// than no-info-at-all (#161).
func memoryBudgetMB(in Input) (budgetMB int64, source string) {
	headroom := func(available int64) (int64, string) {
		raw := available - 2048
		if raw <= fallbackBudgetMB {
			return fallbackBudgetMB, fmt.Sprintf("available_memory_mb=%d - 2048 MB headroom (floored at %d)", available, fallbackBudgetMB)
		}
		return raw, fmt.Sprintf("available_memory_mb=%d - 2048 MB headroom", available)
	}

	switch {
	case in.MaxMemoryMB > 0 && in.AvailableMemoryMB > 0:
		h, hSrc := headroom(in.AvailableMemoryMB)
		if in.MaxMemoryMB < h {
			return in.MaxMemoryMB, fmt.Sprintf("user max_memory_mb=%d", in.MaxMemoryMB)
		}
		return h, hSrc
	case in.MaxMemoryMB > 0:
		return in.MaxMemoryMB, fmt.Sprintf("user max_memory_mb=%d", in.MaxMemoryMB)
	case in.AvailableMemoryMB > 0:
		return headroom(in.AvailableMemoryMB)
	default:
		return fallbackBudgetMB, "fallback default; AvailableMemoryMB and MaxMemoryMB both unset"
	}
}

// applyMemoryClamp enforces the memory budget and the per-target hard
// limit (#166) on out.ChunkSize, recomputing EstimatedMemMB after any
// clamp. Real-bytes comparison (not floor-MB) so a 200.9 MiB footprint
// doesn't slide under a 200 MB cap by integer truncation (#156).
//
// The clamp is intentionally conservative — when input is malformed
// (zero workers, zero buffers, zero rows) it leaves chunk_size alone
// rather than computing a zero ceiling that would block the migration.
func applyMemoryClamp(out *Output, in Input) {
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}

	budgetMB, _ := memoryBudgetMB(in)
	estimatedBytes := int64(out.Workers) *
		int64(out.ReadAheadBuffers+out.WriteAheadWriters) *
		int64(out.ChunkSize) * avg
	budgetBytes := budgetMB * 1024 * 1024

	if estimatedBytes > budgetBytes {
		safe := safeChunkSize(
			budgetMB,
			out.Workers,
			out.ReadAheadBuffers,
			out.WriteAheadWriters,
			avg,
		)
		if safe > 0 {
			out.ChunkSize = int(safe)
		}
	}

	out.EstimatedMemMB = EstimatedMemMB(
		out.Workers,
		out.ReadAheadBuffers,
		out.WriteAheadWriters,
		out.ChunkSize,
		avg,
	)
}
