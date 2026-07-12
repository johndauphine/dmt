package tuning

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

// applyMemoryClamp enforces the memory budget on out.ChunkSize,
// recomputing EstimatedMemMB after any clamp. Real-bytes comparison
// (not floor-MB) so a 200.9 MiB footprint doesn't slide under a 200 MB
// cap by integer truncation (#156).
//
// The per-target HardChunkLimit (#166) is enforced upstream in baseline
// (see chunkRowsFromProfile), not here — capping in baseline keeps the
// hard limit binding regardless of whether the memory budget would have
// fired. This function only handles the memory-budget axis.
//
// MemoryBudgetMB is resolved once by the caller's shared memory envelope;
// this package does not subtract headroom, apply a fallback floor, or resolve
// max_memory_mb again. A non-positive budget is explicitly nonbinding: the
// estimate is still populated, but chunk_size is left unchanged.
//
// The clamp is intentionally conservative when the sizing tuple is malformed
// (zero workers, zero buffers, or zero rows): it leaves chunk_size alone rather
// than computing a zero ceiling that would block the migration.
func applyMemoryClamp(out *Output, in Input) {
	avg := in.AvgRowBytes
	if avg <= 0 {
		avg = 500
	}

	budgetMB := in.MemoryBudgetMB
	estimatedBytes := int64(out.Workers) *
		int64(out.ReadAheadBuffers+out.WriteAheadWriters) *
		int64(out.ChunkSize) * avg

	if budgetMB > 0 && estimatedBytes > budgetMB*1024*1024 {
		safe := safeChunkSize(
			budgetMB,
			out.Workers,
			out.ReadAheadBuffers,
			out.WriteAheadWriters,
			avg,
		)
		if safe > 0 && int(safe) < out.ChunkSize {
			// Append a clamp note so the Reasoning string matches the
			// final out.ChunkSize. Pre-#202, the selector reasoning
			// could say "regression-selected ... chunk_size=43630"
			// while the final value silently dropped to a smaller
			// memory-safe figure (Copilot review on PR #203).
			oldCS := out.ChunkSize
			out.ChunkSize = int(safe)
			out.Reasoning = appendReasoning(out.Reasoning,
				"memory clamp: chunk_size %d → %d rows (budget %d MB)",
				oldCS, out.ChunkSize, budgetMB,
			)
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
