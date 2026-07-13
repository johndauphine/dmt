package tuning

// ConnectionPoolSizes derives the source and target connection-pool sizes
// from the effective concurrency tuple. Each worker may consume one source
// connection per parallel reader and one target connection per write-ahead
// writer; four additional connections leave headroom for orchestration and
// health checks.
//
// Invalid nonpositive inputs retain only the headroom for the affected side.
// Pathological products saturate at the platform's maximum int rather than
// wrapping into an undersized pool.
func ConnectionPoolSizes(workers, parallelReaders, writeAheadWriters int) (source, target int) {
	return connectionPoolSize(workers, parallelReaders), connectionPoolSize(workers, writeAheadWriters)
}

func connectionPoolSize(workers, connectionsPerWorker int) int {
	const headroom = 4
	maxInt := int(^uint(0) >> 1)
	if workers <= 0 || connectionsPerWorker <= 0 {
		return headroom
	}
	if workers > (maxInt-headroom)/connectionsPerWorker {
		return maxInt
	}
	return workers*connectionsPerWorker + headroom
}

func finalizeConnectionPoolSizes(out *Output) {
	if out == nil {
		return
	}
	out.MaxSourceConnections, out.MaxTargetConnections = ConnectionPoolSizes(
		out.Workers,
		out.ParallelReaders,
		out.WriteAheadWriters,
	)
}

// applyEffectiveProjection synchronizes an Output with the candidate tuple
// that can actually run and records requested-to-effective provenance once.
// Selection paths may call it before epsilon exploration; finalization calls it
// again as an idempotent backstop.
func applyEffectiveProjection(out *Output, in Input, profile DriverProfile) candidateProjection {
	if out == nil {
		return candidateProjection{}
	}
	projection := projectCandidate(in, profile, candidateFromOutput(*out, in))
	if projection.changed() {
		memoryAudit := ""
		if projection.MemoryClamped {
			memoryAudit = "; memory clamp applied during effective projection"
		}
		out.Reasoning = appendReasoning(out.Reasoning,
			"effective projection: requested chunk=%d rows/%.1f MB; effective chunk=%d rows/model=%.1f MB (%s)%s",
			projection.Requested.ChunkSize,
			candidateMB(projection.Requested.RequestedChunkBytes),
			projection.Effective.ChunkSize,
			candidateMB(projection.ModelChunkBytes),
			projection.reason(),
			memoryAudit,
		)
	}
	applyProjectedCandidate(out, projection)
	return projection
}

// finalizeOutput applies resource-dependent finalization in its required
// order: first shrink the chunk for the effective concurrency tuple, then
// derive connection pools from that same final tuple.
func finalizeOutput(out *Output, in Input, profile DriverProfile) {
	if out == nil {
		return
	}
	applyEffectiveProjection(out, in, profile)
	applyMemoryClamp(out, in)
	finalizeConnectionPoolSizes(out)
}
