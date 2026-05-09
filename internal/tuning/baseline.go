package tuning

// baseline returns a complete pure-formula Output from system + profile.
// No history consulted, no clamp applied (the caller layers those on).
//
// Source of truth for the per-knob formulas — replaces the
// applyDefaultSuggestions path in internal/driver/ai_smartconfig.go.
func baseline(in Input, profile DriverProfile) Output {
	workers := in.CPUCores - 2
	if workers < 2 {
		workers = 2
	}

	const (
		readAheadBuffers     = 4
		parallelReaders      = 2
		largeTableThreshold  = int64(1_000_000)
		upsertMergeChunkSize = 5000
		checkpointFrequency  = 20
		maxRetries           = 3
	)

	waw := baselineWAW(in.Platform, profile)

	chunkSize := chunkRowsFromProfile(profile, in.AvgRowBytes)

	return Output{
		Workers:              workers,
		ChunkSize:            chunkSize,
		ReadAheadBuffers:     readAheadBuffers,
		WriteAheadWriters:    waw,
		ParallelReaders:      parallelReaders,
		MaxPartitions:        workers,
		LargeTableThreshold:  largeTableThreshold,
		MaxSourceConnections: workers + 4,
		MaxTargetConnections: workers*2 + 4,
		UpsertMergeChunkSize: upsertMergeChunkSize,
		CheckpointFrequency:  checkpointFrequency,
		MaxRetries:           maxRetries,
	}
}

// baselineWAW caps the driver-declared baseline WAW at 1 on platforms with
// virtualized network transports between dmt and the target (Docker
// Desktop on macOS/Windows, WSL2). The transport layer's per-flow
// throughput is lower there, and parallel writers contend; on native
// Linux (Unix socket or real NIC) the driver default applies.
func baselineWAW(platform string, profile DriverProfile) int {
	waw := profile.BaselineWAW
	if waw < 1 {
		waw = 1
	}
	switch platform {
	case "wsl2", "darwin", "windows":
		if waw > 1 {
			waw = 1
		}
	}
	return waw
}

// chunkRowsFromProfile derives the chunk_size in rows from the per-target
// byte-shaped optimum (#166). Falls back to a 10 MB conservative default
// for unmeasured targets.
func chunkRowsFromProfile(profile DriverProfile, avgRowBytes int64) int {
	const fallbackBytes int64 = 10_000_000
	if avgRowBytes <= 0 {
		avgRowBytes = 500
	}
	bytes := profile.OptimumBulkChunkBytes
	if bytes <= 0 {
		bytes = fallbackBytes
	}
	return int(bytes / avgRowBytes)
}
