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

	waw := baselineWAW(in, profile)

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

// baselineWAW computes the platform- and core-aware baseline write_ahead_writers.
//
// Steps, in order:
//  1. Start at profile.BaselineWAW (the driver's declared default).
//  2. If profile.ScaleWritersWithCores is true, bump up to cores/4 if
//     higher. All three current drivers (PG, MSSQL, MySQL) set this true
//     in their Defaults() — PG and MySQL use bulk paths with native
//     parallelism (COPY, multi-value INSERT); MSSQL uses parallel BCP
//     without TABLOCK. Mirrors config.applyDefaults's WAW logic so the
//     tuner's baseline doesn't silently regress the cores-scaled value
//     config would otherwise have set when no history is available.
//  3. Floor at 1 (defensive against zero-valued profiles).
//  4. Cap at 1 on platforms with virtualized network transports between
//     dmt and the target (Docker Desktop on macOS/Windows, WSL2). The
//     transport layer's per-flow throughput is lower there and parallel
//     writers contend; on native Linux (Unix socket or real NIC) the
//     scaled value applies.
func baselineWAW(in Input, profile DriverProfile) int {
	waw := profile.BaselineWAW
	if profile.ScaleWritersWithCores && in.CPUCores > 0 {
		if scaled := in.CPUCores / 4; scaled > waw {
			waw = scaled
		}
	}
	if waw < 1 {
		waw = 1
	}
	switch in.Platform {
	case "wsl2", "darwin", "windows":
		if waw > 1 {
			waw = 1
		}
	}
	return waw
}

// chunkRowsFromProfile derives the chunk_size in rows from the per-target
// byte-shaped optimum (#166), then caps by the protocol-level hard limit
// (e.g. MySQL @@max_allowed_packet — currently 0 for all drivers; MySQL
// probe lands separately). Falls back to a 10 MB conservative default
// for unmeasured targets.
//
// Floors at 1 row to prevent migrations from blocking on pathological
// wide-row + small-byte-budget combinations (e.g. 50 MB row on the
// 10 MB unmeasured fallback divides to 0). At 1 row per chunk the
// throughput is awful but the migration still progresses; the alternative
// of returning 0 silently breaks WriteBatch.
func chunkRowsFromProfile(profile DriverProfile, avgRowBytes int64) int {
	const fallbackBytes int64 = 10_000_000
	if avgRowBytes <= 0 {
		avgRowBytes = 500
	}
	bytes := profile.OptimumBulkChunkBytes
	if bytes <= 0 {
		bytes = fallbackBytes
	}
	rows := int(bytes / avgRowBytes)
	if rows < 1 {
		rows = 1
	}
	if profile.HardChunkLimit > 0 && rows > profile.HardChunkLimit {
		rows = profile.HardChunkLimit
	}
	return rows
}
