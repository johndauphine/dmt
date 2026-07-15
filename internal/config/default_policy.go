package config

import (
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/tuning"
)

const loadTimeFallbackRowBytes int64 = 500

// defaultTuningInput projects the immutable load-time resource snapshot into
// the tuner's formula-only input. No schema statistics exist yet, so the row
// widths are estimates and SafetyRowBytesKnown must remain false (#703).
func (c *Config) defaultTuningInput() tuning.Input {
	envelope := c.autoConfig.MemoryEnvelope
	return tuning.Input{
		CPUCores:               c.autoConfig.CPUCores,
		MemoryGB:               int((envelope.CapacityMB + 1023) / 1024),
		MemoryBudgetMB:         envelope.BudgetMB,
		Platform:               c.autoConfig.Platform,
		SourceDBType:           c.Source.Type,
		TargetDBType:           c.Target.Type,
		TargetMode:             c.Migration.TargetMode,
		AvgRowBytes:            loadTimeFallbackRowBytes,
		RepresentativeRowBytes: loadTimeFallbackRowBytes,
		SafetyRowBytes:         loadTimeFallbackRowBytes,
		SafetyRowBytesKnown:    false,
		UncappedAvgRowBytes:    loadTimeFallbackRowBytes,
	}
}

// defaultTuningOverrides returns only authoritative config/secrets values.
// Generated values must not become accidental overrides if defaults are ever
// reapplied to an already-populated Config.
func (c *Config) defaultTuningOverrides() tuning.DefaultOverrides {
	pinned := func(name string, value int) int {
		switch c.tunableProvenance(name) {
		case ProvenanceUserConfig, ProvenanceSecretsDefault:
			return value
		default:
			return 0
		}
	}
	return tuning.DefaultOverrides{
		Workers:           pinned(provenanceMigrationWorkers, c.Migration.Workers),
		ReadAheadBuffers:  pinned(provenanceMigrationReadAheadBuffers, c.Migration.ReadAheadBuffers),
		WriteAheadWriters: pinned(provenanceMigrationWriteAheadWriters, c.Migration.WriteAheadWriters),
		ParallelReaders:   pinned(provenanceMigrationParallelReaders, c.Migration.ParallelReaders),
	}
}

func (c *Config) loadTimeDefaultOutput() tuning.Output {
	in := c.defaultTuningInput()
	profile := driver.BuildTuningProfile(c.Target.Type, in.SafetyRowBytes, driver.TargetProbe{})
	overrides := c.defaultTuningOverrides()

	workers := legacyLoadTimeWorkers(in.CPUCores)
	if overrides.Workers > 0 {
		workers = overrides.Workers
	}

	chunkSize := legacyLoadTimeChunkSize(c.autoConfig.MemoryEnvelope.AvailableMB)
	// A pinned chunk remains authoritative at applyDefaultPolicy, but the legacy
	// RAB formula historically sized buffers from the chunk that will actually
	// run. Use the pin for that dependency without feeding it through the final
	// clamp as a generated recommendation.
	bufferSizingChunk := chunkSize
	switch c.tunableProvenance(provenanceMigrationChunkSize) {
	case ProvenanceUserConfig, ProvenanceSecretsDefault:
		if c.Migration.ChunkSize > 0 {
			bufferSizingChunk = c.Migration.ChunkSize
		}
	}

	waw := legacyLoadTimeWAW(in.CPUCores, profile)
	if overrides.WriteAheadWriters > 0 {
		waw = overrides.WriteAheadWriters
	}
	parallelReaders := legacyLoadTimeParallelReaders(in.CPUCores)
	if overrides.ParallelReaders > 0 {
		parallelReaders = overrides.ParallelReaders
	}

	targetMemoryMB := legacyLoadTimeTargetMemoryMB(
		c.autoConfig.MemoryEnvelope.AvailableMB,
		c.Migration.MaxMemoryMB,
	)
	readAheadBuffers := legacyLoadTimeReadAheadBuffers(targetMemoryMB, workers, bufferSizingChunk)
	if overrides.ReadAheadBuffers > 0 {
		readAheadBuffers = overrides.ReadAheadBuffers
	}

	out := tuning.Output{
		Workers:              workers,
		ChunkSize:            chunkSize,
		ReadAheadBuffers:     readAheadBuffers,
		WriteAheadWriters:    waw,
		ParallelReaders:      parallelReaders,
		MaxPartitions:        workers,
		LargeTableThreshold:  5_000_000,
		UpsertMergeChunkSize: legacyLoadTimeUpsertChunk(c.Migration.TargetMode, targetMemoryMB),
		CheckpointFrequency:  10,
		MaxRetries:           3,
		Reasoning:            "legacy load-time formula policy",
	}
	return tuning.FinalizeFormulaOutput(in, out)
}

// legacyLoadTimeWorkers preserves the pre-epic generated config default. It is
// intentionally not the history-aware tuner's baselineWorkers policy.
func legacyLoadTimeWorkers(cpuCores int) int {
	if cpuCores <= 6 {
		return 4
	}
	if cpuCores >= 14 {
		return 12
	}
	return cpuCores - 2
}

// legacyLoadTimeChunkSize preserves the old RAM-shaped 75K anchor with the
// documented 50K/200K bounds. Memory comes from the single retained envelope;
// the arithmetic branches before multiplication to stay total for injected
// extreme snapshots.
func legacyLoadTimeChunkSize(availableMemoryMB int64) int {
	const (
		baseRows = int64(75_000)
		minRows  = int64(50_000)
		maxRows  = int64(200_000)
		denomMB  = int64(8 * 1024)
		scale    = int64(25_000)
	)
	if availableMemoryMB <= 0 {
		return int(minRows)
	}
	// At 40 GiB the linear term reaches the upper bound.
	if availableMemoryMB >= 40*1024 {
		return int(maxRows)
	}
	rows := baseRows + availableMemoryMB*scale/denomMB
	if rows < minRows {
		rows = minRows
	}
	if rows > maxRows {
		rows = maxRows
	}
	return int(rows)
}

func legacyLoadTimeTargetMemoryMB(availableMemoryMB, maxMemoryMB int64) int64 {
	base := availableMemoryMB
	if maxMemoryMB > 0 && maxMemoryMB < base {
		base = maxMemoryMB
	}
	if base <= 0 {
		return 0
	}
	return base / 2
}

func legacyLoadTimeWAW(cpuCores int, profile tuning.DriverProfile) int {
	waw := profile.BaselineWAW
	if profile.ScaleWritersWithCores {
		if scaled := cpuCores / 4; scaled > waw {
			waw = scaled
		}
	}
	if waw < 1 {
		return 1
	}
	return waw
}

func legacyLoadTimeParallelReaders(cpuCores int) int {
	readers := cpuCores / 4
	if readers < 2 {
		return 2
	}
	return readers
}

func legacyLoadTimeReadAheadBuffers(targetMemoryMB int64, workers, chunkSize int) int {
	const (
		minBuffers  = 4
		maxBuffers  = 32
		bytesPerRow = int64(500)
		bytesPerMB  = int64(1024 * 1024)
		maxInt64    = int64(^uint64(0) >> 1)
	)
	if targetMemoryMB <= 0 || workers <= 0 || chunkSize <= 0 {
		return minBuffers
	}
	if int64(chunkSize) > maxInt64/bytesPerRow {
		return minBuffers
	}
	bytesPerChunk := int64(chunkSize) * bytesPerRow
	// MemoryEnvelope validation ensures targetMemoryMB*bytesPerMB is safe.
	buffers := targetMemoryMB * bytesPerMB / int64(workers) / bytesPerChunk
	if buffers < minBuffers {
		return minBuffers
	}
	if buffers > maxBuffers {
		return maxBuffers
	}
	return int(buffers)
}

func legacyLoadTimeUpsertChunk(targetMode string, targetMemoryMB int64) int {
	if targetMode != "upsert" {
		return 10_000
	}
	factor := targetMemoryMB / 1024
	if factor < 1 {
		factor = 1
	}
	if factor >= 4 {
		return 20_000
	}
	return int(5_000 * factor)
}

// applyDefaultPolicy fills only omitted tunables. The output has already been
// finalized against effective pinned concurrency, so an automatic chunk is
// safe for the tuple config will actually run. Explicit chunks are preserved.
func (c *Config) applyDefaultPolicy(out tuning.Output) {
	chunkPinned := false
	switch c.tunableProvenance(provenanceMigrationChunkSize) {
	case ProvenanceUserConfig, ProvenanceSecretsDefault:
		chunkPinned = true
	}

	if c.Migration.Workers == 0 {
		c.Migration.Workers = out.Workers
	}
	if c.Migration.ChunkSize == 0 {
		c.Migration.ChunkSize = out.ChunkSize
	}
	if c.Migration.ReadAheadBuffers == 0 {
		c.Migration.ReadAheadBuffers = out.ReadAheadBuffers
	}
	if c.Migration.WriteAheadWriters == 0 {
		c.Migration.WriteAheadWriters = out.WriteAheadWriters
	}
	if c.Migration.ParallelReaders == 0 {
		c.Migration.ParallelReaders = out.ParallelReaders
	}
	if c.Migration.MaxPartitions == 0 {
		c.Migration.MaxPartitions = out.MaxPartitions
	}
	if c.Migration.LargeTableThreshold == 0 {
		c.Migration.LargeTableThreshold = out.LargeTableThreshold
	}
	if c.Migration.UpsertMergeChunkSize == 0 {
		c.Migration.UpsertMergeChunkSize = out.UpsertMergeChunkSize
	}
	if c.Migration.CheckpointFrequency == 0 {
		c.Migration.CheckpointFrequency = out.CheckpointFrequency
	}
	if c.Migration.MaxRetries == 0 {
		c.Migration.MaxRetries = out.MaxRetries
	}

	c.autoConfig.DefaultPolicyReasoning = out.Reasoning
	c.autoConfig.DefaultPolicyChunkPinned = chunkPinned
}
