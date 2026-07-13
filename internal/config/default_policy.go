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
	return tuning.DefaultOutputWithOverrides(in, profile, c.defaultTuningOverrides())
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
