package config

import (
	"fmt"
	"sort"
	"strings"
)

const (
	provenanceMigrationWorkers              = "migration.workers"
	provenanceMigrationChunkSize            = "migration.chunk_size"
	provenanceMigrationReadAheadBuffers     = "migration.read_ahead_buffers"
	provenanceMigrationMaxPartitions        = "migration.max_partitions"
	provenanceMigrationMaxSourceConns       = "migration.max_source_connections"
	provenanceMigrationMaxTargetConns       = "migration.max_target_connections"
	provenanceMigrationWriteAheadWriters    = "migration.write_ahead_writers"
	provenanceMigrationParallelReaders      = "migration.parallel_readers"
	provenanceMigrationLargeTableThreshold  = "migration.large_table_threshold"
	provenanceMigrationSampleSize           = "migration.sample_size"
	provenanceMigrationUpsertMergeChunkSize = "migration.upsert_merge_chunk_size"
	provenanceMigrationCheckpointFrequency  = "migration.checkpoint_frequency"
	provenanceMigrationMaxRetries           = "migration.max_retries"
	provenanceSourceChunkSize               = "source.chunk_size"
	provenanceTargetChunkSize               = "target.chunk_size"
)

func (c *Config) captureRawUserConfigSnapshot() {
	if c.autoConfig.RawUserConfigCaptured {
		return
	}

	c.autoConfig.RawUserSource = c.Source
	c.autoConfig.RawUserTarget = c.Target
	c.autoConfig.RawUserMigration = c.Migration
	c.autoConfig.RawUserConfigCaptured = true
	c.ensureProvenanceMap()
	c.markUserConfiguredTunables()
}

func (c *Config) ensureProvenanceMap() {
	if c.autoConfig.TunableValueProvenance == nil {
		c.autoConfig.TunableValueProvenance = make(map[string]ConfigValueProvenance)
	}
}

func (c *Config) setTunableProvenance(name string, source ConfigValueProvenance) {
	c.ensureProvenanceMap()
	c.autoConfig.TunableValueProvenance[name] = source
}

func (c *Config) tunableProvenance(name string) ConfigValueProvenance {
	if c.autoConfig.TunableValueProvenance == nil {
		return ""
	}
	return c.autoConfig.TunableValueProvenance[name]
}

func (c *Config) setTunableProvenanceIfUnset(name string, source ConfigValueProvenance) {
	if c.tunableProvenance(name) != "" {
		return
	}
	c.setTunableProvenance(name, source)
}

// smartConfigCanOverride makes the ownership policy explicit. Per-config
// values and secrets defaults are pinned; formula, driver, and other generated
// defaults remain overrideable by smartconfig.
func (c *Config) smartConfigCanOverride(name string, originalIsZero bool) bool {
	switch c.tunableProvenance(name) {
	case ProvenanceUserConfig, ProvenanceSecretsDefault:
		return false
	default:
		return originalIsZero
	}
}

func (c *Config) markUserConfiguredTunables() {
	raw := c.autoConfig.RawUserMigration

	markInt := func(name string, value int) {
		if value != 0 {
			c.setTunableProvenance(name, ProvenanceUserConfig)
		}
	}
	markInt64 := func(name string, value int64) {
		if value != 0 {
			c.setTunableProvenance(name, ProvenanceUserConfig)
		}
	}

	markInt(provenanceMigrationWorkers, raw.Workers)
	markInt(provenanceMigrationChunkSize, raw.ChunkSize)
	markInt(provenanceMigrationReadAheadBuffers, raw.ReadAheadBuffers)
	markInt(provenanceMigrationMaxPartitions, raw.MaxPartitions)
	markInt(provenanceMigrationMaxSourceConns, raw.MaxSourceConnections)
	markInt(provenanceMigrationMaxTargetConns, raw.MaxTargetConnections)
	markInt(provenanceMigrationWriteAheadWriters, raw.WriteAheadWriters)
	markInt(provenanceMigrationParallelReaders, raw.ParallelReaders)
	markInt64(provenanceMigrationLargeTableThreshold, raw.LargeTableThreshold)
	markInt(provenanceMigrationSampleSize, raw.SampleSize)
	markInt(provenanceMigrationUpsertMergeChunkSize, raw.UpsertMergeChunkSize)
	markInt(provenanceMigrationCheckpointFrequency, raw.CheckpointFrequency)
	markInt(provenanceMigrationMaxRetries, raw.MaxRetries)
	markInt(provenanceSourceChunkSize, c.autoConfig.RawUserSource.ChunkSize)
	markInt(provenanceTargetChunkSize, c.autoConfig.RawUserTarget.ChunkSize)
}

func (c *Config) markSecretsDefaultTunables() {
	markInt := func(name string, value int) {
		if value != 0 {
			c.setTunableProvenanceIfUnset(name, ProvenanceSecretsDefault)
		}
	}
	markInt64 := func(name string, value int64) {
		if value != 0 {
			c.setTunableProvenanceIfUnset(name, ProvenanceSecretsDefault)
		}
	}

	markInt(provenanceMigrationWorkers, c.Migration.Workers)
	markInt(provenanceMigrationReadAheadBuffers, c.Migration.ReadAheadBuffers)
	markInt(provenanceMigrationMaxSourceConns, c.Migration.MaxSourceConnections)
	markInt(provenanceMigrationMaxTargetConns, c.Migration.MaxTargetConnections)
	markInt(provenanceMigrationWriteAheadWriters, c.Migration.WriteAheadWriters)
	markInt(provenanceMigrationParallelReaders, c.Migration.ParallelReaders)
	markInt(provenanceMigrationSampleSize, c.Migration.SampleSize)
	markInt(provenanceMigrationCheckpointFrequency, c.Migration.CheckpointFrequency)
	markInt(provenanceMigrationMaxRetries, c.Migration.MaxRetries)
	markInt64("migration.max_memory_mb", c.Migration.MaxMemoryMB)
}

func (c *Config) fillDefaultTunableProvenance() {
	markInt := func(name string, value int, source ConfigValueProvenance) {
		if value != 0 {
			c.setTunableProvenanceIfUnset(name, source)
		}
	}
	markInt64 := func(name string, value int64, source ConfigValueProvenance) {
		if value != 0 {
			c.setTunableProvenanceIfUnset(name, source)
		}
	}

	markInt(provenanceMigrationWorkers, c.Migration.Workers, ProvenanceAutoDefault)
	markInt(provenanceMigrationChunkSize, c.Migration.ChunkSize, ProvenanceAutoDefault)
	markInt(provenanceMigrationReadAheadBuffers, c.Migration.ReadAheadBuffers, ProvenanceAutoDefault)
	markInt(provenanceMigrationMaxPartitions, c.Migration.MaxPartitions, ProvenanceAutoDefault)
	markInt(provenanceMigrationMaxSourceConns, c.Migration.MaxSourceConnections, ProvenanceAutoDefault)
	markInt(provenanceMigrationMaxTargetConns, c.Migration.MaxTargetConnections, ProvenanceAutoDefault)
	markInt(provenanceMigrationWriteAheadWriters, c.Migration.WriteAheadWriters, ProvenanceDriverDefault)
	markInt(provenanceMigrationParallelReaders, c.Migration.ParallelReaders, ProvenanceAutoDefault)
	markInt64(provenanceMigrationLargeTableThreshold, c.Migration.LargeTableThreshold, ProvenanceAutoDefault)
	markInt(provenanceMigrationSampleSize, c.Migration.SampleSize, ProvenanceAutoDefault)
	markInt(provenanceMigrationUpsertMergeChunkSize, c.Migration.UpsertMergeChunkSize, ProvenanceAutoDefault)
	markInt(provenanceMigrationCheckpointFrequency, c.Migration.CheckpointFrequency, ProvenanceAutoDefault)
	markInt(provenanceMigrationMaxRetries, c.Migration.MaxRetries, ProvenanceAutoDefault)
	markInt(provenanceSourceChunkSize, c.Source.ChunkSize, ProvenanceAutoDefault)
	markInt(provenanceTargetChunkSize, c.Target.ChunkSize, ProvenanceAutoDefault)
}

// TunableWriteAheadWriters is the exported provenance name for the
// write_ahead_writers knob, for callers (the orchestrator) that need to
// ask whether a specific tunable is pinned (#461).
const (
	TunableWriteAheadWriters = provenanceMigrationWriteAheadWriters
	TunableParallelReaders   = provenanceMigrationParallelReaders
	TunableReadAheadBuffers  = provenanceMigrationReadAheadBuffers
)

// PinnedTunables returns the provenance names of tunables pinned by the
// user config or secrets defaults — the values the tuner will never
// adjust (#461). Sorted for stable output.
func (c *Config) PinnedTunables() []string {
	var pinned []string
	for name, src := range c.autoConfig.TunableValueProvenance {
		if src == ProvenanceUserConfig || src == ProvenanceSecretsDefault {
			pinned = append(pinned, name)
		}
	}
	sort.Strings(pinned)
	return pinned
}

// TuningProvenanceSummary renders one line of who-owns-what for the
// tunable parameters (#461): values pinned by the user or secrets
// defaults (which the tuner will never override) versus values the
// deterministic tuner derived. Logged at run start so pin-rot is
// visible — a pinned knob silently disables tuning for that parameter.
func (c *Config) TuningProvenanceSummary() string {
	type tunable struct {
		name  string
		value int64
	}
	tunables := []tunable{
		{provenanceMigrationWorkers, int64(c.Migration.Workers)},
		{provenanceMigrationChunkSize, int64(c.Migration.ChunkSize)},
		{provenanceMigrationWriteAheadWriters, int64(c.Migration.WriteAheadWriters)},
		{provenanceMigrationParallelReaders, int64(c.Migration.ParallelReaders)},
		{provenanceMigrationReadAheadBuffers, int64(c.Migration.ReadAheadBuffers)},
		{provenanceMigrationMaxPartitions, int64(c.Migration.MaxPartitions)},
		{provenanceMigrationMaxSourceConns, int64(c.Migration.MaxSourceConnections)},
		{provenanceMigrationMaxTargetConns, int64(c.Migration.MaxTargetConnections)},
		{provenanceMigrationLargeTableThreshold, c.Migration.LargeTableThreshold},
	}

	var pinned, derived, defaults []string
	// max_memory_mb has no provenance entry (it's a cap, not a tuned
	// value) but a set cap clamps every derived recommendation — surface
	// it as pinned so the line explains why the tuner chose smaller
	// values (codex review on #461).
	if c.Migration.MaxMemoryMB > 0 {
		pinned = append(pinned, fmt.Sprintf("max_memory_mb=%d", c.Migration.MaxMemoryMB))
	}
	for _, t := range tunables {
		entry := fmt.Sprintf("%s=%d", strings.TrimPrefix(t.name, "migration."), t.value)
		switch c.tunableProvenance(t.name) {
		case ProvenanceUserConfig, ProvenanceSecretsDefault:
			pinned = append(pinned, entry)
		case ProvenanceSmartConfig, ProvenanceRuntimeControl:
			derived = append(derived, entry)
		default:
			defaults = append(defaults, entry)
		}
	}

	var parts []string
	if len(pinned) > 0 {
		parts = append(parts, "pinned (tuner will not adjust): "+strings.Join(pinned, ", "))
	}
	if len(derived) > 0 {
		parts = append(parts, "tuner-derived: "+strings.Join(derived, ", "))
	}
	if len(defaults) > 0 {
		parts = append(parts, "defaults: "+strings.Join(defaults, ", "))
	}
	if len(parts) == 0 {
		return "no tunable parameters resolved"
	}
	return strings.Join(parts, " | ")
}
