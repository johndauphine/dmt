package orchestrator

import (
	"runtime"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/tuning"
	"github.com/shirou/gopsutil/v3/mem"
)

// GetSystemBasedSuggestions returns tuning suggestions based only on system resources.
// Use this when no database connections are available. Tries AI first, falls back to defaults.
func GetSystemBasedSuggestions(cfg *config.Config) *driver.SmartConfigSuggestions {
	suggestions := &driver.SmartConfigSuggestions{}

	cores := runtime.NumCPU()
	memGB := 8 // default assumption
	availableMemoryMB := int64(memGB) * 1024

	// Get memory info if available
	if v, err := mem.VirtualMemory(); err == nil {
		memGB = int(v.Total / (1024 * 1024 * 1024))
		availableMemoryMB = int64(v.Available / (1024 * 1024))
	}

	// Build a tuning.Input directly — offline path has no source schema, so
	// AvgRowBytes defaults to 500 (mirrors calculateAvgRowSize's fallback
	// when no row stats are available, #157). The deterministic tuner
	// handles the rest, including the per-target chunk-byte anchor (#166).
	in := tuning.Input{
		CPUCores:           cores,
		MemoryGB:           memGB,
		AvailableMemoryMB:  availableMemoryMB,
		MaxMemoryMB:        cfg.Migration.MaxMemoryMB,
		Platform:           driver.DetectPlatform(),
		SourceDBType:       cfg.Source.Type,
		TargetDBType:       cfg.Target.Type,
		TargetMode:         cfg.Migration.TargetMode,
		AvgRowBytes:        500,
		ForceExplore:       cfg.Migration.Explore,
		ExplorationEpsilon: driver.ExplorationEpsilon(cfg.Migration.ExploreMode),
	}
	profile := tuning.DriverProfile{Name: cfg.Target.Type, BaselineWAW: 2}
	if d, err := driver.Get(cfg.Target.Type); err == nil {
		defaults := d.Defaults()
		profile.BaselineWAW = defaults.WriteAheadWriters
		profile.ScaleWritersWithCores = defaults.ScaleWritersWithCores
		profile.OptimumBulkChunkBytes = defaults.OptimumBulkChunkBytes
		profile.HardChunkLimit = d.HardChunkLimit(in.AvgRowBytes)
	}
	out := tuning.Tune(in, profile, nil, tuning.DBTuning{})

	suggestions.Workers = out.Workers
	suggestions.ChunkSizeRecommendation = out.ChunkSize
	suggestions.ReadAheadBuffers = out.ReadAheadBuffers
	suggestions.WriteAheadWriters = out.WriteAheadWriters
	suggestions.ParallelReaders = out.ParallelReaders
	suggestions.MaxPartitions = out.MaxPartitions
	suggestions.LargeTableThreshold = out.LargeTableThreshold
	suggestions.MaxSourceConnections = out.MaxSourceConnections
	suggestions.MaxTargetConnections = out.MaxTargetConnections
	suggestions.UpsertMergeChunkSize = out.UpsertMergeChunkSize
	suggestions.CheckpointFrequency = out.CheckpointFrequency
	suggestions.MaxRetries = out.MaxRetries
	suggestions.EstimatedMemMB = out.EstimatedMemMB
	suggestions.Reasoning = out.Reasoning
	return suggestions
}
