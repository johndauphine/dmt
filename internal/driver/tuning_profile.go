package driver

import "github.com/johndauphine/dmt/internal/tuning"

// BuildTuningProfile builds the tuner's target-specific policy from the
// registered driver. A zero-valued probe means no live target information is
// available; in that case the driver's static hard chunk limit is retained.
// Unknown targets preserve the historical conservative fallback used by both
// schema-aware and offline tuning.
func BuildTuningProfile(targetType string, estimatedRowBytes int64, probe TargetProbe) tuning.DriverProfile {
	profile := tuning.DriverProfile{Name: targetType, BaselineWAW: 2}
	d, err := Get(targetType)
	if err != nil {
		return profile
	}

	defaults := d.Defaults()
	profile.BaselineWAW = defaults.WriteAheadWriters
	profile.ScaleWritersWithCores = defaults.ScaleWritersWithCores
	profile.OptimumBulkChunkBytes = defaults.OptimumBulkChunkBytes
	profile.HardChunkLimit = chunkLimitFromProbe(d.HardChunkLimit(estimatedRowBytes), probe, estimatedRowBytes)
	return profile
}
