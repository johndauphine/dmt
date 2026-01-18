// Package calibration provides performance calibration through mini-migrations.
package calibration

// CalibrationConfig defines a single configuration to test during calibration.
// These are base configurations - AI analyzes results and recommends optimal
// settings considering database-specific parameters.
type CalibrationConfig struct {
	Name             string `json:"name"`
	ChunkSize        int    `json:"chunk_size"`
	Workers          int    `json:"workers"`
	ReadAheadBuffers int    `json:"read_ahead_buffers"`
	ParallelReaders  int    `json:"parallel_readers"`
	// Extended parameters (for AI-guided calibration)
	WriteAheadWriters    int `json:"write_ahead_writers,omitempty"`
	MaxPartitions        int `json:"max_partitions,omitempty"`
	SourceChunkSize      int `json:"source_chunk_size,omitempty"` // Batch size for reading
	TargetChunkSize      int `json:"target_chunk_size,omitempty"` // Batch size for writing
	UpsertMergeChunkSize int `json:"upsert_merge_chunk_size,omitempty"`
}

// CalibrationDepth represents how thorough the calibration should be.
type CalibrationDepth string

const (
	DepthQuick    CalibrationDepth = "quick"    // 5 configs, ~2-5 minutes
	DepthStandard CalibrationDepth = "standard" // 8-12 configs, ~5-10 minutes
	DepthThorough CalibrationDepth = "thorough" // 20+ configs, ~15-30 minutes
)

// QuickConfigs returns the 5 preset configurations for quick calibration.
// These cover the key parameter space efficiently:
// - chunk_size: most impactful parameter (25K to 200K)
// - workers: parallelism (4 to 10)
// - read_ahead_buffers: pipelining (4 to 16)
// AI will analyze results and recommend optimal values including database-specific
// parameters like write_ahead_writers based on the driver characteristics.
func QuickConfigs() []CalibrationConfig {
	return []CalibrationConfig{
		{
			Name:             "small_conservative",
			ChunkSize:        25000,
			Workers:          4,
			ReadAheadBuffers: 4,
			ParallelReaders:  2,
		},
		{
			Name:             "medium_balanced",
			ChunkSize:        75000,
			Workers:          6,
			ReadAheadBuffers: 8,
			ParallelReaders:  2,
		},
		{
			Name:             "large_aggressive",
			ChunkSize:        150000,
			Workers:          8,
			ReadAheadBuffers: 12,
			ParallelReaders:  3,
		},
		{
			Name:             "chunk_focus",
			ChunkSize:        200000,
			Workers:          6,
			ReadAheadBuffers: 8,
			ParallelReaders:  2,
		},
		{
			Name:             "parallel_focus",
			ChunkSize:        100000,
			Workers:          10,
			ReadAheadBuffers: 16,
			ParallelReaders:  4,
		},
	}
}

// GetConfigs returns configurations based on the specified depth.
func GetConfigs(depth CalibrationDepth) []CalibrationConfig {
	switch depth {
	case DepthQuick:
		return QuickConfigs()
	case DepthStandard:
		// TODO: Implement standard depth with more configurations
		return QuickConfigs()
	case DepthThorough:
		// TODO: Implement thorough depth with comprehensive grid search
		return QuickConfigs()
	default:
		return QuickConfigs()
	}
}
