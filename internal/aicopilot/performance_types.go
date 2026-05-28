package aicopilot

import "time"

const PerformancePromptVersion = "ai-performance-explanation-v1"

type PerformanceWorkloadSummary struct {
	SourceDBType          string `json:"source_db_type,omitempty"`
	TargetDBType          string `json:"target_db_type,omitempty"`
	TargetMode            string `json:"target_mode,omitempty"`
	Platform              string `json:"platform,omitempty"`
	CPUCores              int    `json:"cpu_cores,omitempty"`
	MemoryGB              int    `json:"memory_gb,omitempty"`
	AvailableMemoryMB     int64  `json:"available_memory_mb,omitempty"`
	MaxMemoryMB           int64  `json:"max_memory_mb,omitempty"`
	TotalTables           int    `json:"total_tables,omitempty"`
	TotalRows             int64  `json:"total_rows,omitempty"`
	AvgRowBytes           int64  `json:"avg_row_bytes,omitempty"`
	UncappedAvgRowBytes   int64  `json:"uncapped_avg_row_bytes,omitempty"`
	LargestTableBytes     int64  `json:"largest_table_bytes,omitempty"`
	EstimatedMemoryMB     int64  `json:"estimated_memory_mb,omitempty"`
	SourceIdentityOmitted bool   `json:"source_identity_omitted"`
	TargetIdentityOmitted bool   `json:"target_identity_omitted"`
}

type PerformanceKnobs struct {
	Workers              int   `json:"workers,omitempty"`
	ChunkSize            int   `json:"chunk_size,omitempty"`
	ReadAheadBuffers     int   `json:"read_ahead_buffers,omitempty"`
	WriteAheadWriters    int   `json:"write_ahead_writers,omitempty"`
	ParallelReaders      int   `json:"parallel_readers,omitempty"`
	MaxPartitions        int   `json:"max_partitions,omitempty"`
	LargeTableThreshold  int64 `json:"large_table_threshold,omitempty"`
	MaxSourceConnections int   `json:"max_source_connections,omitempty"`
	MaxTargetConnections int   `json:"max_target_connections,omitempty"`
	UpsertMergeChunkSize int   `json:"upsert_merge_chunk_size,omitempty"`
	CheckpointFrequency  int   `json:"checkpoint_frequency,omitempty"`
	MaxRetries           int   `json:"max_retries,omitempty"`
}

type PerformanceHistoryRun struct {
	Timestamp            string                     `json:"timestamp,omitempty"`
	SourceDBType         string                     `json:"source_db_type,omitempty"`
	TargetDBType         string                     `json:"target_db_type,omitempty"`
	Workload             PerformanceWorkloadSummary `json:"workload"`
	Knobs                PerformanceKnobs           `json:"knobs"`
	FinalThroughput      float64                    `json:"final_throughput,omitempty"`
	FinalDurationSeconds float64                    `json:"final_duration_seconds,omitempty"`
	ChunkRetryCount      int                        `json:"chunk_retry_count,omitempty"`
	Reasoning            string                     `json:"reasoning,omitempty"`
}

type RuntimeAdjustmentSummary struct {
	Action           string         `json:"action,omitempty"`
	Adjustments      map[string]int `json:"adjustments,omitempty"`
	ThroughputBefore float64        `json:"throughput_before,omitempty"`
	ThroughputAfter  float64        `json:"throughput_after,omitempty"`
	EffectPercent    float64        `json:"effect_percent,omitempty"`
	Reasoning        string         `json:"reasoning,omitempty"`
}

type PerformancePayload struct {
	PromptVersion          string                     `json:"prompt_version"`
	Task                   string                     `json:"task"`
	Workload               PerformanceWorkloadSummary `json:"workload"`
	DeterministicTier      string                     `json:"deterministic_tier,omitempty"`
	DeterministicReasoning string                     `json:"deterministic_reasoning,omitempty"`
	DeterministicKnobs     PerformanceKnobs           `json:"deterministic_knobs"`
	RecentRuns             []PerformanceHistoryRun    `json:"recent_runs,omitempty"`
	RuntimeAdjustments     []RuntimeAdjustmentSummary `json:"runtime_adjustments,omitempty"`
	AllowedKnobs           []string                   `json:"allowed_knobs"`
	Redaction              RedactionSummary           `json:"redaction"`
}

type PerformanceFinding struct {
	Knob       string   `json:"knob"`
	Category   string   `json:"category,omitempty"`
	Rationale  string   `json:"rationale"`
	Evidence   []string `json:"evidence,omitempty"`
	NextAction string   `json:"next_action,omitempty"`
	Source     string   `json:"source"`
}

type PerformanceExplanation struct {
	Enabled       bool                 `json:"enabled"`
	Status        string               `json:"status"`
	Provider      string               `json:"provider,omitempty"`
	Model         string               `json:"model,omitempty"`
	PromptVersion string               `json:"prompt_version"`
	Summary       string               `json:"summary"`
	Findings      []PerformanceFinding `json:"findings,omitempty"`
	Notes         []string             `json:"notes,omitempty"`
	Error         string               `json:"error,omitempty"`
}

func formatPerformanceTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
