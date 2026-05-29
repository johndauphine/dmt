package aicopilot

import "context"

const PreflightPromptVersion = "ai-preflight-review-v1"

const (
	ReviewStatusOK          = "ok"
	ReviewStatusUnavailable = "unavailable"
	ReviewStatusError       = "error"

	ReadinessReady     = "ready"
	ReadinessAttention = "attention"
	ReadinessBlocked   = "blocked"
	ReadinessUnknown   = "unknown"
)

// TextClient is the narrow provider interface the copilot layer needs.
type TextClient interface {
	CallAI(ctx context.Context, prompt string) (string, error)
	ProviderName() string
	Model() string
}

type EndpointSummary struct {
	Type       string `json:"type"`
	Schema     string `json:"schema,omitempty"`
	Auth       string `json:"auth,omitempty"`
	SSLMode    string `json:"ssl_mode,omitempty"`
	Encrypt    string `json:"encrypt,omitempty"`
	PacketSize int    `json:"packet_size,omitempty"`
}

type SchemaContractSummary struct {
	Enabled  bool   `json:"enabled"`
	Tables   string `json:"tables,omitempty"`
	Columns  string `json:"columns,omitempty"`
	DataType string `json:"data_type,omitempty"`
}

type SchemaEvolutionSummary struct {
	Enabled           bool   `json:"enabled"`
	AddedColumn       string `json:"added_column"`
	NullabilityChange string `json:"nullability_change"`
	TypeChange        string `json:"type_change"`
}

type DeleteSummary struct {
	Mode              string `json:"mode"`
	TargetBehavior    string `json:"target_behavior,omitempty"`
	ReconcileSchedule string `json:"reconcile_schedule,omitempty"`
	ReconcileInterval string `json:"reconcile_interval,omitempty"`
	BatchSize         int    `json:"batch_size,omitempty"`
	RequirePrimaryKey bool   `json:"require_primary_key,omitempty"`
}

type ValidationSummary struct {
	Mode           string  `json:"mode"`
	SampleRows     int     `json:"sample_rows,omitempty"`
	SamplePercent  float64 `json:"sample_rows_percent,omitempty"`
	FailOnMismatch bool    `json:"fail_on_mismatch"`
	Timeout        string  `json:"timeout,omitempty"`
	MaxParallel    int     `json:"max_parallel,omitempty"`
}

type CheckpointSummary struct {
	FrequencyChunks      int `json:"frequency_chunks"`
	MaxRetries           int `json:"max_retries"`
	HistoryRetentionDays int `json:"history_retention_days"`
}

type NotificationSummary struct {
	SlackConfigured bool `json:"slack_configured"`
	SlackEnabled    bool `json:"slack_enabled"`
	OnSuccess       bool `json:"on_success"`
	OnFailure       bool `json:"on_failure"`
}

type SecretsPlacementSummary struct {
	SecretsFile      string   `json:"secrets_file"`
	KeepOutOfConfig  []string `json:"keep_out_of_config"`
	ConfigMayContain []string `json:"config_may_contain"`
}

type MigrationSummary struct {
	TargetMode             string                 `json:"target_mode"`
	Workers                int                    `json:"workers"`
	ChunkSize              int                    `json:"chunk_size"`
	ReadAheadBuffers       int                    `json:"read_ahead_buffers"`
	WriteAheadWriters      int                    `json:"write_ahead_writers"`
	ParallelReaders        int                    `json:"parallel_readers"`
	MaxSourceConnections   int                    `json:"max_source_connections"`
	MaxTargetConnections   int                    `json:"max_target_connections"`
	MaxPartitions          int                    `json:"max_partitions"`
	LargeTableThreshold    int64                  `json:"large_table_threshold"`
	StrictConsistency      bool                   `json:"strict_consistency"`
	CreateIndexes          bool                   `json:"create_indexes"`
	CreateForeignKeys      bool                   `json:"create_foreign_keys"`
	CreateCheckConstraints bool                   `json:"create_check_constraints"`
	AllowPartial           bool                   `json:"allow_partial"`
	UnmappedTypeAction     string                 `json:"unmapped_type_action,omitempty"`
	ApproxTypeAction       string                 `json:"approx_type_action,omitempty"`
	SchemaContract         SchemaContractSummary  `json:"schema_contract"`
	SchemaEvolution        SchemaEvolutionSummary `json:"schema_evolution"`
	Deletes                DeleteSummary          `json:"deletes"`
	Validation             ValidationSummary      `json:"validation"`
	Checkpoint             CheckpointSummary      `json:"checkpoint"`
	Notifications          NotificationSummary    `json:"notifications"`
}

type ConfigSummary struct {
	Source           EndpointSummary         `json:"source"`
	Target           EndpointSummary         `json:"target"`
	Migration        MigrationSummary        `json:"migration"`
	SecretsPlacement SecretsPlacementSummary `json:"secrets_placement"`
}

type HealthSummary struct {
	Timestamp        string `json:"timestamp"`
	SourceConnected  bool   `json:"source_connected"`
	SourceLatencyMs  int64  `json:"source_latency_ms"`
	SourceDBType     string `json:"source_db_type"`
	SourceTableCount int    `json:"source_table_count,omitempty"`
	SourceError      string `json:"source_error,omitempty"`
	TargetConnected  bool   `json:"target_connected"`
	TargetLatencyMs  int64  `json:"target_latency_ms"`
	TargetDBType     string `json:"target_db_type"`
	TargetError      string `json:"target_error,omitempty"`
	Healthy          bool   `json:"healthy"`
}

type PreflightFinding struct {
	Severity string `json:"severity"`
	Check    string `json:"check"`
	Side     string `json:"side"`
	Message  string `json:"message"`
	Remedy   string `json:"remedy,omitempty"`
}

type RunHistorySummary struct {
	Timestamp         string  `json:"timestamp"`
	TotalTables       int     `json:"total_tables"`
	TotalRows         int64   `json:"total_rows"`
	AvgRowSizeBytes   int64   `json:"avg_row_size_bytes"`
	Workers           int     `json:"workers"`
	ChunkSize         int     `json:"chunk_size"`
	WriteAheadWriters int     `json:"write_ahead_writers"`
	ParallelReaders   int     `json:"parallel_readers"`
	FinalThroughput   float64 `json:"final_throughput,omitempty"`
	FinalDurationSecs float64 `json:"final_duration_seconds,omitempty"`
	ChunkRetryCount   int     `json:"chunk_retry_count,omitempty"`
}

type RedactionSummary struct {
	OmittedFields []string `json:"omitted_fields"`
	ScrubbedText  bool     `json:"scrubbed_text"`
}

type PreflightPayload struct {
	PromptVersion         string              `json:"prompt_version"`
	Task                  string              `json:"task"`
	Config                ConfigSummary       `json:"config"`
	Health                HealthSummary       `json:"health"`
	PreflightFindings     []PreflightFinding  `json:"preflight_findings"`
	DeterministicBlockers []string            `json:"deterministic_blockers,omitempty"`
	RecentRuns            []RunHistorySummary `json:"recent_runs,omitempty"`
	Redaction             RedactionSummary    `json:"redaction"`
}

type ReviewFinding struct {
	Severity   string `json:"severity"`
	Category   string `json:"category"`
	Affected   string `json:"affected,omitempty"`
	Rationale  string `json:"rationale"`
	NextAction string `json:"next_action"`
	Source     string `json:"source"`
}

type PreflightReview struct {
	Enabled               bool            `json:"enabled"`
	Status                string          `json:"status"`
	Provider              string          `json:"provider,omitempty"`
	Model                 string          `json:"model,omitempty"`
	PromptVersion         string          `json:"prompt_version"`
	Readiness             string          `json:"readiness"`
	Summary               string          `json:"summary"`
	DeterministicBlockers []string        `json:"deterministic_blockers,omitempty"`
	Findings              []ReviewFinding `json:"findings,omitempty"`
	Notes                 []string        `json:"notes,omitempty"`
	Error                 string          `json:"error,omitempty"`
}
