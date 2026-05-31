// Package secrets provides secure configuration loading for API keys and encryption keys.
package secrets

const (
	// DefaultSecretsDir is the default directory for secrets
	DefaultSecretsDir = ".secrets"
	// DefaultSecretsFile is the default filename for secrets
	DefaultSecretsFile = "dmt-config.yaml"
	// SecretsFileEnvVar allows overriding the secrets file location
	SecretsFileEnvVar = "DMT_SECRETS_FILE"
	// SecureDirMode is the permission mode for the secrets directory
	SecureDirMode = 0700
	// SecureFileMode is the permission mode for the secrets file
	SecureFileMode = 0600
)

// Config represents the complete secrets configuration
type Config struct {
	AI                AIConfig            `yaml:"ai"`
	Encryption        EncryptionConfig    `yaml:"encryption"`
	Notifications     NotificationsConfig `yaml:"notifications"`
	MigrationDefaults MigrationDefaults   `yaml:"migration_defaults"`
}

// MigrationDefaults holds global default settings for migrations.
// These can be overridden in individual migration config files.
type MigrationDefaults struct {
	// Performance settings (machine-dependent)
	Workers              int   `yaml:"workers,omitempty"`                // Number of parallel workers (default: auto based on CPU)
	MaxSourceConnections int   `yaml:"max_source_connections,omitempty"` // Max source DB connections
	MaxTargetConnections int   `yaml:"max_target_connections,omitempty"` // Max target DB connections
	MaxMemoryMB          int64 `yaml:"max_memory_mb,omitempty"`          // Max memory usage in MB
	ReadAheadBuffers     int   `yaml:"read_ahead_buffers,omitempty"`     // Chunks to buffer ahead
	WriteAheadWriters    int   `yaml:"write_ahead_writers,omitempty"`    // Parallel writers per job
	ParallelReaders      int   `yaml:"parallel_readers,omitempty"`       // Parallel readers per job

	// Schema creation defaults (use *bool to distinguish "not set" from "false")
	CreateIndexes          *bool `yaml:"create_indexes,omitempty"`           // Create non-PK indexes (default: true)
	CreateForeignKeys      *bool `yaml:"create_foreign_keys,omitempty"`      // Create FK constraints (default: true)
	CreateCheckConstraints *bool `yaml:"create_check_constraints,omitempty"` // Create CHECK constraints (default: false)

	// Consistency and validation
	StrictConsistency *bool `yaml:"strict_consistency,omitempty"` // Use table locks instead of NOLOCK
	SampleValidation  *bool `yaml:"sample_validation,omitempty"`  // Enable sample data validation
	SampleSize        int   `yaml:"sample_size,omitempty"`        // Rows to sample for validation

	// Checkpoint and recovery
	CheckpointFrequency  int `yaml:"checkpoint_frequency,omitempty"`   // Save progress every N chunks
	MaxRetries           int `yaml:"max_retries,omitempty"`            // Retry failed tables N times
	HistoryRetentionDays int `yaml:"history_retention_days,omitempty"` // Keep run history for N days

	// Rule-based runtime tuning (#211 rename from `ai_adjust`). The
	// new fields are canonical; the old AIAdjust* fields below are
	// preserved during the deprecation cycle so existing global
	// secrets files keep working unchanged.
	RuntimeTuning         *bool  `yaml:"runtime_tuning,omitempty"`          // Enable rule-based parameter adjustment (default: true)
	RuntimeTuningInterval string `yaml:"runtime_tuning_interval,omitempty"` // How often the controller evaluates metrics (default: 5s)

	// Legacy alias for RuntimeTuning. Renamed in #211; still parsed so
	// existing secrets files keep working. config.applyGlobalDefaults
	// reads this field if RuntimeTuning is unset. Slated for removal
	// in a future release.
	AIAdjust *bool `yaml:"ai_adjust,omitempty"`
	// Legacy alias for RuntimeTuningInterval. See AIAdjust above.
	AIAdjustInterval string `yaml:"ai_adjust_interval,omitempty"`

	// Data directory
	DataDir string `yaml:"data_dir,omitempty"` // Directory for state/checkpoint files
}

// AIConfig holds AI provider configuration
type AIConfig struct {
	DefaultProvider string               `yaml:"default_provider"`
	Providers       map[string]*Provider `yaml:"providers"`
}

// Provider represents an AI provider configuration
type Provider struct {
	APIKey         string `yaml:"api_key,omitempty"`         // Required for cloud providers
	BaseURL        string `yaml:"base_url,omitempty"`        // Required for local providers, optional for cloud
	Model          string `yaml:"model,omitempty"`           // Optional, uses smart defaults
	ContextWindow  int    `yaml:"context_window,omitempty"`  // Optional, context window size in tokens (for Ollama/local providers)
	MaxTokens      int    `yaml:"max_tokens,omitempty"`      // Optional, max output tokens (default: 16000 for local, 4000 for cloud)
	MaxRequests    int    `yaml:"max_requests,omitempty"`    // Optional, max uncached AI provider calls per dmt process/run
	TimeoutSeconds int    `yaml:"timeout_seconds,omitempty"` // Optional, API timeout in seconds (default: 30 for cloud, 120 for local)
}

// EncryptionConfig holds encryption-related secrets
type EncryptionConfig struct {
	MasterKey string `yaml:"master_key"`
}

// NotificationsConfig holds notification service credentials
type NotificationsConfig struct {
	Slack SlackConfig `yaml:"slack"`
}

// SlackConfig holds Slack webhook configuration
type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
}

// ProviderType categorizes providers by their API style
type ProviderType int

const (
	ProviderTypeCloud ProviderType = iota // Requires API key
	ProviderTypeLocal                     // Uses local base_url, no API key
)

// KnownProviders maps provider names to their types and default base URLs
var KnownProviders = map[string]struct {
	Type       ProviderType
	DefaultURL string
}{
	"anthropic": {ProviderTypeCloud, "https://api.anthropic.com"},
	"openai":    {ProviderTypeCloud, "https://api.openai.com"},
	"gemini":    {ProviderTypeCloud, "https://generativelanguage.googleapis.com"},
	"ollama":    {ProviderTypeLocal, "http://localhost:11434"},
	"lmstudio":  {ProviderTypeLocal, "http://localhost:1234"},
}

// DefaultModels maps providers to their default models
var DefaultModels = map[string]string{
	"anthropic": "claude-haiku-4-5-20251001",
	"openai":    "gpt-5.5",
	"gemini":    "gemini-2.0-flash",
	"ollama":    "llama3",
	"lmstudio":  "local-model",
}
