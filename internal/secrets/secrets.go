// Package secrets provides secure configuration loading for API keys and encryption keys.
package secrets

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
)

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
	AI                AIConfig              `yaml:"ai"`
	Encryption        EncryptionConfig      `yaml:"encryption"`
	Notifications     NotificationsConfig   `yaml:"notifications"`
	MigrationDefaults MigrationDefaults     `yaml:"migration_defaults"`
}

// MigrationDefaults holds global default settings for migrations.
// These can be overridden in individual migration config files.
type MigrationDefaults struct {
	// Performance settings (machine-dependent)
	Workers           int   `yaml:"workers,omitempty"`             // Number of parallel workers (default: auto based on CPU)
	MaxConnections    int   `yaml:"max_connections,omitempty"`     // Max total DB connections
	MaxMemoryMB       int64 `yaml:"max_memory_mb,omitempty"`       // Max memory usage in MB
	ReadAheadBuffers  int   `yaml:"read_ahead_buffers,omitempty"`  // Chunks to buffer ahead
	WriteAheadWriters int   `yaml:"write_ahead_writers,omitempty"` // Parallel writers per job
	ParallelReaders   int   `yaml:"parallel_readers,omitempty"`    // Parallel readers per job

	// Schema creation defaults
	CreateIndexes          bool `yaml:"create_indexes"`           // Create non-PK indexes (default: true)
	CreateForeignKeys      bool `yaml:"create_foreign_keys"`      // Create FK constraints (default: true)
	CreateCheckConstraints bool `yaml:"create_check_constraints"` // Create CHECK constraints (default: false)

	// Consistency and validation
	StrictConsistency bool `yaml:"strict_consistency"` // Use table locks instead of NOLOCK
	SampleValidation  bool `yaml:"sample_validation"`  // Enable sample data validation
	SampleSize        int  `yaml:"sample_size"`        // Rows to sample for validation

	// Checkpoint and recovery
	CheckpointFrequency  int `yaml:"checkpoint_frequency"`   // Save progress every N chunks
	MaxRetries           int `yaml:"max_retries"`            // Retry failed tables N times
	HistoryRetentionDays int `yaml:"history_retention_days"` // Keep run history for N days

	// AI features (enabled by default when AI provider is configured)
	AIAdjust         bool   `yaml:"ai_adjust"`          // Enable AI-driven parameter adjustment (default: true)
	AIAdjustInterval string `yaml:"ai_adjust_interval"` // How often AI evaluates metrics (default: 30s)

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
	APIKey        string `yaml:"api_key,omitempty"`        // Required for cloud providers
	BaseURL       string `yaml:"base_url,omitempty"`       // Required for local providers, optional for cloud
	Model         string `yaml:"model,omitempty"`          // Optional, uses smart defaults
	ContextWindow int    `yaml:"context_window,omitempty"` // Optional, context window size in tokens (for Ollama/local providers)
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
	"claude":   {ProviderTypeCloud, "https://api.anthropic.com"},
	"openai":   {ProviderTypeCloud, "https://api.openai.com"},
	"gemini":   {ProviderTypeCloud, "https://generativelanguage.googleapis.com"},
	"ollama":   {ProviderTypeLocal, "http://localhost:11434"},
	"lmstudio": {ProviderTypeLocal, "http://localhost:1234"},
}

// DefaultModels maps providers to their default models
var DefaultModels = map[string]string{
	"claude":   "claude-sonnet-4-20250514",
	"openai":   "gpt-4o",
	"gemini":   "gemini-2.0-flash",
	"ollama":   "llama3",
	"lmstudio": "local-model",
}

var (
	globalConfig *Config
	configOnce   sync.Once
	configErr    error
)

// Load loads the secrets configuration from the default or override location.
// It caches the result and returns the same config on subsequent calls.
func Load() (*Config, error) {
	configOnce.Do(func() {
		globalConfig, configErr = loadFromFile()
	})
	return globalConfig, configErr
}

// Reset clears the cached config (useful for testing)
func Reset() {
	configOnce = sync.Once{}
	globalConfig = nil
	configErr = nil
}

// GetSecretsPath returns the path to the secrets file
func GetSecretsPath() string {
	if envPath := os.Getenv(SecretsFileEnvVar); envPath != "" {
		return envPath
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", DefaultSecretsDir, DefaultSecretsFile)
	}
	return filepath.Join(homeDir, DefaultSecretsDir, DefaultSecretsFile)
}

// EnsureSecretsDir creates the secrets directory with secure permissions if it doesn't exist
func EnsureSecretsDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %w", err)
	}

	secretsDir := filepath.Join(homeDir, DefaultSecretsDir)

	// Check if directory exists
	info, err := os.Stat(secretsDir)
	if os.IsNotExist(err) {
		// Create directory with secure permissions
		if err := os.MkdirAll(secretsDir, SecureDirMode); err != nil {
			return "", fmt.Errorf("creating secrets directory: %w", err)
		}
		return secretsDir, nil
	} else if err != nil {
		return "", fmt.Errorf("checking secrets directory: %w", err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%s exists but is not a directory", secretsDir)
	}

	return secretsDir, nil
}

func loadFromFile() (*Config, error) {
	path := GetSecretsPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &SecretsNotFoundError{Path: path}
		}
		return nil, fmt.Errorf("reading secrets file: %w", err)
	}

	// Check file permissions - reject if too permissive (security requirement)
	info, err := os.Stat(path)
	if err == nil {
		mode := info.Mode().Perm()
		if mode&0077 != 0 {
			return nil, fmt.Errorf("secrets file %s has insecure permissions (%04o). "+
				"Other users can read your API keys. Run: chmod 600 %s", path, mode, path)
		}
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing secrets file: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	// AI settings are optional - only validate if configured
	if c.AI.DefaultProvider != "" {
		// Check that default provider exists
		provider, ok := c.AI.Providers[c.AI.DefaultProvider]
		if !ok {
			return fmt.Errorf("default provider %q not found in providers", c.AI.DefaultProvider)
		}

		// Validate the default provider has required fields
		if err := validateProvider(c.AI.DefaultProvider, provider); err != nil {
			return err
		}
	}

	return nil
}

func validateProvider(name string, p *Provider) error {
	known, isKnown := KnownProviders[name]

	if isKnown {
		if known.Type == ProviderTypeCloud && p.APIKey == "" {
			return fmt.Errorf("provider %q requires api_key", name)
		}
		if known.Type == ProviderTypeLocal && p.BaseURL == "" {
			// Use default URL for known local providers
			p.BaseURL = known.DefaultURL
		}
	} else {
		// Unknown provider - must have either API key or base URL
		if p.APIKey == "" && p.BaseURL == "" {
			return fmt.Errorf("provider %q requires either api_key or base_url", name)
		}
	}

	return nil
}

// GetDefaultProvider returns the configured default AI provider
func (c *Config) GetDefaultProvider() (*Provider, string, error) {
	if c.AI.DefaultProvider == "" {
		return nil, "", fmt.Errorf("no default provider configured")
	}

	provider, ok := c.AI.Providers[c.AI.DefaultProvider]
	if !ok {
		return nil, "", fmt.Errorf("default provider %q not found", c.AI.DefaultProvider)
	}

	return provider, c.AI.DefaultProvider, nil
}

// GetProvider returns a specific AI provider by name
func (c *Config) GetProvider(name string) (*Provider, error) {
	provider, ok := c.AI.Providers[name]
	if !ok {
		return nil, fmt.Errorf("provider %q not found", name)
	}
	return provider, nil
}

// GetMasterKey returns the encryption master key
func (c *Config) GetMasterKey() string {
	return c.Encryption.MasterKey
}

// GetMigrationDefaults returns the global migration defaults
func (c *Config) GetMigrationDefaults() *MigrationDefaults {
	return &c.MigrationDefaults
}

// GetEffectiveBaseURL returns the base URL for a provider, using defaults if not specified
func (p *Provider) GetEffectiveBaseURL(providerName string) string {
	if p.BaseURL != "" {
		return p.BaseURL
	}
	if known, ok := KnownProviders[providerName]; ok {
		return known.DefaultURL
	}
	return ""
}

// GetEffectiveModel returns the model for a provider, using defaults if not specified
func (p *Provider) GetEffectiveModel(providerName string) string {
	if p.Model != "" {
		return p.Model
	}
	if defaultModel, ok := DefaultModels[providerName]; ok {
		return defaultModel
	}
	return ""
}

// GetEffectiveContextWindow returns the context window size for a provider.
// Returns the configured value if set, otherwise returns a conservative default of 8192 tokens.
// Users should configure this based on their specific model's capabilities:
// - llama3:8b, llama3.2: 8192 tokens
// - llama3:70b, llama3.1: 131072 tokens (128K)
// - qwen2.5, deepseek: 32768 tokens (32K)
// - mistral, mixtral: 8192-32768 tokens (varies by version)
func (p *Provider) GetEffectiveContextWindow() int {
	if p.ContextWindow > 0 {
		return p.ContextWindow
	}
	// Conservative default that works with most models
	return 8192
}

// IsLocalProvider returns true if the provider is a local provider (no API key needed)
func IsLocalProvider(name string) bool {
	if known, ok := KnownProviders[name]; ok {
		return known.Type == ProviderTypeLocal
	}
	return false
}

// SecretsNotFoundError is returned when the secrets file doesn't exist
type SecretsNotFoundError struct {
	Path string
}

func (e *SecretsNotFoundError) Error() string {
	return fmt.Sprintf(`secrets file not found: %s

To create a secrets file, run:
  dmt init-secrets

Or create %s manually with:

ai:
  default_provider: claude
  providers:
    claude:
      api_key: "your-api-key"

encryption:
  master_key: "your-master-key"
`, e.Path, e.Path)
}

// GenerateTemplate returns a template secrets file content
func GenerateTemplate() string {
	return `# DMT Secrets Configuration
# This file contains sensitive configuration that should not be committed to version control.
# Permissions should be restricted: chmod 600 ~/.secrets/dmt-config.yaml

ai:
  default_provider: claude  # Which provider to use by default

  providers:
    # Cloud providers (require API key)
    claude:
      api_key: ""  # Get from https://console.anthropic.com/
      model: "claude-sonnet-4-20250514"  # optional

    openai:
      api_key: ""  # Get from https://platform.openai.com/
      model: "gpt-4o"  # optional

    gemini:
      api_key: ""  # Get from https://makersuite.google.com/
      model: "gemini-2.0-flash"  # optional

    # Local providers (no API key needed)
    ollama:
      base_url: "http://localhost:11434"
      model: "llama3"
      # context_window: 8192  # optional, defaults to 8192 (conservative)
      # Common values:
      # - llama3:8b, llama3.2: 8192
      # - llama3:70b, llama3.1: 131072 (128K)
      # - qwen2.5, deepseek: 32768 (32K)
      # - mistral, mixtral: 8192-32768 (varies)

    lmstudio:
      base_url: "http://localhost:1234/v1"
      model: "local-model"
      # context_window: 8192  # optional, configure based on your model

encryption:
  master_key: ""  # Used for encrypting profiles, generate with: openssl rand -base64 32

notifications:
  slack:
    webhook_url: ""  # Slack webhook URL for migration notifications

# Global migration defaults (can be overridden per-migration)
migration_defaults:
  # Performance settings (auto-tuned if not set)
  # workers: 4                    # Parallel workers (default: based on CPU cores)
  # max_memory_mb: 0              # Max memory in MB (default: 70% of available)
  # read_ahead_buffers: 8         # Chunks to buffer ahead
  # write_ahead_writers: 2        # Parallel writers per job
  # parallel_readers: 2           # Parallel readers per job

  # Schema creation defaults
  create_indexes: true            # Create non-PK indexes
  create_foreign_keys: true       # Create FK constraints
  create_check_constraints: false # Create CHECK constraints

  # Consistency and validation
  strict_consistency: false       # Use table locks instead of NOLOCK/MVCC
  sample_validation: false        # Validate sample data after migration
  sample_size: 100                # Rows to sample for validation

  # Checkpoint and recovery
  checkpoint_frequency: 10        # Save progress every N chunks
  max_retries: 3                  # Retry failed tables N times
  history_retention_days: 30      # Keep run history for N days

  # AI features (enabled by default when AI provider is configured)
  ai_adjust: true                 # Enable AI-driven parameter adjustment
  ai_adjust_interval: "30s"       # How often AI evaluates metrics
`
}
