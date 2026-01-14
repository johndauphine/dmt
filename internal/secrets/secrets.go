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
	AI            AIConfig              `yaml:"ai"`
	Encryption    EncryptionConfig      `yaml:"encryption"`
	Notifications NotificationsConfig   `yaml:"notifications"`
}

// AIConfig holds AI provider configuration
type AIConfig struct {
	DefaultProvider string               `yaml:"default_provider"`
	Providers       map[string]*Provider `yaml:"providers"`
}

// Provider represents an AI provider configuration
type Provider struct {
	APIKey  string `yaml:"api_key,omitempty"`  // Required for cloud providers
	BaseURL string `yaml:"base_url,omitempty"` // Required for local providers, optional for cloud
	Model   string `yaml:"model,omitempty"`    // Optional, uses smart defaults
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
	if c.AI.DefaultProvider == "" {
		return fmt.Errorf("ai.default_provider is required")
	}

	// Check that default provider exists
	provider, ok := c.AI.Providers[c.AI.DefaultProvider]
	if !ok {
		return fmt.Errorf("default provider %q not found in providers", c.AI.DefaultProvider)
	}

	// Validate the default provider has required fields
	return validateProvider(c.AI.DefaultProvider, provider)
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

    lmstudio:
      base_url: "http://localhost:1234/v1"
      model: "local-model"

encryption:
  master_key: ""  # Used for encrypting profiles, generate with: openssl rand -base64 32

notifications:
  slack:
    webhook_url: ""  # Slack webhook URL for migration notifications
`
}
