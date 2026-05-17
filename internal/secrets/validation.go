package secrets

import "fmt"

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

// GetMigrationDefaults returns the global migration defaults with smart defaults applied.
// AI adjust is enabled by default when an AI provider is configured.
func (c *Config) GetMigrationDefaults() *MigrationDefaults {
	defaults := c.MigrationDefaults

	// Apply smart defaults for runtime tuning (#211 rename):
	// If neither runtime_tuning nor the legacy ai_adjust is set,
	// enable it by default. Pre-#172 this was gated on an AI provider
	// being configured (the AI runtime monitor needed an LLM); post-#172
	// the rule-based controller runs without any AI dependency, so the
	// default-enable is unconditional.
	//
	// We check both fields here because the secrets file may carry
	// either name during the deprecation cycle; either being set
	// counts as "user intent recorded, don't auto-enable."
	if defaults.RuntimeTuning == nil && defaults.AIAdjust == nil &&
		defaults.RuntimeTuningInterval == "" && defaults.AIAdjustInterval == "" {
		enabled := true
		defaults.RuntimeTuning = &enabled
		// 5s tick is appropriate for the rule-based controller — near-
		// zero per-tick cost (no LLM round-trip) so we can poll at
		// fine resolution. Gives 3-tick rules (queue growth, throughput
		// stability) ~15s of accumulated history, which fits within
		// short migration runtimes (18-27s for SO2010). Pre-#172 the
		// default was 30s, gated on AI-call latency budget.
		defaults.RuntimeTuningInterval = "5s"
	}

	return &defaults
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

// GetEffectiveMaxTokens returns the max output tokens for a provider.
// Returns the configured value if set, otherwise returns a default based on provider type.
// Local providers default to 16000 (reasoning models need headroom for thinking + output).
// Cloud providers default to 4000.
func (p *Provider) GetEffectiveMaxTokens(providerName string) int {
	if p.MaxTokens > 0 {
		return p.MaxTokens
	}
	// Treat as local if it's a known local provider OR has a base_url without an API key
	// (custom OpenAI-compatible local providers)
	if IsLocalProvider(providerName) || (p.BaseURL != "" && p.APIKey == "") {
		return 16000
	}
	return 4000
}

// IsLocalProvider returns true if the provider is a local provider (no API key needed)
func IsLocalProvider(name string) bool {
	if known, ok := KnownProviders[name]; ok {
		return known.Type == ProviderTypeLocal
	}
	return false
}
