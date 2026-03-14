package setup

import (
	"os"

	"github.com/johndauphine/dmt/internal/secrets"
	"gopkg.in/yaml.v3"
)

// WriteSecretsFile writes the AI configuration to the secrets file.
// It uses secrets.Save() which merges into any existing secrets config.
func (s *State) WriteSecretsFile() error {
	provider := &secrets.Provider{}
	if secrets.IsLocalProvider(s.AIProvider) {
		provider.BaseURL = s.AIKey
	} else {
		provider.APIKey = s.AIKey
	}
	if model, ok := secrets.DefaultModels[s.AIProvider]; ok {
		provider.Model = model
	}

	updates := &secrets.Config{
		AI: secrets.AIConfig{
			DefaultProvider: s.AIProvider,
			Providers: map[string]*secrets.Provider{
				s.AIProvider: provider,
			},
		},
	}

	return secrets.Save(updates)
}

// WriteConfigFile marshals the config and writes it to the configured path.
func (s *State) WriteConfigFile() error {
	data, err := yaml.Marshal(&s.Config)
	if err != nil {
		return err
	}
	return os.WriteFile(s.ConfigPath, data, 0600)
}

// CheckExistingSecrets checks if a secrets file with valid AI config exists.
// Returns "has_ai" if a valid AI provider is configured, "no_ai" otherwise.
// Does not use secrets.Load() singleton to avoid caching issues.
func CheckExistingSecrets() string {
	path := secrets.GetSecretsPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return "no_ai"
	}

	var cfg secrets.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "no_ai"
	}

	if cfg.AI.DefaultProvider == "" {
		return "no_ai"
	}

	p, ok := cfg.AI.Providers[cfg.AI.DefaultProvider]
	if !ok || (p.APIKey == "" && p.BaseURL == "") {
		return "no_ai"
	}

	return "has_ai"
}
