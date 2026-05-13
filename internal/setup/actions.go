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

// WriteSlackSecret persists the wizard's Slack webhook URL to the global
// secrets file. Empty string explicitly clears the value (see
// secrets.SaveSlackWebhook for why this can't go through secrets.Save).
func (s *State) WriteSlackSecret() error {
	return secrets.SaveSlackWebhook(s.SlackWebhook)
}

// LoadExistingSlackWebhook returns the webhook URL currently stored in
// the global secrets file, or "" if none is set or the file can't be
// read. Callers seed State.SlackWebhook with this before entering the
// wizard so edit mode shows the current value as a hit-Enter-to-keep
// default.
func LoadExistingSlackWebhook() string {
	secrets.Reset()
	cfg, err := secrets.Load()
	if err != nil {
		return ""
	}
	return cfg.Notifications.Slack.WebhookURL
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
// Uses secrets.Reset() + secrets.Load() for full validation including
// file permission checks and config validation.
func CheckExistingSecrets() string {
	secrets.Reset()
	cfg, err := secrets.Load()
	if err != nil {
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
