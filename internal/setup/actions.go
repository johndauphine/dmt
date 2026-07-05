package setup

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/johndauphine/dmt/internal/config"
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
// When ExternalizeSecrets is set (the default), DB passwords are moved to 0600
// sidecar files referenced by ${file:…} so no plaintext credential lands in
// the config YAML (#597). Externalization operates on a copy, leaving the
// in-memory State.Config (with literal passwords) intact for any later step.
func (s *State) WriteConfigFile() error {
	cfg := s.Config
	if s.ExternalizeSecrets {
		if err := s.externalizeSecrets(&cfg); err != nil {
			return fmt.Errorf("externalizing secrets: %w", err)
		}
	}
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(s.ConfigPath, data, 0600)
}

// externalizeSecrets writes each non-empty, non-reference DB password to a 0600
// file beside the config and rewrites the field to a ${file:…} reference.
// Passwords already given as a template (${env:…}/${file:…}) are left as-is so
// re-running setup on an externalized config is idempotent.
func (s *State) externalizeSecrets(cfg *config.Config) error {
	// Keep the extension in the sidecar name so same-dir configs that differ
	// only by extension (mig.yaml vs mig.yml) don't share a secret file.
	base := filepath.Base(s.ConfigPath)
	dir := filepath.Join(filepath.Dir(s.ConfigPath), "secrets")

	put := func(side string, pw *string) error {
		// Skip empties and values that are already a resolvable template — use
		// config's own detection so a literal password shaped like "${p@ss}"
		// (not a valid template) is still externalized rather than left plain.
		if *pw == "" || config.IsTemplateValue(*pw) {
			return nil
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		path := filepath.Join(dir, base+"."+side+".secret")
		if err := os.WriteFile(path, []byte(*pw), 0o600); err != nil {
			return err
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			abs = path
		}
		*pw = "${file:" + abs + "}"
		return nil
	}

	if err := put("source", &cfg.Source.Password); err != nil {
		return err
	}
	return put("target", &cfg.Target.Password)
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
