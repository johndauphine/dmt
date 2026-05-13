package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSecretsFile(t *testing.T) {
	// Create a temporary secrets file
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")

	content := `
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "test-key"
      model: "claude-haiku-4-5-20251001"
    ollama:
      base_url: "http://localhost:11434"
      model: "llama3"

encryption:
  master_key: "test-master-key"
`
	if err := os.WriteFile(secretsFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to write test secrets file: %v", err)
	}

	// Set env var to use test file
	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)

	// Reset cached config
	Reset()

	config, err := Load()
	if err != nil {
		t.Fatalf("Failed to load secrets: %v", err)
	}

	// Verify AI config
	if config.AI.DefaultProvider != "anthropic" {
		t.Errorf("Expected default_provider 'anthropic', got %q", config.AI.DefaultProvider)
	}

	provider, name, err := config.GetDefaultProvider()
	if err != nil {
		t.Fatalf("Failed to get default provider: %v", err)
	}
	if name != "anthropic" {
		t.Errorf("Expected provider name 'anthropic', got %q", name)
	}
	if provider.APIKey != "test-key" {
		t.Errorf("Expected api_key 'test-key', got %q", provider.APIKey)
	}

	// Verify encryption config
	if config.GetMasterKey() != "test-master-key" {
		t.Errorf("Expected master_key 'test-master-key', got %q", config.GetMasterKey())
	}
}

func TestSaveSlackWebhookSetAndClear(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")
	// Seed with pre-existing AI + master_key so we can also verify those
	// survive a Slack write — the only thing that must change is the
	// webhook URL.
	seed := `
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "preserve-me"
encryption:
  master_key: "preserve-me-too"
`
	if err := os.WriteFile(secretsFile, []byte(seed), 0600); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)

	// Set a webhook.
	if err := SaveSlackWebhook("https://hooks.example.invalid/services/T/B/X"); err != nil {
		t.Fatalf("SaveSlackWebhook set: %v", err)
	}
	Reset()
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load after set: %v", err)
	}
	if cfg.Notifications.Slack.WebhookURL != "https://hooks.example.invalid/services/T/B/X" {
		t.Fatalf("webhook not persisted, got %q", cfg.Notifications.Slack.WebhookURL)
	}
	if cfg.AI.Providers["anthropic"].APIKey != "preserve-me" {
		t.Fatal("AI provider api_key clobbered by Slack write")
	}
	if cfg.Encryption.MasterKey != "preserve-me-too" {
		t.Fatal("master_key clobbered by Slack write")
	}

	// Clear by passing empty — the merge-skips-empty semantics in Save
	// can't do this; SaveSlackWebhook bypasses merge for exactly this case.
	if err := SaveSlackWebhook(""); err != nil {
		t.Fatalf("SaveSlackWebhook clear: %v", err)
	}
	Reset()
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load after clear: %v", err)
	}
	if cfg.Notifications.Slack.WebhookURL != "" {
		t.Fatalf("clear failed, webhook still %q", cfg.Notifications.Slack.WebhookURL)
	}
	// Other sections still intact.
	if cfg.AI.Providers["anthropic"].APIKey != "preserve-me" {
		t.Fatal("AI provider api_key clobbered by Slack clear")
	}
}

func TestSaveSlackWebhookRefusesToOverwriteMalformedFile(t *testing.T) {
	// Regression: a malformed existing secrets file used to silently
	// parse into a zero-value Config, and the subsequent write would
	// clobber whatever AI/encryption/migration_defaults content the
	// operator had — destroying credentials. SaveSlackWebhook now
	// surfaces the parse error and refuses to write.
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")
	malformed := "ai:\n  default_provider: anthropic\n  providers:\n    anthropic:\n      api_key: \"oh-no-this-is-not-closed\n"
	if err := os.WriteFile(secretsFile, []byte(malformed), 0600); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)

	if err := SaveSlackWebhook("https://hooks.example.invalid/services/T/B/X"); err == nil {
		t.Fatal("expected parse error refusing to overwrite malformed secrets file")
	}

	// File contents must be unchanged after the refused write.
	got, err := os.ReadFile(secretsFile)
	if err != nil {
		t.Fatalf("read after refused write: %v", err)
	}
	if string(got) != malformed {
		t.Fatalf("malformed secrets file was modified despite refused write\nwant: %q\ngot:  %q", malformed, string(got))
	}
}

func TestValidateCloudProviderRequiresAPIKey(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")

	// Missing API key for cloud provider
	content := `
ai:
  default_provider: anthropic
  providers:
    anthropic:
      model: "claude-haiku-4-5-20251001"

encryption:
  master_key: "test"
`
	if err := os.WriteFile(secretsFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to write test secrets file: %v", err)
	}

	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)
	Reset()

	_, err := Load()
	if err == nil {
		t.Fatal("Expected error for missing API key, got nil")
	}
}

func TestLocalProviderNoAPIKeyRequired(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")

	content := `
ai:
  default_provider: ollama
  providers:
    ollama:
      model: "llama3"

encryption:
  master_key: "test"
`
	if err := os.WriteFile(secretsFile, []byte(content), 0600); err != nil {
		t.Fatalf("Failed to write test secrets file: %v", err)
	}

	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)
	Reset()

	config, err := Load()
	if err != nil {
		t.Fatalf("Local provider should not require API key: %v", err)
	}

	provider, _, err := config.GetDefaultProvider()
	if err != nil {
		t.Fatalf("Failed to get provider: %v", err)
	}

	// Should have default base URL
	baseURL := provider.GetEffectiveBaseURL("ollama")
	if baseURL != "http://localhost:11434" {
		t.Errorf("Expected default Ollama URL, got %q", baseURL)
	}
}

func TestGetEffectiveModel(t *testing.T) {
	provider := &Provider{}

	// Should return default model when not specified
	model := provider.GetEffectiveModel("anthropic")
	if model != "claude-haiku-4-5-20251001" {
		t.Errorf("Expected default Anthropic model, got %q", model)
	}

	// Should return custom model when specified
	provider.Model = "custom-model"
	model = provider.GetEffectiveModel("anthropic")
	if model != "custom-model" {
		t.Errorf("Expected custom model, got %q", model)
	}
}

func TestGetEffectiveContextWindow(t *testing.T) {
	tests := []struct {
		name           string
		contextWindow  int
		expectedWindow int
	}{
		{
			name:           "default context window",
			contextWindow:  0,
			expectedWindow: 8192, // conservative default
		},
		{
			name:           "custom 4K context window",
			contextWindow:  4096,
			expectedWindow: 4096,
		},
		{
			name:           "custom 32K context window",
			contextWindow:  32768,
			expectedWindow: 32768,
		},
		{
			name:           "custom 128K context window",
			contextWindow:  131072,
			expectedWindow: 131072,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &Provider{
				ContextWindow: tt.contextWindow,
			}
			got := provider.GetEffectiveContextWindow()
			if got != tt.expectedWindow {
				t.Errorf("GetEffectiveContextWindow() = %d, want %d", got, tt.expectedWindow)
			}
		})
	}
}

func TestIsLocalProvider(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"anthropic", false},
		{"openai", false},
		{"gemini", false},
		{"ollama", true},
		{"lmstudio", true},
		{"unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsLocalProvider(tt.name)
			if got != tt.expected {
				t.Errorf("IsLocalProvider(%q) = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestSecretsNotFoundError(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "nonexistent.yaml")

	os.Setenv(SecretsFileEnvVar, secretsFile)
	defer os.Unsetenv(SecretsFileEnvVar)
	Reset()

	_, err := Load()
	if err == nil {
		t.Fatal("Expected error for nonexistent file, got nil")
	}

	_, ok := err.(*SecretsNotFoundError)
	if !ok {
		t.Errorf("Expected SecretsNotFoundError, got %T: %v", err, err)
	}
}

func TestGenerateTemplate_DefaultIsAIFree(t *testing.T) {
	// Per #174, the default template no longer seeds an AI provider
	// section — `dmt init-secrets` produces an AI-free file; users who
	// want AI opt in via `--with-ai`. Critical sections that must
	// always be present.
	template := GenerateTemplate()
	if len(template) == 0 {
		t.Fatal("Template should not be empty")
	}

	mustContain := []string{"master_key", "encryption:", "notifications:", "migration_defaults:"}
	for _, s := range mustContain {
		if !contains(template, s) {
			t.Errorf("Template missing expected string: %q", s)
		}
	}

	// AI provider section must NOT appear in the default template;
	// references to `--with-ai` should explain the opt-in path.
	mustNotContain := []string{"default_provider", "anthropic:", "openai:", "api_key:"}
	for _, s := range mustNotContain {
		if contains(template, s) {
			t.Errorf("Default template should not contain %q — AI is opt-in via --with-ai (#174)", s)
		}
	}
	// Template should point users at a non-destructive opt-in path
	// for AI (NOT --force --with-ai, which would wipe master_key etc).
	// Codex review on #174 caught this.
	if !contains(template, "optional-ai-enhancements") {
		t.Error("Default template should reference the README anchor for the safe AI opt-in path")
	}
	if !contains(template, "Do NOT run") || !contains(template, "--force") {
		t.Error("Default template should warn against the destructive --force --with-ai overwrite path")
	}
}

func TestGenerateTemplateWithAI_IncludesProviderSection(t *testing.T) {
	// The --with-ai opt-in variant seeds all providers from the
	// AISection() snippet so users can fill in just the key.
	template := GenerateTemplateWithAI()
	mustContain := []string{
		"default_provider",
		"anthropic:",
		"openai:",
		"ollama:",
		"lmstudio:",
		"master_key",
	}
	for _, s := range mustContain {
		if !contains(template, s) {
			t.Errorf("AI-enabled template missing expected string: %q", s)
		}
	}
}

func TestAISection_IsStandaloneSnippet(t *testing.T) {
	// AISection() is the snippet users can paste into a previously
	// AI-free secrets file. Must contain the provider list.
	section := AISection()
	for _, s := range []string{"default_provider", "anthropic:", "openai:"} {
		if !contains(section, s) {
			t.Errorf("AISection missing %q", s)
		}
	}
	// Must NOT contain non-AI sections — it's an AI-only snippet.
	for _, s := range []string{"encryption:", "notifications:", "migration_defaults:"} {
		if contains(section, s) {
			t.Errorf("AISection should be AI-only; contains %q", s)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
