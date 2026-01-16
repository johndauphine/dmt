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
  default_provider: claude
  providers:
    claude:
      api_key: "test-key"
      model: "claude-sonnet-4-20250514"
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
	if config.AI.DefaultProvider != "claude" {
		t.Errorf("Expected default_provider 'claude', got %q", config.AI.DefaultProvider)
	}

	provider, name, err := config.GetDefaultProvider()
	if err != nil {
		t.Fatalf("Failed to get default provider: %v", err)
	}
	if name != "claude" {
		t.Errorf("Expected provider name 'claude', got %q", name)
	}
	if provider.APIKey != "test-key" {
		t.Errorf("Expected api_key 'test-key', got %q", provider.APIKey)
	}

	// Verify encryption config
	if config.GetMasterKey() != "test-master-key" {
		t.Errorf("Expected master_key 'test-master-key', got %q", config.GetMasterKey())
	}
}

func TestValidateCloudProviderRequiresAPIKey(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")

	// Missing API key for cloud provider
	content := `
ai:
  default_provider: claude
  providers:
    claude:
      model: "claude-sonnet-4-20250514"

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
	model := provider.GetEffectiveModel("claude")
	if model != "claude-sonnet-4-20250514" {
		t.Errorf("Expected default Claude model, got %q", model)
	}

	// Should return custom model when specified
	provider.Model = "custom-model"
	model = provider.GetEffectiveModel("claude")
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
		{"claude", false},
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

func TestGenerateTemplate(t *testing.T) {
	template := GenerateTemplate()

	// Verify template contains expected sections
	if len(template) == 0 {
		t.Fatal("Template should not be empty")
	}

	// Check for key sections
	expectedStrings := []string{
		"default_provider",
		"claude:",
		"openai:",
		"ollama:",
		"lmstudio:",
		"master_key",
	}

	for _, s := range expectedStrings {
		if !contains(template, s) {
			t.Errorf("Template missing expected string: %q", s)
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
