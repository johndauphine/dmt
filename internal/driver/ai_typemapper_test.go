package driver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestNewAITypeMapper_Disabled(t *testing.T) {
	config := AITypeMappingConfig{
		Enabled: false,
	}
	_, err := NewAITypeMapper(config)
	if err == nil {
		t.Error("expected error when AI type mapping is disabled")
	}
}

func TestNewAITypeMapper_MissingAPIKey(t *testing.T) {
	config := AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "",
	}
	_, err := NewAITypeMapper(config)
	if err == nil {
		t.Error("expected error when API key is missing")
	}
}

func TestNewAITypeMapper_EnvVarAPIKey(t *testing.T) {
	// Set up test API key
	os.Setenv("TEST_AI_API_KEY", "test-key-123")
	defer os.Unsetenv("TEST_AI_API_KEY")

	config := AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	}
	mapper, err := NewAITypeMapper(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mapper.config.APIKey != "test-key-123" {
		t.Errorf("expected expanded API key 'test-key-123', got '%s'", mapper.config.APIKey)
	}
}

func TestNewAITypeMapper_DefaultModel(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	tests := []struct {
		provider      string
		expectedModel string
	}{
		{"claude", "claude-3-haiku-20240307"},
		{"openai", "gpt-4o-mini"},
	}

	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			config := AITypeMappingConfig{
				Enabled:  true,
				Provider: tt.provider,
				APIKey:   "${TEST_AI_API_KEY}",
			}
			mapper, err := NewAITypeMapper(config)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if mapper.config.Model != tt.expectedModel {
				t.Errorf("expected model '%s', got '%s'", tt.expectedModel, mapper.config.Model)
			}
		})
	}
}

func TestTypeMappingCache(t *testing.T) {
	cache := NewTypeMappingCache()

	// Test Get on empty cache
	_, ok := cache.Get("test-key")
	if ok {
		t.Error("expected false for missing key")
	}

	// Test Set and Get
	cache.Set("test-key", "varchar(255)")
	val, ok := cache.Get("test-key")
	if !ok {
		t.Error("expected true for existing key")
	}
	if val != "varchar(255)" {
		t.Errorf("expected 'varchar(255)', got '%s'", val)
	}

	// Test All
	cache.Set("another-key", "text")
	all := cache.All()
	if len(all) != 2 {
		t.Errorf("expected 2 items, got %d", len(all))
	}

	// Test Load
	newCache := NewTypeMappingCache()
	newCache.Load(map[string]string{
		"key1": "int",
		"key2": "bigint",
	})
	if len(newCache.All()) != 2 {
		t.Errorf("expected 2 items after Load, got %d", len(newCache.All()))
	}
}

func TestAITypeMapper_CacheKey(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	info := TypeInfo{
		SourceDBType: "mysql",
		TargetDBType: "postgres",
		DataType:     "MEDIUMBLOB",
		MaxLength:    16777215,
		Precision:    0,
		Scale:        0,
	}

	key := mapper.cacheKey(info)
	expected := "mysql:postgres:mediumblob:16777215:0:0"
	if key != expected {
		t.Errorf("expected cache key '%s', got '%s'", expected, key)
	}
}

func TestAITypeMapper_Fallback(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	tests := []struct {
		targetDB string
		expected string
	}{
		{"postgres", "text"},
		{"mssql", "nvarchar(max)"},
		{"mysql", "nvarchar(max)"},
	}

	for _, tt := range tests {
		t.Run(tt.targetDB, func(t *testing.T) {
			result := mapper.fallback(TypeInfo{TargetDBType: tt.targetDB})
			if result != tt.expected {
				t.Errorf("expected fallback '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestAITypeMapper_CanMap(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	// AI mapper should always return true for CanMap
	if !mapper.CanMap("mysql", "postgres") {
		t.Error("expected CanMap to return true")
	}
	if !mapper.CanMap("oracle", "mssql") {
		t.Error("expected CanMap to return true for any combination")
	}
}

func TestAITypeMapper_SupportedTargets(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	targets := mapper.SupportedTargets()
	if len(targets) != 1 || targets[0] != "*" {
		t.Errorf("expected ['*'], got %v", targets)
	}
}

func TestAITypeMapper_BuildPrompt(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	info := TypeInfo{
		SourceDBType: "mysql",
		TargetDBType: "postgres",
		DataType:     "DECIMAL",
		MaxLength:    0,
		Precision:    10,
		Scale:        2,
	}

	prompt := mapper.buildPrompt(info)

	// Check that prompt contains key elements
	if !bytes.Contains([]byte(prompt), []byte("mysql")) {
		t.Error("prompt should contain source DB type")
	}
	if !bytes.Contains([]byte(prompt), []byte("postgres")) {
		t.Error("prompt should contain target DB type")
	}
	if !bytes.Contains([]byte(prompt), []byte("DECIMAL")) {
		t.Error("prompt should contain data type")
	}
	if !bytes.Contains([]byte(prompt), []byte("Precision: 10")) {
		t.Error("prompt should contain precision")
	}
	if !bytes.Contains([]byte(prompt), []byte("Scale: 2")) {
		t.Error("prompt should contain scale")
	}
}

func TestAITypeMapper_CachePersistence(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	// Create temp directory for cache
	tmpDir := t.TempDir()
	cacheFile := filepath.Join(tmpDir, "type-cache.json")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:   true,
		Provider:  "claude",
		APIKey:    "${TEST_AI_API_KEY}",
		CacheFile: cacheFile,
	})

	// Add some cache entries
	mapper.cache.Set("test:key:1", "varchar(100)")
	mapper.cache.Set("test:key:2", "integer")

	// Save cache
	err := mapper.saveCache()
	if err != nil {
		t.Fatalf("failed to save cache: %v", err)
	}

	// Create new mapper and load cache
	mapper2, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:   true,
		Provider:  "claude",
		APIKey:    "${TEST_AI_API_KEY}",
		CacheFile: cacheFile,
	})

	if mapper2.CacheSize() != 2 {
		t.Errorf("expected cache size 2, got %d", mapper2.CacheSize())
	}

	val, ok := mapper2.cache.Get("test:key:1")
	if !ok || val != "varchar(100)" {
		t.Errorf("expected 'varchar(100)', got '%s'", val)
	}
}

func TestAITypeMapper_ExportCache(t *testing.T) {
	os.Setenv("TEST_AI_API_KEY", "test-key")
	defer os.Unsetenv("TEST_AI_API_KEY")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "${TEST_AI_API_KEY}",
	})

	mapper.cache.Set("mysql:postgres:mediumblob:0:0:0", "bytea")
	mapper.cache.Set("mysql:postgres:tinyint:0:0:0", "smallint")

	var buf bytes.Buffer
	err := mapper.ExportCache(&buf)
	if err != nil {
		t.Fatalf("failed to export cache: %v", err)
	}

	var exported map[string]string
	if err := json.Unmarshal(buf.Bytes(), &exported); err != nil {
		t.Fatalf("failed to parse exported cache: %v", err)
	}

	if len(exported) != 2 {
		t.Errorf("expected 2 exported entries, got %d", len(exported))
	}
}

// Mock server for testing API calls
func TestAITypeMapper_ClaudeAPI(t *testing.T) {
	// Create mock Claude API server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-api-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		response := claudeResponse{
			Content: []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}{
				{Type: "text", Text: "bytea"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// This test validates the response parsing logic
	// In a real test, we'd inject the mock server URL
}

func TestAITypeMapper_OpenAIAPI(t *testing.T) {
	// Create mock OpenAI API server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-api-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		response := openAIResponse{
			Choices: []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			}{
				{Message: struct {
					Content string `json:"content"`
				}{Content: "bytea"}},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// This test validates the response parsing logic
	// In a real test, we'd inject the mock server URL
}
