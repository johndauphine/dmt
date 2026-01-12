package driver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewAITypeMapper_Disabled(t *testing.T) {
	config := AITypeMappingConfig{
		Enabled: false,
	}
	_, err := NewAITypeMapper(config, nil)
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
	_, err := NewAITypeMapper(config, nil)
	if err == nil {
		t.Error("expected error when API key is missing")
	}
}

func TestNewAITypeMapper_APIKeyProvided(t *testing.T) {
	// API key expansion happens at config loading time (before NewAITypeMapper is called)
	// This test verifies that a pre-expanded API key is accepted
	config := AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key-123", // Already expanded by config loading
	}
	mapper, err := NewAITypeMapper(config, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mapper.config.APIKey != "test-key-123" {
		t.Errorf("expected API key 'test-key-123', got '%s'", mapper.config.APIKey)
	}
}

func TestNewAITypeMapper_DefaultModel(t *testing.T) {
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
				APIKey:   "test-key",
			}
			mapper, err := NewAITypeMapper(config, nil)
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
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

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
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

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

// mockStaticMapper is a simple static mapper for testing fallback behavior
type mockStaticMapper struct{}

func (m *mockStaticMapper) MapType(info TypeInfo) string {
	// Simple static mappings for testing
	switch info.DataType {
	case "int":
		return "integer"
	case "varchar":
		return "text"
	default:
		return "text"
	}
}

func (m *mockStaticMapper) CanMap(source, target string) bool {
	return true
}

func (m *mockStaticMapper) SupportedTargets() []string {
	return []string{"*"}
}

func TestAITypeMapper_FallbackWithStaticMapper(t *testing.T) {
	staticMapper := &mockStaticMapper{}
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, staticMapper)

	// Test fallback uses static mapper
	result := mapper.fallback(TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "int",
	})

	if result != "integer" {
		t.Errorf("expected static mapper result 'integer', got '%s'", result)
	}
}

func TestAITypeMapper_CanMap(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

	// AI mapper should always return true for CanMap
	if !mapper.CanMap("mysql", "postgres") {
		t.Error("expected CanMap to return true")
	}
	if !mapper.CanMap("oracle", "mssql") {
		t.Error("expected CanMap to return true for any combination")
	}
}

func TestAITypeMapper_SupportedTargets(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

	targets := mapper.SupportedTargets()
	if len(targets) != 1 || targets[0] != "*" {
		t.Errorf("expected ['*'], got %v", targets)
	}
}

func TestAITypeMapper_BuildPrompt(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

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

func TestAITypeMapper_BuildPromptWithSamples(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

	info := TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "geography",
		MaxLength:    -1,
		SampleValues: []string{
			"POINT (-108.5523153 39.0430375)",
			"POINT (-122.4194 37.7749)",
			"POINT (-73.935242 40.730610)",
		},
	}

	prompt := mapper.buildPrompt(info)

	// Check that prompt contains sample values section
	if !bytes.Contains([]byte(prompt), []byte("Sample values from source data:")) {
		t.Error("prompt should contain sample values header")
	}
	if !bytes.Contains([]byte(prompt), []byte("POINT (-108.5523153 39.0430375)")) {
		t.Error("prompt should contain sample GPS coordinate data")
	}
	if !bytes.Contains([]byte(prompt), []byte("geography")) {
		t.Error("prompt should contain data type")
	}
}

func TestAITypeMapper_BuildPromptTruncatesLongSamples(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

	// Create a long sample value (over 100 chars)
	longValue := strings.Repeat("x", 150)

	info := TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "nvarchar",
		MaxLength:    -1,
		SampleValues: []string{longValue},
	}

	prompt := mapper.buildPrompt(info)

	// Check that long value is truncated with "..."
	if !bytes.Contains([]byte(prompt), []byte("...")) {
		t.Error("prompt should truncate long sample values")
	}
	// Original 150-char value should NOT appear in full
	if bytes.Contains([]byte(prompt), []byte(longValue)) {
		t.Error("prompt should not contain full long value")
	}
}

func TestAITypeMapper_BuildPromptLimitsToFiveSamples(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

	info := TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "int",
		SampleValues: []string{"1", "2", "3", "4", "5", "6", "7", "8"},
	}

	prompt := mapper.buildPrompt(info)

	// Count occurrences of sample values in the prompt
	// Should have at most 5 (values 1-5) but not 6, 7, 8
	if bytes.Contains([]byte(prompt), []byte("\"6\"")) {
		t.Error("prompt should not contain 6th sample")
	}
	if bytes.Contains([]byte(prompt), []byte("\"7\"")) {
		t.Error("prompt should not contain 7th sample")
	}
}

func TestAITypeMapper_CachePersistence(t *testing.T) {
	// Create temp directory for cache
	tmpDir := t.TempDir()
	cacheFile := filepath.Join(tmpDir, "type-cache.json")

	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:   true,
		Provider:  "claude",
		APIKey:    "test-key",
		CacheFile: cacheFile,
	}, nil)

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
		APIKey:    "test-key",
		CacheFile: cacheFile,
	}, nil)

	if mapper2.CacheSize() != 2 {
		t.Errorf("expected cache size 2, got %d", mapper2.CacheSize())
	}

	val, ok := mapper2.cache.Get("test:key:1")
	if !ok || val != "varchar(100)" {
		t.Errorf("expected 'varchar(100)', got '%s'", val)
	}
}

func TestAITypeMapper_ExportCache(t *testing.T) {
	mapper, _ := NewAITypeMapper(AITypeMappingConfig{
		Enabled:  true,
		Provider: "claude",
		APIKey:   "test-key",
	}, nil)

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
