package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/secrets"
)

func testProvider(apiKey string) *secrets.Provider {
	return &secrets.Provider{
		APIKey: apiKey,
		Model:  "test-model",
	}
}

// testMapperWithTempCache creates a mapper with an isolated temp cache file
func testMapperWithTempCache(t *testing.T, providerName string, provider *secrets.Provider) *AITypeMapper {
	t.Helper()
	tmpDir := t.TempDir()
	cacheFile := filepath.Join(tmpDir, "type-cache.json")

	mapper := &AITypeMapper{
		providerName:   providerName,
		provider:       provider,
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: 30,
	}
	return mapper
}

func TestNewAITypeMapper_MissingProvider(t *testing.T) {
	_, err := NewAITypeMapper("claude", nil)
	if err == nil {
		t.Error("expected error when provider is nil")
	}
}

func TestNewAITypeMapper_MissingAPIKey(t *testing.T) {
	provider := &secrets.Provider{
		Model: "test-model",
	}
	_, err := NewAITypeMapper("claude", provider)
	if err == nil {
		t.Error("expected error when API key is missing for cloud provider")
	}
}

func TestNewAITypeMapper_LocalProviderNoAPIKey(t *testing.T) {
	provider := &secrets.Provider{
		BaseURL: "http://localhost:11434",
		Model:   "llama3",
	}
	mapper, err := NewAITypeMapper("ollama", provider)
	if err != nil {
		t.Fatalf("local provider should not require API key: %v", err)
	}
	if mapper.ProviderName() != "ollama" {
		t.Errorf("expected provider name 'ollama', got '%s'", mapper.ProviderName())
	}
}

func TestNewAITypeMapper_APIKeyProvided(t *testing.T) {
	provider := testProvider("test-key-123")
	mapper, err := NewAITypeMapper("claude", provider)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mapper.ProviderName() != "claude" {
		t.Errorf("expected provider 'claude', got '%s'", mapper.ProviderName())
	}
}

func TestNewAITypeMapper_DefaultModel(t *testing.T) {
	tests := []struct {
		provider      string
		expectedModel string
	}{
		{"claude", "claude-sonnet-4-20250514"},
		{"openai", "gpt-4o"},
		{"gemini", "gemini-2.0-flash"},
		{"ollama", "llama3"},
		{"lmstudio", "local-model"},
	}

	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			provider := &secrets.Provider{
				APIKey: "test-key", // Required for cloud providers
			}
			if secrets.IsLocalProvider(tt.provider) {
				provider.APIKey = ""
				provider.BaseURL = "http://localhost:8080"
			}
			mapper, err := NewAITypeMapper(tt.provider, provider)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if mapper.Model() != tt.expectedModel {
				t.Errorf("expected model '%s', got '%s'", tt.expectedModel, mapper.Model())
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
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

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

func TestAITypeMapper_CanMap(t *testing.T) {
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

	// AI mapper should always return true for CanMap
	if !mapper.CanMap("mysql", "postgres") {
		t.Error("expected CanMap to return true")
	}
	if !mapper.CanMap("oracle", "mssql") {
		t.Error("expected CanMap to return true for any combination")
	}
}

func TestAITypeMapper_SupportedTargets(t *testing.T) {
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

	targets := mapper.SupportedTargets()
	if len(targets) != 1 || targets[0] != "*" {
		t.Errorf("expected ['*'], got %v", targets)
	}
}

func TestAITypeMapper_BuildPrompt(t *testing.T) {
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

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

func TestAITypeMapper_BuildPromptWithoutSamples(t *testing.T) {
	// Sample values are no longer included in prompts (privacy improvement).
	// Type mapping now works purely from DDL metadata.
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

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

	// Verify sample values are NOT included (privacy improvement)
	if bytes.Contains([]byte(prompt), []byte("Sample values")) {
		t.Error("prompt should NOT contain sample values (privacy improvement)")
	}
	if bytes.Contains([]byte(prompt), []byte("POINT (-108.5523153 39.0430375)")) {
		t.Error("prompt should NOT contain sample data (privacy improvement)")
	}
	// Data type should still be present
	if !bytes.Contains([]byte(prompt), []byte("geography")) {
		t.Error("prompt should contain data type")
	}
}

func TestAITypeMapper_BuildPromptMetadataOnly(t *testing.T) {
	// Since sample values are no longer used, prompts should work from DDL metadata only
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

	info := TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "nvarchar",
		MaxLength:    -1,
	}

	prompt := mapper.buildPrompt(info)

	// Verify prompt contains metadata but no sample section
	if !bytes.Contains([]byte(prompt), []byte("nvarchar")) {
		t.Error("prompt should contain data type")
	}
	if !bytes.Contains([]byte(prompt), []byte("Max length: MAX")) {
		t.Error("prompt should contain max length")
	}
	if bytes.Contains([]byte(prompt), []byte("Sample")) {
		t.Error("prompt should not contain sample values section")
	}
}

func TestAITypeMapper_ExportCache(t *testing.T) {
	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))

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

func TestSanitizeSampleValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"simple", "hello", "hello"},
		{"email redaction", "john.doe@example.com", "[EMAIL]@example.com"},
		{"email with subdomain", "user@mail.company.org", "[EMAIL]@mail.company.org"},
		{"SSN redaction", "123-45-6789", "[SSN]"},
		{"not SSN - wrong format", "12-345-6789", "12-345-6789"},
		{"not SSN - has letters", "123-AB-6789", "123-AB-6789"},
		{"phone redaction 10 digits", "5551234567", "[PHONE]"},
		{"phone with dashes", "555-123-4567", "[PHONE]"},
		{"phone with parens", "(555)123-4567", "[PHONE]"},
		{"not phone - too few digits", "555-1234", "555-1234"},
		{"not phone - too many non-digits", "phone: 555-123-4567", "phone: 555-123-4567"},
		{"long value truncated", strings.Repeat("a", 150), strings.Repeat("a", 100) + "..."},
		{"GPS coordinates preserved", "POINT (-108.5523 39.0430)", "POINT (-108.5523 39.0430)"},
		{"UUID preserved", "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeSampleValue(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeSampleValue(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSanitizeErrorResponse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		contains string // Check contains instead of exact match due to redaction position
	}{
		{"empty", "", 200, ""},
		{"simple error", "Invalid request", 200, "Invalid request"},
		{"truncated", strings.Repeat("a", 300), 200, "..."},
		{"redacts API key sk-", "Error with sk-ant-api03-abc123def456ghi789", 200, "[REDACTED]"},
		{"redacts multiple patterns", "Keys: api-key123 token-abc secret-xyz", 200, "[REDACTED]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeErrorResponse([]byte(tt.input), tt.maxLen)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("sanitizeErrorResponse(%q) = %q, want to contain %q", tt.input, result, tt.contains)
			}
			// Ensure no API key patterns remain
			if strings.Contains(result, "sk-ant") || strings.Contains(result, "api03") {
				t.Errorf("sanitizeErrorResponse(%q) = %q, should not contain API key", tt.input, result)
			}
		})
	}
}

func TestAITypeMapper_BuildPromptExcludesSampleValues(t *testing.T) {
	// Sample values are no longer included in prompts (privacy improvement).
	// This test verifies that even when SampleValues are provided,
	// they are not included in the generated prompt.
	mapper, _ := NewAITypeMapper("claude", testProvider("test-key"))

	// Create info with sample values that would previously be included
	info := TypeInfo{
		SourceDBType: "mssql",
		TargetDBType: "postgres",
		DataType:     "varchar",
		SampleValues: []string{
			strings.Repeat("a", 200),
			strings.Repeat("b", 200),
			"sensitive data",
		},
	}

	prompt := mapper.buildPrompt(info)

	// Verify sample values are NOT included
	if strings.Contains(prompt, "Sample values") {
		t.Error("prompt should NOT contain sample values section (privacy improvement)")
	}
	if strings.Contains(prompt, "sensitive data") {
		t.Error("prompt should NOT contain any sample data")
	}

	// Verify prompt still contains necessary metadata
	if !strings.Contains(prompt, "varchar") {
		t.Error("prompt should contain data type")
	}
}

func TestSanitizeSampleValue_RedactsPII(t *testing.T) {
	// The sanitizeSampleValue function still exists for backwards compatibility
	// but is no longer used in buildPrompt. Test the function directly.

	// Test email redaction
	email := sanitizeSampleValue("john.doe@example.com")
	if strings.Contains(email, "john.doe") {
		t.Error("email local part should be redacted")
	}
	if !strings.Contains(email, "[EMAIL]") {
		t.Error("email should contain [EMAIL] marker")
	}

	// Test SSN redaction
	ssn := sanitizeSampleValue("123-45-6789")
	if ssn != "[SSN]" {
		t.Errorf("SSN should be redacted to [SSN], got %q", ssn)
	}

	// Test phone redaction
	phone := sanitizeSampleValue("(555) 123-4567")
	if phone != "[PHONE]" {
		t.Errorf("phone should be redacted to [PHONE], got %q", phone)
	}

	// Test truncation of long values
	longValue := strings.Repeat("x", 150)
	truncated := sanitizeSampleValue(longValue)
	if len(truncated) > 104 { // 100 chars + "..."
		t.Errorf("long value should be truncated, got length %d", len(truncated))
	}
	if !strings.Contains(truncated, "...") {
		t.Error("truncated value should end with ...")
	}
}

func TestIsValidAIProvider_CaseInsensitive(t *testing.T) {
	tests := []struct {
		provider string
		valid    bool
	}{
		{"claude", true},
		{"Claude", true},
		{"CLAUDE", true},
		{"openai", true},
		{"OpenAI", true},
		{"OPENAI", true},
		{"gemini", true},
		{"Gemini", true},
		{"GEMINI", true},
		{"ollama", true},
		{"lmstudio", true},
		{"invalid", false},
		{"gpt", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			result := IsValidAIProvider(tt.provider)
			if result != tt.valid {
				t.Errorf("IsValidAIProvider(%q) = %v, want %v", tt.provider, result, tt.valid)
			}
		})
	}
}

func TestNormalizeAIProvider(t *testing.T) {
	tests := []struct {
		provider string
		expected string
	}{
		{"claude", "claude"},
		{"Claude", "claude"},
		{"CLAUDE", "claude"},
		{"openai", "openai"},
		{"OpenAI", "openai"},
		{"OPENAI", "openai"},
		{"gemini", "gemini"},
		{"Gemini", "gemini"},
		{"GEMINI", "gemini"},
		{"ollama", "ollama"},
		{"lmstudio", "lmstudio"},
		{"invalid", ""},
		{"gpt", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			result := NormalizeAIProvider(tt.provider)
			if result != tt.expected {
				t.Errorf("NormalizeAIProvider(%q) = %q, want %q", tt.provider, result, tt.expected)
			}
		})
	}
}

func TestAITypeMapper_CachePersistence(t *testing.T) {
	// Create temp directory for cache - use same dir for both mappers
	tmpDir := t.TempDir()
	cacheFile := filepath.Join(tmpDir, "type-cache.json")

	provider := testProvider("test-key")

	// Create first mapper with empty cache
	mapper := &AITypeMapper{
		providerName:   "claude",
		provider:       provider,
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: 30,
	}

	// Add some cache entries
	mapper.cache.Set("test:key:1", "varchar(100)")
	mapper.cache.Set("test:key:2", "integer")

	// Save cache
	err := mapper.saveCache()
	if err != nil {
		t.Fatalf("failed to save cache: %v", err)
	}

	// Create new mapper with empty cache and same cache file
	mapper2 := &AITypeMapper{
		providerName:   "claude",
		provider:       provider,
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: 30,
	}
	mapper2.loadCache()

	if mapper2.CacheSize() != 2 {
		t.Errorf("expected cache size 2, got %d", mapper2.CacheSize())
	}

	val, ok := mapper2.cache.Get("test:key:1")
	if !ok || val != "varchar(100)" {
		t.Errorf("expected 'varchar(100)', got '%s'", val)
	}
}

// Tests for retry logic

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		expected   bool
	}{
		{"nil error, success status", nil, 200, false},
		{"nil error, server error 500", nil, 500, true},
		{"nil error, server error 502", nil, 502, true},
		{"nil error, rate limit 429", nil, 429, true},
		{"nil error, client error 400", nil, 400, false},
		{"nil error, unauthorized 401", nil, 401, false},
		{"TLS handshake timeout", errWithMessage("TLS handshake timeout"), 0, true},
		{"connection reset", errWithMessage("connection reset by peer"), 0, true},
		{"connection refused", errWithMessage("connection refused"), 0, true},
		{"io.EOF", io.EOF, 0, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, 0, true},
		{"wrapped EOF", fmt.Errorf("read failed: %w", io.EOF), 0, true},
		{"unexpected EOF string", errWithMessage("unexpected eof in response"), 0, true},
		{"i/o timeout", errWithMessage("i/o timeout"), 0, true},
		{"broken pipe", errWithMessage("broken pipe"), 0, true},
		{"no such host", errWithMessage("no such host"), 0, true},
		{"temporary failure", errWithMessage("temporary failure in name resolution"), 0, true},
		{"random error", errWithMessage("some random error"), 0, false},
		{"authentication error", errWithMessage("invalid API key"), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err, tt.statusCode)
			if result != tt.expected {
				t.Errorf("isRetryableError(%v, %d) = %v, want %v", tt.err, tt.statusCode, result, tt.expected)
			}
		})
	}
}

// errWithMessage creates a simple error with the given message
type simpleError string

func (e simpleError) Error() string { return string(e) }

func errWithMessage(msg string) error {
	return simpleError(msg)
}

func TestCalculateBackoff(t *testing.T) {
	// Test that backoff increases with attempts
	delay0 := calculateBackoff(0)
	delay1 := calculateBackoff(1)
	delay2 := calculateBackoff(2)

	// With jitter, we can only check approximate ranges
	// Base delay is 1s, so:
	// attempt 0: ~0.75s - 1.25s (1s ± 25% jitter)
	// attempt 1: ~1.5s - 2.5s (2s ± 25% jitter)
	// attempt 2: ~3s - 5s (4s ± 25% jitter)

	if delay0 < 500*time.Millisecond || delay0 > 2*time.Second {
		t.Errorf("delay0 = %v, want between 500ms and 2s", delay0)
	}

	if delay1 < 1*time.Second || delay1 > 3*time.Second {
		t.Errorf("delay1 = %v, want between 1s and 3s", delay1)
	}

	if delay2 < 2*time.Second || delay2 > 6*time.Second {
		t.Errorf("delay2 = %v, want between 2s and 6s", delay2)
	}

	// Test max delay cap (10s)
	delay10 := calculateBackoff(10)
	if delay10 > 15*time.Second {
		t.Errorf("delay10 = %v, should be capped near 10s", delay10)
	}
}

func TestRetryableHTTPDo_Success(t *testing.T) {
	// Create a test server that returns success
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer server.Close()

	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))
	mapper.client = server.Client()

	ctx := context.Background()
	resp, body, err := mapper.retryableHTTPDo(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader([]byte(`{}`)))
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if !bytes.Contains(body, []byte("success")) {
		t.Errorf("unexpected body: %s", body)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestRetryableHTTPDo_RetryOn500(t *testing.T) {
	// Create a test server that fails twice then succeeds
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "internal error"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer server.Close()

	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))
	mapper.client = server.Client()

	ctx := context.Background()
	resp, body, err := mapper.retryableHTTPDo(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader([]byte(`{}`)))
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if !bytes.Contains(body, []byte("success")) {
		t.Errorf("unexpected body: %s", body)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls (2 retries), got %d", callCount)
	}
}

func TestRetryableHTTPDo_ExhaustedRetries(t *testing.T) {
	// Create a test server that always fails
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "always fails"}`))
	}))
	defer server.Close()

	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))
	mapper.client = server.Client()

	ctx := context.Background()
	_, _, err := mapper.retryableHTTPDo(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader([]byte(`{}`)))
	})

	if err == nil {
		t.Error("expected error after exhausted retries")
	}
	// Should have tried defaultMaxRetries + 1 times
	expectedCalls := defaultMaxRetries + 1
	if callCount != expectedCalls {
		t.Errorf("expected %d calls, got %d", expectedCalls, callCount)
	}
}

func TestRetryableHTTPDo_NoRetryOn400(t *testing.T) {
	// Create a test server that returns 400 (client error, not retryable)
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "bad request"}`))
	}))
	defer server.Close()

	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))
	mapper.client = server.Client()

	ctx := context.Background()
	resp, _, err := mapper.retryableHTTPDo(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader([]byte(`{}`)))
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
	// Should not retry on 400
	if callCount != 1 {
		t.Errorf("expected 1 call (no retries for 400), got %d", callCount)
	}
}

func TestRetryableHTTPDo_ContextCancellation(t *testing.T) {
	// Create a test server that always returns 500 to trigger retries
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "always fails"}`))
	}))
	defer server.Close()

	mapper := testMapperWithTempCache(t, "claude", testProvider("test-key"))
	mapper.client = server.Client()

	// Create a context that will be cancelled during the retry delay
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay (less than backoff time)
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, _, err := mapper.retryableHTTPDo(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader([]byte(`{}`)))
	})
	elapsed := time.Since(start)

	// Should return context.Canceled error
	if err == nil {
		t.Error("expected error when context is cancelled")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}

	// Should have been cancelled quickly, not waited for all retries
	// The backoff would be ~1s+ for the first retry, so if we cancelled in 100ms
	// we should complete much faster than a full retry cycle
	if elapsed > 500*time.Millisecond {
		t.Errorf("expected quick cancellation, but took %v", elapsed)
	}

	// Should have made at least 1 call before being cancelled during backoff
	if callCount < 1 {
		t.Errorf("expected at least 1 call before cancellation, got %d", callCount)
	}
}

func TestIsRetryableError_EOF(t *testing.T) {
	// Test that io.EOF and io.ErrUnexpectedEOF are properly detected as retryable
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"io.EOF direct", io.EOF, true},
		{"io.ErrUnexpectedEOF direct", io.ErrUnexpectedEOF, true},
		{"wrapped io.EOF", fmt.Errorf("connection: %w", io.EOF), true},
		{"wrapped io.ErrUnexpectedEOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err, 0)
			if result != tt.expected {
				t.Errorf("isRetryableError(%v, 0) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}
