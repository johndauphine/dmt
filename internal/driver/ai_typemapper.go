package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
)

// AIProvider represents supported AI providers for type mapping.
type AIProvider string

const (
	// ProviderClaude uses Anthropic's Claude API.
	ProviderClaude AIProvider = "claude"
	// ProviderOpenAI uses OpenAI's API.
	ProviderOpenAI AIProvider = "openai"
	// ProviderGemini uses Google's Gemini API.
	ProviderGemini AIProvider = "gemini"
)

// ValidAIProviders returns the list of supported AI provider names.
func ValidAIProviders() []string {
	return []string{string(ProviderClaude), string(ProviderOpenAI), string(ProviderGemini)}
}

// IsValidAIProvider returns true if the provider name is valid (case-insensitive).
func IsValidAIProvider(provider string) bool {
	switch AIProvider(strings.ToLower(provider)) {
	case ProviderClaude, ProviderOpenAI, ProviderGemini:
		return true
	}
	return false
}

// NormalizeAIProvider returns the canonical lowercase provider name.
// Returns empty string if the provider is invalid.
func NormalizeAIProvider(provider string) string {
	normalized := strings.ToLower(provider)
	if IsValidAIProvider(normalized) {
		return normalized
	}
	return ""
}

// AITypeMappingConfig contains configuration for AI-assisted type mapping.
// This is a copy of config.AITypeMappingConfig to avoid import cycles.
type AITypeMappingConfig struct {
	Enabled        bool   // Enable AI type mapping
	Provider       string // "claude", "openai", or "gemini"
	APIKey         string // API key - supports same patterns as password:
	//                       ${file:/path/to/key} - read from file
	//                       ${env:VAR_NAME} - read from env var
	//                       ${VAR_NAME} - legacy env var syntax
	//                       or literal value (not recommended)
	CacheFile      string // Path to cache file
	Model          string // Model to use (optional)
	TimeoutSeconds int    // API timeout
}

// AITypeMapper uses AI to map unknown database types.
// It implements the TypeMapper interface and can be used as a fallback
// in a ChainedTypeMapper for types not covered by static mappings.
type AITypeMapper struct {
	config         AITypeMappingConfig
	client         *http.Client
	cache          *TypeMappingCache
	cacheMu        sync.RWMutex
	requestsMu     sync.Mutex  // Serialize API requests to avoid rate limiting
	inflight       sync.Map    // Track in-flight requests to avoid duplicate API calls
	fallbackMapper TypeMapper  // Static mapper to use when AI is unavailable
}

// inflightRequest tracks an in-progress API request for a specific type.
type inflightRequest struct {
	done   chan struct{}
	result string
	err    error
}

// NewAITypeMapper creates a new AI-powered type mapper.
// The fallbackMapper is used when AI API calls fail.
func NewAITypeMapper(config AITypeMappingConfig, fallbackMapper TypeMapper) (*AITypeMapper, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("AI type mapping is not enabled")
	}

	// API key should already be expanded by config loading (supports ${file:...}, ${env:...}, ${VAR})
	// Just verify it's not empty after expansion
	if config.APIKey == "" {
		return nil, fmt.Errorf("AI type mapping requires an API key")
	}

	// Set defaults
	if config.TimeoutSeconds <= 0 {
		config.TimeoutSeconds = 30
	}
	if config.CacheFile == "" {
		homeDir, _ := os.UserHomeDir()
		config.CacheFile = filepath.Join(homeDir, ".mssql-pg-migrate", "type-cache.json")
	}
	config.CacheFile = os.ExpandEnv(config.CacheFile)

	// Set default models - use smarter models for better type inference accuracy
	if config.Model == "" {
		switch AIProvider(config.Provider) {
		case ProviderClaude:
			config.Model = "claude-sonnet-4-20250514"
		case ProviderOpenAI:
			config.Model = "gpt-4o"
		case ProviderGemini:
			config.Model = "gemini-2.0-flash"
		}
	}

	mapper := &AITypeMapper{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(config.TimeoutSeconds) * time.Second,
		},
		cache:          NewTypeMappingCache(),
		fallbackMapper: fallbackMapper,
	}

	// Load existing cache
	if err := mapper.loadCache(); err != nil {
		logging.Warn("Failed to load AI type mapping cache: %v", err)
	}

	return mapper, nil
}

// MapType maps a source type to the target type using AI-first with aggressive caching.
// This approach maximizes database agnosticism - AI determines mappings, cache prevents
// repeated API calls. Static mappings are only used as fallback when AI fails.
// This method is safe to call concurrently - it uses in-flight request tracking
// to avoid duplicate API calls for the same type.
func (m *AITypeMapper) MapType(info TypeInfo) string {
	cacheKey := m.cacheKey(info)

	// Check cache first (fast path)
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached
	}
	m.cacheMu.RUnlock()

	// Check if there's already an in-flight request for this key
	// Use LoadOrStore to atomically check and register if not present
	req := &inflightRequest{done: make(chan struct{})}
	if existing, loaded := m.inflight.LoadOrStore(cacheKey, req); loaded {
		// Another goroutine is already fetching this type, wait for it
		existingReq := existing.(*inflightRequest)
		<-existingReq.done
		if existingReq.err != nil {
			// The other request failed, use fallback
			return m.fallback(info)
		}
		return existingReq.result
	}

	// We're the first to request this type, do the API call
	defer func() {
		close(req.done) // Signal waiting goroutines
		m.inflight.Delete(cacheKey)
	}()

	// Double-check cache after acquiring the slot (another goroutine may have
	// completed just before our LoadOrStore)
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		req.result = cached
		return cached
	}
	m.cacheMu.RUnlock()

	// Log that we're calling AI (not in cache)
	logging.Debug("AI type mapping: %s.%s -> %s (not cached, calling API)", info.SourceDBType, info.DataType, info.TargetDBType)

	// Call AI API
	result, err := m.queryAI(info)
	if err != nil {
		logging.Warn("AI type mapping failed for %s: %v, using fallback", info.DataType, err)
		req.err = err
		result = m.fallback(info)
		req.result = result
		return result
	}

	// Cache the result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, result)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI type mapping cache: %v", err)
	}

	sampleInfo := ""
	if len(info.SampleValues) > 0 {
		sampleInfo = fmt.Sprintf(" (with %d sample values)", len(info.SampleValues))
	}
	logging.Info("AI mapped %s.%s -> %s.%s%s (cached for future use)",
		info.SourceDBType, info.DataType, info.TargetDBType, result, sampleInfo)

	req.result = result
	return result
}

// CanMap returns true - AI mapper can attempt to map any type.
func (m *AITypeMapper) CanMap(sourceDBType, targetDBType string) bool {
	return true
}

// SupportedTargets returns ["*"] indicating AI can map to any target.
func (m *AITypeMapper) SupportedTargets() []string {
	return []string{"*"}
}

func (m *AITypeMapper) cacheKey(info TypeInfo) string {
	return fmt.Sprintf("%s:%s:%s:%d:%d:%d",
		info.SourceDBType, info.TargetDBType, strings.ToLower(info.DataType),
		info.MaxLength, info.Precision, info.Scale)
}

func (m *AITypeMapper) fallback(info TypeInfo) string {
	// Use static mapper fallback if available
	if m.fallbackMapper != nil {
		result := m.fallbackMapper.MapType(info)
		logging.Info("AI fallback: using static mapping for %s -> %s", info.DataType, result)
		return result
	}

	// Ultimate fallback if no static mapper
	if info.TargetDBType == "postgres" {
		return "text"
	}
	return "nvarchar(max)"
}

func (m *AITypeMapper) queryAI(info TypeInfo) (string, error) {
	// Serialize API requests to avoid rate limiting
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	prompt := m.buildPrompt(info)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.config.TimeoutSeconds)*time.Second)
	defer cancel()

	var result string
	var err error

	switch AIProvider(m.config.Provider) {
	case ProviderClaude:
		result, err = m.queryClaudeAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt)
	case ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	default:
		return "", fmt.Errorf("unsupported AI provider: %s", m.config.Provider)
	}

	if err != nil {
		return "", err
	}

	// Clean up the result (remove quotes, whitespace)
	result = strings.TrimSpace(result)
	result = strings.Trim(result, "\"'`")
	result = strings.ToLower(result)

	return result, nil
}

// maxSampleValueLen is the maximum length of a single sample value in prompts.
const maxSampleValueLen = 100

// maxSamplesInPrompt is the maximum number of sample values to include in prompts.
const maxSamplesInPrompt = 5

// maxTotalSampleBytes is the maximum total bytes of sample data to include.
const maxTotalSampleBytes = 500

// sanitizeSampleValue cleans and truncates a sample value for inclusion in AI prompts.
// It redacts potential PII patterns and limits length.
func sanitizeSampleValue(value string) string {
	if value == "" {
		return value
	}

	// Truncate to max length
	if len(value) > maxSampleValueLen {
		value = value[:maxSampleValueLen] + "..."
	}

	// Redact potential email addresses
	if strings.Contains(value, "@") && strings.Contains(value, ".") {
		// Simple email pattern detection - redact the local part
		parts := strings.SplitN(value, "@", 2)
		if len(parts) == 2 && len(parts[0]) > 0 {
			value = "[EMAIL]@" + parts[1]
		}
	}

	// Redact potential SSN patterns (XXX-XX-XXXX)
	if len(value) == 11 && value[3] == '-' && value[6] == '-' {
		allDigits := true
		for i, c := range value {
			if i == 3 || i == 6 {
				continue
			}
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			value = "[SSN]"
		}
	}

	// Redact potential phone numbers (10+ consecutive digits)
	digitCount := 0
	for _, c := range value {
		if c >= '0' && c <= '9' {
			digitCount++
		}
	}
	if digitCount >= 10 && digitCount <= 15 {
		// Check if it looks like a phone number (mostly digits with optional formatting)
		nonDigits := len(value) - digitCount
		if nonDigits <= 4 { // Allow for dashes, spaces, parens
			value = "[PHONE]"
		}
	}

	return value
}

func (m *AITypeMapper) buildPrompt(info TypeInfo) string {
	var sb strings.Builder
	sb.WriteString("You are a database type mapping expert.\n\n")
	sb.WriteString(fmt.Sprintf("Map this %s type to %s:\n", info.SourceDBType, info.TargetDBType))
	sb.WriteString(fmt.Sprintf("- Type: %s\n", info.DataType))
	if info.MaxLength > 0 {
		sb.WriteString(fmt.Sprintf("- Max length: %d\n", info.MaxLength))
	} else if info.MaxLength == -1 {
		sb.WriteString("- Max length: MAX\n")
	}
	if info.Precision > 0 {
		sb.WriteString(fmt.Sprintf("- Precision: %d\n", info.Precision))
	}
	if info.Scale > 0 {
		sb.WriteString(fmt.Sprintf("- Scale: %d\n", info.Scale))
	}

	// Include sanitized sample values if available for better context
	if len(info.SampleValues) > 0 {
		sb.WriteString("\nSample values from source data (this is how the data will be transferred):\n")
		totalBytes := 0
		samplesIncluded := 0
		for _, v := range info.SampleValues {
			if samplesIncluded >= maxSamplesInPrompt {
				break
			}
			// Sanitize the value (truncate, redact PII)
			display := sanitizeSampleValue(v)
			if display == "" {
				continue
			}
			// Check total size limit
			if totalBytes+len(display) > maxTotalSampleBytes {
				break
			}
			sb.WriteString(fmt.Sprintf("  - %q\n", display))
			totalBytes += len(display)
			samplesIncluded++
		}
		sb.WriteString("The target column must be able to store these exact values.\n")
	}

	// Add target database context and encoding guidance
	switch info.TargetDBType {
	case "postgres":
		sb.WriteString("\nTarget: Standard PostgreSQL (no extensions installed).\n")
	case "mssql":
		sb.WriteString("\nTarget: SQL Server with full native type support.\n")
		// Add encoding context for string types from PostgreSQL
		if info.SourceDBType == "postgres" && (strings.HasPrefix(strings.ToLower(info.DataType), "varchar") ||
			strings.HasPrefix(strings.ToLower(info.DataType), "char") ||
			strings.ToLower(info.DataType) == "text") {
			sb.WriteString("Note: PostgreSQL string types store characters. SQL Server varchar stores bytes, nvarchar stores characters.\n")
		}
	}

	sb.WriteString("\nReturn ONLY the ")
	sb.WriteString(info.TargetDBType)
	sb.WriteString(" type name (e.g., varchar(255), numeric(10,2), text).\n")
	sb.WriteString("No explanation, just the type.")

	return sb.String()
}

// Claude API types
type claudeRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	Messages  []claudeMessage `json:"messages"`
}

type claudeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type claudeResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// sanitizeErrorResponse truncates and sanitizes API error response bodies
// to prevent potential API key or sensitive data leakage in error messages.
func sanitizeErrorResponse(body []byte, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 200
	}

	s := string(body)

	// Truncate to max length
	if len(s) > maxLen {
		s = s[:maxLen] + "..."
	}

	// Remove potential API keys (patterns like sk-..., api-..., key-...)
	// This is a defense-in-depth measure
	keyPatterns := []string{"sk-", "api-", "key-", "secret-", "token-"}
	for _, pattern := range keyPatterns {
		for {
			idx := strings.Index(strings.ToLower(s), pattern)
			if idx == -1 {
				break
			}
			// Redact 40 chars after the pattern (typical key length)
			endIdx := idx + len(pattern) + 40
			if endIdx > len(s) {
				endIdx = len(s)
			}
			s = s[:idx] + "[REDACTED]" + s[endIdx:]
		}
	}

	return s
}

func (m *AITypeMapper) queryClaudeAPI(ctx context.Context, prompt string) (string, error) {
	reqBody := claudeRequest{
		Model:     m.config.Model,
		MaxTokens: 1024, // Enough for auto-tune JSON with reasoning
		Messages: []claudeMessage{
			{Role: "user", Content: prompt},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", m.config.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var claudeResp claudeResponse
	if err := json.Unmarshal(body, &claudeResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if claudeResp.Error != nil {
		return "", fmt.Errorf("API error: %s", claudeResp.Error.Message)
	}

	if len(claudeResp.Content) == 0 || claudeResp.Content[0].Text == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return claudeResp.Content[0].Text, nil
}

// OpenAI API types
type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens"`
	Temperature float64         `json:"temperature"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (m *AITypeMapper) queryOpenAIAPI(ctx context.Context, prompt string) (string, error) {
	reqBody := openAIRequest{
		Model: m.config.Model,
		Messages: []openAIMessage{
			{Role: "system", Content: "You are a database type mapping expert. Respond with only the target type, no explanation."},
			{Role: "user", Content: prompt},
		},
		MaxTokens:   100,
		Temperature: 0, // Deterministic
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.config.APIKey)

	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var openAIResp openAIResponse
	if err := json.Unmarshal(body, &openAIResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if openAIResp.Error != nil {
		return "", fmt.Errorf("API error: %s", openAIResp.Error.Message)
	}

	if len(openAIResp.Choices) == 0 || openAIResp.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return openAIResp.Choices[0].Message.Content, nil
}

// Gemini API types
type geminiRequest struct {
	Contents         []geminiContent  `json:"contents"`
	GenerationConfig geminiGenConfig  `json:"generationConfig"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiGenConfig struct {
	MaxOutputTokens int     `json:"maxOutputTokens"`
	Temperature     float64 `json:"temperature"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (m *AITypeMapper) queryGeminiAPI(ctx context.Context, prompt string) (string, error) {
	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Parts: []geminiPart{
					{Text: prompt},
				},
			},
		},
		GenerationConfig: geminiGenConfig{
			MaxOutputTokens: 100,
			Temperature:     0, // Deterministic
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	// Gemini uses API key in URL query parameter
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s",
		m.config.Model, m.config.APIKey)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var geminiResp geminiResponse
	if err := json.Unmarshal(body, &geminiResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if geminiResp.Error != nil {
		return "", fmt.Errorf("API error: %s", geminiResp.Error.Message)
	}

	if len(geminiResp.Candidates) == 0 ||
		len(geminiResp.Candidates[0].Content.Parts) == 0 ||
		geminiResp.Candidates[0].Content.Parts[0].Text == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return geminiResp.Candidates[0].Content.Parts[0].Text, nil
}

// TypeMappingCache stores AI-generated type mappings.
type TypeMappingCache struct {
	mu       sync.RWMutex
	mappings map[string]string
}

// NewTypeMappingCache creates a new empty cache.
func NewTypeMappingCache() *TypeMappingCache {
	return &TypeMappingCache{
		mappings: make(map[string]string),
	}
}

// Get retrieves a cached mapping.
func (c *TypeMappingCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.mappings[key]
	return val, ok
}

// Set stores a mapping in the cache.
func (c *TypeMappingCache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mappings[key] = value
}

// All returns all cached mappings.
func (c *TypeMappingCache) All() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]string, len(c.mappings))
	for k, v := range c.mappings {
		result[k] = v
	}
	return result
}

// Load populates the cache from a map.
func (c *TypeMappingCache) Load(mappings map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range mappings {
		c.mappings[k] = v
	}
}

func (m *AITypeMapper) loadCache() error {
	data, err := os.ReadFile(m.config.CacheFile)
	if os.IsNotExist(err) {
		return nil // No cache file yet
	}
	if err != nil {
		return fmt.Errorf("reading cache file: %w", err)
	}

	var mappings map[string]string
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("parsing cache file: %w", err)
	}

	m.cache.Load(mappings)
	logging.Info("Loaded %d AI type mappings from cache", len(mappings))
	return nil
}

func (m *AITypeMapper) saveCache() error {
	// Ensure directory exists
	dir := filepath.Dir(m.config.CacheFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}

	if err := os.WriteFile(m.config.CacheFile, data, 0600); err != nil {
		return fmt.Errorf("writing cache file: %w", err)
	}

	return nil
}

// CacheSize returns the number of cached mappings.
func (m *AITypeMapper) CacheSize() int {
	return len(m.cache.All())
}

// ClearCache removes all cached mappings.
func (m *AITypeMapper) ClearCache() error {
	m.cacheMu.Lock()
	m.cache = NewTypeMappingCache()
	m.cacheMu.Unlock()

	// Remove cache file
	if err := os.Remove(m.config.CacheFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing cache file: %w", err)
	}
	return nil
}

// ExportCache exports cached mappings for review or sharing.
func (m *AITypeMapper) ExportCache(w io.Writer) error {
	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}
	_, err = w.Write(data)
	return err
}

// CallAI sends a prompt to the configured AI provider and returns the response.
// This is a generic method for arbitrary prompts (not just type mapping).
func (m *AITypeMapper) CallAI(ctx context.Context, prompt string) (string, error) {
	// Serialize API requests to avoid rate limiting
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	// Use provided context or create one with timeout
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(m.config.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	var result string
	var err error

	switch AIProvider(m.config.Provider) {
	case ProviderClaude:
		result, err = m.queryClaudeAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt)
	case ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	default:
		return "", fmt.Errorf("unsupported AI provider: %s", m.config.Provider)
	}

	if err != nil {
		return "", err
	}

	return result, nil
}
