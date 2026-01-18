package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
)

// Retry configuration constants
const (
	// defaultMaxRetries is the default number of retry attempts for transient failures.
	defaultMaxRetries = 3

	// defaultBaseDelay is the initial delay between retries.
	defaultBaseDelay = 1 * time.Second

	// defaultMaxDelay is the maximum delay between retries (cap for exponential backoff).
	defaultMaxDelay = 10 * time.Second
)

func init() {
	// Seed the random number generator for jitter in backoff calculations.
	// Go 1.20+ seeds automatically, but this ensures compatibility with older versions.
	rand.Seed(time.Now().UnixNano())
}

// AIProvider represents supported AI providers for type mapping.
type AIProvider string

const (
	// ProviderClaude uses Anthropic's Claude API.
	ProviderClaude AIProvider = "claude"
	// ProviderOpenAI uses OpenAI's API.
	ProviderOpenAI AIProvider = "openai"
	// ProviderGemini uses Google's Gemini API.
	ProviderGemini AIProvider = "gemini"
	// ProviderOllama uses local Ollama with OpenAI-compatible API.
	ProviderOllama AIProvider = "ollama"
	// ProviderLMStudio uses local LM Studio with OpenAI-compatible API.
	ProviderLMStudio AIProvider = "lmstudio"
)

// ValidAIProviders returns the list of supported AI provider names.
func ValidAIProviders() []string {
	return []string{
		string(ProviderClaude),
		string(ProviderOpenAI),
		string(ProviderGemini),
		string(ProviderOllama),
		string(ProviderLMStudio),
	}
}

// IsValidAIProvider returns true if the provider name is valid (case-insensitive).
func IsValidAIProvider(provider string) bool {
	switch AIProvider(strings.ToLower(provider)) {
	case ProviderClaude, ProviderOpenAI, ProviderGemini, ProviderOllama, ProviderLMStudio:
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

// AITypeMapper uses AI to map database types.
// It implements the TypeMapper interface.
type AITypeMapper struct {
	providerName   string
	provider       *secrets.Provider
	client         *http.Client
	cache          *TypeMappingCache
	cacheFile      string
	cacheMu        sync.RWMutex
	requestsMu     sync.Mutex // Serialize API requests to avoid rate limiting
	inflight       sync.Map   // Track in-flight requests to avoid duplicate API calls
	timeoutSeconds int
}

// inflightRequest tracks an in-progress API request for a specific type.
type inflightRequest struct {
	done   chan struct{}
	result string
	err    error
}

// NewAITypeMapper creates a new AI-powered type mapper using the secrets configuration.
func NewAITypeMapper(providerName string, provider *secrets.Provider) (*AITypeMapper, error) {
	if provider == nil {
		return nil, fmt.Errorf("AI provider configuration is required")
	}

	// Validate cloud providers have API key
	if !secrets.IsLocalProvider(providerName) && provider.APIKey == "" {
		return nil, fmt.Errorf("AI provider %q requires an API key", providerName)
	}

	// Get effective model
	model := provider.GetEffectiveModel(providerName)
	if model == "" {
		return nil, fmt.Errorf("no model specified for provider %q", providerName)
	}

	// Set up cache file
	homeDir, _ := os.UserHomeDir()
	cacheFile := filepath.Join(homeDir, ".dmt", "type-cache.json")

	mapper := &AITypeMapper{
		providerName: providerName,
		provider:     provider,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: 30,
	}

	// Load existing cache
	if err := mapper.loadCache(); err != nil {
		logging.Warn("Failed to load AI type mapping cache: %v", err)
	}

	return mapper, nil
}

// NewAITypeMapperFromSecrets creates an AI type mapper from the global secrets configuration.
func NewAITypeMapperFromSecrets() (*AITypeMapper, error) {
	config, err := secrets.Load()
	if err != nil {
		return nil, fmt.Errorf("loading secrets: %w", err)
	}

	provider, name, err := config.GetDefaultProvider()
	if err != nil {
		return nil, fmt.Errorf("getting default AI provider: %w", err)
	}

	return NewAITypeMapper(name, provider)
}

// MapType maps a source type to the target type using AI.
// This method is safe to call concurrently - it uses in-flight request tracking
// to avoid duplicate API calls for the same type.
// Note: For table-level DDL generation, use GenerateTableDDL instead.
// This method panics on error - use MapTypeWithError for error handling.
func (m *AITypeMapper) MapType(info TypeInfo) string {
	result, err := m.MapTypeWithError(info)
	if err != nil {
		panic(fmt.Sprintf("AI type mapping failed for %s.%s: %v", info.SourceDBType, info.DataType, err))
	}
	return result
}

// MapTypeWithError maps a source type to the target type using AI, returning any error.
func (m *AITypeMapper) MapTypeWithError(info TypeInfo) (string, error) {
	// Validate input
	if info.DataType == "" {
		return "", fmt.Errorf("DataType is required")
	}
	if info.SourceDBType == "" {
		return "", fmt.Errorf("SourceDBType is required")
	}
	if info.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	cacheKey := m.cacheKey(info)

	// Check cache first (fast path)
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached, nil
	}
	m.cacheMu.RUnlock()

	// Check if there's already an in-flight request for this key
	req := &inflightRequest{done: make(chan struct{})}
	if existing, loaded := m.inflight.LoadOrStore(cacheKey, req); loaded {
		// Another goroutine is already fetching this type, wait for it
		existingReq := existing.(*inflightRequest)
		<-existingReq.done
		if existingReq.err != nil {
			return "", existingReq.err
		}
		return existingReq.result, nil
	}

	// We're the first to request this type, do the API call
	defer func() {
		close(req.done) // Signal waiting goroutines
		m.inflight.Delete(cacheKey)
	}()

	// Double-check cache after acquiring the slot
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		req.result = cached
		return cached, nil
	}
	m.cacheMu.RUnlock()

	// Log that we're calling AI (not in cache)
	logging.Debug("AI type mapping: %s.%s -> %s (not cached, calling API)", info.SourceDBType, info.DataType, info.TargetDBType)

	// Call AI API
	result, err := m.queryAI(info)
	if err != nil {
		req.err = err
		return "", fmt.Errorf("AI type mapping failed for %s.%s -> %s: %w",
			info.SourceDBType, info.DataType, info.TargetDBType, err)
	}

	// Cache the result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, result)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI type mapping cache: %v", err)
	}

	logging.Debug("AI mapped %s.%s -> %s.%s (cached for future use)",
		info.SourceDBType, info.DataType, info.TargetDBType, result)

	req.result = result
	return result, nil
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

func (m *AITypeMapper) queryAI(info TypeInfo) (string, error) {
	// Serialize API requests to avoid rate limiting
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	prompt := m.buildPrompt(info)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.timeoutSeconds)*time.Second)
	defer cancel()

	var result string
	var err error

	switch AIProvider(m.providerName) {
	case ProviderClaude:
		result, err = m.queryClaudeAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		// Try OpenAI-compatible API for unknown providers with base_url
		if m.provider.BaseURL != "" {
			result, err = m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		} else {
			return "", fmt.Errorf("unsupported AI provider: %s", m.providerName)
		}
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
		nonDigits := len(value) - digitCount
		if nonDigits <= 4 {
			value = "[PHONE]"
		}
	}

	return value
}

func (m *AITypeMapper) buildPrompt(info TypeInfo) string {
	var sb strings.Builder
	sb.WriteString("You are a database type mapping expert.\n\n")
	sb.WriteString("Based on DDL metadata only (no sample data), ")
	sb.WriteString(fmt.Sprintf("map this %s type to %s:\n", info.SourceDBType, info.TargetDBType))
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

	// Sample values are no longer collected (privacy improvement)
	// Type mapping now works purely from DDL metadata

	// Add target database context
	switch info.TargetDBType {
	case "postgres":
		sb.WriteString("\nTarget: Standard PostgreSQL (no extensions installed).\n")
	case "mssql":
		sb.WriteString("\nTarget: SQL Server with full native type support.\n")
		if info.SourceDBType == "postgres" && (strings.HasPrefix(strings.ToLower(info.DataType), "varchar") ||
			strings.HasPrefix(strings.ToLower(info.DataType), "char") ||
			strings.ToLower(info.DataType) == "text") {
			sb.WriteString("Note: PostgreSQL string types store characters. SQL Server varchar stores bytes, nvarchar stores characters.\n")
		}
	case "mysql":
		sb.WriteString("\nTarget: MySQL 8.0+ or MariaDB 10.5+ with InnoDB engine.\n")
		sb.WriteString("Note: MySQL varchar has 65535 byte max (use TEXT for longer). Use utf8mb4 charset.\n")
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

// sanitizeErrorResponse truncates and sanitizes API error response bodies.
func sanitizeErrorResponse(body []byte, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 200
	}

	s := string(body)
	if len(s) > maxLen {
		s = s[:maxLen] + "..."
	}

	keyPatterns := []string{"sk-", "api-", "key-", "secret-", "token-"}
	for _, pattern := range keyPatterns {
		for {
			idx := strings.Index(strings.ToLower(s), pattern)
			if idx == -1 {
				break
			}
			endIdx := idx + len(pattern) + 40
			if endIdx > len(s) {
				endIdx = len(s)
			}
			s = s[:idx] + "[REDACTED]" + s[endIdx:]
		}
	}

	return s
}

// isRetryableError determines if an error is transient and should be retried.
// Returns true for network timeouts, temporary network errors, connection errors,
// server errors (5xx), and rate limiting responses (429).
func isRetryableError(err error, statusCode int) bool {
	// Check for retryable HTTP status codes (server errors, rate limiting)
	if statusCode >= 500 || statusCode == 429 {
		return true
	}

	if err == nil {
		return false
	}

	// Check for context deadline exceeded (timeout)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for EOF errors
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network errors (timeout or temporary)
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Retry on timeout or temporary network errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		return netErr.Timeout() || netErr.Temporary()
	}

	// Check for connection errors - only retry on temporary or dial errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Only retry dial errors (connection refused, etc.) or temporary errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		if opErr.Op == "dial" || opErr.Temporary() {
			return true
		}
		return false
	}

	// Check for common retryable error messages (fallback for wrapped errors)
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"tls handshake timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"no such host",
		"temporary failure",
		"i/o timeout",
		"unexpected eof", // Fallback for wrapped EOF errors
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// retryableHTTPDo executes an HTTP request with exponential backoff retry logic.
// It retries on transient network errors and server errors (5xx, 429).
func (m *AITypeMapper) retryableHTTPDo(ctx context.Context, reqFunc func() (*http.Request, error)) (*http.Response, []byte, error) {
	var lastErr error
	var lastStatusCode int

	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		// Check context before each attempt
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		// Create fresh request for each attempt
		req, err := reqFunc()
		if err != nil {
			return nil, nil, fmt.Errorf("creating request: %w", err)
		}

		// Execute request
		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = err
			lastStatusCode = 0

			if !isRetryableError(err, 0) {
				return nil, nil, fmt.Errorf("API request failed: %w", err)
			}

			// Log retry attempt
			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API request failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, err, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Read response body
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			lastErr = readErr

			// Only retry if the read error is retryable
			if !isRetryableError(readErr, 0) {
				return nil, nil, fmt.Errorf("reading response body: %w", readErr)
			}

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API response read failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, readErr, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		lastStatusCode = resp.StatusCode

		// Check for retryable status codes
		if isRetryableError(nil, resp.StatusCode) {
			lastErr = fmt.Errorf("API returned status %d", resp.StatusCode)

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API returned status %d (attempt %d/%d), retrying in %v",
					resp.StatusCode, attempt+1, defaultMaxRetries+1, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Success or non-retryable error
		return resp, body, nil
	}

	// All retries exhausted
	if lastErr != nil {
		return nil, nil, fmt.Errorf("API request failed after %d attempts: %w", defaultMaxRetries+1, lastErr)
	}
	return nil, nil, fmt.Errorf("API request failed after %d attempts (status %d)", defaultMaxRetries+1, lastStatusCode)
}

// calculateBackoff returns the delay for a given retry attempt using exponential backoff with jitter.
func calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: baseDelay * 2^attempt
	delay := defaultBaseDelay * time.Duration(1<<attempt)

	// Cap at max delay
	if delay > defaultMaxDelay {
		delay = defaultMaxDelay
	}

	// Add jitter (±25% randomization to prevent thundering herd)
	jitter := time.Duration(rand.Int63n(int64(delay) / 2))
	delay = delay - delay/4 + jitter

	return delay
}

func (m *AITypeMapper) queryClaudeAPI(ctx context.Context, prompt string) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerName)
	reqBody := claudeRequest{
		Model:     model,
		MaxTokens: 1024,
		Messages: []claudeMessage{
			{Role: "user", Content: prompt},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", m.provider.APIKey)
		req.Header.Set("anthropic-version", "2023-06-01")
		return req, nil
	})
	if err != nil {
		return "", err
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
	Model       string                 `json:"model"`
	Messages    []openAIMessage        `json:"messages"`
	MaxTokens   int                    `json:"max_tokens"`
	Temperature float64                `json:"temperature"`
	Options     map[string]interface{} `json:"options,omitempty"` // Provider-specific options (e.g., Ollama's num_ctx for context window size)
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

func (m *AITypeMapper) queryOpenAIAPI(ctx context.Context, prompt string, url string) (string, error) {
	return m.queryOpenAIAPIWithTokens(ctx, prompt, url, 100)
}

// queryOpenAIAPIWithTokens queries OpenAI API with configurable max tokens.
func (m *AITypeMapper) queryOpenAIAPIWithTokens(ctx context.Context, prompt string, url string, maxTokens int) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerName)

	// Detect if this is a type mapping query (short, simple) vs general AI query (long, complex)
	systemMsg := "You are a helpful AI assistant."
	isTypeMapping := len(prompt) < 500 && maxTokens <= 100
	if isTypeMapping {
		systemMsg = "You are a database type mapping expert. Respond with only the target type, no explanation."
	} else {
		// For complex queries with OpenAI, use more tokens (costs more but necessary)
		if maxTokens <= 100 {
			maxTokens = 4000 // Reasonable default for analysis tasks
		}
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxTokens:   maxTokens,
		Temperature: 0,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+m.provider.APIKey)
		return req, nil
	})
	if err != nil {
		return "", err
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

// queryOpenAICompatAPI queries local providers using OpenAI-compatible API (no auth required).
func (m *AITypeMapper) queryOpenAICompatAPI(ctx context.Context, prompt string, url string) (string, error) {
	return m.queryOpenAICompatAPIWithTokens(ctx, prompt, url, 100)
}

// queryOpenAICompatAPIWithTokens queries local providers with configurable max tokens.
func (m *AITypeMapper) queryOpenAICompatAPIWithTokens(ctx context.Context, prompt string, url string, maxTokens int) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerName)

	// Detect if this is a type mapping query (short, simple) vs general AI query (long, complex)
	systemMsg := "You are a helpful AI assistant."
	isTypeMapping := len(prompt) < 500 && maxTokens <= 100
	if isTypeMapping {
		systemMsg = "You are a database type mapping expert. Respond with only the target type, no explanation."
	}

	// For Ollama (local, free), use larger token budget for complex queries
	if AIProvider(m.providerName) == ProviderOllama && !isTypeMapping {
		maxTokens = 16000 // Allow longer responses for detailed analysis/recommendations
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxTokens:   maxTokens,
		Temperature: 0,
	}

	// For Ollama, set context window from configuration (or use conservative default)
	if AIProvider(m.providerName) == ProviderOllama {
		contextWindow := m.provider.GetEffectiveContextWindow()
		reqBody.Options = map[string]interface{}{
			"num_ctx": contextWindow, // Use configured context window (default: 8192)
		}
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	providerName := m.providerName // capture for closure

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		// No Authorization header for local providers
		return req, nil
	})
	if err != nil {
		return "", fmt.Errorf("API request failed (is %s running?): %w", providerName, err)
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
	Contents         []geminiContent `json:"contents"`
	GenerationConfig geminiGenConfig `json:"generationConfig"`
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
			Temperature:     0,
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	model := m.provider.GetEffectiveModel(m.providerName)
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", model)

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-goog-api-key", m.provider.APIKey)
		return req, nil
	})
	if err != nil {
		return "", err
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
	data, err := os.ReadFile(m.cacheFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading cache file: %w", err)
	}

	var mappings map[string]string
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("parsing cache file: %w", err)
	}

	m.cache.Load(mappings)
	logging.Debug("Loaded %d AI type mappings from cache", len(mappings))
	return nil
}

func (m *AITypeMapper) saveCache() error {
	dir := filepath.Dir(m.cacheFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}

	if err := os.WriteFile(m.cacheFile, data, 0600); err != nil {
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

	if err := os.Remove(m.cacheFile); err != nil && !os.IsNotExist(err) {
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
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(m.timeoutSeconds)*time.Second)
		defer cancel()
	}

	var result string
	var err error

	switch AIProvider(m.providerName) {
	case ProviderClaude:
		result, err = m.queryClaudeAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		if m.provider.BaseURL != "" {
			result, err = m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		} else {
			return "", fmt.Errorf("unsupported AI provider: %s", m.providerName)
		}
	}

	return result, err
}

// ProviderName returns the name of the configured provider.
func (m *AITypeMapper) ProviderName() string {
	return m.providerName
}

// Model returns the model being used.
func (m *AITypeMapper) Model() string {
	return m.provider.GetEffectiveModel(m.providerName)
}

// GenerateTableDDL generates complete CREATE TABLE DDL for the target database.
// This method provides full table context to the AI for smarter type mapping decisions.
func (m *AITypeMapper) GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error) {
	if req.SourceTable == nil {
		return nil, fmt.Errorf("SourceTable is required")
	}
	if req.SourceDBType == "" {
		return nil, fmt.Errorf("SourceDBType is required")
	}
	if req.TargetDBType == "" {
		return nil, fmt.Errorf("TargetDBType is required")
	}

	// Build cache key based on table structure
	cacheKey := m.tableCacheKey(req)

	// Check cache first
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return m.parseTableDDLFromCache(cached, req.SourceTable)
	}
	m.cacheMu.RUnlock()

	// Build the prompt with full table context
	prompt := m.buildTableDDLPrompt(req)

	logging.Debug("AI table DDL generation: %s.%s (%s -> %s)",
		req.SourceTable.Schema, req.SourceTable.Name, req.SourceDBType, req.TargetDBType)

	// Call AI API
	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI table DDL generation failed for %s.%s: %w",
			req.SourceTable.Schema, req.SourceTable.Name, err)
	}

	// Parse the response to extract DDL
	response, err := m.parseTableDDLResponse(result, req.SourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AI response for %s.%s: %w",
			req.SourceTable.Schema, req.SourceTable.Name, err)
	}

	// Cache the raw DDL result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, response.CreateTableDDL)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI table DDL cache: %v", err)
	}

	logging.Debug("AI generated DDL for %s.%s (%d columns mapped)",
		req.SourceTable.Schema, req.SourceTable.Name, len(response.ColumnTypes))

	return response, nil
}

// tableCacheKey generates a cache key for table-level DDL.
// Uses a hash of the table structure to handle schema changes.
func (m *AITypeMapper) tableCacheKey(req TableDDLRequest) string {
	// Build a deterministic representation of the table structure
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("table:%s:%s:%s.%s:",
		req.SourceDBType, req.TargetDBType, req.SourceTable.Schema, req.SourceTable.Name))

	for _, col := range req.SourceTable.Columns {
		sb.WriteString(fmt.Sprintf("%s:%s:%d:%d:%d:%v;",
			col.Name, col.DataType, col.MaxLength, col.Precision, col.Scale, col.IsNullable))
	}

	// Add PK info
	sb.WriteString("pk:")
	for _, pk := range req.SourceTable.PrimaryKey {
		sb.WriteString(pk + ",")
	}

	return sb.String()
}

// buildTableDDLPrompt creates the AI prompt for table-level DDL generation.
func (m *AITypeMapper) buildTableDDLPrompt(req TableDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a CREATE TABLE statement.\n\n")

	// === SOURCE DATABASE CONTEXT ===
	sb.WriteString("=== SOURCE DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.SourceDBType))
	if req.SourceContext != nil {
		m.writeContextDetails(&sb, req.SourceContext, "Source")
	}
	sb.WriteString("\n")

	// === TARGET DATABASE CONTEXT ===
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		m.writeContextDetails(&sb, req.TargetContext, "Target")
	}
	sb.WriteString("\n")

	// === SOURCE TABLE DDL ===
	sb.WriteString("=== SOURCE TABLE DDL ===\n")
	sb.WriteString(m.buildSourceDDL(req.SourceTable, req.SourceDBType))
	sb.WriteString("\n\n")

	// === MIGRATION RULES ===
	sb.WriteString("=== MIGRATION RULES ===\n")
	m.writeMigrationRules(&sb, req)

	// === OUTPUT REQUIREMENTS ===
	sb.WriteString("\n=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete CREATE TABLE statement for the target database.\n")
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("- Use fully qualified table name: %s.<TABLENAME>\n", req.TargetSchema))
	}
	sb.WriteString("- Include all columns with appropriate target types\n")
	sb.WriteString("- Make ALL non-primary-key columns nullable (omit NOT NULL) to allow data migration flexibility\n")
	sb.WriteString("- Primary key columns must be NOT NULL\n")
	sb.WriteString("- Include PRIMARY KEY constraint\n")
	sb.WriteString("- Do NOT include foreign keys (created separately in Finalize)\n")
	sb.WriteString("- Do NOT include indexes (created separately in Finalize)\n")
	sb.WriteString("- Do NOT include CHECK constraints (created separately in Finalize)\n")
	sb.WriteString("- Return ONLY the CREATE TABLE statement, no explanation or markdown\n")

	// Check for reserved words in source table columns
	reservedWords := m.findReservedWords(req.SourceTable, req.TargetDBType)
	if len(reservedWords) > 0 {
		sb.WriteString("\nWARNING: The following source columns are reserved words in the target database:\n")
		for _, rw := range reservedWords {
			switch req.TargetDBType {
			case "oracle":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as \"%s\"\n", rw, strings.ToUpper(rw)))
			case "mssql":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as [%s]\n", rw, rw))
			case "mysql":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as `%s`\n", rw, rw))
			case "postgres":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as \"%s\"\n", rw, strings.ToLower(rw)))
			}
		}
	}

	return sb.String()
}

// writeContextDetails writes database context details to the prompt.
func (m *AITypeMapper) writeContextDetails(sb *strings.Builder, ctx *DatabaseContext, label string) {
	if ctx.Version != "" {
		sb.WriteString(fmt.Sprintf("Version: %s\n", ctx.Version))
	}
	if ctx.DatabaseName != "" {
		sb.WriteString(fmt.Sprintf("Database: %s\n", ctx.DatabaseName))
	}

	// Character encoding section
	sb.WriteString("Character Encoding:\n")
	if ctx.Charset != "" {
		sb.WriteString(fmt.Sprintf("  Charset: %s\n", ctx.Charset))
	}
	if ctx.NationalCharset != "" {
		sb.WriteString(fmt.Sprintf("  National Charset: %s\n", ctx.NationalCharset))
	}
	if ctx.Encoding != "" {
		sb.WriteString(fmt.Sprintf("  Encoding: %s\n", ctx.Encoding))
	}
	if ctx.CodePage > 0 {
		sb.WriteString(fmt.Sprintf("  Code Page: %d\n", ctx.CodePage))
	}
	if ctx.Collation != "" {
		sb.WriteString(fmt.Sprintf("  Collation: %s\n", ctx.Collation))
	}
	if ctx.BytesPerChar > 0 {
		sb.WriteString(fmt.Sprintf("  Max Bytes Per Char: %d\n", ctx.BytesPerChar))
	}

	// Case sensitivity section
	sb.WriteString("Case Sensitivity:\n")
	if ctx.IdentifierCase != "" {
		sb.WriteString(fmt.Sprintf("  Identifier Case: %s\n", ctx.IdentifierCase))
	}
	if ctx.CaseSensitiveIdentifiers {
		sb.WriteString("  Identifiers: case-sensitive\n")
	} else {
		sb.WriteString("  Identifiers: case-insensitive\n")
	}
	if ctx.CaseSensitiveData {
		sb.WriteString("  String Comparisons: case-sensitive\n")
	} else {
		sb.WriteString("  String Comparisons: case-insensitive (collation-dependent)\n")
	}

	// Limits section
	sb.WriteString("Limits:\n")
	if ctx.MaxIdentifierLength > 0 {
		sb.WriteString(fmt.Sprintf("  Max Identifier Length: %d\n", ctx.MaxIdentifierLength))
	}
	if ctx.MaxVarcharLength > 0 {
		sb.WriteString(fmt.Sprintf("  Max VARCHAR Length: %d\n", ctx.MaxVarcharLength))
	}
	if ctx.VarcharSemantics != "" {
		sb.WriteString(fmt.Sprintf("  VARCHAR Semantics: %s (lengths are in %ss)\n", ctx.VarcharSemantics, ctx.VarcharSemantics))
	}

	// Features section
	if ctx.StorageEngine != "" {
		sb.WriteString(fmt.Sprintf("Storage Engine: %s\n", ctx.StorageEngine))
	}
	if len(ctx.Features) > 0 {
		sb.WriteString(fmt.Sprintf("Features: %s\n", strings.Join(ctx.Features, ", ")))
	}
	if ctx.Notes != "" {
		sb.WriteString(fmt.Sprintf("Notes: %s\n", ctx.Notes))
	}
}

// writeMigrationRules writes migration guidance derived dynamically from database context.
// All rules are generated from runtime metadata - no hardcoded database-specific rules.
func (m *AITypeMapper) writeMigrationRules(sb *strings.Builder, req TableDDLRequest) {
	// Source database characteristics - derived from SourceContext
	sb.WriteString("Source database characteristics:\n")
	if req.SourceContext != nil {
		m.writeVarcharGuidance(sb, req.SourceContext, "source")
		m.writeEncodingGuidance(sb, req.SourceContext, "source")
	} else {
		sb.WriteString("- No source context available, using standard type semantics\n")
	}

	sb.WriteString("\n")

	// Target database rules - derived from TargetContext
	sb.WriteString("Target database rules:\n")
	if req.TargetContext != nil {
		m.writeVarcharGuidance(sb, req.TargetContext, "target")
		m.writeEncodingGuidance(sb, req.TargetContext, "target")
		m.writeIdentifierGuidance(sb, req.TargetContext)
		m.writeLimitsGuidance(sb, req.TargetContext)
	} else {
		sb.WriteString("- No target context available, use standard type mappings\n")
	}

	// Cross-database conversion guidance
	sb.WriteString("\nConversion guidance:\n")
	m.writeConversionGuidance(sb, req.SourceContext, req.TargetContext)

	// Reserved words note
	sb.WriteString("\nReserved words: If any column name is a SQL reserved word, quote it appropriately for the target database.\n")
}

// capitalizeFirst returns the string with its first character uppercased.
// This replaces the deprecated strings.Title function.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// writeVarcharGuidance writes VARCHAR semantics guidance based on context.
func (m *AITypeMapper) writeVarcharGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.VarcharSemantics == "" {
		return
	}

	if ctx.VarcharSemantics == "char" {
		sb.WriteString(fmt.Sprintf("- %s VARCHAR lengths are in CHARACTERS\n", capitalizeFirst(role)))
	} else if ctx.VarcharSemantics == "byte" {
		sb.WriteString(fmt.Sprintf("- %s VARCHAR lengths are in BYTES\n", capitalizeFirst(role)))
		if ctx.BytesPerChar > 1 {
			sb.WriteString(fmt.Sprintf("- Each character may take up to %d bytes\n", ctx.BytesPerChar))
		}
	}
}

// writeEncodingGuidance writes character encoding guidance based on context.
func (m *AITypeMapper) writeEncodingGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.Charset != "" {
		sb.WriteString(fmt.Sprintf("- Character set: %s\n", ctx.Charset))
	}
	if ctx.BytesPerChar > 0 {
		sb.WriteString(fmt.Sprintf("- Max bytes per character: %d\n", ctx.BytesPerChar))
	}
	if ctx.Encoding != "" && ctx.Encoding != ctx.Charset {
		sb.WriteString(fmt.Sprintf("- Encoding: %s\n", ctx.Encoding))
	}
}

// writeIdentifierGuidance writes identifier handling guidance based on context.
func (m *AITypeMapper) writeIdentifierGuidance(sb *strings.Builder, ctx *DatabaseContext) {
	if ctx.IdentifierCase != "" {
		switch strings.ToLower(ctx.IdentifierCase) {
		case "upper":
			sb.WriteString("- CRITICAL: Unquoted identifiers are folded to UPPERCASE\n")
			sb.WriteString("- Use UPPERCASE for all unquoted table and column names\n")
			sb.WriteString("- Only quote identifiers that are reserved words\n")
		case "lower":
			sb.WriteString("- CRITICAL: Unquoted identifiers are folded to lowercase\n")
			sb.WriteString("- Use lowercase for all table and column names (e.g., UserId -> userid, not user_id)\n")
			sb.WriteString("- Do NOT convert to snake_case - just lowercase the original name directly\n")
		case "preserve":
			sb.WriteString("- Identifier case is preserved as written\n")
		}
	}

	if ctx.CaseSensitiveIdentifiers {
		sb.WriteString("- Identifiers are case-sensitive when quoted\n")
	}
}

// writeLimitsGuidance writes database limits guidance based on context.
func (m *AITypeMapper) writeLimitsGuidance(sb *strings.Builder, ctx *DatabaseContext) {
	if ctx.MaxIdentifierLength > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum identifier length: %d characters\n", ctx.MaxIdentifierLength))
	}
	if ctx.MaxVarcharLength > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum VARCHAR length: %d\n", ctx.MaxVarcharLength))
		if ctx.VarcharSemantics == "byte" {
			sb.WriteString("- Use CLOB/TEXT equivalent for content exceeding max VARCHAR\n")
		}
	}
}

// writeConversionGuidance writes guidance for cross-database type conversion.
func (m *AITypeMapper) writeConversionGuidance(sb *strings.Builder, srcCtx, tgtCtx *DatabaseContext) {
	if srcCtx == nil || tgtCtx == nil {
		sb.WriteString("- Map types based on semantic equivalence\n")
		return
	}

	// VARCHAR semantics conversion
	if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "byte" {
		sb.WriteString("- CRITICAL: Source uses CHARACTER lengths, target uses BYTE lengths\n")
		if tgtCtx.BytesPerChar > 1 {
			sb.WriteString(fmt.Sprintf("- Multiply source VARCHAR lengths by %d for target, or use CHAR semantics if available\n", tgtCtx.BytesPerChar))
		}
		sb.WriteString("- If target supports CHAR semantics (e.g., VARCHAR2(n CHAR)), prefer that over byte multiplication\n")
	} else if srcCtx.VarcharSemantics == "byte" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Source uses BYTE lengths, target uses CHARACTER lengths\n")
		if srcCtx.BytesPerChar > 1 {
			sb.WriteString(fmt.Sprintf("- Source VARCHAR(n) with %d bytes/char = approximately n/%d characters\n", srcCtx.BytesPerChar, srcCtx.BytesPerChar))
		}
	} else if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Both source and target use CHARACTER lengths - preserve lengths directly\n")
	}

	// Case handling guidance
	if srcCtx.IdentifierCase != tgtCtx.IdentifierCase && tgtCtx.IdentifierCase != "" {
		switch strings.ToLower(tgtCtx.IdentifierCase) {
		case "upper":
			sb.WriteString("- Convert all identifiers to UPPERCASE for target\n")
		case "lower":
			sb.WriteString("- Convert all identifiers to lowercase for target\n")
		}
	}
}

// findReservedWords checks source table columns for SQL reserved words.
func (m *AITypeMapper) findReservedWords(t *Table, targetDBType string) []string {
	// Common SQL reserved words that cause issues
	reservedWords := map[string]bool{
		"date": true, "time": true, "timestamp": true, "year": true, "month": true, "day": true,
		"user": true, "order": true, "group": true, "table": true, "index": true, "key": true,
		"type": true, "name": true, "value": true, "size": true, "number": true, "level": true,
		"comment": true, "desc": true, "asc": true, "limit": true, "offset": true,
		"select": true, "insert": true, "update": true, "delete": true, "from": true, "where": true,
		"and": true, "or": true, "not": true, "null": true, "true": true, "false": true,
		"primary": true, "foreign": true, "references": true, "constraint": true,
		"create": true, "alter": true, "drop": true, "truncate": true,
		"row": true, "rows": true, "column": true, "schema": true, "database": true,
		"function": true, "procedure": true, "trigger": true, "view": true,
		"id": false, // not reserved in most DBs
	}

	// Oracle-specific reserved words
	if targetDBType == "oracle" {
		reservedWords["uid"] = true
		reservedWords["sysdate"] = true
		reservedWords["rownum"] = true
		reservedWords["rowid"] = true
		reservedWords["access"] = true
		reservedWords["file"] = true
		reservedWords["long"] = true
		reservedWords["raw"] = true
		reservedWords["session"] = true
		reservedWords["start"] = true
	}

	var found []string
	for _, col := range t.Columns {
		colLower := strings.ToLower(col.Name)
		if reservedWords[colLower] {
			found = append(found, col.Name)
		}
	}
	return found
}

// buildSourceDDL creates a DDL-like representation of the source table.
func (m *AITypeMapper) buildSourceDDL(t *Table, sourceDBType string) string {
	var sb strings.Builder

	tableName := t.Name
	if t.Schema != "" {
		tableName = t.Schema + "." + t.Name
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	for i, col := range t.Columns {
		sb.WriteString("    ")
		sb.WriteString(col.Name)
		sb.WriteString(" ")

		// Build type with length/precision
		typeStr := col.DataType
		if col.MaxLength > 0 {
			typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.MaxLength)
		} else if col.MaxLength == -1 {
			typeStr = fmt.Sprintf("%s(MAX)", col.DataType)
		} else if col.Precision > 0 {
			if col.Scale > 0 {
				typeStr = fmt.Sprintf("%s(%d,%d)", col.DataType, col.Precision, col.Scale)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.Precision)
			}
		}
		sb.WriteString(typeStr)

		// NULL constraint
		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		}

		// Identity
		if col.IsIdentity {
			switch sourceDBType {
			case "postgres":
				sb.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
			case "mssql":
				sb.WriteString(" IDENTITY")
			case "mysql":
				sb.WriteString(" AUTO_INCREMENT")
			}
		}

		if i < len(t.Columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	// Primary key
	if len(t.PrimaryKey) > 0 {
		sb.WriteString(fmt.Sprintf("    ,PRIMARY KEY (%s)\n", strings.Join(t.PrimaryKey, ", ")))
	}

	sb.WriteString(");")

	return sb.String()
}

// parseTableDDLResponse extracts the DDL and column types from AI response.
func (m *AITypeMapper) parseTableDDLResponse(response string, sourceTable *Table) (*TableDDLResponse, error) {
	// Clean up the response
	ddl := strings.TrimSpace(response)

	// Remove markdown code blocks if present
	ddl = strings.TrimPrefix(ddl, "```sql")
	ddl = strings.TrimPrefix(ddl, "```")
	ddl = strings.TrimSuffix(ddl, "```")
	ddl = strings.TrimSpace(ddl)

	// Basic validation - should start with CREATE TABLE
	upperDDL := strings.ToUpper(ddl)
	if !strings.HasPrefix(upperDDL, "CREATE TABLE") {
		return nil, fmt.Errorf("response does not contain valid CREATE TABLE statement: %s", truncateString(ddl, 100))
	}

	// Extract column types from DDL for reference
	columnTypes := m.extractColumnTypesFromDDL(ddl, sourceTable)

	return &TableDDLResponse{
		CreateTableDDL: ddl,
		ColumnTypes:    columnTypes,
		Notes:          "",
	}, nil
}

// parseTableDDLFromCache creates a response from cached DDL.
func (m *AITypeMapper) parseTableDDLFromCache(cachedDDL string, sourceTable *Table) (*TableDDLResponse, error) {
	columnTypes := m.extractColumnTypesFromDDL(cachedDDL, sourceTable)

	return &TableDDLResponse{
		CreateTableDDL: cachedDDL,
		ColumnTypes:    columnTypes,
		Notes:          "(from cache)",
	}, nil
}

// extractColumnTypesFromDDL attempts to extract column name -> type mappings from DDL.
// This is best-effort for logging/debugging purposes.
func (m *AITypeMapper) extractColumnTypesFromDDL(ddl string, sourceTable *Table) map[string]string {
	columnTypes := make(map[string]string)

	// Simple extraction: look for each source column name in the DDL
	for _, col := range sourceTable.Columns {
		// Look for patterns like: column_name TYPE or "column_name" TYPE
		patterns := []string{
			col.Name + " ",
			col.Name + "\t",
			`"` + col.Name + `" `,
			`"` + col.Name + `"	`,
			strings.ToUpper(col.Name) + " ",
			strings.ToLower(col.Name) + " ",
		}

		for _, pattern := range patterns {
			idx := strings.Index(ddl, pattern)
			if idx >= 0 {
				// Extract the type after the column name
				start := idx + len(pattern)
				rest := ddl[start:]

				// Find end of type (comma, newline, or closing paren)
				end := strings.IndexAny(rest, ",\n)")
				if end > 0 {
					typeStr := strings.TrimSpace(rest[:end])
					// Remove NOT NULL, NULL, etc.
					typeStr = strings.Split(typeStr, " NOT ")[0]
					typeStr = strings.Split(typeStr, " NULL")[0]
					typeStr = strings.Split(typeStr, " DEFAULT")[0]
					typeStr = strings.TrimSpace(typeStr)
					if typeStr != "" {
						columnTypes[col.Name] = typeStr
					}
				}
				break
			}
		}
	}

	return columnTypes
}

// truncateString truncates a string to maxLen and adds "..." if truncated.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
