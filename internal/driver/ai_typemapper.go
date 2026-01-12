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
)

// AITypeMappingConfig contains configuration for AI-assisted type mapping.
// This is a copy of config.AITypeMappingConfig to avoid import cycles.
type AITypeMappingConfig struct {
	Enabled        bool   // Enable AI type mapping
	Provider       string // "claude" or "openai"
	APIKey         string // API key (can use ${ENV_VAR} syntax)
	CacheFile      string // Path to cache file
	Model          string // Model to use (optional)
	TimeoutSeconds int    // API timeout
}

// AITypeMapper uses AI to map unknown database types.
// It implements the TypeMapper interface and can be used as a fallback
// in a ChainedTypeMapper for types not covered by static mappings.
type AITypeMapper struct {
	config     AITypeMappingConfig
	client     *http.Client
	cache      *TypeMappingCache
	cacheMu    sync.RWMutex
	requestsMu sync.Mutex // Serialize API requests to avoid rate limiting
}

// NewAITypeMapper creates a new AI-powered type mapper.
func NewAITypeMapper(config AITypeMappingConfig) (*AITypeMapper, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("AI type mapping is not enabled")
	}

	if config.APIKey == "" {
		return nil, fmt.Errorf("AI type mapping requires an API key")
	}

	// Expand environment variables in API key
	apiKey := os.ExpandEnv(config.APIKey)
	if apiKey == "" || apiKey == config.APIKey && strings.HasPrefix(config.APIKey, "${") {
		return nil, fmt.Errorf("AI API key environment variable not set")
	}
	config.APIKey = apiKey

	// Set defaults
	if config.TimeoutSeconds <= 0 {
		config.TimeoutSeconds = 30
	}
	if config.CacheFile == "" {
		homeDir, _ := os.UserHomeDir()
		config.CacheFile = filepath.Join(homeDir, ".mssql-pg-migrate", "type-cache.json")
	}
	config.CacheFile = os.ExpandEnv(config.CacheFile)

	// Set default models
	if config.Model == "" {
		switch AIProvider(config.Provider) {
		case ProviderClaude:
			config.Model = "claude-3-haiku-20240307"
		case ProviderOpenAI:
			config.Model = "gpt-4o-mini"
		}
	}

	mapper := &AITypeMapper{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(config.TimeoutSeconds) * time.Second,
		},
		cache: NewTypeMappingCache(),
	}

	// Load existing cache
	if err := mapper.loadCache(); err != nil {
		logging.Warn("Failed to load AI type mapping cache: %v", err)
	}

	return mapper, nil
}

// MapType uses AI to determine the target type for an unknown source type.
func (m *AITypeMapper) MapType(info TypeInfo) string {
	// Check cache first
	cacheKey := m.cacheKey(info)
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached
	}
	m.cacheMu.RUnlock()

	// Call AI API
	result, err := m.queryAI(info)
	if err != nil {
		logging.Warn("AI type mapping failed for %s: %v, using fallback", info.DataType, err)
		return m.fallback(info)
	}

	// Cache the result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, result)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI type mapping cache: %v", err)
	}

	logging.Info("AI mapped %s.%s -> %s.%s: %s",
		info.SourceDBType, info.DataType, info.TargetDBType, result, "(cached for future use)")

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
	sb.WriteString("\nReturn ONLY the equivalent ")
	sb.WriteString(info.TargetDBType)
	sb.WriteString(" type with appropriate size/precision.\n")
	sb.WriteString("Example: varchar(255) or numeric(10,2)\n")
	sb.WriteString("Do not include any explanation, just the type name.")

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

func (m *AITypeMapper) queryClaudeAPI(ctx context.Context, prompt string) (string, error) {
	reqBody := claudeRequest{
		Model:     m.config.Model,
		MaxTokens: 100,
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
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
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
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
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
