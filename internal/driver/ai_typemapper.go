package driver

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
)

// AIProvider represents supported AI providers for type mapping.
type AIProvider string

const (
	// ProviderAnthropic uses Anthropic's Claude API.
	ProviderAnthropic AIProvider = "anthropic"
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
		string(ProviderAnthropic),
		string(ProviderOpenAI),
		string(ProviderGemini),
		string(ProviderOllama),
		string(ProviderLMStudio),
	}
}

// IsValidAIProvider returns true if the provider name is valid (case-insensitive).
func IsValidAIProvider(provider string) bool {
	switch AIProvider(strings.ToLower(provider)) {
	case ProviderAnthropic, ProviderOpenAI, ProviderGemini, ProviderOllama, ProviderLMStudio:
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

	// Determine API timeout: user-configured > local provider default > cloud default.
	// Local providers and thinking models need more time for inference.
	timeoutSec := 60
	if IsLocalProvider(providerName) {
		timeoutSec = 120
	}
	if provider.TimeoutSeconds > 0 {
		timeoutSec = provider.TimeoutSeconds
	}

	mapper := &AITypeMapper{
		providerName: providerName,
		provider:     provider,
		client: &http.Client{
			Timeout: time.Duration(timeoutSec) * time.Second,
		},
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: timeoutSec,
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
	if cached, ok := m.cache.GetAI(cacheKey, m.providerName, m.Model()); ok {
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
	if cached, ok := m.cache.GetAI(cacheKey, m.providerName, m.Model()); ok {
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

	// Cache the result. Tagged with current model so `dmt cache clear
	// --ai-only` can invalidate after model upgrade (#177).
	m.cacheMu.Lock()
	m.cache.SetAIWithMetadata(cacheKey, result, m.providerName, m.Model())
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
	case ProviderAnthropic:
		result, err = m.queryAnthropicAPI(ctx, prompt)
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
	case ProviderAnthropic:
		result, err = m.queryAnthropicAPI(ctx, prompt)
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

// TimeoutSeconds returns the configured API timeout.
func (m *AITypeMapper) TimeoutSeconds() int {
	return m.timeoutSeconds
}

// IsLocalProvider returns true if the provider runs inference locally
// (Ollama or LMStudio) rather than calling a cloud API.
func IsLocalProvider(providerName string) bool {
	return providerName == string(ProviderOllama) || providerName == string(ProviderLMStudio)
}

// Model returns the model being used.
func (m *AITypeMapper) Model() string {
	return m.provider.GetEffectiveModel(m.providerName)
}
