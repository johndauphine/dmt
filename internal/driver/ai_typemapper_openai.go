package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// OpenAI API types
type openAIRequest struct {
	Model               string                 `json:"model"`
	Messages            []openAIMessage        `json:"messages"`
	MaxCompletionTokens int                    `json:"max_completion_tokens,omitempty"`
	MaxTokens           int                    `json:"max_tokens,omitempty"`
	Temperature         float64                `json:"temperature"`
	Options             map[string]interface{} `json:"options,omitempty"` // Provider-specific options (e.g., Ollama's num_ctx for context window size)
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"` // Reasoning/thinking models (e.g., Qwen3)
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	// Error is stored as RawMessage so that both shapes are accepted:
	//   OpenAI/Anthropic style: {"error": {"message": "...", "type": "..."}}
	//   LM Studio style:        {"error": "..."}
	// Without this, a string-shaped error blows up the entire response unmarshal,
	// turning a meaningful provider error message into "cannot unmarshal string
	// into Go struct field openAIResponse.error".
	Error json.RawMessage `json:"error,omitempty"`
}

// ErrorMessage extracts a human-readable error message from openAIResponse.Error,
// handling both the struct shape ({"message": "..."}) used by OpenAI/Anthropic
// and the bare-string shape ("...") used by LM Studio. Returns "" if there is
// no error in the response. All non-empty results pass through
// sanitizeErrorResponse for length capping and API-key redaction, matching the
// treatment given to non-200 response bodies elsewhere in this file.
func (r *openAIResponse) ErrorMessage() string {
	// Trim whitespace so " null\n" and similar are recognized as "no error".
	trimmed := bytes.TrimSpace(r.Error)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	// Try struct shape first.
	var asStruct struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(trimmed, &asStruct); err == nil && asStruct.Message != "" {
		return sanitizeErrorResponse([]byte(asStruct.Message), 200)
	}
	// Fall back to string shape.
	var asString string
	if err := json.Unmarshal(trimmed, &asString); err == nil && asString != "" {
		return sanitizeErrorResponse([]byte(asString), 200)
	}
	// Unknown shape — surface the raw JSON so the user can at least see it,
	// but truncated and key-redacted like every other error surface.
	return sanitizeErrorResponse(trimmed, 200)
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
		// For complex queries, use the provider's configured max tokens
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxCompletionTokens: maxTokens,
		Temperature:         0,
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

	if msg := openAIResp.ErrorMessage(); msg != "" {
		return "", fmt.Errorf("API error: %s", msg)
	}

	if len(openAIResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from API")
	}

	content := openAIResp.Choices[0].Message.Content
	if content == "" {
		if openAIResp.Choices[0].Message.ReasoningContent != "" {
			return "", fmt.Errorf("model used all tokens on reasoning with no output — try increasing max_tokens or using a non-reasoning model")
		}
		return "", fmt.Errorf("empty response from API")
	}

	return content, nil
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

	// For complex queries, use the provider's configured max tokens.
	// Reasoning models (e.g., Qwen3) consume tokens on thinking before generating,
	// so they need significantly more headroom.
	if !isTypeMapping {
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxCompletionTokens: maxTokens,
		Temperature:         0,
	}

	// For local providers (Ollama/LMStudio), use max_tokens (older OpenAI-compatible API)
	if AIProvider(m.providerName) == ProviderOllama || AIProvider(m.providerName) == ProviderLMStudio {
		reqBody.MaxTokens = reqBody.MaxCompletionTokens
		reqBody.MaxCompletionTokens = 0
	}
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

	if msg := openAIResp.ErrorMessage(); msg != "" {
		return "", fmt.Errorf("API error: %s", msg)
	}

	if len(openAIResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from API")
	}

	content := openAIResp.Choices[0].Message.Content
	if content == "" {
		// Reasoning models (e.g., Qwen3) may put all output in reasoning_content
		// and leave content empty when max_tokens is too low for both thinking + output.
		if openAIResp.Choices[0].Message.ReasoningContent != "" {
			return "", fmt.Errorf("model used all tokens on reasoning with no output — try increasing max_tokens or using a non-reasoning model")
		}
		return "", fmt.Errorf("empty response from API")
	}

	return content, nil
}
