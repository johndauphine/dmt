package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Anthropic API types
type anthropicRequest struct {
	Model       string             `json:"model"`
	MaxTokens   int                `json:"max_tokens"`
	Temperature *float64           `json:"temperature,omitempty"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (m *AITypeMapper) queryAnthropicAPI(ctx context.Context, prompt string) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerName)

	// Detect if this is a type mapping query (short, simple) vs a complex query.
	// DDL generation prompts need raw SQL output, while runtime controller/smart config
	// prompts need structured JSON output. Use prompt content to distinguish.
	maxTokens := 1024
	systemPrompt := ""
	if len(prompt) > 500 {
		maxTokens = 4096
		upperPrompt := strings.ToUpper(prompt[:min(len(prompt), 200)])
		isDDL := strings.Contains(upperPrompt, "CREATE TABLE") ||
			strings.Contains(upperPrompt, "CREATE INDEX") ||
			strings.Contains(upperPrompt, "ALTER TABLE") ||
			strings.Contains(upperPrompt, "DROP TABLE")
		if isDDL {
			systemPrompt = "You are a database migration expert. Return ONLY the raw SQL statement. No JSON, no markdown, no explanation."
		} else {
			systemPrompt = "You are a database migration tuning assistant. Return ONLY valid JSON. No markdown fences, no explanation outside the JSON."
		}
	}

	reqBody := newAnthropicRequest(model, maxTokens, systemPrompt, prompt)

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

	var anthropicResp anthropicResponse
	if err := json.Unmarshal(body, &anthropicResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if anthropicResp.Error != nil {
		return "", fmt.Errorf("API error: %s", anthropicResp.Error.Message)
	}

	text := anthropicResponseText(anthropicResp)
	if text == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return text, nil
}

func newAnthropicRequest(model string, maxTokens int, systemPrompt, prompt string) anthropicRequest {
	reqBody := anthropicRequest{
		Model:     model,
		MaxTokens: maxTokens,
		System:    systemPrompt,
		Messages: []anthropicMessage{
			{Role: "user", Content: prompt},
		},
	}

	if !anthropicRejectsNonDefaultSampling(model) {
		temperature := 0.0
		reqBody.Temperature = &temperature
	}

	return reqBody
}

func anthropicRejectsNonDefaultSampling(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	for _, family := range []string{
		"claude-sonnet-5",
		"claude-opus-4-7",
		"claude-opus-4-8",
		"claude-fable-5",
		"claude-mythos-5",
	} {
		if model == family || strings.HasPrefix(model, family+"-") {
			return true
		}
	}
	return false
}

func anthropicResponseText(resp anthropicResponse) string {
	var parts []string
	for _, content := range resp.Content {
		if content.Text == "" {
			continue
		}
		if content.Type != "" && content.Type != "text" {
			continue
		}
		parts = append(parts, content.Text)
	}
	return strings.Join(parts, "\n")
}
