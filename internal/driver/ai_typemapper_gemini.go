package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

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
	// Short prompts (type mapping) need ~100 tokens; complex prompts
	// (DDL generation, smart config) need the provider's configured max.
	// Gemini 3+ models use thinking tokens, so they need more headroom.
	maxTokens := 100
	if len(prompt) > 500 {
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
		if maxTokens < 8192 {
			maxTokens = 8192 // Gemini thinking models need headroom
		}
	}

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Parts: []geminiPart{
					{Text: prompt},
				},
			},
		},
		GenerationConfig: geminiGenConfig{
			MaxOutputTokens: maxTokens,
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
