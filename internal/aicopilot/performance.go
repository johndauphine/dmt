package aicopilot

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

func GeneratePerformanceExplanation(ctx context.Context, client TextClient, payload PerformancePayload) (*PerformanceExplanation, error) {
	if IsNilTextClient(client) {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildPerformancePrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	explanation, err := ParsePerformanceExplanation(raw, payload)
	if err != nil {
		return nil, err
	}
	explanation.Enabled = true
	explanation.Status = ReviewStatusOK
	explanation.Provider = client.ProviderName()
	explanation.Model = client.Model()
	explanation.PromptVersion = PerformancePromptVersion
	if explanation.Summary == "" {
		explanation.Summary = deterministicPerformanceSummary(payload)
	}
	for i := range explanation.Findings {
		explanation.Findings[i].Source = "ai_advisory"
	}
	return explanation, nil
}

func UnavailablePerformanceExplanation(reason string, payload PerformancePayload) *PerformanceExplanation {
	reason = sanitizePerformanceDisplayText(logging.Scrub(reason))
	return &PerformanceExplanation{
		Enabled:       false,
		Status:        ReviewStatusUnavailable,
		PromptVersion: PerformancePromptVersion,
		Summary:       "AI performance explanation unavailable: " + reason + ". Deterministic tuning choices are unchanged.",
		Notes:         deterministicPerformanceNotes(payload),
	}
}

func ErrorPerformanceExplanation(provider, model string, err error, payload PerformancePayload) *PerformanceExplanation {
	errMsg := ""
	if err != nil {
		errMsg = sanitizePerformanceDisplayText(logging.Scrub(err.Error()))
	}
	return &PerformanceExplanation{
		Enabled:       true,
		Status:        ReviewStatusError,
		Provider:      provider,
		Model:         model,
		PromptVersion: PerformancePromptVersion,
		Summary:       "AI performance explanation failed. Deterministic tuning choices are unchanged.",
		Notes:         deterministicPerformanceNotes(payload),
		Error:         errMsg,
	}
}

func deterministicPerformanceSummary(payload PerformancePayload) string {
	if payload.DeterministicReasoning != "" {
		return limitText(sanitizePerformanceDisplayText(logging.Scrub(payload.DeterministicReasoning)), 800)
	}
	if payload.DeterministicTier != "" {
		return "Deterministic tuning selected the " + sanitizePerformanceDisplayText(logging.Scrub(payload.DeterministicTier)) + " tier."
	}
	return "Deterministic tuning choices are available in the payload."
}

func deterministicPerformanceNotes(payload PerformancePayload) []string {
	notes := []string{}
	if payload.DeterministicTier != "" {
		notes = append(notes, "tier: "+sanitizePerformanceDisplayText(logging.Scrub(payload.DeterministicTier)))
	}
	if payload.DeterministicReasoning != "" {
		notes = append(notes, "reasoning: "+limitText(sanitizePerformanceDisplayText(logging.Scrub(payload.DeterministicReasoning)), 400))
	}
	return notes
}
