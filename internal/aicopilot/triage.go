package aicopilot

import (
	"context"
	"fmt"
)

func GenerateTriageReview(ctx context.Context, client TextClient, payload TriagePayload) (*TriageReview, error) {
	if IsNilTextClient(client) {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildTriagePrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	review, err := ParseTriageReview(raw)
	if err != nil {
		return nil, err
	}
	review.Enabled = true
	review.Status = ReviewStatusOK
	review.Provider = client.ProviderName()
	review.Model = client.Model()
	review.PromptVersion = TriagePromptVersion
	review.Kind = normalizeTriageKind(payload.Kind)
	review.Impact = applyDeterministicTriageImpactFloor(review.Impact, deterministicTriageImpact(payload))
	review.DeterministicFacts = scrubTriageFacts(payload.DeterministicFacts)
	if review.Summary == "" {
		review.Summary = deterministicTriageSummary(payload)
	}
	for i := range review.Findings {
		review.Findings[i].Source = TriageFindingSourceAIAdvisory
	}
	return review, nil
}

func UnavailableTriageReview(reason string, payload TriagePayload) *TriageReview {
	return &TriageReview{
		Enabled:            false,
		Status:             ReviewStatusUnavailable,
		PromptVersion:      TriagePromptVersion,
		Kind:               normalizeTriageKind(payload.Kind),
		Impact:             deterministicTriageImpact(payload),
		Summary:            "AI triage unavailable: " + scrubTriageText(reason, 500) + ". Deterministic triage facts are unchanged.",
		DeterministicFacts: scrubTriageFacts(payload.DeterministicFacts),
	}
}

func ErrorTriageReview(provider, model string, err error, payload TriagePayload) *TriageReview {
	return &TriageReview{
		Enabled:            true,
		Status:             ReviewStatusError,
		Provider:           provider,
		Model:              model,
		PromptVersion:      TriagePromptVersion,
		Kind:               normalizeTriageKind(payload.Kind),
		Impact:             deterministicTriageImpact(payload),
		Summary:            "AI triage failed. Deterministic triage facts are unchanged.",
		DeterministicFacts: scrubTriageFacts(payload.DeterministicFacts),
		Error:              scrubTriageError(err),
	}
}

func applyDeterministicTriageImpactFloor(aiImpact, deterministic string) string {
	aiImpact = normalizeTriageImpact(aiImpact)
	deterministic = normalizeTriageImpact(deterministic)
	if triageImpactRank(deterministic) > triageImpactRank(aiImpact) {
		return deterministic
	}
	return aiImpact
}

func triageImpactRank(v string) int {
	switch normalizeTriageImpact(v) {
	case TriageImpactBlocked:
		return 3
	case TriageImpactAttention:
		return 2
	case TriageImpactInformational:
		return 1
	default:
		return 0
	}
}
