package aicopilot

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/logging"
)

func GenerateSchemaAdvisorReview(ctx context.Context, client TextClient, payload SchemaAdvisorPayload) (*SchemaAdvisorReview, error) {
	if IsNilTextClient(client) {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildSchemaAdvisorPrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	review, err := ParseSchemaAdvisorReview(raw)
	if err != nil {
		return nil, err
	}
	review.Enabled = true
	review.Status = ReviewStatusOK
	review.Provider = client.ProviderName()
	review.Model = client.Model()
	review.PromptVersion = SchemaAdvisorPromptVersion
	review.DeterministicBlockers = append([]string(nil), payload.DeterministicBlockers...)
	if review.Summary == "" {
		review.Summary = deterministicSchemaAdvisorSummary(payload)
	}
	applySchemaAdvisorPolicyGates(review, payload)
	if len(review.Recommendations) == 0 {
		review.Recommendations = deterministicSchemaAdvisorReview(payload).Recommendations
	}
	return review, nil
}

func UnavailableSchemaAdvisorReview(reason string, payload SchemaAdvisorPayload) *SchemaAdvisorReview {
	review := deterministicSchemaAdvisorReview(payload)
	review.Enabled = false
	review.Status = ReviewStatusUnavailable
	review.Summary = "AI schema advisor unavailable: " + logging.Scrub(reason) + ". Deterministic schema policy results are unchanged."
	return review
}

func ErrorSchemaAdvisorReview(provider, model string, err error, payload SchemaAdvisorPayload) *SchemaAdvisorReview {
	review := deterministicSchemaAdvisorReview(payload)
	review.Enabled = true
	review.Status = ReviewStatusError
	review.Provider = provider
	review.Model = model
	review.Summary = "AI schema advisor failed. Deterministic schema policy results are unchanged."
	if err != nil {
		review.Error = logging.Scrub(err.Error())
	}
	return review
}
