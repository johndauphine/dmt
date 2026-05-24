package aicopilot

import (
	"context"
	"fmt"
	"reflect"

	"github.com/johndauphine/dmt/internal/logging"
)

func GeneratePreflightReview(ctx context.Context, client TextClient, payload PreflightPayload) (*PreflightReview, error) {
	if IsNilTextClient(client) {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildPreflightPrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	review, err := ParsePreflightReview(raw)
	if err != nil {
		return nil, err
	}
	review.Enabled = true
	review.Status = ReviewStatusOK
	review.Provider = client.ProviderName()
	review.Model = client.Model()
	review.PromptVersion = PreflightPromptVersion
	review.DeterministicBlockers = append([]string(nil), payload.DeterministicBlockers...)
	review.Readiness = applyDeterministicReadinessFloor(review.Readiness, deterministicReadiness(payload))
	if review.Summary == "" {
		review.Summary = deterministicSummary(payload)
	}
	for i := range review.Findings {
		review.Findings[i].Source = "ai_advisory"
	}
	return review, nil
}

func UnavailablePreflightReview(reason string, payload PreflightPayload) *PreflightReview {
	return &PreflightReview{
		Enabled:               false,
		Status:                ReviewStatusUnavailable,
		PromptVersion:         PreflightPromptVersion,
		Readiness:             deterministicReadiness(payload),
		Summary:               "AI review unavailable: " + logging.Scrub(reason) + ". Deterministic preflight results are unchanged.",
		DeterministicBlockers: append([]string(nil), payload.DeterministicBlockers...),
	}
}

// IsNilTextClient reports whether client is nil, including typed-nil values
// stored in the TextClient interface.
func IsNilTextClient(client TextClient) bool {
	if client == nil {
		return true
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func ErrorPreflightReview(provider, model string, err error, payload PreflightPayload) *PreflightReview {
	errMsg := ""
	if err != nil {
		errMsg = logging.Scrub(err.Error())
	}
	return &PreflightReview{
		Enabled:               true,
		Status:                ReviewStatusError,
		Provider:              provider,
		Model:                 model,
		PromptVersion:         PreflightPromptVersion,
		Readiness:             deterministicReadiness(payload),
		Summary:               "AI review failed. Deterministic preflight results are unchanged.",
		DeterministicBlockers: append([]string(nil), payload.DeterministicBlockers...),
		Error:                 errMsg,
	}
}
