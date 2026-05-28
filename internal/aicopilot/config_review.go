package aicopilot

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/logging"
)

func GenerateConfigReview(ctx context.Context, client TextClient, payload ConfigReviewPayload) (*ConfigReview, error) {
	if payload.PromptVersion == "" {
		payload.PromptVersion = ConfigReviewPromptVersion
	}
	if payload.Safety.RefusalReason != "" {
		return RefusedConfigReview(payload.Safety.RefusalReason, payload), nil
	}
	if IsNilTextClient(client) {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildConfigReviewPrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	review, err := ParseConfigReview(raw)
	if err != nil {
		return nil, err
	}
	normalizeConfigReview(review, &payload)
	if review.RefusalReason != "" {
		review.Status = ReviewStatusRefused
	} else {
		review.Status = ReviewStatusOK
	}
	review.Enabled = true
	review.Provider = client.ProviderName()
	review.Model = client.Model()
	review.PromptVersion = ConfigReviewPromptVersion
	if review.Summary == "" {
		review.Summary = defaultConfigReviewSummary(payload)
	}
	return review, nil
}

func UnavailableConfigReview(reason string, payload ConfigReviewPayload) *ConfigReview {
	review := deterministicConfigReviewFallback("AI config review unavailable: "+logging.Scrub(reason)+".", payload)
	review.Enabled = false
	review.Status = ReviewStatusUnavailable
	return review
}

func ErrorConfigReview(provider, model string, err error, payload ConfigReviewPayload) *ConfigReview {
	errMsg := ""
	if err != nil {
		errMsg = logging.Scrub(err.Error())
	}
	review := deterministicConfigReviewFallback("AI config review failed; using deterministic safety guidance.", payload)
	review.Enabled = true
	review.Status = ReviewStatusError
	review.Provider = provider
	review.Model = model
	review.Error = errMsg
	return review
}

func RefusedConfigReview(reason string, payload ConfigReviewPayload) *ConfigReview {
	review := deterministicConfigReviewFallback("AI config review refused: "+logging.Scrub(reason)+".", payload)
	review.Enabled = true
	review.Status = ReviewStatusRefused
	review.RefusalReason = configReviewSafeText(reason, &payload, 400)
	review.PatchRecommendations = nil
	return review
}

func deterministicConfigReviewFallback(summary string, payload ConfigReviewPayload) *ConfigReview {
	review := &ConfigReview{
		PromptVersion: ConfigReviewPromptVersion,
		Summary:       configReviewSafeText(summary, &payload, 800),
		Runbook: ConfigRunbook{
			Title:   "DMT migration review runbook",
			Summary: "Review redacted configuration settings, run deterministic preflight checks, and require operator confirmation for destructive target modes.",
			BeforeRun: []string{
				"Verify source and target connection settings outside AI-generated output.",
				"Run deterministic preflight checks and resolve any blocking findings.",
				"Confirm target backups before using drop_recreate or other destructive workflows.",
			},
			Run: []string{
				"Start with a bounded worker and chunk-size profile appropriate for the database pool limits.",
				"Monitor throughput, retries, and validation failures during the run.",
			},
			Validation: []string{
				"Review row-count or configured validation results before declaring the migration complete.",
				"Investigate mismatches before enabling downstream consumers.",
			},
			Rollback: []string{
				"Stop the migration if destructive behavior or validation failures are unexpected.",
				"Restore target data from the verified backup when rollback is required.",
			},
		},
		Notes: []string{
			"Fallback output intentionally omits connection values and secrets.",
		},
	}
	normalizeConfigReview(review, &payload)
	return review
}

func defaultConfigReviewSummary(payload ConfigReviewPayload) string {
	if payload.Config.Migration.TargetMode == "drop_recreate" {
		return "Review target backup confirmation and validation settings before running this drop_recreate migration."
	}
	return "Review the recommended migration settings and run deterministic preflight before execution."
}
