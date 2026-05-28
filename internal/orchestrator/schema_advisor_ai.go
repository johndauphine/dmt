package orchestrator

import (
	"context"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// ReviewSchemaDriftWithAI asks the configured AI provider for an advisory
// explanation of schema drift and policy-gated next actions. Failures never
// change deterministic schema evolution or schema contract decisions.
func (o *Orchestrator) ReviewSchemaDriftWithAI(
	ctx context.Context,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
) *aicopilot.SchemaAdvisorReview {
	payload := aicopilot.BuildSchemaAdvisorPayload(o.config, report, tables, allowSchemaEvolution)
	mapper := o.aiReviewClient()
	if aicopilot.IsNilTextClient(mapper) {
		return aicopilot.UnavailableSchemaAdvisorReview("no AI provider configured in secrets", payload)
	}

	review, err := aicopilot.GenerateSchemaAdvisorReview(ctx, mapper, payload)
	if err != nil {
		logging.WarnEvent("AI schema advisor failed",
			"provider", mapper.ProviderName(),
			"model", mapper.Model(),
			"error", logging.Scrub(err.Error()),
		)
		return aicopilot.ErrorSchemaAdvisorReview(mapper.ProviderName(), mapper.Model(), err, payload)
	}
	logging.InfoEvent("AI schema advisor completed",
		"provider", review.Provider,
		"model", review.Model,
		"recommendations", len(review.Recommendations),
	)
	return review
}
