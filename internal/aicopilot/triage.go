package aicopilot

import (
	"context"
	"fmt"
	"strings"
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
	calibrateTriageReviewEvidence(payload, review)
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

func calibrateTriageReviewEvidence(payload TriagePayload, review *TriageReview) {
	if review == nil {
		return
	}
	sparseValidation := isSparseValidationMismatchPayload(payload)
	if sparseValidation && containsUnsupportedSparseValidationClaim(review.Summary) {
		review.Summary = "Sparse validation mismatch facts are available; deterministic evidence does not identify a root cause."
	}
	for i := range review.Findings {
		finding := &review.Findings[i]
		replaceSuppressedTriageFieldFallbacks(payload, finding)
		explicitEvidence := findingHasExplicitDeterministicEvidence(payload, *finding)
		if sparseValidation && containsUnsupportedSparseValidationClaim(finding.LikelyCause) {
			finding.LikelyCause = sparseValidationLikelyCauseFallback(payload)
		}
		if sparseValidation && containsUnsupportedSparseValidationClaim(finding.ManualInspection) {
			finding.ManualInspection = deterministicManualInspectionFallback(payload)
		}
		if sparseValidation && containsUnsupportedSparseValidationClaim(finding.NextAction) {
			finding.NextAction = sparseValidationNextActionFallback(payload)
		}
		for j := range finding.Hypotheses {
			h := &finding.Hypotheses[j]
			if sparseValidation {
				if h.Confidence == "high" || containsUnsupportedSparseValidationClaim(h.Rationale) {
					h.Confidence = "low"
					h.Rationale = sparseValidationHypothesisFallback(payload)
				}
				continue
			}
			if h.Confidence == "high" && !explicitEvidence {
				h.Confidence = "medium"
				if h.Rationale == "" || containsUnsupportedHighConfidenceClaim(h.Rationale) {
					h.Rationale = "Deterministic facts support investigation, but they do not prove a root cause."
				}
			}
		}
		if sparseValidation && finding.NextAction == "" {
			finding.NextAction = sparseValidationNextActionFallback(payload)
		}
	}
}

func replaceSuppressedTriageFieldFallbacks(payload TriagePayload, finding *TriageFinding) {
	if finding == nil {
		return
	}
	if isSuppressedTriageField(finding.LikelyCause) {
		finding.LikelyCause = deterministicLikelyCauseFallback(payload)
	}
	if isSuppressedTriageField(finding.ManualInspection) {
		finding.ManualInspection = deterministicManualInspectionFallback(payload)
	}
	if isSuppressedTriageField(finding.NextAction) {
		finding.NextAction = deterministicNextActionFallback(payload)
	}
}

func isSuppressedTriageField(value string) bool {
	switch strings.TrimSpace(value) {
	case triageUnsafeAdvisorySuppressed, triageDestructiveSuppressed, triageCommandSuppressed:
		return true
	default:
		return false
	}
}

func isSparseValidationMismatchPayload(payload TriagePayload) bool {
	if normalizeTriageKind(payload.Kind) != TriageKindValidationMismatch || payload.ValidationMismatch == nil {
		return false
	}
	facts := payload.ValidationMismatch
	return facts.HasRowCountFacts &&
		len(facts.Differences) == 0 &&
		len(facts.SchemaDrift) == 0 &&
		facts.Error == "" &&
		!facts.TimedOut &&
		!facts.ExactTimedOut
}

func findingHasExplicitDeterministicEvidence(payload TriagePayload, finding TriageFinding) bool {
	if normalizeTriageKind(payload.Kind) == TriageKindMigrationFailure {
		return payload.MigrationFailure != nil && len(payload.MigrationFailure.DeterministicDiagnoses) > 0
	}
	if payload.ValidationMismatch == nil {
		return false
	}
	facts := payload.ValidationMismatch
	if len(facts.SchemaDrift) > 0 || facts.Error != "" || facts.TimedOut || facts.ExactTimedOut {
		return true
	}
	for _, diff := range facts.Differences {
		switch normalizeValidationCategory(diff.Category) {
		case ValidationCategoryDeleteDrift,
			ValidationCategoryTypeCoercion,
			ValidationCategoryTimezoneDateHandling,
			ValidationCategoryWatermarkIssue,
			ValidationCategoryTargetTriggerDefaultBehavior:
			return true
		}
	}
	for _, fact := range finding.DeterministicFacts {
		switch {
		case strings.Contains(fact, "validation."+ValidationCategoryDeleteDrift),
			strings.Contains(fact, "validation."+ValidationCategoryTypeCoercion),
			strings.Contains(fact, "validation."+ValidationCategoryTimezoneDateHandling),
			strings.Contains(fact, "validation."+ValidationCategoryWatermarkIssue),
			strings.Contains(fact, "validation."+ValidationCategoryTargetTriggerDefaultBehavior),
			strings.Contains(fact, "schema_drift."):
			return true
		}
	}
	return false
}

func containsUnsupportedSparseValidationClaim(value string) bool {
	lower := strings.ToLower(value)
	return containsAny(lower, []string{
		"checkpoint false success",
		"false success",
		"writer bottleneck",
		"durability",
		"schema evolution",
		"schema drift",
		"manual delete",
		"manual deletion",
		"manually deleted",
		"operator deleted",
		"stale checkpoint",
		"checkpoint was wrong",
		"checkpoint is wrong",
		"trigger deleted",
		"trigger removed",
	})
}

func containsUnsupportedHighConfidenceClaim(value string) bool {
	lower := strings.ToLower(value)
	return containsAny(lower, []string{
		"proves",
		"confirmed",
		"definitely",
		"root cause is",
		"must be",
		"caused by",
	})
}

func deterministicLikelyCauseFallback(payload TriagePayload) string {
	if isSparseValidationMismatchPayload(payload) {
		return sparseValidationLikelyCauseFallback(payload)
	}
	switch normalizeTriageKind(payload.Kind) {
	case TriageKindValidationMismatch:
		return "Deterministic validation facts identify a mismatch, but the AI-supplied cause was not safe to display."
	case TriageKindMigrationFailure:
		return "Deterministic failure facts are available, but the AI-supplied cause was not safe to display."
	default:
		return "Deterministic triage facts are available, but the AI-supplied cause was not safe to display."
	}
}

func deterministicManualInspectionFallback(payload TriagePayload) string {
	if isSparseValidationMismatchPayload(payload) {
		return "Inspect read-only validation output for the affected table and compare source and target counts before choosing a recovery path."
	}
	return "Use read-only DMT diagnosis, status, validation, or analysis output to inspect the deterministic facts."
}

func deterministicNextActionFallback(payload TriagePayload) string {
	if isSparseValidationMismatchPayload(payload) {
		return sparseValidationNextActionFallback(payload)
	}
	return "Use read-only DMT diagnosis or validation output, then follow documented recovery procedures with operator review."
}

func sparseValidationLikelyCauseFallback(payload TriagePayload) string {
	if payload.ValidationMismatch != nil && payload.ValidationMismatch.Table != "" {
		return "Row counts differ for " + payload.ValidationMismatch.Table + ", but deterministic facts do not identify the root cause."
	}
	return "Row counts differ, but deterministic facts do not identify the root cause."
}

func sparseValidationHypothesisFallback(payload TriagePayload) string {
	if payload.ValidationMismatch != nil && payload.ValidationMismatch.Table != "" {
		return "Only sparse validation mismatch facts are available for " + payload.ValidationMismatch.Table + "; treat root-cause hypotheses as insufficient evidence."
	}
	return "Only sparse validation mismatch facts are available; treat root-cause hypotheses as insufficient evidence."
}

func sparseValidationNextActionFallback(payload TriagePayload) string {
	if payload.ValidationMismatch != nil && payload.ValidationMismatch.Table != "" {
		return "Run read-only diagnosis or validation for " + payload.ValidationMismatch.Table + " and inspect deterministic mismatch details before taking recovery action."
	}
	return "Run read-only diagnosis or validation and inspect deterministic mismatch details before taking recovery action."
}
