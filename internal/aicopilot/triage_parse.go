package aicopilot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

func ParseTriageReview(raw string) (*TriageReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review TriageReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI triage review JSON: %w", err)
	}
	review.Kind = normalizeTriageKind(review.Kind)
	review.Impact = normalizeTriageImpact(review.Impact)
	review.Summary = scrubTriageText(review.Summary, 800)
	for i := range review.Findings {
		f := &review.Findings[i]
		f.Severity = normalizeSeverity(f.Severity)
		f.Category = scrubTriageText(f.Category, 80)
		f.Affected = scrubTriageText(f.Affected, 160)
		for j := range f.DeterministicFacts {
			f.DeterministicFacts[j] = scrubTriageText(f.DeterministicFacts[j], 220)
		}
		for j := range f.Hypotheses {
			f.Hypotheses[j].Confidence = normalizeConfidence(f.Hypotheses[j].Confidence)
			f.Hypotheses[j].Rationale = scrubTriageText(f.Hypotheses[j].Rationale, 600)
		}
		if len(f.Hypotheses) > 3 {
			f.Hypotheses = f.Hypotheses[:3]
		}
		f.NextAction = sanitizeTriageNextAction(f.NextAction)
		f.Source = TriageFindingSourceAIAdvisory
	}
	for i := range review.Notes {
		review.Notes[i] = scrubTriageText(review.Notes[i], 400)
	}
	if len(review.Findings) > 5 {
		review.Findings = review.Findings[:5]
	}
	return &review, nil
}

func normalizeTriageKind(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case TriageKindMigrationFailure:
		return TriageKindMigrationFailure
	case TriageKindValidationMismatch:
		return TriageKindValidationMismatch
	default:
		return ""
	}
}

func normalizeTriageImpact(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case TriageImpactBlocked, TriageImpactAttention, TriageImpactInformational:
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return TriageImpactUnknown
	}
}

func normalizeConfidence(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "high", "medium", "low":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "low"
	}
}

func sanitizeTriageNextAction(action string) string {
	action = scrubTriageText(action, 600)
	if !containsDestructiveRecommendation(action) || includesBackupAndConfirmation(action) {
		return action
	}
	return "Destructive recommendation suppressed. Verify backups, gather operator confirmation, and use documented DMT recovery procedures before any target data change."
}

func containsDestructiveRecommendation(s string) bool {
	lower := strings.ToLower(s)
	terms := []string{"drop ", "truncate", "delete ", "wipe", "purge", "recreate", "overwrite", "remove target", "clear target"}
	for _, term := range terms {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func includesBackupAndConfirmation(s string) bool {
	lower := strings.ToLower(s)
	hasBackup := strings.Contains(lower, "backup")
	hasConfirmation := strings.Contains(lower, "confirm") || strings.Contains(lower, "confirmation") || strings.Contains(lower, "operator approval")
	return hasBackup && hasConfirmation
}

func formatInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func deterministicTriageImpact(payload TriagePayload) string {
	switch normalizeTriageKind(payload.Kind) {
	case TriageKindMigrationFailure:
		return TriageImpactBlocked
	case TriageKindValidationMismatch:
		if payload.ValidationMismatch != nil && (payload.ValidationMismatch.TimedOut || payload.ValidationMismatch.Error != "") {
			return TriageImpactBlocked
		}
		return TriageImpactAttention
	default:
		return TriageImpactUnknown
	}
}

func deterministicTriageSummary(payload TriagePayload) string {
	switch normalizeTriageKind(payload.Kind) {
	case TriageKindMigrationFailure:
		return "Deterministic migration failure facts are available for operator triage."
	case TriageKindValidationMismatch:
		return "Deterministic validation mismatch facts are available for operator triage."
	default:
		return "Deterministic triage facts are available for operator review."
	}
}

func scrubTriageFacts(facts []TriageFact) []TriageFact {
	out := make([]TriageFact, 0, len(facts))
	for _, fact := range facts {
		out = append(out, TriageFact{
			Category: scrubTriageText(fact.Category, 80),
			Affected: scrubTriageText(fact.Affected, 160),
			Detail:   scrubTriageText(fact.Detail, 500),
		})
	}
	return compactTriageFacts(out)
}

func scrubTriageError(err error) string {
	if err == nil {
		return ""
	}
	return scrubTriageText(logging.Scrub(err.Error()), 500)
}
