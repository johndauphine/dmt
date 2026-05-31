package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

func ParseSchemaAdvisorReview(raw string) (*SchemaAdvisorReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review SchemaAdvisorReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI schema advisor JSON: %w", err)
	}
	review.Summary = limitText(logging.Scrub(strings.TrimSpace(review.Summary)), 800)
	for i := range review.Recommendations {
		r := &review.Recommendations[i]
		r.DriftKind = limitText(logging.Scrub(strings.TrimSpace(r.DriftKind)), 80)
		r.Classification = normalizeSchemaClassification(r.Classification)
		r.Risk = normalizeSchemaRisk(r.Risk)
		r.Schema = limitText(logging.Scrub(strings.TrimSpace(r.Schema)), 160)
		r.Table = limitText(logging.Scrub(strings.TrimSpace(r.Table)), 160)
		r.Column = limitText(logging.Scrub(strings.TrimSpace(r.Column)), 160)
		r.Reason = limitText(logging.Scrub(strings.TrimSpace(r.Reason)), 700)
		r.SuggestedPolicy = limitText(logging.Scrub(strings.TrimSpace(r.SuggestedPolicy)), 160)
		if reason := invalidConfigAssignmentInText(r.SuggestedPolicy, aiConfigChangeContext{}); reason != "" {
			r.SuggestedPolicy = "manual_review_required"
			r.SuggestedAction = appendGateReason(r.SuggestedAction, reason)
		}
		r.SuggestedAction = safeSchemaAdvisorAction(r.SuggestedAction)
		r.Source = "ai_advisory"
		if len(r.ManualGuidance) > 5 {
			r.ManualGuidance = r.ManualGuidance[:5]
		}
		for j := range r.ManualGuidance {
			r.ManualGuidance[j] = safeSchemaAdvisorAction(r.ManualGuidance[j])
		}
	}
	if len(review.Recommendations) > 10 {
		review.Recommendations = review.Recommendations[:10]
	}
	for i := range review.Notes {
		review.Notes[i] = limitText(logging.Scrub(strings.TrimSpace(review.Notes[i])), 400)
	}
	return &review, nil
}

func applySchemaAdvisorPolicyGates(review *SchemaAdvisorReview, payload SchemaAdvisorPayload) {
	if review == nil {
		return
	}
	gates := make(map[string]SchemaDriftAdvisoryChange, len(payload.Changes))
	for _, change := range payload.Changes {
		gates[schemaAdvisorKey(change.DriftKind, change.Schema, change.Table, change.Column)] = change
	}
	for i := range review.Recommendations {
		r := &review.Recommendations[i]
		change, ok := gates[schemaAdvisorKey(r.DriftKind, r.Schema, r.Table, r.Column)]
		if !ok {
			change, ok = findSchemaAdvisorChange(payload.Changes, *r)
		}
		if !ok {
			continue
		}
		r.DriftKind = change.DriftKind
		if r.Classification == "" || r.Classification == SchemaRiskUnknown {
			r.Classification = change.Classification
		}
		r.DeterministicGate = change.DeterministicGate
		if !change.DeterministicGate.Allowed {
			r.Risk = SchemaRiskBlocked
			if recommendsBypass(r.SuggestedPolicy, r.SuggestedAction) {
				r.SuggestedPolicy = change.SuggestedPolicy
				r.SuggestedAction = change.SuggestedAction
			}
			r.SuggestedAction = appendGateReason(r.SuggestedAction, change.DeterministicGate.Reason)
		}
	}
}

func deterministicSchemaAdvisorReview(payload SchemaAdvisorPayload) *SchemaAdvisorReview {
	recommendations := make([]SchemaAdvisorRecommendation, 0, len(payload.Changes))
	for _, change := range payload.Changes {
		recommendations = append(recommendations, SchemaAdvisorRecommendation{
			DriftKind:         change.DriftKind,
			Classification:    change.Classification,
			Risk:              change.Risk,
			Schema:            change.Schema,
			Table:             change.Table,
			Column:            change.Column,
			Reason:            change.Reason,
			SuggestedPolicy:   change.SuggestedPolicy,
			SuggestedAction:   change.SuggestedAction,
			ManualGuidance:    append([]string(nil), change.ManualGuidance...),
			DeterministicGate: change.DeterministicGate,
			Source:            "deterministic",
		})
	}
	return &SchemaAdvisorReview{
		PromptVersion:         SchemaAdvisorPromptVersion,
		Summary:               deterministicSchemaAdvisorSummary(payload),
		DeterministicBlockers: append([]string(nil), payload.DeterministicBlockers...),
		Recommendations:       recommendations,
	}
}

func deterministicSchemaAdvisorSummary(payload SchemaAdvisorPayload) string {
	if len(payload.Changes) == 0 {
		return "No schema drift changes were available for advisory review."
	}
	if len(payload.DeterministicBlockers) > 0 {
		return fmt.Sprintf("Deterministic schema policy blocks %d drift change(s); AI advice cannot override those gates.", len(payload.DeterministicBlockers))
	}
	return fmt.Sprintf("Deterministic schema policy classified %d drift change(s); AI advice is advisory only.", len(payload.Changes))
}

func normalizeSchemaRisk(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case SchemaRiskLow, SchemaRiskMedium, SchemaRiskHigh, SchemaRiskBlocked:
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return SchemaRiskUnknown
	}
}

func normalizeSchemaClassification(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "additive", "additive_table", "widened", "nullable_relaxation", "narrowing",
		"lossy_conversion", "dropped_column", "dropped_table", "nullability_tightening",
		"default_change", "key_constraint_risk", "unsupported_dialect_edge":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "unsupported_dialect_edge"
	}
}

func safeSchemaAdvisorAction(action string) string {
	action = limitText(logging.Scrub(strings.TrimSpace(action)), 700)
	if action == "" {
		return action
	}
	lower := strings.ToLower(action)
	destructive := strings.Contains(lower, "drop ") ||
		strings.Contains(lower, "drop_") ||
		strings.Contains(lower, "truncate") ||
		strings.Contains(lower, "delete from") ||
		strings.Contains(lower, "purge") ||
		strings.Contains(lower, "recreate")
	if !destructive {
		return action
	}
	if strings.Contains(lower, "backup") && strings.Contains(lower, "confirm") {
		return action
	}
	return "Manual inspection required; destructive target action omitted because backup verification and operator confirmation were not explicit."
}

func schemaAdvisorKey(kind, schema, table, column string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "\x00" +
		strings.ToLower(strings.TrimSpace(schema)) + "\x00" +
		strings.ToLower(strings.TrimSpace(table)) + "\x00" +
		strings.ToLower(strings.TrimSpace(column))
}

func findSchemaAdvisorChange(changes []SchemaDriftAdvisoryChange, rec SchemaAdvisorRecommendation) (SchemaDriftAdvisoryChange, bool) {
	for _, change := range changes {
		if !strings.EqualFold(change.Table, rec.Table) {
			continue
		}
		if rec.Column != "" && !strings.EqualFold(change.Column, rec.Column) {
			continue
		}
		if rec.DriftKind != "" && !strings.EqualFold(change.DriftKind, rec.DriftKind) {
			continue
		}
		return change, true
	}
	return SchemaDriftAdvisoryChange{}, false
}

func recommendsBypass(policy, action string) bool {
	text := strings.ToLower(policy + " " + action)
	for _, term := range []string{"auto", "apply", "evolve", "bypass", "ignore", "force"} {
		if strings.Contains(text, term) {
			return true
		}
	}
	return false
}

func appendGateReason(action, reason string) string {
	if reason == "" {
		return action
	}
	if action == "" {
		return "Deterministic gate: " + reason
	}
	if strings.Contains(action, reason) {
		return action
	}
	return limitText(action+" Deterministic gate: "+reason, 900)
}
