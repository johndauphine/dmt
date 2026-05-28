package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

func ParseConfigReview(raw string) (*ConfigReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review ConfigReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI config review JSON: %w", err)
	}
	normalizeConfigReview(&review, nil)
	return &review, nil
}

func normalizeConfigReview(review *ConfigReview, payload *ConfigReviewPayload) {
	if review == nil {
		return
	}
	review.Summary = configReviewSafeText(review.Summary, payload, 800)
	review.RefusalReason = configReviewSafeText(review.RefusalReason, payload, 400)
	if len(review.PatchRecommendations) > 5 {
		review.PatchRecommendations = review.PatchRecommendations[:5]
	}
	for i := range review.PatchRecommendations {
		p := &review.PatchRecommendations[i]
		p.Operation = normalizePatchOperation(p.Operation)
		p.Path = configReviewSafeText(strings.TrimPrefix(strings.TrimSpace(p.Path), "."), payload, 120)
		p.Value = sanitizeConfigReviewValue(p.Value, payload)
		p.Rationale = configReviewSafeText(p.Rationale, payload, 600)
		p.Risk = configReviewSafeText(p.Risk, payload, 300)
		p.WhenToApply = configReviewSafeText(p.WhenToApply, payload, 300)
		if !configReviewPathAllowed(p.Path, payload) {
			p.Operation = "set"
			p.Value = "[REDACTED]"
			p.Rationale = "Recommendation referenced a path outside the safe config review allowlist and was redacted."
			p.Risk = "Do not apply without manual review."
			p.RequiresConfirmation = true
		}
	}
	review.Runbook.Title = configReviewSafeText(review.Runbook.Title, payload, 120)
	review.Runbook.Summary = configReviewSafeText(review.Runbook.Summary, payload, 500)
	review.Runbook.BeforeRun = sanitizeConfigReviewStrings(review.Runbook.BeforeRun, payload, 5, 300)
	review.Runbook.Run = sanitizeConfigReviewStrings(review.Runbook.Run, payload, 5, 300)
	review.Runbook.Validation = sanitizeConfigReviewStrings(review.Runbook.Validation, payload, 5, 300)
	review.Runbook.Rollback = sanitizeConfigReviewStrings(review.Runbook.Rollback, payload, 5, 300)
	review.Notes = sanitizeConfigReviewStrings(review.Notes, payload, 5, 300)
}

func normalizePatchOperation(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "add", "remove":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "set"
	}
}

func sanitizeConfigReviewStrings(values []string, payload *ConfigReviewPayload, maxItems, maxLen int) []string {
	if len(values) > maxItems {
		values = values[:maxItems]
	}
	for i := range values {
		values[i] = configReviewSafeText(values[i], payload, maxLen)
	}
	return values
}

func sanitizeConfigReviewValue(v any, payload *ConfigReviewPayload) any {
	switch x := v.(type) {
	case string:
		return configReviewSafeText(x, payload, 240)
	case []any:
		if len(x) > 10 {
			x = x[:10]
		}
		for i := range x {
			x[i] = sanitizeConfigReviewValue(x[i], payload)
		}
		return x
	case map[string]any:
		for k, val := range x {
			x[k] = sanitizeConfigReviewValue(val, payload)
		}
		return x
	default:
		return v
	}
}

func configReviewSafeText(s string, payload *ConfigReviewPayload, max int) string {
	s = limitText(logging.Scrub(strings.TrimSpace(s)), max)
	if payload == nil {
		return s
	}
	for _, value := range payload.sensitiveValues {
		if value == "" {
			continue
		}
		s = strings.ReplaceAll(s, value, logging.RedactedToken)
	}
	return s
}

func configReviewPathAllowed(path string, payload *ConfigReviewPayload) bool {
	if path == "" {
		return false
	}
	exactPaths := []string{
		"source.type", "source.schema", "source.ssl_mode", "source.encrypt", "source.packet_size",
		"target.type", "target.schema", "target.ssl_mode", "target.encrypt", "target.packet_size",
		"migration.target_mode", "migration.workers", "migration.chunk_size", "migration.read_ahead_buffers",
		"migration.write_ahead_writers", "migration.parallel_readers", "migration.max_source_connections",
		"migration.max_target_connections", "migration.max_partitions", "migration.large_table_threshold",
		"migration.strict_consistency", "migration.create_indexes", "migration.create_foreign_keys",
		"migration.create_check_constraints", "migration.allow_partial", "migration.unmapped_type_action",
		"migration.approx_type_action", "migration.schema_contract", "migration.schema_evolution",
		"migration.deletes", "migration.validation",
	}
	if payload != nil && len(payload.Safety.AllowedPatchPaths) > 0 {
		exactPaths = payload.Safety.AllowedPatchPaths
	}
	for _, allowed := range exactPaths {
		if path == allowed || configReviewExpandablePath(allowed) && strings.HasPrefix(path, allowed+".") {
			return true
		}
	}
	return false
}

func configReviewExpandablePath(path string) bool {
	switch path {
	case "migration.schema_contract",
		"migration.schema_evolution",
		"migration.deletes",
		"migration.validation":
		return true
	default:
		return false
	}
}
