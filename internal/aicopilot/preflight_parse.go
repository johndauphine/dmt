package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

func ParsePreflightReview(raw string) (*PreflightReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review PreflightReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI preflight review JSON: %w", err)
	}
	review.Readiness = normalizeReadiness(review.Readiness)
	review.Summary = limitText(logging.Scrub(strings.TrimSpace(review.Summary)), 800)
	for i := range review.Findings {
		f := &review.Findings[i]
		f.Severity = normalizeSeverity(f.Severity)
		f.Category = limitText(logging.Scrub(strings.TrimSpace(f.Category)), 80)
		f.Affected = limitText(logging.Scrub(strings.TrimSpace(f.Affected)), 160)
		f.Rationale = limitText(logging.Scrub(strings.TrimSpace(f.Rationale)), 600)
		f.NextAction = limitText(logging.Scrub(strings.TrimSpace(f.NextAction)), 600)
		f.Source = "ai_advisory"
	}
	for i := range review.Notes {
		review.Notes[i] = limitText(logging.Scrub(strings.TrimSpace(review.Notes[i])), 400)
	}
	if len(review.Findings) > 5 {
		review.Findings = review.Findings[:5]
	}
	return &review, nil
}

func applyDeterministicReadinessFloor(aiReadiness, deterministic string) string {
	aiReadiness = normalizeReadiness(aiReadiness)
	deterministic = normalizeReadiness(deterministic)
	if readinessRank(deterministic) > readinessRank(aiReadiness) {
		return deterministic
	}
	return aiReadiness
}

func readinessRank(v string) int {
	switch normalizeReadiness(v) {
	case ReadinessBlocked:
		return 3
	case ReadinessAttention:
		return 2
	case ReadinessReady:
		return 1
	default:
		return 0
	}
}

func extractJSONObject(raw string) string {
	if strings.HasPrefix(raw, "```") {
		lines := strings.Split(raw, "\n")
		if len(lines) >= 3 {
			lines = lines[1:]
			if strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
				lines = lines[:len(lines)-1]
			}
			raw = strings.Join(lines, "\n")
		}
	}
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end >= start {
		return raw[start : end+1]
	}
	return raw
}

func normalizeReadiness(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case ReadinessReady, ReadinessAttention, ReadinessBlocked:
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ReadinessUnknown
	}
}

func normalizeSeverity(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case string(driver.SeverityError):
		return string(driver.SeverityError)
	case string(driver.SeverityInfo):
		return string(driver.SeverityInfo)
	default:
		return string(driver.SeverityWarn)
	}
}

func limitText(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
