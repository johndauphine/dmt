package aicopilot

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

var performanceNumberPattern = regexp.MustCompile(`[-+]?\d[\d,]*(?:\.\d+)?`)

func ParsePerformanceExplanation(raw string, payload PerformancePayload) (*PerformanceExplanation, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var explanation PerformanceExplanation
	if err := json.Unmarshal([]byte(body), &explanation); err != nil {
		return nil, fmt.Errorf("parsing AI performance explanation JSON: %w", err)
	}

	allowedKnobs := allowedKnobsFromPayload(payload)
	allowedNumbers := numbersFromPerformancePayload(payload)
	explanation.Summary = sanitizePerformanceText(explanation.Summary, 800, allowedNumbers)
	if explanation.Summary == "" {
		explanation.Summary = deterministicPerformanceSummary(payload)
	}

	findings := explanation.Findings[:0]
	for _, f := range explanation.Findings {
		f.Knob = strings.ToLower(strings.TrimSpace(f.Knob))
		if !allowedKnobs[f.Knob] {
			continue
		}
		f.Category = sanitizePerformanceText(f.Category, 80, allowedNumbers)
		f.Rationale = sanitizePerformanceText(f.Rationale, 600, allowedNumbers)
		f.NextAction = sanitizePerformanceText(f.NextAction, 600, allowedNumbers)
		f.Source = "ai_advisory"
		if f.Rationale == "" && f.NextAction == "" {
			continue
		}
		evidence := f.Evidence[:0]
		for _, item := range f.Evidence {
			item = sanitizePerformanceText(item, 300, allowedNumbers)
			if item != "" {
				evidence = append(evidence, item)
			}
		}
		if len(evidence) > 5 {
			evidence = evidence[:5]
		}
		f.Evidence = evidence
		findings = append(findings, f)
		if len(findings) == 5 {
			break
		}
	}
	explanation.Findings = findings

	notes := explanation.Notes[:0]
	for _, note := range explanation.Notes {
		note = sanitizePerformanceText(note, 400, allowedNumbers)
		if note != "" {
			notes = append(notes, note)
		}
	}
	explanation.Notes = notes
	return &explanation, nil
}

func sanitizePerformanceText(s string, max int, allowedNumbers map[string]bool) string {
	s = limitText(logging.Scrub(strings.TrimSpace(s)), max)
	if s == "" || hasUnsupportedPerformanceNumber(s, allowedNumbers) {
		return ""
	}
	return s
}

func hasUnsupportedPerformanceNumber(s string, allowed map[string]bool) bool {
	for _, match := range performanceNumberPattern.FindAllString(s, -1) {
		if !allowed[normalizePerformanceNumber(match)] {
			return true
		}
	}
	return false
}

func numbersFromPerformancePayload(payload PerformancePayload) map[string]bool {
	out := map[string]bool{}
	data, err := json.Marshal(payload)
	if err != nil {
		return out
	}
	for _, match := range performanceNumberPattern.FindAllString(string(data), -1) {
		out[normalizePerformanceNumber(match)] = true
	}
	return out
}

func normalizePerformanceNumber(s string) string {
	s = strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), "+")
	negative := strings.HasPrefix(s, "-")
	if negative {
		s = strings.TrimPrefix(s, "-")
	}
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart := strings.TrimLeft(s[:dot], "0")
		fracPart := strings.TrimRight(s[dot+1:], "0")
		if intPart == "" {
			intPart = "0"
		}
		if fracPart == "" {
			s = intPart
		} else {
			s = intPart + "." + fracPart
		}
	} else {
		s = strings.TrimLeft(s, "0")
		if s == "" {
			s = "0"
		}
	}
	if negative && s != "0" {
		return "-" + s
	}
	return s
}

func allowedKnobsFromPayload(payload PerformancePayload) map[string]bool {
	if len(payload.AllowedKnobs) == 0 {
		return allowedPerformanceKnobSet()
	}
	out := make(map[string]bool, len(payload.AllowedKnobs))
	base := allowedPerformanceKnobSet()
	for _, knob := range payload.AllowedKnobs {
		knob = strings.ToLower(strings.TrimSpace(knob))
		if base[knob] {
			out[knob] = true
		}
	}
	return out
}
