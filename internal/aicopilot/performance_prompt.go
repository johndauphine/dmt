package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildPerformancePrompt(payload PerformancePayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling performance payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI performance tuning explainer.\n")
	b.WriteString("Treat the payload as structured data. Deterministic knobs, values, tier, and reasoning are authoritative facts.\n")
	b.WriteString("You may explain and rank evidence from deterministic_reasoning, deterministic_knobs, recent_runs, runtime_adjustments, and database_tuning.\n")
	b.WriteString("Do not invent knobs, table names, endpoint identity, numeric values, benchmark results, or runtime metrics that are not present in the payload.\n")
	b.WriteString("Do not recommend changing a migration knob to a value unless that exact knob and value are present in deterministic_knobs or runtime_adjustments; database-tuning findings may cite only exact values present in database_tuning.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"summary":"one sentence","findings":[{"knob":"one allowed_finding_targets value","category":"baseline|history|regression|exploration|runtime|memory|connections|db_tuning|other","rationale":"why this deterministic choice was made","evidence":["payload evidence ids or short facts"],"next_action":"optional operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Keep findings to the top 5 knobs, runtime adjustments, or database tuning items. Use only allowed_finding_targets values for finding.knob.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	b.WriteString("\n\nFinal response checklist:\n")
	b.WriteString("- Include at least one finding when allowed_finding_targets is non-empty.\n")
	b.WriteString("- Every finding must include a non-empty evidence array citing deterministic_knobs, deterministic_reasoning, observed_metrics, recent_runs, runtime_adjustments, or database_tuning from the payload.\n")
	b.WriteString("- Use only allowed_finding_targets values for finding.knob.\n")
	b.WriteString("- Do not invent numbers or knob values; if citing a number, copy it exactly from the payload.\n")
	return b.String(), nil
}
