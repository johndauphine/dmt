package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildTriagePrompt(payload TriagePayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling triage payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI failure and validation triage reviewer.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for secrets and do not infer values from omitted fields.\n")
	b.WriteString("deterministic_facts are authoritative. Keep them separate from hypotheses and never contradict them.\n")
	b.WriteString("Your output is advisory only. DMT policy gates and deterministic checks decide what can run or change.\n")
	b.WriteString("Never recommend destructive target actions unless next_action explicitly includes backup verification and operator confirmation.\n")
	b.WriteString("For validation mismatches, use these categories when supported by evidence: delete_drift, type_coercion, timezone_date_handling, watermark_issue, target_trigger_default_behavior.\n")
	b.WriteString("Return likely causes as hypotheses with confidence. Include affected tables, suggested read-only commands/config changes, and when to stop and inspect manually.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"impact":"blocked|attention|informational|unknown","summary":"one sentence","findings":[{"severity":"error|warn|info","category":"short","affected":"table/phase/check","affected_tables":["table"],"deterministic_facts":["fact ids or short fact text"],"likely_cause":"short advisory cause","hypotheses":[{"confidence":"high|medium|low","rationale":"why this might explain the facts"}],"suggested_commands":["read-only or DMT command"],"suggested_config_changes":["config path=value"],"manual_inspection":"when to stop and inspect manually","next_action":"operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Keep findings to the top 5 actionable advisory items. Prefer verification steps over changes when evidence is incomplete.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	return b.String(), nil
}
