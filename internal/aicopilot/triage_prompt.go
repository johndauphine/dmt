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
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"impact":"blocked|attention|informational|unknown","summary":"one sentence","findings":[{"severity":"error|warn|info","category":"short","affected":"table/phase/check","deterministic_facts":["fact ids or short fact text"],"hypotheses":[{"confidence":"high|medium|low","rationale":"why this might explain the facts"}],"next_action":"operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Keep findings to the top 5 actionable advisory items. Prefer verification steps over changes when evidence is incomplete.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	return b.String(), nil
}
