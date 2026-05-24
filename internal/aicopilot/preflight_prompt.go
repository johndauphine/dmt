package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildPreflightPrompt(payload PreflightPayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling preflight payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI preflight readiness reviewer.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for secrets and do not infer values from omitted fields.\n")
	b.WriteString("Deterministic preflight findings are authoritative facts. You may explain or prioritize them, but you must not suppress or contradict them.\n")
	b.WriteString("Your recommendations are advisory. DMT policy gates and deterministic checks decide what can run or change.\n")
	b.WriteString("Never recommend destructive target actions unless the next_action explicitly includes backup verification and operator confirmation.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"readiness":"ready|attention|blocked|unknown","summary":"one sentence","findings":[{"severity":"error|warn|info","category":"short","affected":"table/setting/check","rationale":"why this matters","next_action":"operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Use readiness=blocked when deterministic_blockers is non-empty or either connection failed. Keep findings to the top 5 actionable advisory items.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	return b.String(), nil
}
