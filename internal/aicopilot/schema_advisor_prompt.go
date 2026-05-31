package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildSchemaAdvisorPrompt(payload SchemaAdvisorPayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling schema advisor payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI schema evolution advisor.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for secrets and do not infer values from omitted fields.\n")
	b.WriteString("Deterministic policy gates are authoritative. You may explain them, but you must not recommend bypassing them.\n")
	b.WriteString("Copy payload.deterministic_blockers into the top-level deterministic_blockers field when present.\n")
	b.WriteString("For each recommendation that matches a payload change, copy that change's deterministic_gate object into the recommendation. For blocked gates, risk must be blocked and deterministic_gate.allowed must be false.\n")
	b.WriteString("Your recommendations are advisory only. DMT schema contract and schema evolution policy decide what can run or change.\n")
	b.WriteString("For unsafe, narrowing, lossy, source-column removal, key, or constraint risks, prefer manual migration, report, freeze, discard_row, or discard_value where the deterministic gate allows it.\n")
	b.WriteString("Never recommend destructive target actions. Do not use words such as drop, dropped, delete, truncate, recreate, remove target, reload target, or wipe in suggested_action or manual_guidance.\n")
	b.WriteString("Do not write negative destructive advice like do not drop; say avoid target DDL or data-changing actions instead.\n")
	b.WriteString("Avoid causal-certainty wording: do not use root cause, proves, definitely, certainly, must be caused by, or is caused by.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"summary":"one sentence","deterministic_blockers":["copy from payload when present"],"recommendations":[{"drift_kind":"copy from payload","classification":"copy from payload","risk":"low|medium|high|blocked|unknown","schema":"optional","table":"table","column":"optional","reason":"why this matters","suggested_policy":"config policy/action","suggested_action":"operator action","manual_guidance":["optional non-destructive guidance"],"deterministic_gate":{"allowed":false,"action":"copy from payload","policy":"copy from payload","reason":"copy from payload"}}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Keep recommendations to the top 10 actionable items. Include affected table and column when present.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	b.WriteString("\n\nFinal response checklist:\n")
	b.WriteString("- Copy deterministic_blockers from the payload and deterministic_gate from each matching payload change.\n")
	b.WriteString("- For blocked changes, use risk=blocked and keep suggested_action to manual review or manual conversion planning only.\n")
	b.WriteString("- Do not mention target DDL, SQL statements, cleanup, or destructive action terms, even as negative advice.\n")
	b.WriteString("- Do not use causal-certainty wording in any generated string.\n")
	b.WriteString("- Forbidden output tokens in JSON values: drop, dropped, dropping, delete, deleted, deleting, truncate, recreate, remove, reload, wipe, root cause, caused by, proves, definitely, certainly. Use neutral wording such as target DDL change, target-changing action, or deterministic gate instead.\n")
	b.WriteString("- For a blocked data-type drift, set suggested_policy=freeze, suggested_action exactly to Keep deterministic block and plan manual conversion, manual_guidance to [], and notes to []. Do not add counterexamples.\n")
	return b.String(), nil
}
