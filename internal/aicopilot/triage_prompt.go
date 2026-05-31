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
	b.WriteString("Never recommend destructive target actions. Do not use words such as drop, delete, truncate, recreate, remove target, reload target, wipe, force resume, skip validation, or bypass validation.\n")
	b.WriteString("Do not write negative destructive advice like do not drop or do not delete; say keep target unchanged and inspect read-only evidence instead.\n")
	b.WriteString("suggested_commands may contain only read-only DMT commands: dmt validate, dmt status, dmt diagnose, or dmt analyze. Never suggest dmt run, dmt resume, --confirm-backup, shell operators, SQL, or mutation commands.\n")
	b.WriteString("next_action and manual_inspection must be read-only inspection or validation steps, not recovery, rerun, resume, cleanup, or target-change steps.\n")
	b.WriteString("Avoid causal-certainty wording: do not use root cause, proves, definitely, certainly, must be caused by, is caused by, or the cause is. Use possible, may, or could.\n")
	b.WriteString("High confidence requires explicit deterministic evidence in the payload. Sparse validation count mismatches are insufficient evidence for checkpoint, writer, durability, schema evolution, trigger, or manual-delete root-cause claims.\n")
	b.WriteString("For validation mismatches, use these categories when supported by evidence: delete_drift, type_coercion, timezone_date_handling, watermark_issue, target_trigger_default_behavior.\n")
	b.WriteString("Return likely causes as hypotheses with confidence. Include affected tables, suggested read-only commands/config changes, and when to stop and inspect manually.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"impact":"blocked|attention|informational|unknown","summary":"one sentence","findings":[{"severity":"error|warn|info","category":"short","affected":"table/phase/check","affected_tables":["table"],"deterministic_facts":["fact ids or short fact text"],"likely_cause":"short advisory cause","hypotheses":[{"confidence":"high|medium|low","rationale":"why this might explain the facts"}],"suggested_commands":["read-only or DMT command"],"suggested_config_changes":["config path=value"],"manual_inspection":"when to stop and inspect manually","next_action":"operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Keep findings to the top 5 actionable advisory items. Prefer verification steps over changes when evidence is incomplete.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	b.WriteString("\n\nFinal response checklist:\n")
	b.WriteString("- suggested_commands must be read-only only: validate, status, diagnose, or analyze.\n")
	b.WriteString("- Do not mention run, resume, recovery, rerun, cleanup, target mutation, SQL mutation, or destructive action terms.\n")
	b.WriteString("- Do not use causal-certainty wording in any generated string.\n")
	b.WriteString("- Each finding must include deterministic_facts from the payload and hypotheses must use low or medium confidence unless explicit deterministic evidence supports high.\n")
	b.WriteString("- Forbidden output tokens in JSON values: drop, dropped, dropping, delete, deleted, deleting, truncate, recreate, remove, reload, wipe, root cause, caused by, proves, definitely, certainly. Use neutral wording such as row-count mismatch, target-changing action, or read-only evidence instead.\n")
	b.WriteString("- Do not introduce delete_drift unless the payload includes an exact deterministic fact or difference category for validation.delete_drift.\n")
	b.WriteString("- For null-parity or row-count mismatches, use neutral phrases such as null-count mismatch, row-count mismatch, and read-only validation evidence.\n")
	return b.String(), nil
}
