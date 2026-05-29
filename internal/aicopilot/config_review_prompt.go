package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildConfigReviewPrompt(payload ConfigReviewPayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling config review payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI config reviewer and runbook assistant.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for, infer, reveal, or fabricate omitted connection values.\n")
	b.WriteString("Only recommend patch-style changes under safety.allowed_patch_paths. Never recommend edits to secrets, hosts, ports, users, database names, DSNs, API keys, or webhook URLs.\n")
	b.WriteString("Cover target mode, schema contract/evolution, validation, deletes, checkpoint/retry settings, workers/chunking, notifications, and secrets placement when relevant.\n")
	b.WriteString("Recommendations are advisory and must be safe for an operator to review manually; do not claim that changes have been applied.\n")
	b.WriteString("Refuse with status=refused when safety.refusal_reason is present or the operator request is ambiguous, asks for credentials, or asks to bypass preflight/validation controls.\n")
	b.WriteString("Never recommend destructive target actions unless requires_confirmation=true and the rationale mentions backup verification and operator confirmation.\n")
	b.WriteString("The runbook must include the exact commands from payload.commands, explicit prerequisites, expected validation, rollback notes, and concise risk callouts.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"summary":"one sentence","patch_recommendations":[{"operation":"set|add|remove","path":"migration.validation.mode","value":"sample","rationale":"why","risk":"short risk","when_to_apply":"condition","requires_confirmation":false}],"runbook":{"title":"short","summary":"short","before_run":["steps"],"run":["steps"],"validation":["steps"],"rollback":["steps"]},"notes":["optional"],"refusal_reason":"optional"}` + "\n")
	b.WriteString("Keep patch_recommendations to the top 5. Keep each runbook list to at most 5 concise steps.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	return b.String(), nil
}
