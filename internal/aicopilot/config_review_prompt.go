package aicopilot

import (
	"encoding/json"
	"fmt"
	"strings"
)

func BuildConfigReviewPrompt(payload ConfigReviewPayload) (string, error) {
	promptPayload := configReviewPromptPayload(payload)
	targetChangingReview := configReviewTargetChangingMode(payload.Safety.TargetMode)
	data, err := json.MarshalIndent(promptPayload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling config review payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI config reviewer and runbook assistant.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for, infer, reveal, or fabricate omitted connection values.\n")
	if targetChangingReview {
		b.WriteString("This is a target-changing review-only payload. Treat that as authoritative without naming or describing the underlying mode.\n")
	}
	b.WriteString("Only recommend patch-style changes under safety.allowed_patch_paths. Copy paths exactly from that list; do not invent alternate path spellings or related notification paths.\n")
	b.WriteString("For migration.validation.mode, only use count_only, null_parity, or sample. Never suggest full, row_hash, row_sample, or custom validation modes.\n")
	b.WriteString("Never recommend edits to secrets, hosts, ports, users, database names, DSNs, API keys, or webhook URLs.\n")
	b.WriteString("Cover target-mode review state, schema contract/evolution, validation, checkpoint/retry settings, workers/chunking, notifications, and secrets placement when relevant.\n")
	b.WriteString("Recommendations are advisory and must be safe for an operator to review manually; do not claim that changes have been applied.\n")
	b.WriteString("Refuse with status=refused when safety.refusal_reason is present or the operator request is ambiguous, asks for credentials, or asks to bypass preflight/validation controls.\n")
	b.WriteString("If safety.requires_review_only is true or safety.target_mode is the destructive target mode, every patch recommendation must set requires_confirmation=true.\n")
	b.WriteString("Never output requires_confirmation=false. If a recommendation does not need confirmation, omit that patch recommendation.\n")
	b.WriteString("Never recommend target-changing actions. If a target-changing mode is already configured, discuss only review gates, validation, backups, and operator confirmation.\n")
	b.WriteString("Do not include shell commands or DMT command lines in your JSON. The deterministic post-processor will insert exact commands from the original command context.\n")
	b.WriteString("Do not use destructive words or strings such as drop_recreate, drop, delete, truncate, recreate, remove target, reload target, or wipe in generated prose; say configured target mode instead of repeating a destructive mode value.\n")
	b.WriteString("Do not write negative destructive advice like do not drop; say avoid target DDL or data-changing actions instead.\n")
	b.WriteString("Avoid causal-certainty wording in all output: do not use the exact phrase root cause, even in negated sentences; do not use proves, definitely, certainly, must be caused by, is caused by, or the cause is.\n")
	b.WriteString("The runbook must include command intent labels, explicit prerequisites, expected validation, rollback notes, and concise risk callouts.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"summary":"one sentence","patch_recommendations":[{"operation":"set|add|remove","path":"migration.validation.mode","value":"sample","rationale":"why","risk":"short risk","when_to_apply":"condition","requires_confirmation":true}],"runbook":{"title":"short","summary":"short","before_run":["steps without command text"],"run":["steps without command text"],"validation":["steps without command text"],"rollback":["steps without command text"]},"notes":["optional"],"refusal_reason":"optional"}` + "\n")
	b.WriteString("Keep patch_recommendations to the top 5. Keep each runbook list to at most 5 concise steps.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	b.WriteString("\n\nFinal response checklist:\n")
	b.WriteString("- Every patch path must be copied exactly from payload.safety.allowed_patch_paths; if a path is not listed, omit that patch.\n")
	b.WriteString("- Every patch recommendation must have requires_confirmation=true.\n")
	b.WriteString("- Do not quote or invent shell command lines, DMT command lines, or the underlying target-mode value in the JSON.\n")
	b.WriteString("- Do not use destructive or causal-certainty wording in any generated string.\n")
	b.WriteString("- Forbidden output tokens in JSON values: drop, dropped, dropping, drop_recreate, delete, deleted, deleting, truncate, recreate, remove, reload, wipe, root cause, caused by, proves, definitely, certainly. Use neutral wording such as configured target mode, target-changing risk, or deterministic evidence instead.\n")
	b.WriteString("- Do not explain what the configured target mode does to target rows, tables, or schema. For any target-mode risk, use exactly this sentence: Requires operator review because this is a target-changing migration mode.\n")
	b.WriteString("- Use this exact summary for a target-changing review-only payload: Review-only config guidance with deterministic validation and operator gates.\n")
	b.WriteString("- For every patch risk value, use either an empty string or exactly: Requires operator review and verified backup before applying.\n")
	b.WriteString("- When safety.requires_review_only is true or this is a target-changing review-only payload, prefer only these neutral patches when the exact path is allowed: migration.validation.mode=sample; migration.validation.fail_on_mismatch=true; migration.max_retries=3; migration.notify.on_failure=true. Omit all other patches.\n")
	b.WriteString("- For those neutral patches, use only these rationale sentences: Adds deterministic validation evidence before operator review.; Keeps mismatch handling strict for operator review.; Keeps retries bounded for checkpoint recovery.; Routes failure signals to configured notification channels.\n")
	b.WriteString("- In patch rationale and runbook text, discuss only validation, checkpointing, worker/chunk sizing, notifications, secrets placement, and operator review; do not discuss target data effects.\n")
	b.WriteString("- Do not put row, rows, table, tables, schema, or schemas in the same generated sentence as configured target mode or target-changing risk.\n")
	return b.String(), nil
}

type configReviewPromptPayloadEnvelope struct {
	PromptVersion   string                    `json:"prompt_version"`
	Task            string                    `json:"task"`
	OperatorRequest string                    `json:"operator_request,omitempty"`
	Config          configReviewPromptConfig  `json:"config"`
	CommandIntents  []string                  `json:"command_intents"`
	Safety          ConfigReviewSafetyContext `json:"safety"`
	Redaction       RedactionSummary          `json:"redaction"`
}

type configReviewPromptConfig struct {
	Source           EndpointSummary                    `json:"source"`
	Target           EndpointSummary                    `json:"target"`
	Migration        configReviewPromptMigrationSummary `json:"migration"`
	SecretsPlacement SecretsPlacementSummary            `json:"secrets_placement"`
}

type configReviewPromptMigrationSummary struct {
	TargetMode             string                 `json:"target_mode"`
	Workers                int                    `json:"workers"`
	ChunkSize              int                    `json:"chunk_size"`
	ReadAheadBuffers       int                    `json:"read_ahead_buffers"`
	WriteAheadWriters      int                    `json:"write_ahead_writers"`
	ParallelReaders        int                    `json:"parallel_readers"`
	MaxSourceConnections   int                    `json:"max_source_connections"`
	MaxTargetConnections   int                    `json:"max_target_connections"`
	MaxPartitions          int                    `json:"max_partitions"`
	LargeTableThreshold    int64                  `json:"large_table_threshold"`
	StrictConsistency      bool                   `json:"strict_consistency"`
	CreateIndexes          bool                   `json:"create_indexes"`
	CreateForeignKeys      bool                   `json:"create_foreign_keys"`
	CreateCheckConstraints bool                   `json:"create_check_constraints"`
	AllowPartial           bool                   `json:"allow_partial"`
	UnmappedTypeAction     string                 `json:"unmapped_type_action,omitempty"`
	ApproxTypeAction       string                 `json:"approx_type_action,omitempty"`
	SchemaContract         SchemaContractSummary  `json:"schema_contract"`
	SchemaEvolution        SchemaEvolutionSummary `json:"schema_evolution"`
	Validation             ValidationSummary      `json:"validation"`
	Checkpoint             CheckpointSummary      `json:"checkpoint"`
	Notifications          NotificationSummary    `json:"notifications"`
}

func configReviewPromptPayload(payload ConfigReviewPayload) configReviewPromptPayloadEnvelope {
	safety := payload.Safety
	migration := payload.Config.Migration
	targetMode := migration.TargetMode
	task := configReviewNeutralTargetModeText(payload.Task)
	if configReviewTargetChangingMode(payload.Safety.TargetMode) || configReviewTargetChangingMode(targetMode) {
		safety.TargetMode = "configured_target_mode"
		targetMode = "configured_target_mode"
	}
	return configReviewPromptPayloadEnvelope{
		PromptVersion:   payload.PromptVersion,
		Task:            task,
		OperatorRequest: payload.OperatorRequest,
		Config: configReviewPromptConfig{
			Source: payload.Config.Source,
			Target: payload.Config.Target,
			Migration: configReviewPromptMigrationSummary{
				TargetMode:             targetMode,
				Workers:                migration.Workers,
				ChunkSize:              migration.ChunkSize,
				ReadAheadBuffers:       migration.ReadAheadBuffers,
				WriteAheadWriters:      migration.WriteAheadWriters,
				ParallelReaders:        migration.ParallelReaders,
				MaxSourceConnections:   migration.MaxSourceConnections,
				MaxTargetConnections:   migration.MaxTargetConnections,
				MaxPartitions:          migration.MaxPartitions,
				LargeTableThreshold:    migration.LargeTableThreshold,
				StrictConsistency:      migration.StrictConsistency,
				CreateIndexes:          migration.CreateIndexes,
				CreateForeignKeys:      migration.CreateForeignKeys,
				CreateCheckConstraints: migration.CreateCheckConstraints,
				AllowPartial:           migration.AllowPartial,
				UnmappedTypeAction:     migration.UnmappedTypeAction,
				ApproxTypeAction:       migration.ApproxTypeAction,
				SchemaContract:         migration.SchemaContract,
				SchemaEvolution:        migration.SchemaEvolution,
				Validation:             migration.Validation,
				Checkpoint:             migration.Checkpoint,
				Notifications:          migration.Notifications,
			},
			SecretsPlacement: payload.Config.SecretsPlacement,
		},
		CommandIntents: []string{"preflight", "run", "validate", "resume"},
		Safety:         safety,
		Redaction:      payload.Redaction,
	}
}

func configReviewTargetChangingMode(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "drop_recreate")
}

func configReviewNeutralTargetModeText(value string) string {
	replacer := strings.NewReplacer(
		"drop_recreate", "configured target mode",
		"drop recreate", "configured target mode",
		"Drop recreate", "configured target mode",
		"Drop Recreate", "configured target mode",
	)
	return replacer.Replace(value)
}
