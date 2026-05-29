package aicopilot

const TriagePromptVersion = "ai-triage-review-v1"

const (
	TriageKindMigrationFailure    = "migration_failure"
	TriageKindValidationMismatch  = "validation_mismatch"
	TriageImpactBlocked           = "blocked"
	TriageImpactAttention         = "attention"
	TriageImpactInformational     = "informational"
	TriageImpactUnknown           = "unknown"
	TriageFindingSourceAIAdvisory = "ai_advisory"
)

const (
	ValidationCategoryDeleteDrift                  = "delete_drift"
	ValidationCategoryTypeCoercion                 = "type_coercion"
	ValidationCategoryTimezoneDateHandling         = "timezone_date_handling"
	ValidationCategoryWatermarkIssue               = "watermark_issue"
	ValidationCategoryTargetTriggerDefaultBehavior = "target_trigger_default_behavior"
	ValidationCategoryRowCountMismatch             = "row_count_mismatch"
	ValidationCategoryNullMismatch                 = "null_mismatch"
	ValidationCategorySampleMismatch               = "sample_mismatch"
	ValidationCategoryValidationRuntime            = "validation_runtime"
	ValidationCategoryUnknown                      = "unknown"
)

type TriageFact struct {
	Category string `json:"category"`
	Affected string `json:"affected,omitempty"`
	Detail   string `json:"detail"`
}

type RunStatusFact struct {
	RunID         string `json:"run_id,omitempty"`
	Status        string `json:"status,omitempty"`
	Phase         string `json:"phase,omitempty"`
	StartedAt     string `json:"started_at,omitempty"`
	CompletedAt   string `json:"completed_at,omitempty"`
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
	Error         string `json:"error,omitempty"`
}

type FailedTableFact struct {
	Table string `json:"table"`
	Error string `json:"error,omitempty"`
}

type TaskFailureFact struct {
	TaskKey    string `json:"task_key"`
	TaskType   string `json:"task_type,omitempty"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
	RowsDone   int64  `json:"rows_done,omitempty"`
	RowsTotal  int64  `json:"rows_total,omitempty"`
	RetryCount int    `json:"retry_count,omitempty"`
}

type CheckpointFact struct {
	RunID          string `json:"run_id,omitempty"`
	Phase          string `json:"phase,omitempty"`
	LastHeartbeat  string `json:"last_heartbeat,omitempty"`
	TasksTotal     int    `json:"tasks_total,omitempty"`
	TasksPending   int    `json:"tasks_pending,omitempty"`
	TasksRunning   int    `json:"tasks_running,omitempty"`
	TasksSucceeded int    `json:"tasks_succeeded,omitempty"`
	TasksFailed    int    `json:"tasks_failed,omitempty"`
	RowsDone       int64  `json:"rows_done,omitempty"`
	RowsTotal      int64  `json:"rows_total,omitempty"`
}

type SchemaDriftFact struct {
	Entity string `json:"entity,omitempty"`
	Table  string `json:"table,omitempty"`
	Object string `json:"object,omitempty"`
	Drift  string `json:"drift,omitempty"`
	Action string `json:"action,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type DeterministicDiagnosisFact struct {
	Category    string   `json:"category,omitempty"`
	Cause       string   `json:"cause,omitempty"`
	Confidence  string   `json:"confidence,omitempty"`
	PatternName string   `json:"pattern_name,omitempty"`
	Suggestions []string `json:"suggestions,omitempty"`
}

type MigrationFailureFacts struct {
	RunID                  string                       `json:"run_id,omitempty"`
	Phase                  string                       `json:"phase,omitempty"`
	Table                  string                       `json:"table,omitempty"`
	Error                  string                       `json:"error,omitempty"`
	LastSQL                string                       `json:"last_sql,omitempty"`
	SafeSQLShape           string                       `json:"safe_sql_shape,omitempty"`
	RunStatus              *RunStatusFact               `json:"run_status,omitempty"`
	FailedTables           []FailedTableFact            `json:"failed_tables,omitempty"`
	TaskFailures           []TaskFailureFact            `json:"task_failures,omitempty"`
	DeterministicDiagnoses []DeterministicDiagnosisFact `json:"deterministic_diagnoses,omitempty"`
	Checkpoint             *CheckpointFact              `json:"checkpoint,omitempty"`
	SchemaDrift            []SchemaDriftFact            `json:"schema_drift,omitempty"`
	RecentCheckpoints      []CheckpointFact             `json:"recent_checkpoints,omitempty"`
	RowsCopied             int64                        `json:"rows_copied,omitempty"`
	ElapsedSeconds         float64                      `json:"elapsed_seconds,omitempty"`
	Retryable              bool                         `json:"retryable"`
}

type ValidationPassFact struct {
	Name   string `json:"name"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

type ValidationDifferenceFact struct {
	Category string `json:"category"`
	Table    string `json:"table,omitempty"`
	Pass     string `json:"pass,omitempty"`
	Column   string `json:"column,omitempty"`
	Severity string `json:"severity,omitempty"`
	Detail   string `json:"detail"`
}

type ValidationMismatchFacts struct {
	Mode              string                     `json:"mode,omitempty"`
	Table             string                     `json:"table,omitempty"`
	SourceCount       int64                      `json:"source_count,omitempty"`
	TargetCount       int64                      `json:"target_count,omitempty"`
	Difference        int64                      `json:"difference,omitempty"`
	HasRowCountFacts  bool                       `json:"has_row_count_facts,omitempty"`
	UsedEstimate      bool                       `json:"used_estimate"`
	ExactTimedOut     bool                       `json:"exact_timed_out"`
	TimedOut          bool                       `json:"timed_out"`
	Error             string                     `json:"error,omitempty"`
	Passes            []ValidationPassFact       `json:"passes,omitempty"`
	Differences       []ValidationDifferenceFact `json:"differences,omitempty"`
	SchemaDrift       []SchemaDriftFact          `json:"schema_drift,omitempty"`
	Checkpoint        *CheckpointFact            `json:"checkpoint,omitempty"`
	RecentCheckpoints []CheckpointFact           `json:"recent_checkpoints,omitempty"`
}

type TriagePayload struct {
	PromptVersion      string                   `json:"prompt_version"`
	Task               string                   `json:"task"`
	Kind               string                   `json:"kind"`
	Config             ConfigSummary            `json:"config,omitempty"`
	MigrationFailure   *MigrationFailureFacts   `json:"migration_failure,omitempty"`
	ValidationMismatch *ValidationMismatchFacts `json:"validation_mismatch,omitempty"`
	DeterministicFacts []TriageFact             `json:"deterministic_facts"`
	Redaction          RedactionSummary         `json:"redaction"`
}

type TriageHypothesis struct {
	Confidence string `json:"confidence"`
	Rationale  string `json:"rationale"`
}

type TriageFinding struct {
	Severity               string             `json:"severity"`
	Category               string             `json:"category"`
	Affected               string             `json:"affected,omitempty"`
	AffectedTables         []string           `json:"affected_tables,omitempty"`
	DeterministicFacts     []string           `json:"deterministic_facts,omitempty"`
	LikelyCause            string             `json:"likely_cause,omitempty"`
	Hypotheses             []TriageHypothesis `json:"hypotheses,omitempty"`
	SuggestedCommands      []string           `json:"suggested_commands,omitempty"`
	SuggestedConfigChanges []string           `json:"suggested_config_changes,omitempty"`
	ManualInspection       string             `json:"manual_inspection,omitempty"`
	NextAction             string             `json:"next_action"`
	Source                 string             `json:"source"`
}

type TriageReview struct {
	Enabled            bool            `json:"enabled"`
	Status             string          `json:"status"`
	Provider           string          `json:"provider,omitempty"`
	Model              string          `json:"model,omitempty"`
	PromptVersion      string          `json:"prompt_version"`
	Kind               string          `json:"kind"`
	Impact             string          `json:"impact"`
	Summary            string          `json:"summary"`
	DeterministicFacts []TriageFact    `json:"deterministic_facts,omitempty"`
	Findings           []TriageFinding `json:"findings,omitempty"`
	Notes              []string        `json:"notes,omitempty"`
	Error              string          `json:"error,omitempty"`
}
