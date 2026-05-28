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

type TriageFact struct {
	Category string `json:"category"`
	Affected string `json:"affected,omitempty"`
	Detail   string `json:"detail"`
}

type FailedTableFact struct {
	Table string `json:"table"`
	Error string `json:"error,omitempty"`
}

type MigrationFailureFacts struct {
	RunID          string            `json:"run_id,omitempty"`
	Phase          string            `json:"phase,omitempty"`
	Table          string            `json:"table,omitempty"`
	Error          string            `json:"error,omitempty"`
	LastSQL        string            `json:"last_sql,omitempty"`
	FailedTables   []FailedTableFact `json:"failed_tables,omitempty"`
	RowsCopied     int64             `json:"rows_copied,omitempty"`
	ElapsedSeconds float64           `json:"elapsed_seconds,omitempty"`
	Retryable      bool              `json:"retryable"`
}

type ValidationPassFact struct {
	Name   string `json:"name"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

type ValidationMismatchFacts struct {
	Mode          string               `json:"mode,omitempty"`
	Table         string               `json:"table,omitempty"`
	SourceCount   int64                `json:"source_count,omitempty"`
	TargetCount   int64                `json:"target_count,omitempty"`
	Difference    int64                `json:"difference,omitempty"`
	UsedEstimate  bool                 `json:"used_estimate"`
	ExactTimedOut bool                 `json:"exact_timed_out"`
	TimedOut      bool                 `json:"timed_out"`
	Error         string               `json:"error,omitempty"`
	Passes        []ValidationPassFact `json:"passes,omitempty"`
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
	Severity           string             `json:"severity"`
	Category           string             `json:"category"`
	Affected           string             `json:"affected,omitempty"`
	DeterministicFacts []string           `json:"deterministic_facts,omitempty"`
	Hypotheses         []TriageHypothesis `json:"hypotheses,omitempty"`
	NextAction         string             `json:"next_action"`
	Source             string             `json:"source"`
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
