package aicopilot

const SchemaAdvisorPromptVersion = "ai-schema-evolution-advisor-v1"

const (
	SchemaRiskLow     = "low"
	SchemaRiskMedium  = "medium"
	SchemaRiskHigh    = "high"
	SchemaRiskBlocked = "blocked"
	SchemaRiskUnknown = "unknown"
)

type SchemaAdvisorPolicyGate struct {
	Allowed bool   `json:"allowed"`
	Action  string `json:"action"`
	Policy  string `json:"policy,omitempty"`
	Reason  string `json:"reason"`
}

type SchemaDriftAdvisoryChange struct {
	DriftKind         string                  `json:"drift_kind"`
	Classification    string                  `json:"classification"`
	Risk              string                  `json:"risk"`
	Schema            string                  `json:"schema,omitempty"`
	Table             string                  `json:"table"`
	Column            string                  `json:"column,omitempty"`
	Previous          string                  `json:"previous,omitempty"`
	Current           string                  `json:"current,omitempty"`
	Reason            string                  `json:"reason"`
	SuggestedPolicy   string                  `json:"suggested_policy"`
	SuggestedAction   string                  `json:"suggested_action"`
	ManualGuidance    []string                `json:"manual_guidance,omitempty"`
	DeterministicGate SchemaAdvisorPolicyGate `json:"deterministic_gate"`
}

type SchemaAdvisorPayload struct {
	PromptVersion         string                      `json:"prompt_version"`
	Task                  string                      `json:"task"`
	Config                ConfigSummary               `json:"config"`
	SourceDBType          string                      `json:"source_db_type,omitempty"`
	TargetDBType          string                      `json:"target_db_type,omitempty"`
	AllowSchemaEvolution  bool                        `json:"allow_schema_evolution"`
	Changes               []SchemaDriftAdvisoryChange `json:"changes"`
	DeterministicBlockers []string                    `json:"deterministic_blockers,omitempty"`
	Redaction             RedactionSummary            `json:"redaction"`
}

type SchemaAdvisorRecommendation struct {
	DriftKind         string                  `json:"drift_kind"`
	Classification    string                  `json:"classification"`
	Risk              string                  `json:"risk"`
	Schema            string                  `json:"schema,omitempty"`
	Table             string                  `json:"table"`
	Column            string                  `json:"column,omitempty"`
	Reason            string                  `json:"reason"`
	SuggestedPolicy   string                  `json:"suggested_policy"`
	SuggestedAction   string                  `json:"suggested_action"`
	ManualGuidance    []string                `json:"manual_guidance,omitempty"`
	DeterministicGate SchemaAdvisorPolicyGate `json:"deterministic_gate"`
	Source            string                  `json:"source"`
}

type SchemaAdvisorReview struct {
	Enabled               bool                          `json:"enabled"`
	Status                string                        `json:"status"`
	Provider              string                        `json:"provider,omitempty"`
	Model                 string                        `json:"model,omitempty"`
	PromptVersion         string                        `json:"prompt_version"`
	Summary               string                        `json:"summary"`
	DeterministicBlockers []string                      `json:"deterministic_blockers,omitempty"`
	Recommendations       []SchemaAdvisorRecommendation `json:"recommendations,omitempty"`
	Notes                 []string                      `json:"notes,omitempty"`
	Error                 string                        `json:"error,omitempty"`
}
