package aicopilot

const ConfigReviewPromptVersion = "ai-config-review-v1"

const ReviewStatusRefused = "refused"

type ConfigReviewOptions struct {
	OperatorRequest string
}

type ConfigReviewSafetyContext struct {
	AllowedPatchPaths  []string `json:"allowed_patch_paths"`
	DisallowedContent  []string `json:"disallowed_content"`
	RefusalReason      string   `json:"refusal_reason,omitempty"`
	RequiresReviewOnly bool     `json:"requires_review_only"`
}

type ConfigReviewPayload struct {
	PromptVersion   string                    `json:"prompt_version"`
	Task            string                    `json:"task"`
	OperatorRequest string                    `json:"operator_request,omitempty"`
	Config          ConfigSummary             `json:"config"`
	Safety          ConfigReviewSafetyContext `json:"safety"`
	Redaction       RedactionSummary          `json:"redaction"`

	sensitiveValues []string
}

type ConfigPatchRecommendation struct {
	Operation            string `json:"operation"`
	Path                 string `json:"path"`
	Value                any    `json:"value,omitempty"`
	Rationale            string `json:"rationale"`
	Risk                 string `json:"risk,omitempty"`
	WhenToApply          string `json:"when_to_apply,omitempty"`
	RequiresConfirmation bool   `json:"requires_confirmation"`
}

type ConfigRunbook struct {
	Title      string   `json:"title"`
	Summary    string   `json:"summary"`
	BeforeRun  []string `json:"before_run,omitempty"`
	Run        []string `json:"run,omitempty"`
	Validation []string `json:"validation,omitempty"`
	Rollback   []string `json:"rollback,omitempty"`
}

type ConfigReview struct {
	Enabled              bool                        `json:"enabled"`
	Status               string                      `json:"status"`
	Provider             string                      `json:"provider,omitempty"`
	Model                string                      `json:"model,omitempty"`
	PromptVersion        string                      `json:"prompt_version"`
	Summary              string                      `json:"summary"`
	PatchRecommendations []ConfigPatchRecommendation `json:"patch_recommendations,omitempty"`
	Runbook              ConfigRunbook               `json:"runbook,omitempty"`
	Notes                []string                    `json:"notes,omitempty"`
	RefusalReason        string                      `json:"refusal_reason,omitempty"`
	Error                string                      `json:"error,omitempty"`
}
