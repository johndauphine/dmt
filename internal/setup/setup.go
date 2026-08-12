// Package setup provides a shared state machine for the unified setup wizard.
// Both CLI (`dmt setup`) and TUI (`/setup`) drive the same state machine
// with their own I/O layer.
package setup

import "github.com/johndauphine/dmt/v5/internal/config"

// Step represents a setup wizard step.
type Step int

const (
	// Phase 0: pre-flight (only entered when an existing config was loaded)
	StepEditOrNew Step = iota // prompt: edit existing config or start fresh?

	// Phase 1: AI/Secrets
	StepCheckSecrets // auto: check if secrets exist with valid AI
	StepConfigureAI  // prompt: Configure AI features? (y/n)
	StepAIProvider   // prompt: choose provider
	StepAIKey        // prompt: API key or base URL
	StepWriteSecrets // auto: write secrets file

	// Phase 1b: Slack notifications (always runs, independent of AI choice)
	StepSlackWebhook     // prompt: Slack webhook URL (blank = skip, '-' = clear)
	StepWriteSlackSecret // auto: persist webhook to secrets file

	// Phase 2: Source Database
	StepSourceType
	StepSourceHost
	StepSourcePort
	StepSourceDB
	StepSourceUser
	StepSourcePass
	StepSourceSchema
	StepSourceSSL

	// Phase 3: Source Connection Test
	StepSourceConnTest   // auto: test connection
	StepSourceConnResult // prompt: retry/edit/skip (only if failed)

	// Phase 4: Target Database
	StepTargetType
	StepTargetHost
	StepTargetPort
	StepTargetDB
	StepTargetUser
	StepTargetPass
	StepTargetSchema
	StepTargetSSL

	// Phase 5: Target Connection Test
	StepTargetConnTest   // auto: test connection
	StepTargetConnResult // prompt: retry/edit/skip (only if failed)

	// Phase 6: Migration Settings
	StepTargetMode
	StepDateColumns // prompt: date_updated_columns (only when target_mode=upsert)
	StepCreateIndexes
	StepCreateFKs
	StepWorkers

	// Phase 7: Save
	StepConfigPath
	StepSecretStorage // prompt: store DB passwords as ${file:} refs vs plaintext
	StepWriteConfig   // auto: write config file

	// Phase 8: Optional Smartconfig Analysis
	StepRunAnalysis // prompt: Run smartconfig analysis? (y/n)

	StepDone
)

// PromptInfo describes what to display for the current step.
type PromptInfo struct {
	Text          string   // the prompt text to display
	Default       string   // default value
	Choices       []string // valid choices (for display)
	IsMasked      bool     // password field
	IsAutoAction  bool     // caller should execute action, not prompt user
	SectionHeader string   // section header to display before prompt
}

// State holds the setup wizard's complete state.
type State struct {
	CurrentStep   Step
	Config        config.Config
	AIProvider    string // selected AI provider name
	AIKey         string // API key or base URL
	AIConfigured  bool   // whether AI was configured (either existing or new)
	SourceConnOK  bool   // source connection test passed
	TargetConnOK  bool   // target connection test passed
	ConfigPath    string // output config file path
	Force         bool   // overwrite existing files
	RunAnalysis   bool   // user wants to run smartconfig analysis after setup
	LastConnError string // last connection test error message
	// SlackWebhook holds the webhook URL for migration notifications.
	// Callers should pre-populate from the existing secrets file before
	// entering the wizard so edit mode shows the current value as a
	// hit-Enter-to-keep default; the wizard writes it back via
	// State.WriteSlackSecret(). On write failure the in-memory value
	// is reverted to SlackWebhookOriginal so the prompt accurately
	// reflects what's actually on disk.
	SlackWebhook string
	// SlackWebhookOriginal is the loaded-from-disk value captured at
	// wizard start. The prompt's "currently configured" hint reads
	// this (not SlackWebhook) so a failed write that leaves an unsaved
	// URL in SlackWebhook doesn't mislead the user into thinking it's
	// persisted.
	SlackWebhookOriginal string
	// EditMode is true when the wizard was launched against an existing
	// config file (caller set CurrentStep=StepEditOrNew + populated Config).
	// Pointer-bool prompt defaults (create_indexes, create_foreign_keys)
	// use this flag to decide whether a nil loaded value means "field
	// omitted in an existing config" or "fresh setup"; both fall back to
	// dmt's documented defaults.
	EditMode bool
	// ExternalizeSecrets, when true (the default), writes DB passwords to
	// 0600 sidecar files referenced by ${file:...} instead of writing them
	// as plaintext into the config YAML (#597).
	ExternalizeSecrets bool
}

// NewState creates a new setup state with sensible defaults.
func NewState() *State {
	return &State{
		CurrentStep:        StepCheckSecrets,
		ConfigPath:         "config.yaml",
		ExternalizeSecrets: true,
	}
}
