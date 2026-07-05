package setup

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
)

// Prompt returns prompt info for the current step.
func (s *State) Prompt() PromptInfo {
	switch s.CurrentStep {
	// Phase 0: pre-flight (only reached when caller pre-loads a config)
	case StepEditOrNew:
		return PromptInfo{
			Text:          fmt.Sprintf("Found existing config at %s. Edit it or start fresh? (e/n)", s.ConfigPath),
			Default:       "e",
			Choices:       []string{"e", "n"},
			SectionHeader: "Existing configuration detected",
		}

	// Phase 1: AI/Secrets (optional since #167)
	case StepCheckSecrets:
		return PromptInfo{
			Text:          "Checking for existing AI configuration...",
			IsAutoAction:  true,
			SectionHeader: "Phase 1: Optional AI Setup",
		}
	case StepConfigureAI:
		// AI is optional in dmt (#167). The deterministic type mapper,
		// error catalog, and DB tuning analyzer all run without it.
		// Default flipped from "y" to "n" so a fresh install completes
		// without prompting for API keys (#174). Users who want AI for
		// vendor-specific type mapping (Oracle hierarchyid, MSSQL
		// geography, etc.) can opt in.
		return PromptInfo{
			Text:    "Configure optional AI features (vendor-type fallback)? (y/n)",
			Default: "n",
			Choices: []string{"y", "n"},
		}
	case StepAIProvider:
		providers := sortedProviderNames()
		return PromptInfo{
			Text:    fmt.Sprintf("AI provider (%s)", strings.Join(providers, "/")),
			Default: "anthropic",
			Choices: providers,
		}
	case StepAIKey:
		if secrets.IsLocalProvider(s.AIProvider) {
			known := secrets.KnownProviders[s.AIProvider]
			return PromptInfo{
				Text:    "Base URL",
				Default: known.DefaultURL,
			}
		}
		return PromptInfo{
			Text:     "API key",
			IsMasked: true,
		}
	case StepWriteSecrets:
		return PromptInfo{
			Text:         "Saving AI configuration...",
			IsAutoAction: true,
		}

	// Phase 1b: Slack notifications (independent of AI choice)
	case StepSlackWebhook:
		// Webhook URLs are credentials in this repo (see logging/scrub.go's
		// slack_webhook regex). Echoing the value in PromptInfo.Default
		// would leak it to the CLI prompt's "[default]" rendering and into
		// the TUI scrollback that `/logs` can persist. So we deliberately
		// leave Default empty here and tell the user via Text whether a
		// value is already set. Enter still preserves it because the
		// Process side reads from s.SlackWebhook, not from Default.
		text := "Slack webhook URL (paste to set, '-' to clear, Enter to skip)"
		if s.SlackWebhookOriginal != "" {
			text = "Slack webhook URL - currently configured (Enter to keep, '-' to clear, or paste a new URL)"
		}
		return PromptInfo{
			Text:          text,
			SectionHeader: "Phase 1b: Optional Slack Notifications",
			// Mask input echo too. The TUI scrollback rendering at
			// handleSetupStep echoes any non-masked input as `> <input>`,
			// which would persist the pasted webhook URL in the buffer
			// that `/logs` can dump.
			IsMasked: true,
		}
	case StepWriteSlackSecret:
		return PromptInfo{
			Text:         "Saving Slack webhook...",
			IsAutoAction: true,
		}

	// Phase 2: Source Database
	case StepSourceType:
		types := driver.Available()
		return PromptInfo{
			Text:          fmt.Sprintf("Database type (%s)", strings.Join(types, "/")),
			Default:       defaultIfEmpty(s.Config.Source.Type, "mssql"),
			Choices:       types,
			SectionHeader: "Phase 2: Source Database",
		}
	case StepSourceHost:
		return PromptInfo{
			Text:    "Host",
			Default: defaultIfEmpty(s.Config.Source.Host, "localhost"),
		}
	case StepSourcePort:
		def := s.defaultPort(s.Config.Source.Type)
		if s.Config.Source.Port != 0 {
			def = s.Config.Source.Port
		}
		return PromptInfo{Text: "Port", Default: strconv.Itoa(def)}
	case StepSourceDB:
		return PromptInfo{Text: "Database name", Default: s.Config.Source.Database}
	case StepSourceUser:
		def := s.defaultUser(s.Config.Source.Type)
		if s.Config.Source.User != "" {
			def = s.Config.Source.User
		}
		return PromptInfo{Text: "Username", Default: def}
	case StepSourcePass:
		return PromptInfo{Text: "Password", IsMasked: true}
	case StepSourceSchema:
		def := s.defaultSchema(s.Config.Source.Type)
		if s.Config.Source.Schema != "" {
			def = s.Config.Source.Schema
		}
		return PromptInfo{Text: "Schema", Default: def}
	case StepSourceSSL:
		return s.sslPrompt(s.Config.Source.Type, s.Config.Source.SSLMode, s.Config.Source.TrustServerCert, s.Config.Source.Encrypt)

	// Phase 3: Source Connection Test
	case StepSourceConnTest:
		return PromptInfo{
			Text:          "Testing source connection...",
			IsAutoAction:  true,
			SectionHeader: "Phase 3: Source Connection Test",
		}
	case StepSourceConnResult:
		return PromptInfo{
			Text:    fmt.Sprintf("Connection failed: %s\n(r)etry / (e)dit / (s)kip", s.LastConnError),
			Default: "r",
			Choices: []string{"r", "e", "s"},
		}

	// Phase 4: Target Database
	case StepTargetType:
		types := driver.Available()
		return PromptInfo{
			Text:          fmt.Sprintf("Database type (%s)", strings.Join(types, "/")),
			Default:       defaultIfEmpty(s.Config.Target.Type, "postgres"),
			Choices:       types,
			SectionHeader: "Phase 4: Target Database",
		}
	case StepTargetHost:
		return PromptInfo{
			Text:    "Host",
			Default: defaultIfEmpty(s.Config.Target.Host, "localhost"),
		}
	case StepTargetPort:
		def := s.defaultPort(s.Config.Target.Type)
		if s.Config.Target.Port != 0 {
			def = s.Config.Target.Port
		}
		return PromptInfo{Text: "Port", Default: strconv.Itoa(def)}
	case StepTargetDB:
		return PromptInfo{Text: "Database name", Default: s.Config.Target.Database}
	case StepTargetUser:
		def := s.defaultUser(s.Config.Target.Type)
		if s.Config.Target.User != "" {
			def = s.Config.Target.User
		}
		return PromptInfo{Text: "Username", Default: def}
	case StepTargetPass:
		return PromptInfo{Text: "Password", IsMasked: true}
	case StepTargetSchema:
		def := s.defaultSchema(s.Config.Target.Type)
		if s.Config.Target.Schema != "" {
			def = s.Config.Target.Schema
		}
		return PromptInfo{Text: "Schema", Default: def}
	case StepTargetSSL:
		return s.sslPrompt(s.Config.Target.Type, s.Config.Target.SSLMode, s.Config.Target.TrustServerCert, s.Config.Target.Encrypt)

	// Phase 5: Target Connection Test
	case StepTargetConnTest:
		return PromptInfo{
			Text:          "Testing target connection...",
			IsAutoAction:  true,
			SectionHeader: "Phase 5: Target Connection Test",
		}
	case StepTargetConnResult:
		return PromptInfo{
			Text:    fmt.Sprintf("Connection failed: %s\n(r)etry / (e)dit / (s)kip", s.LastConnError),
			Default: "r",
			Choices: []string{"r", "e", "s"},
		}

	// Phase 6: Migration Settings
	case StepTargetMode:
		return PromptInfo{
			Text:          "Target mode (drop_recreate/upsert)",
			Default:       defaultIfEmpty(s.Config.Migration.TargetMode, "drop_recreate"),
			Choices:       []string{"drop_recreate", "upsert"},
			SectionHeader: "Phase 6: Migration Settings",
		}
	case StepDateColumns:
		return PromptInfo{
			Text:    "Date columns for incremental sync (comma-separated; Enter = keep, '-' = clear)",
			Default: strings.Join(s.Config.Migration.DateUpdatedColumns, ","),
		}
	case StepCreateIndexes:
		return PromptInfo{
			Text:    "Create indexes? (y/n)",
			Default: s.boolDefault(s.Config.Migration.CreateIndexes, "y"),
			Choices: []string{"y", "n"},
		}
	case StepCreateFKs:
		return PromptInfo{
			Text:    "Create foreign keys? (y/n)",
			Default: s.boolDefault(s.Config.Migration.CreateForeignKeys, "y"),
			Choices: []string{"y", "n"},
		}
	case StepWorkers:
		if s.EditMode && s.Config.Migration.Workers == 0 {
			// Loaded config omitted `workers:`. Preserve the omission so
			// runtime auto-tuning still applies. Enter leaves it unset.
			return PromptInfo{
				Text: "Workers (Enter = auto-detect at runtime)",
			}
		}
		def := runtime.NumCPU()
		if def > 8 {
			def = 8
		}
		if s.Config.Migration.Workers > 0 {
			def = s.Config.Migration.Workers
		}
		return PromptInfo{
			Text:    "Workers",
			Default: strconv.Itoa(def),
		}

	// Phase 7: Save
	case StepConfigPath:
		return PromptInfo{
			Text:          "Config file path",
			Default:       s.ConfigPath,
			SectionHeader: "Phase 7: Save Configuration",
		}
	case StepSecretStorage:
		return PromptInfo{
			Text:    "Store DB passwords in separate 0600 files (referenced by ${file:…}) instead of plaintext in the config? (y/n)",
			Default: "y",
			Choices: []string{"y", "n"},
		}
	case StepWriteConfig:
		return PromptInfo{
			Text:         "Saving configuration...",
			IsAutoAction: true,
		}

	// Phase 8: Smartconfig analysis (deterministic; AI not required —
	// the deterministic analyze flow is run via `dmt analyze` separately.
	// This step is the wizard-integrated convenience pass).
	case StepRunAnalysis:
		return PromptInfo{
			Text:          "Run analysis on source database? (y/n)",
			Default:       "y",
			Choices:       []string{"y", "n"},
			SectionHeader: "Phase 8: Optional Source Analysis",
		}

	case StepDone:
		return PromptInfo{
			Text:         "Setup complete!",
			IsAutoAction: true,
		}
	}

	return PromptInfo{Text: "Unknown step"}
}
