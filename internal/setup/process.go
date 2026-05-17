package setup

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
)

// Process handles input for the current step.
// For auto-action steps, input contains the result of the action
// (e.g., "" for success, error message for failure, "has_ai"/"no_ai" for secrets check).
// Returns an error message if validation fails, or "" on success.
func (s *State) Process(input string) string {
	input = strings.TrimSpace(input)

	switch s.CurrentStep {
	// Phase 0: pre-flight
	case StepEditOrNew:
		v := strings.ToLower(input)
		if v == "" {
			v = "e"
		}
		switch v {
		case "e", "edit":
			// Keep pre-loaded Config as-is; defaults will appear in each step.
		case "n", "new":
			// User chose to start fresh; discard the pre-loaded config but
			// preserve ConfigPath so we save back to the same file.
			// Also clear EditMode so bool defaults revert to "fresh" values.
			path := s.ConfigPath
			s.Config = config.Config{}
			s.ConfigPath = path
			s.EditMode = false
		default:
			return "Enter e to edit, n to start new"
		}
		s.CurrentStep = StepCheckSecrets

	// Phase 1: AI
	case StepCheckSecrets:
		if input == "has_ai" {
			s.AIConfigured = true
			s.CurrentStep = StepSlackWebhook
		} else {
			s.CurrentStep = StepConfigureAI
		}

	case StepConfigureAI:
		v := strings.ToLower(input)
		if v == "" {
			// Default flipped to "n" in #174. AI is optional and
			// skipping is the friction-free path for a fresh install.
			v = "n"
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		if v == "n" {
			s.CurrentStep = StepSlackWebhook
		} else {
			s.CurrentStep = StepAIProvider
		}

	case StepAIProvider:
		if input == "" {
			input = "anthropic"
		}
		input = strings.ToLower(input)
		if _, ok := secrets.KnownProviders[input]; !ok {
			providers := sortedProviderNames()
			return fmt.Sprintf("Unknown provider. Options: %s", strings.Join(providers, ", "))
		}
		s.AIProvider = input
		s.CurrentStep = StepAIKey

	case StepAIKey:
		if secrets.IsLocalProvider(s.AIProvider) {
			if input == "" {
				known := secrets.KnownProviders[s.AIProvider]
				input = known.DefaultURL
			}
			s.AIKey = input
		} else {
			if input == "" {
				return "API key is required for cloud providers"
			}
			s.AIKey = input
		}
		s.AIConfigured = true
		s.CurrentStep = StepWriteSecrets

	case StepWriteSecrets:
		if input != "" {
			// Write failed; go back to provider selection so user can fix.
			s.CurrentStep = StepAIProvider
			return fmt.Sprintf("Failed to save secrets: %s", input)
		}
		s.CurrentStep = StepSlackWebhook

	// Phase 1b: Slack
	case StepSlackWebhook:
		switch input {
		case "":
			// Wizard convention: blank = keep the displayed default.
			// Nothing to write; secrets file already has SlackWebhook
			// (or is empty and we're skipping).
			s.CurrentStep = StepSourceType
		case "-":
			// Explicit clear is required since blank means "keep" everywhere
			// else in the wizard. Single canonical sentinel keeps the prompt
			// text and accepted inputs in lockstep.
			s.SlackWebhook = ""
			s.CurrentStep = StepWriteSlackSecret
		default:
			s.SlackWebhook = input
			s.CurrentStep = StepWriteSlackSecret
		}

	case StepWriteSlackSecret:
		if input != "" {
			// Write failed; revert SlackWebhook to the loaded-from-disk
			// value so the next prompt's "currently configured" hint
			// matches what's actually on disk. Without this revert, the
			// failed write's URL stays in SlackWebhook and Enter (= keep
			// current) would silently skip the save retry, completing
			// the wizard with no persisted webhook.
			s.SlackWebhook = s.SlackWebhookOriginal
			s.CurrentStep = StepSlackWebhook
			return fmt.Sprintf("Failed to save Slack webhook: %s (in-memory change reverted; re-paste to retry)", input)
		}
		// Write succeeded; update Original so a subsequent Enter on the
		// prompt won't re-trigger a save.
		s.SlackWebhookOriginal = s.SlackWebhook
		s.CurrentStep = StepSourceType

	// Phase 2: Source
	case StepSourceType:
		if input == "" {
			input = defaultIfEmpty(s.Config.Source.Type, "mssql")
		}
		if !driver.IsRegistered(input) {
			return fmt.Sprintf("Unknown database type. Options: %s", strings.Join(driver.Available(), ", "))
		}
		s.Config.Source.Type = driver.Canonicalize(input)
		s.CurrentStep = StepSourceHost

	case StepSourceHost:
		if input == "" {
			input = defaultIfEmpty(s.Config.Source.Host, "localhost")
		}
		s.Config.Source.Host = input
		s.CurrentStep = StepSourcePort

	case StepSourcePort:
		if input == "" {
			def := s.defaultPort(s.Config.Source.Type)
			if s.Config.Source.Port != 0 {
				def = s.Config.Source.Port
			}
			s.Config.Source.Port = def
		} else {
			port, err := strconv.Atoi(input)
			if err != nil || port <= 0 || port > 65535 {
				return "Port must be a number between 1 and 65535"
			}
			s.Config.Source.Port = port
		}
		s.CurrentStep = StepSourceDB

	case StepSourceDB:
		if input == "" && s.Config.Source.Database == "" {
			return "Database name is required"
		}
		if input != "" {
			s.Config.Source.Database = input
		}
		s.CurrentStep = StepSourceUser

	case StepSourceUser:
		if input == "" {
			if s.Config.Source.User == "" {
				s.Config.Source.User = s.defaultUser(s.Config.Source.Type)
			}
		} else {
			s.Config.Source.User = input
		}
		s.CurrentStep = StepSourcePass

	case StepSourcePass:
		if input != "" {
			s.Config.Source.Password = input
		}
		s.CurrentStep = StepSourceSchema

	case StepSourceSchema:
		if input == "" {
			if s.Config.Source.Schema == "" {
				s.Config.Source.Schema = s.defaultSchema(s.Config.Source.Type)
			}
		} else {
			s.Config.Source.Schema = input
		}
		s.CurrentStep = StepSourceSSL

	case StepSourceSSL:
		s.processSSL(input, true)
		s.CurrentStep = StepSourceConnTest

	// Phase 3: Source Connection Test
	case StepSourceConnTest:
		if input == "" {
			s.SourceConnOK = true
			s.CurrentStep = StepTargetType
		} else {
			s.SourceConnOK = false
			s.LastConnError = input
			s.CurrentStep = StepSourceConnResult
		}

	case StepSourceConnResult:
		v := strings.ToLower(input)
		if v == "" {
			v = "r"
		}
		switch v {
		case "r", "retry":
			s.CurrentStep = StepSourceConnTest
		case "e", "edit":
			s.CurrentStep = StepSourceType
		case "s", "skip":
			s.CurrentStep = StepTargetType
		default:
			return "Enter r to retry, e to edit, or s to skip"
		}

	// Phase 4: Target
	case StepTargetType:
		if input == "" {
			input = defaultIfEmpty(s.Config.Target.Type, "postgres")
		}
		if !driver.IsRegistered(input) {
			return fmt.Sprintf("Unknown database type. Options: %s", strings.Join(driver.Available(), ", "))
		}
		s.Config.Target.Type = driver.Canonicalize(input)
		s.CurrentStep = StepTargetHost

	case StepTargetHost:
		if input == "" {
			input = defaultIfEmpty(s.Config.Target.Host, "localhost")
		}
		s.Config.Target.Host = input
		s.CurrentStep = StepTargetPort

	case StepTargetPort:
		if input == "" {
			def := s.defaultPort(s.Config.Target.Type)
			if s.Config.Target.Port != 0 {
				def = s.Config.Target.Port
			}
			s.Config.Target.Port = def
		} else {
			port, err := strconv.Atoi(input)
			if err != nil || port <= 0 || port > 65535 {
				return "Port must be a number between 1 and 65535"
			}
			s.Config.Target.Port = port
		}
		s.CurrentStep = StepTargetDB

	case StepTargetDB:
		if input == "" && s.Config.Target.Database == "" {
			return "Database name is required"
		}
		if input != "" {
			s.Config.Target.Database = input
		}
		s.CurrentStep = StepTargetUser

	case StepTargetUser:
		if input == "" {
			if s.Config.Target.User == "" {
				s.Config.Target.User = s.defaultUser(s.Config.Target.Type)
			}
		} else {
			s.Config.Target.User = input
		}
		s.CurrentStep = StepTargetPass

	case StepTargetPass:
		if input != "" {
			s.Config.Target.Password = input
		}
		s.CurrentStep = StepTargetSchema

	case StepTargetSchema:
		if input == "" {
			if s.Config.Target.Schema == "" {
				s.Config.Target.Schema = s.defaultSchema(s.Config.Target.Type)
			}
		} else {
			s.Config.Target.Schema = input
		}
		s.CurrentStep = StepTargetSSL

	case StepTargetSSL:
		s.processSSL(input, false)
		s.CurrentStep = StepTargetConnTest

	// Phase 5: Target Connection Test
	case StepTargetConnTest:
		if input == "" {
			s.TargetConnOK = true
			s.CurrentStep = StepTargetMode
		} else {
			s.TargetConnOK = false
			s.LastConnError = input
			s.CurrentStep = StepTargetConnResult
		}

	case StepTargetConnResult:
		v := strings.ToLower(input)
		if v == "" {
			v = "r"
		}
		switch v {
		case "r", "retry":
			s.CurrentStep = StepTargetConnTest
		case "e", "edit":
			s.CurrentStep = StepTargetType
		case "s", "skip":
			s.CurrentStep = StepTargetMode
		default:
			return "Enter r to retry, e to edit, or s to skip"
		}

	// Phase 6: Migration Settings
	case StepTargetMode:
		if input == "" {
			input = defaultIfEmpty(s.Config.Migration.TargetMode, "drop_recreate")
		}
		if input != "drop_recreate" && input != "upsert" {
			return "Options: drop_recreate, upsert"
		}
		s.Config.Migration.TargetMode = input
		if input == "upsert" {
			s.CurrentStep = StepDateColumns
		} else {
			s.CurrentStep = StepCreateIndexes
		}

	case StepDateColumns:
		switch input {
		case "":
			// Empty input preserves the existing list (wizard convention:
			// Enter accepts the displayed default).
		case "-":
			// Explicit clear is required to remove a previously-set list,
			// since blank means "keep" everywhere else in the wizard.
			// Single canonical sentinel matches the prompt text.
			s.Config.Migration.DateUpdatedColumns = nil
		default:
			parts := strings.Split(input, ",")
			cols := make([]string, 0, len(parts))
			for _, p := range parts {
				if p = strings.TrimSpace(p); p != "" {
					cols = append(cols, p)
				}
			}
			s.Config.Migration.DateUpdatedColumns = cols
		}
		s.CurrentStep = StepCreateIndexes

	case StepCreateIndexes:
		v := strings.ToLower(input)
		if v == "" {
			v = s.boolDefault(s.Config.Migration.CreateIndexes, "y")
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		enabled := v == "y"
		s.Config.Migration.CreateIndexes = &enabled
		s.CurrentStep = StepCreateFKs

	case StepCreateFKs:
		v := strings.ToLower(input)
		if v == "" {
			v = s.boolDefault(s.Config.Migration.CreateForeignKeys, "y")
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		enabled := v == "y"
		s.Config.Migration.CreateForeignKeys = &enabled
		s.CurrentStep = StepWorkers

	case StepWorkers:
		if input == "" {
			// Fresh setup (Workers==0): apply NumCPU-capped default.
			// EditMode + Workers==0: leave it 0 so the YAML round-trip
			// preserves the omission and runtime auto-tunes.
			// Either mode + Workers>0: keep the existing value.
			if !s.EditMode && s.Config.Migration.Workers == 0 {
				def := runtime.NumCPU()
				if def > 8 {
					def = 8
				}
				s.Config.Migration.Workers = def
			}
		} else {
			workers, err := strconv.Atoi(input)
			if err != nil || workers <= 0 {
				return "Workers must be a positive number"
			}
			s.Config.Migration.Workers = workers
		}
		s.CurrentStep = StepConfigPath

	// Phase 7: Save
	case StepConfigPath:
		if input == "" {
			input = s.ConfigPath
		}
		s.ConfigPath = input
		s.CurrentStep = StepWriteConfig

	case StepWriteConfig:
		if input != "" {
			// Write failed; go back to config path so user can fix.
			s.CurrentStep = StepConfigPath
			return fmt.Sprintf("Failed to save config: %s", input)
		}
		if s.AIConfigured && s.SourceConnOK {
			s.CurrentStep = StepRunAnalysis
		} else {
			s.CurrentStep = StepDone
		}

	// Phase 8: AI Analysis
	case StepRunAnalysis:
		v := strings.ToLower(input)
		if v == "" {
			v = "y"
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		s.RunAnalysis = (v == "y")
		s.CurrentStep = StepDone

	case StepDone:
		// Nothing to do
	}

	return ""
}
