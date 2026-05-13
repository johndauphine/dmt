// Package setup provides a shared state machine for the unified setup wizard.
// Both CLI (`dmt setup`) and TUI (`/setup`) drive the same state machine
// with their own I/O layer.
package setup

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
)

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
	StepWriteConfig // auto: write config file

	// Phase 8: Optional AI Analysis
	StepRunAnalysis // prompt: Run AI analysis? (y/n)

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
	RunAnalysis   bool   // user wants to run AI analysis after setup
	LastConnError string // last connection test error message
	// EditMode is true when the wizard was launched against an existing
	// config file (caller set CurrentStep=StepEditOrNew + populated Config).
	// Bool-typed prompt defaults (create_indexes, create_foreign_keys)
	// can't otherwise distinguish "user set false" from "field unset", so
	// they look at this flag to decide whether to honor the loaded value
	// or fall back to dmt's documented defaults.
	EditMode bool
}

// NewState creates a new setup state with sensible defaults.
func NewState() *State {
	return &State{
		CurrentStep: StepCheckSecrets,
		ConfigPath:  "config.yaml",
	}
}

// ConnConfig returns a copy of s.Config with secret placeholders resolved,
// suitable for connection testing. The wizard keeps raw `${env:...}` /
// `${file:...}` values in s.Config so a re-save preserves them on disk;
// connection tests need the resolved values to authenticate.
//
// In non-edit mode (fresh setup), s.Config has no placeholders and this
// is effectively a deep copy. On expansion failure (env var missing,
// file not found), falls back to returning s.Config so the test reports
// an authentication failure rather than a setup error — the user can
// then enter credentials manually.
func (s *State) ConnConfig() config.Config {
	resolved, err := s.Config.ResolveSecrets()
	if err != nil {
		return s.Config
	}
	return *resolved
}

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
		// AI is optional in dmt (#167) — the deterministic type mapper,
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
		return s.sslPrompt(s.Config.Source.Type, s.Config.Source.SSLMode, s.Config.Source.TrustServerCert)

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
		return s.sslPrompt(s.Config.Target.Type, s.Config.Target.SSLMode, s.Config.Target.TrustServerCert)

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
			// Loaded config omitted `workers:` — preserve the omission so
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
	case StepWriteConfig:
		return PromptInfo{
			Text:         "Saving configuration...",
			IsAutoAction: true,
		}

	// Phase 8: AI Analysis (only reached when AI was configured;
	// the deterministic analyze flow is run via `dmt analyze` separately
	// — this step is the wizard-integrated convenience pass).
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
			// User chose to start fresh — discard the pre-loaded config but
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
			s.CurrentStep = StepSourceType
		} else {
			s.CurrentStep = StepConfigureAI
		}

	case StepConfigureAI:
		v := strings.ToLower(input)
		if v == "" {
			// Default flipped to "n" in #174 — AI is optional and
			// skipping is the friction-free path for a fresh install.
			v = "n"
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		if v == "n" {
			s.CurrentStep = StepSourceType
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
			// Write failed — go back to provider selection so user can fix
			s.CurrentStep = StepAIProvider
			return fmt.Sprintf("Failed to save secrets: %s", input)
		}
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
		case "-", "none":
			// Explicit clear — required to remove a previously-set list,
			// since blank means "keep" everywhere else in the wizard.
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
		s.Config.Migration.CreateIndexes = (v == "y")
		s.CurrentStep = StepCreateFKs

	case StepCreateFKs:
		v := strings.ToLower(input)
		if v == "" {
			v = s.boolDefault(s.Config.Migration.CreateForeignKeys, "y")
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		s.Config.Migration.CreateForeignKeys = (v == "y")
		s.CurrentStep = StepWorkers

	case StepWorkers:
		if input == "" {
			// Fresh setup (Workers==0): apply NumCPU-capped default.
			// EditMode + Workers==0: leave it 0 so the YAML round-trip
			//   preserves the omission and runtime auto-tunes.
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
			// Write failed — go back to config path so user can fix
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

// Helper methods

func (s *State) defaultPort(dbType string) int {
	d, err := driver.Get(dbType)
	if err == nil {
		return d.Defaults().Port
	}
	switch dbType {
	case "mssql":
		return 1433
	case "postgres":
		return 5432
	case "mysql":
		return 3306
	default:
		return 5432
	}
}

func (s *State) defaultSchema(dbType string) string {
	d, err := driver.Get(dbType)
	if err == nil && d.Defaults().Schema != "" {
		return d.Defaults().Schema
	}
	switch dbType {
	case "mssql":
		return "dbo"
	case "postgres":
		return "public"
	default:
		return ""
	}
}

func (s *State) defaultUser(dbType string) string {
	switch dbType {
	case "mssql":
		return "sa"
	case "postgres":
		return "postgres"
	case "mysql":
		return "root"
	default:
		return ""
	}
}

func (s *State) sslPrompt(dbType, sslMode string, trustCert bool) PromptInfo {
	switch dbType {
	case "postgres":
		def := "prefer"
		if sslMode != "" {
			def = sslMode
		}
		return PromptInfo{
			Text:    "SSL mode (disable/prefer/require/verify-ca/verify-full)",
			Default: def,
		}
	case "mssql":
		def := "n"
		if trustCert {
			def = "y"
		}
		return PromptInfo{
			Text:    "Trust server certificate? (y/n)",
			Default: def,
			Choices: []string{"y", "n"},
		}
	case "mysql":
		// Default is "require" rather than "preferred" — preferred
		// allows silent plaintext downgrade and isn't safe as a
		// default. (#252) Operators who explicitly want downgradeable
		// TLS can type "preferred" at the prompt. Form is the
		// Postgres-style canonical (disable/require/verify-ca/verify-full)
		// documented in dbconfig.SourceConfig.SSLMode; the dialect
		// accepts the MySQL-native variants as aliases.
		def := "require"
		if sslMode != "" {
			def = sslMode
		}
		return PromptInfo{
			Text:    "SSL mode (disable/require/verify-ca/verify-full, or 'preferred' for downgradeable TLS)",
			Default: def,
		}
	default:
		return PromptInfo{Text: "SSL mode", Default: "disable"}
	}
}

func (s *State) processSSL(input string, isSource bool) {
	if isSource {
		switch s.Config.Source.Type {
		case "postgres":
			if input == "" {
				if s.Config.Source.SSLMode == "" {
					s.Config.Source.SSLMode = "prefer"
				}
			} else {
				s.Config.Source.SSLMode = input
			}
		case "mssql":
			if input != "" {
				v := strings.ToLower(input)
				s.Config.Source.TrustServerCert = (v == "y" || v == "yes" || v == "true")
			}
		case "mysql":
			if input == "" {
				if s.Config.Source.SSLMode == "" {
					s.Config.Source.SSLMode = "require" // #252 safe default
				}
			} else {
				s.Config.Source.SSLMode = input
			}
		}
	} else {
		switch s.Config.Target.Type {
		case "postgres":
			if input == "" {
				if s.Config.Target.SSLMode == "" {
					s.Config.Target.SSLMode = "prefer"
				}
			} else {
				s.Config.Target.SSLMode = input
			}
		case "mssql":
			if input != "" {
				v := strings.ToLower(input)
				s.Config.Target.TrustServerCert = (v == "y" || v == "yes" || v == "true")
			}
		case "mysql":
			if input == "" {
				if s.Config.Target.SSLMode == "" {
					s.Config.Target.SSLMode = "require" // #252 safe default
				}
			} else {
				s.Config.Target.SSLMode = input
			}
		}
	}
}

func sortedProviderNames() []string {
	var names []string
	for name := range secrets.KnownProviders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func defaultIfEmpty(val, def string) string {
	if val != "" {
		return val
	}
	return def
}

// boolDefault returns "y"/"n" for prompt display.
// In EditMode (config was loaded), the loaded value wins regardless of its
// zero-ness — a user who explicitly set `create_indexes: false` must see
// "n" as the default and Enter must preserve it. In fresh-setup mode,
// fall back to the supplied dmt default ("y" for create_indexes/FKs).
func (s *State) boolDefault(loaded bool, freshDefault string) string {
	if s.EditMode {
		if loaded {
			return "y"
		}
		return "n"
	}
	return freshDefault
}
