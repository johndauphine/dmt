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
	// Phase 1: AI/Secrets
	StepCheckSecrets Step = iota // auto: check if secrets exist with valid AI
	StepConfigureAI              // prompt: Configure AI features? (y/n)
	StepAIProvider               // prompt: choose provider
	StepAIKey                    // prompt: API key or base URL
	StepWriteSecrets             // auto: write secrets file

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
}

// NewState creates a new setup state with sensible defaults.
func NewState() *State {
	return &State{
		CurrentStep: StepCheckSecrets,
		ConfigPath:  "config.yaml",
	}
}

// Prompt returns prompt info for the current step.
func (s *State) Prompt() PromptInfo {
	switch s.CurrentStep {
	// Phase 1: AI/Secrets
	case StepCheckSecrets:
		return PromptInfo{
			Text:          "Checking AI configuration...",
			IsAutoAction:  true,
			SectionHeader: "Phase 1: AI Configuration",
		}
	case StepConfigureAI:
		return PromptInfo{
			Text:    "Configure AI features? (y/n)",
			Default: "y",
			Choices: []string{"y", "n"},
		}
	case StepAIProvider:
		providers := sortedProviderNames()
		return PromptInfo{
			Text:    fmt.Sprintf("AI provider (%s)", strings.Join(providers, "/")),
			Default: "claude",
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
			Default:       "drop_recreate",
			Choices:       []string{"drop_recreate", "upsert"},
			SectionHeader: "Phase 6: Migration Settings",
		}
	case StepCreateIndexes:
		return PromptInfo{
			Text:    "Create indexes? (y/n)",
			Default: "y",
			Choices: []string{"y", "n"},
		}
	case StepCreateFKs:
		return PromptInfo{
			Text:    "Create foreign keys? (y/n)",
			Default: "y",
			Choices: []string{"y", "n"},
		}
	case StepWorkers:
		def := runtime.NumCPU()
		if def > 8 {
			def = 8
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

	// Phase 8: AI Analysis
	case StepRunAnalysis:
		return PromptInfo{
			Text:          "Run AI analysis on source database? (y/n)",
			Default:       "y",
			Choices:       []string{"y", "n"},
			SectionHeader: "Phase 8: AI Analysis",
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
			v = "y"
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
			input = "claude"
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
			return input
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
			input = "drop_recreate"
		}
		if input != "drop_recreate" && input != "upsert" {
			return "Options: drop_recreate, upsert"
		}
		s.Config.Migration.TargetMode = input
		s.CurrentStep = StepCreateIndexes

	case StepCreateIndexes:
		v := strings.ToLower(input)
		if v == "" {
			v = "y"
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		s.Config.Migration.CreateIndexes = (v == "y")
		s.CurrentStep = StepCreateFKs

	case StepCreateFKs:
		v := strings.ToLower(input)
		if v == "" {
			v = "y"
		}
		if v != "y" && v != "n" {
			return "Please enter y or n"
		}
		s.Config.Migration.CreateForeignKeys = (v == "y")
		s.CurrentStep = StepWorkers

	case StepWorkers:
		def := runtime.NumCPU()
		if def > 8 {
			def = 8
		}
		if input == "" {
			s.Config.Migration.Workers = def
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
			return input
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
		def := "preferred"
		if sslMode != "" {
			def = sslMode
		}
		return PromptInfo{
			Text:    "SSL mode (disabled/preferred/required/verify_ca/verify_identity)",
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
					s.Config.Source.SSLMode = "preferred"
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
					s.Config.Target.SSLMode = "preferred"
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
