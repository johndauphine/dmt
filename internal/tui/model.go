package tui

import (
	"context"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/setup"
)

// maxContentLines limits output retained in memory to prevent unbounded growth
// during long migrations or verbose commands. 2000 lines provides sufficient
// scrollback for interactive use while keeping memory bounded.
const maxContentLines = 2000

// AppMode represents the current application mode
type AppMode int

const (
	ModeNormal AppMode = iota
	ModeWizard
	ModeMigration
	ModeSetup
)

type wizardStep int

const (
	stepSourceType wizardStep = iota
	stepSourceHost
	stepSourcePort
	stepSourceDB
	stepSourceUser
	stepSourcePass
	stepSourceSSL
	stepTargetType
	stepTargetHost
	stepTargetPort
	stepTargetDB
	stepTargetUser
	stepTargetPass
	stepTargetSSL
	stepWorkers
	stepDone
)

// Model is the main TUI model - simplified single-viewport architecture
type Model struct {
	// session holds /session sticky defaults (#444); nil until first set.
	session map[string]string

	// Core components
	viewport  viewport.Model
	textInput textinput.Model
	ready     bool
	width     int
	height    int

	// Git integration
	gitInfo GitInfo
	cwd     string

	// Single content buffer with memory management
	content      *strings.Builder
	lineBuffer   string
	progressLine string

	// History & completion
	history       []string
	historyIdx    int
	suggestions   []string
	suggestionIdx int
	lastInput     string

	// Application mode
	mode AppMode

	// Single migration state (one at a time)
	migrationCancel context.CancelFunc
	migrationStatus string // "", "running", "completed", "failed", "cancelled"

	// Wizard state
	wizardStep wizardStep
	wizardData config.Config
	wizardFile string

	// Setup wizard state
	setupState *setup.State

	// Explore policy (#182). exploreArmed is a one-shot flag set by
	// `/explore on` (or bare `/explore`) and consumed by the next `/run`
	// — it surfaces as cfg.Migration.Explore on that run, forcing the
	// tuner's planned-grid probe. exploreMode mirrors
	// cfg.Migration.ExploreMode and persists for the session; empty means
	// "don't override what the config/secrets layer set."
	exploreArmed bool
	exploreMode  string
}

type commandInfo struct {
	Name        string
	Description string
}

var availableCommands = []commandInfo{
	{"/run", "Start migration (default: config.yaml)"},
	{"/resume", "Resume an interrupted migration"},
	{"/validate", "Validate migration row counts"},
	{"/config", "Show configuration details"},
	{"/analyze", "Analyze source database and suggest config (--apply writes config)"},
	{"/status", "Show migration status (--detailed for tasks)"},
	{"/history", "Show migration history"},
	{"/setup", "Guided setup: secrets, config, connection test, smartconfig analysis"},
	{"/wizard", "Launch configuration wizard"},
	{"/logs", "Save session logs to file"},
	{"/profile", "Manage encrypted profiles (save/list/delete/export)"},
	{"/verbosity", "Set log level (debug, info, warn, error)"},
	{"/explore", "Force exploration probe (on/off) or set steady-state ε (low/balanced/high)"},
	{"/about", "Show application information"},
	{"/help", "Show available commands"},
	{"/clear", "Clear screen"},
	{"/quit", "Exit application"},
}

// Message types

// TickMsg is used to update the UI periodically
type TickMsg time.Time

// OutputMsg is sent when new output is captured
type OutputMsg string

// BoxedOutputMsg is output that should be displayed in a bordered box
type BoxedOutputMsg string

// ProgressMsg updates the progress line
type ProgressMsg string

// MigrationDoneMsg signals migration completion
type MigrationDoneMsg struct {
	Status  string // "completed", "failed", "cancelled"
	Message string
}

// WizardFinishedMsg indicates the wizard completed
type WizardFinishedMsg struct {
	Err     error
	Message string
}

// SetupConnTestMsg carries a connection test result with step correlation
type SetupConnTestMsg struct {
	Step   setup.Step
	Result *setup.ConnTestResult
}

// migrationStartedMsg carries the cancel function
type migrationStartedMsg struct {
	cancel context.CancelFunc
}
