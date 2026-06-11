package tui

import (
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/setup"
	"github.com/johndauphine/dmt/internal/version"
)

func (m *Model) handleCommand(cmdStr string) tea.Cmd {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil
	}
	cmd := parts[0]

	// Handle shell commands (starting with !)
	if strings.HasPrefix(cmd, "!") {
		shellCmd := strings.TrimPrefix(cmdStr, "!")
		return m.runShellCmd(shellCmd)
	}

	switch cmd {
	case "/quit", "/exit":
		return tea.Quit

	case "/clear":
		m.content.Reset()
		m.content.WriteString(m.welcomeMessage())
		m.viewport.SetContent(m.content.String())
		return nil

	case "/help":
		help := `Available Commands:
  /setup                Guided setup: secrets, config, connection test,
                        optional smartconfig analysis (the richer path)
  /wizard               Lightweight config editor (create or edit one
                        config file; /setup covers secrets + testing too)
  /init-secrets         Create ~/.secrets/dmt-config.yaml (--with-ai
                        seeds the optional AI section, --force overwrites)
  /cache clear          Clear the local type-mapping cache (--ai-only
                        keeps deterministic entries; asks for --confirm)
  /run [config_file]    Start migration (default: config.yaml)
  /run --profile NAME   Start migration using a saved profile
  /run --dry-run        Preview the migration plan without executing
                        (add --ai-schema-advisor for advisory AI schema review)
  /run flags            --source-schema NAME, --target-schema NAME,
                        --workers N, --skip-preflight LIST|all
  /resume [config_file] Resume an interrupted migration
  /resume --profile NAME Resume using a saved profile
  /resume flags         --force-resume (resume despite config changes),
                        --skip-preflight LIST|all
  /preflight [config_file]
                        Verify connectivity, privileges, version, and
                        other readiness checks (alias: /health-check)
  /preflight flags      --skip-preflight LIST|all, --ai-review (advisory
                        AI readiness review)
  /validate             Validate migration (--ai-triage for advisory AI
                        review of failures, --timeout D e.g. 120s)
  /diagnose             Diagnose the latest failed run (--run ID for a
                        specific run, --ai-triage, --timeout D)
  /config [config_file] Show configuration details
  /analyze [--apply]     Analyze source database and suggest configuration
                        (--apply writes config, --ai-explain adds an
                        advisory AI explanation)
  /ai config-review     Advisory config patch recommendations + migration
                        runbook (alias: /ai runbook); flags: --timeout D,
                        --request TEXT (free text, put it last)
  /status [-d]          Show migration status (--detailed for task list)
  /history              Show migration history
  /profile save NAME    Save an encrypted profile
  /profile list         List saved profiles
  /profile delete NAME  Delete a saved profile
  /profile export NAME  Export a profile to a config file
  /session              Show sticky session defaults (config, profile, state-file, verbosity)
  /session KEY VALUE    Set a session default; /session clear [KEY] unsets
  /verbosity [LEVEL]    Set log level (debug, info, warn, error)
  /explore              Show current exploration state
  /explore on|off       Arm/disarm next-run exploration probe
  /explore low|balanced|high
                        Set steady-state ε strength for this session
  /logs                 Save session logs to a file
  /clear                Clear screen
  /quit                 Exit application
  !<command>            Run a shell command

Note: You can use @/path/to/file for config files.`
		return func() tea.Msg { return BoxedOutputMsg(help) }

	case "/session":
		return m.handleSessionCommand(parts)

	case "/init-secrets":
		return handleInitSecretsCommand(parts)

	case "/cache":
		return handleCacheCommand(parts)

	case "/logs":
		logFile := "session.log"
		err := os.WriteFile(logFile, []byte(m.content.String()), 0644)
		if err != nil {
			return func() tea.Msg { return OutputMsg(fmt.Sprintf("Error saving logs: %v\n", err)) }
		}
		return func() tea.Msg { return OutputMsg(fmt.Sprintf("Logs saved to %s\n", logFile)) }

	case "/verbosity":
		if len(parts) < 2 {
			// Show current level
			currentLevel := logging.GetLevel()
			return func() tea.Msg {
				return OutputMsg(fmt.Sprintf("Current log level: %s\nUsage: /verbosity <debug|info|warn|error>\n", currentLevel))
			}
		}
		levelStr := parts[1]
		level, err := logging.ParseLevel(levelStr)
		if err != nil {
			return func() tea.Msg {
				return OutputMsg(fmt.Sprintf("Invalid log level: %s\nValid levels: debug, info, warn, error\n", levelStr))
			}
		}
		logging.SetLevel(level)
		return func() tea.Msg {
			return OutputMsg(fmt.Sprintf("Log level set to: %s\n", levelStr))
		}

	case "/explore":
		return m.handleExploreCommand(parts)

	case "/about":
		about := fmt.Sprintf(`dmt v%s

%s

Features:
- Parallel transfer with auto-tuning
- Resume capability (chunk-level)
- Data validation
- Encrypted profile storage
- Configuration wizard

Built with Go and Bubble Tea.`, version.Version, version.Description)
		return func() tea.Msg { return BoxedOutputMsg(about) }

	case "/setup":
		m.mode = ModeSetup
		m.setupState = setup.NewState()
		m.textInput.Reset()
		m.textInput.Placeholder = ""

		// Seed Slack webhook from the global secrets file so the wizard
		// shows the current value as Enter-to-keep, regardless of whether
		// AI was previously configured. Capture Original too so a failed
		// write can revert without leaving a misleading "configured" hint.
		loadedWebhook := setup.LoadExistingSlackWebhook()
		m.setupState.SlackWebhook = loadedWebhook
		m.setupState.SlackWebhookOriginal = loadedWebhook

		configFile, profileName, err := m.parseOriginArgs("/setup", parts)
		if err != nil {
			return errOutput(err)
		}
		// Always honor the requested path so /setup @newfile.yaml saves
		// to newfile.yaml even when it doesn't exist yet. The os.Stat
		// branch below only decides whether to *load* it.
		m.setupState.ConfigPath = configFile
		var loaded bool

		if profileName != "" {
			if cfg, err := loadProfileConfig(profileName); err == nil {
				m.setupState.Config = *cfg
				m.setupState.ConfigPath = profileName + ".yaml"
				loaded = true
			}
		}
		if !loaded {
			if _, err := os.Stat(configFile); err == nil {
				// LoadRaw, not LoadWithOptions — placeholders like ${env:PASS}
				// must survive a re-save so the wizard never writes resolved
				// secrets to disk.
				if cfg, err := config.LoadRaw(configFile); err == nil {
					m.setupState.Config = *cfg
					loaded = true
				}
			}
		}

		var header string
		if loaded {
			header = fmt.Sprintf("\n--- SETUP WIZARD: %s ---\n", m.setupState.ConfigPath)
			m.setupState.CurrentStep = setup.StepEditOrNew
			m.setupState.EditMode = true
		} else {
			header = "\n--- SETUP WIZARD ---\n"
		}
		m.appendOutput(header)
		return m.processSetupAutoSteps()

	case "/wizard":
		m.mode = ModeWizard
		m.wizardStep = stepSourceType
		m.textInput.Reset()
		m.textInput.Placeholder = ""

		configFile, profileName, err := m.parseOriginArgs("/wizard", parts)
		if err != nil {
			return errOutput(err)
		}
		m.wizardFile = configFile

		var headerMsg string
		var loaded bool

		// Try to load from profile first
		if profileName != "" {
			if cfg, err := loadProfileConfig(profileName); err == nil {
				m.wizardData = *cfg
				m.wizardFile = profileName + ".yaml"
				headerMsg = fmt.Sprintf("\n--- EDITING PROFILE: %s ---\n", profileName)
				loaded = true
			}
		}

		// Try config file
		if !loaded {
			if _, err := os.Stat(m.wizardFile); err == nil {
				if cfg, err := config.LoadWithOptions(m.wizardFile, config.LoadOptions{SuppressWarnings: true}); err == nil {
					m.wizardData = *cfg
					headerMsg = fmt.Sprintf("\n--- EDITING CONFIGURATION: %s ---\n", m.wizardFile)
					loaded = true
				}
			}
		}

		if !loaded {
			headerMsg = fmt.Sprintf("\n--- CONFIGURATION WIZARD: %s ---\n", m.wizardFile)
		}

		prompt := m.renderWizardPrompt()
		m.appendOutput(headerMsg + prompt)
		return nil

	case "/run":
		// Block if migration already running
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A migration is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		configFile, profileName, overrides, err := m.parseRunArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		// Consume the one-shot Explore arm (#182): subsequent /run calls
		// don't re-trigger it unless the user re-arms. A dry-run preview
		// doesn't consume it — nothing actually ran.
		if !overrides.dryRun {
			overrides.exploreOnce = m.exploreArmed
			m.exploreArmed = false
		}
		overrides.exploreMode = m.exploreMode
		return m.runMigrationCmd(configFile, profileName, overrides)

	case "/resume":
		// Block if migration already running
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A migration is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		configFile, profileName, forceResume, skipPreflight, err := m.parseResumeArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runResumeCmd(configFile, profileName, forceResume, skipPreflight)

	case "/preflight", "/health-check":
		configFile, profileName, skipPreflight, aiReview, err := m.parsePreflightArgs(cmd, parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runPreflightCmd(configFile, profileName, skipPreflight, aiReview)

	case "/validate":
		configFile, profileName, aiTriage, timeout, err := m.parseValidateArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runValidateCmd(configFile, profileName, aiTriage, timeout)

	case "/diagnose":
		configFile, profileName, runID, aiTriage, timeout, err := m.parseDiagnoseArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runDiagnoseCmd(configFile, profileName, runID, aiTriage, timeout)

	case "/status":
		configFile, profileName, detailed, err := m.parseStatusArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runStatusCmd(configFile, profileName, detailed)

	case "/history":
		configFile, profileName, runID, err := m.parseHistoryArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runHistoryCmd(configFile, profileName, runID)

	case "/profile":
		return m.handleProfileCommand(parts)

	case "/analyze":
		configFile, profileName, apply, aiExplain, err := m.parseAnalyzeArgs(parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runAnalyzeCmd(configFile, profileName, apply, aiExplain)

	case "/ai":
		return m.handleAICommand(parts)

	case "/config":
		configFile, profileName, err := m.parseOriginArgs("/config", parts)
		if err != nil {
			return errOutput(err)
		}
		return m.runConfigCmd(configFile, profileName)

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// handleAICommand dispatches /ai subcommands (#442). config-review and
// its runbook alias generate advisory patch recommendations plus a
// migration runbook; ai evals stays CLI-only (developer/eval harness,
// see the parity registry).
func (m *Model) handleAICommand(parts []string) tea.Cmd {
	if len(parts) < 2 {
		return errOutput(fmt.Errorf("/ai: usage: /ai config-review [@config|--profile NAME] [--timeout D] [--request TEXT]"))
	}
	switch parts[1] {
	case "config-review", "runbook":
		synthetic := append([]string{"/ai " + parts[1]}, parts[2:]...)
		configFile, profileName, request, timeout, err := parseAIConfigReviewArgs(m, synthetic)
		if err != nil {
			return errOutput(err)
		}
		return m.runAIConfigReviewCmd(configFile, profileName, request, timeout)
	default:
		return errOutput(fmt.Errorf("/ai: unknown subcommand %q (try /ai config-review; ai evals is CLI-only)", parts[1]))
	}
}

// Exploration policy commands (#182). Arms a one-shot probe on the next
// /run, or sets the steady-state ε mode for this session. Both surfaces
// mirror cfg.Migration.Explore / cfg.Migration.ExploreMode and are
// applied by runMigrationCmd at run time.
func (m *Model) handleExploreCommand(parts []string) tea.Cmd {
	if len(parts) < 2 {
		armed := "off"
		if m.exploreArmed {
			armed = "on"
		}
		mode := m.exploreMode
		if mode == "" {
			mode = "(unset — config/secrets value applies)"
		}
		return func() tea.Msg {
			return OutputMsg(fmt.Sprintf(
				"Explore state:\n  next-run probe: %s\n  steady-state ε mode: %s\nUsage: /explore on|off | /explore low|balanced|high\n",
				armed, mode,
			))
		}
	}
	arg := strings.ToLower(parts[1])
	switch arg {
	case "on":
		m.exploreArmed = true
		return func() tea.Msg {
			return OutputMsg("Explore armed — next /run will force an exploration probe.\n")
		}
	case "off":
		m.exploreArmed = false
		m.exploreMode = ""
		return func() tea.Msg {
			return OutputMsg("Explore disarmed; steady-state ε mode cleared.\n")
		}
	case "low", "balanced", "high":
		m.exploreMode = arg
		return func() tea.Msg {
			return OutputMsg(fmt.Sprintf("Explore mode set to %s for this session.\n", arg))
		}
	default:
		return func() tea.Msg {
			return OutputMsg(fmt.Sprintf("Unknown /explore argument: %s\nUsage: /explore on|off | /explore low|balanced|high\n", arg))
		}
	}
}
