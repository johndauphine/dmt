package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/version"
	"gopkg.in/yaml.v3"
)

// maxContentLines limits output retained in memory to prevent unbounded growth
// during long migrations or verbose commands. 2000 lines provides sufficient
// scrollback for interactive use while keeping memory bounded.
const maxContentLines = 2000

// safeCmd wraps a tea.Cmd to recover from panics
func safeCmd(cmd tea.Cmd) tea.Cmd {
	if cmd == nil {
		return nil
	}
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = OutputMsg(fmt.Sprintf("\n[ERROR] %v\n", r))
			}
		}()
		return cmd()
	}
}

// AppMode represents the current application mode
type AppMode int

const (
	ModeNormal AppMode = iota
	ModeWizard
	ModeMigration
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
	{"/analyze", "Analyze source database and suggest config"},
	{"/calibrate", "Run AI calibration to find optimal config"},
	{"/status", "Show migration status (--detailed for tasks)"},
	{"/history", "Show migration history"},
	{"/wizard", "Launch configuration wizard"},
	{"/logs", "Save session logs to file"},
	{"/profile", "Manage encrypted profiles (save/list/delete/export)"},
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

// migrationStartedMsg carries the cancel function
type migrationStartedMsg struct {
	cancel context.CancelFunc
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		textinput.Blink,
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second*5, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

// InitialModel returns the initial model state
func InitialModel() Model {
	ti := textinput.New()
	ti.Placeholder = "Type your message or @path/to/file"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 20
	ti.Prompt = "❯ "
	ti.PromptStyle = stylePrompt

	cwd, _ := os.Getwd()

	return Model{
		textInput:  ti,
		gitInfo:    GetGitInfo(),
		cwd:        cwd,
		content:    &strings.Builder{},
		history:    []string{},
		historyIdx: -1,
		mode:       ModeNormal,
	}
}

// appendOutput adds text to the content buffer with memory management
func (m *Model) appendOutput(text string) {
	m.content.WriteString(text)

	// Trim to last N lines if exceeded
	content := m.content.String()
	lines := strings.Split(content, "\n")
	if len(lines) > maxContentLines {
		lines = lines[len(lines)-maxContentLines:]
		m.content.Reset()
		m.content.WriteString(strings.Join(lines, "\n"))
	}

	// Update viewport - only auto-scroll if at bottom
	wasAtBottom := m.viewport.AtBottom()
	m.viewport.SetContent(m.getDisplayContent())
	if wasAtBottom {
		m.viewport.GotoBottom()
	}
}

// getDisplayContent returns content with progress line appended if present
func (m *Model) getDisplayContent() string {
	content := m.content.String()
	if m.progressLine != "" && m.mode != ModeWizard {
		content += styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}
	return content
}

// Update handles messages and updates the model
func (m Model) Update(msg tea.Msg) (model tea.Model, cmd tea.Cmd) {
	// Recover from panics in Update
	defer func() {
		if r := recover(); r != nil {
			m.appendOutput(fmt.Sprintf("\n[ERROR] %v\n", r))
			model = m
			cmd = nil
		}
	}()

	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Handle suggestion navigation if active
		if len(m.suggestions) > 0 {
			switch msg.Type {
			case tea.KeyUp:
				m.suggestionIdx--
				if m.suggestionIdx < 0 {
					m.suggestionIdx = len(m.suggestions) - 1
				}
				return m, nil
			case tea.KeyDown:
				m.suggestionIdx++
				if m.suggestionIdx >= len(m.suggestions) {
					m.suggestionIdx = 0
				}
				return m, nil
			case tea.KeyEnter, tea.KeyTab:
				if m.suggestionIdx >= 0 && m.suggestionIdx < len(m.suggestions) {
					selection := m.suggestions[m.suggestionIdx]
					completion := strings.Fields(selection)[0]

					input := m.textInput.Value()

					// File completion (@)
					if idx := strings.LastIndex(input, "@"); idx != -1 && (idx == 0 || input[idx-1] == ' ') {
						newValue := input[:idx+1] + completion
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break
						}
						m.textInput.SetValue(newValue)
						m.textInput.SetCursor(len(newValue))
					} else if strings.HasPrefix(input, "/") {
						// Command completion
						newValue := completion
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break
						}
						m.textInput.SetValue(newValue)
						m.textInput.SetCursor(len(newValue))
					}

					m.suggestions = nil
					m.suggestionIdx = 0
					return m, nil
				}
			case tea.KeyEsc:
				m.suggestions = nil
				return m, nil
			}
		}

		switch msg.Type {
		case tea.KeyCtrlC:
			// Cancel wizard
			if m.mode == ModeWizard {
				m.mode = ModeNormal
				m.appendOutput(styleSystemOutput.Render("Wizard cancelled") + "\n")
				return m, nil
			}
			// Cancel running migration
			if m.migrationCancel != nil && m.migrationStatus == "running" {
				m.migrationCancel()
				m.appendOutput(styleSystemOutput.Render("Cancelling migration... please wait") + "\n")
				return m, nil
			}
			// Quit if nothing running
			return m, tea.Quit

		case tea.KeyEsc:
			if m.mode == ModeWizard {
				m.mode = ModeNormal
				m.appendOutput(styleSystemOutput.Render("Wizard cancelled") + "\n")
				return m, nil
			}
			return m, tea.Quit

		case tea.KeyEnter:
			value := m.textInput.Value()
			if m.mode == ModeWizard {
				return m, safeCmd(m.handleWizardStep(value))
			}
			if value != "" {
				m.appendOutput(styleUserInput.Render("> "+value) + "\n")
				m.textInput.Reset()
				m.history = append(m.history, value)
				m.historyIdx = len(m.history)
				return m, safeCmd(m.handleCommand(value))
			}

		case tea.KeyTab:
			if m.mode == ModeNormal {
				m.autocompleteCommand()
			}

		case tea.KeyPgUp:
			m.viewport.ScrollUp(m.viewport.Height / 2)
			return m, nil

		case tea.KeyPgDown:
			m.viewport.ScrollDown(m.viewport.Height / 2)
			return m, nil

		case tea.KeyHome:
			m.viewport.GotoTop()
			return m, nil

		case tea.KeyEnd:
			m.viewport.GotoBottom()
			return m, nil

		case tea.KeyUp:
			if m.textInput.Value() == "" && len(m.suggestions) == 0 {
				m.viewport.ScrollUp(1)
				return m, nil
			}
			if m.historyIdx > 0 {
				m.historyIdx--
				m.textInput.SetValue(m.history[m.historyIdx])
			}
			return m, nil

		case tea.KeyDown:
			if m.textInput.Value() == "" && len(m.suggestions) == 0 {
				m.viewport.ScrollDown(1)
				return m, nil
			}
			if m.historyIdx < len(m.history)-1 {
				m.historyIdx++
				m.textInput.SetValue(m.history[m.historyIdx])
			} else {
				m.historyIdx = len(m.history)
				m.textInput.Reset()
			}
			return m, nil
		}

	case tea.WindowSizeMsg:
		headerHeight := 0
		footerHeight := 7 // Input box (3) + Status bar (1) + Separator (1) + Suggestions (1) + Safety (1)
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width-2, msg.Height-verticalMarginHeight)
			m.viewport.YPosition = headerHeight
			m.content.WriteString(m.welcomeMessage())
			m.viewport.SetContent(m.content.String())
			m.ready = true
		} else {
			m.viewport.Width = msg.Width - 2
			m.viewport.Height = msg.Height - verticalMarginHeight
		}
		m.width = msg.Width
		m.height = msg.Height
		m.textInput.Width = msg.Width - 4

	case migrationStartedMsg:
		m.migrationCancel = msg.cancel
		m.migrationStatus = "running"
		m.mode = ModeMigration

	case MigrationDoneMsg:
		m.migrationStatus = msg.Status
		m.migrationCancel = nil
		m.progressLine = ""
		m.mode = ModeNormal

		prefix := styleSuccess.Render("✔ ")
		if msg.Status == "failed" || msg.Status == "cancelled" {
			prefix = styleError.Render("✖ ")
		}
		m.appendOutput(prefix + msg.Message + "\n")

	case WizardFinishedMsg:
		m.mode = ModeNormal

		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80
		}

		text := msg.Message
		if msg.Err != nil {
			text = wrapLine(msg.Err.Error(), wrapWidth)
			text = styleError.Render("✖ " + text)
		} else {
			text = wrapLine(text, wrapWidth)
			text = styleSuccess.Render("✔ " + text)
		}

		m.appendOutput("\n" + text + "\n")

	case BoxedOutputMsg:
		output := strings.TrimSpace(string(msg))
		if output == "" {
			break
		}

		boxWidth := m.viewport.Width - 4
		if boxWidth < 40 {
			boxWidth = 80
		}

		boxedOutput := styleOutputBox.Width(boxWidth).Render(output)
		m.appendOutput(boxedOutput + "\n")

	case OutputMsg:
		m.lineBuffer += string(msg)

		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80
		}

		// Process complete lines
		for {
			newlineIdx := strings.Index(m.lineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}

			m.progressLine = ""
			line := m.lineBuffer[:newlineIdx]
			m.lineBuffer = m.lineBuffer[newlineIdx+1:]

			// Handle carriage returns
			if lastCR := strings.LastIndex(line, "\r"); lastCR != -1 {
				line = line[lastCR+1:]
			}

			// Wrap and style
			wrappedLines := strings.Split(wrapLine(line, wrapWidth), "\n")
			for _, wrappedLine := range wrappedLines {
				lowerText := strings.ToLower(line)
				prefix := "  "

				isError := strings.Contains(lowerText, "error") ||
					(strings.Contains(lowerText, "fail") && !strings.Contains(lowerText, "0 failed"))

				if isError {
					wrappedLine = styleError.Render(wrappedLine)
					prefix = styleError.Render("✖ ")
				} else if strings.Contains(lowerText, "success") || strings.Contains(lowerText, "passed") || strings.Contains(lowerText, "complete") {
					wrappedLine = styleSuccess.Render(wrappedLine)
					prefix = styleSuccess.Render("✔ ")
				} else {
					wrappedLine = styleSystemOutput.Render(wrappedLine)
				}

				m.appendOutput(prefix + wrappedLine + "\n")
			}
		}

		// Handle progress bar updates (lines with \r but no \n)
		if strings.Contains(m.lineBuffer, "\r") {
			if lastCR := strings.LastIndex(m.lineBuffer, "\r"); lastCR != -1 {
				m.progressLine = strings.TrimSpace(m.lineBuffer[lastCR+1:])
				m.lineBuffer = m.lineBuffer[:lastCR+1]
			}
		}

		// Update viewport
		wasAtBottom := m.viewport.AtBottom()
		m.viewport.SetContent(m.getDisplayContent())
		if wasAtBottom {
			m.viewport.GotoBottom()
		}

	case TickMsg:
		m.gitInfo = GetGitInfo()
		return m, tickCmd()
	}

	m.textInput, tiCmd = m.textInput.Update(msg)

	// Handle auto-completion suggestions
	input := m.textInput.Value()
	if input != m.lastInput {
		m.lastInput = input
		m.suggestions = nil

		// File completion (@)
		if idx := strings.LastIndex(input, "@"); idx != -1 {
			if idx == 0 || input[idx-1] == ' ' {
				prefix := input[idx+1:]
				matches, err := filepath.Glob(prefix + "*")
				if err == nil {
					if len(matches) > 15 {
						matches = matches[:15]
					}
					m.suggestions = matches
					m.suggestionIdx = 0
				}
			}
		}

		// Command completion (/)
		if len(m.suggestions) == 0 && strings.HasPrefix(input, "/") {
			for _, cmd := range availableCommands {
				if strings.HasPrefix(cmd.Name, input) {
					m.suggestions = append(m.suggestions, fmt.Sprintf("%-10s %s", cmd.Name, cmd.Description))
				}
			}
			if len(m.suggestions) > 0 {
				m.suggestionIdx = 0
			}
		}
	}

	// Handle viewport updates (but not for arrow keys)
	handleViewport := true
	if key, ok := msg.(tea.KeyMsg); ok {
		if key.Type == tea.KeyUp || key.Type == tea.KeyDown {
			handleViewport = false
		}
	}

	if handleViewport {
		m.viewport, vpCmd = m.viewport.Update(msg)
	}

	return m, tea.Batch(tiCmd, vpCmd)
}

// autocompleteCommand attempts to complete the current input
func (m *Model) autocompleteCommand() {
	input := m.textInput.Value()

	// File completion
	if idx := strings.LastIndex(input, "@"); idx != -1 {
		prefix := input[idx+1:]
		matches, err := filepath.Glob(prefix + "*")
		if err == nil && len(matches) > 0 {
			completion := matches[0]
			newValue := input[:idx+1] + completion
			m.textInput.SetValue(newValue)
			m.textInput.SetCursor(len(newValue))
			m.suggestions = nil
			return
		}
	}

	commands := []string{"/run", "/resume", "/validate", "/analyze", "/calibrate", "/status", "/history", "/wizard", "/logs", "/profile", "/clear", "/quit", "/help"}

	for _, cmd := range commands {
		if strings.HasPrefix(cmd, input) {
			m.textInput.SetValue(cmd)
			m.textInput.SetCursor(len(cmd))
			return
		}
	}
}

// View renders the TUI
func (m Model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	// Suggestions popup
	suggestionsView := ""
	if len(m.suggestions) > 0 {
		var lines []string
		for i, s := range m.suggestions {
			style := lipgloss.NewStyle().Foreground(colorGray).PaddingLeft(2)
			if i == m.suggestionIdx {
				style = lipgloss.NewStyle().
					Foreground(colorWhite).
					Background(colorPurple).
					PaddingLeft(2).
					PaddingRight(2).
					Bold(true)
			}
			lines = append(lines, style.Render(s))
		}
		suggestionsView = strings.Join(lines, "\n") + "\n"
	}

	// Main viewport
	viewportView := styleViewport.Width(m.viewport.Width + 2).Render(m.viewport.View())

	// Progress line (if migration running)
	progressView := ""
	if m.progressLine != "" && m.mode == ModeMigration {
		progressView = styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}

	return fmt.Sprintf("%s%s\n%s\n%s%s",
		viewportView,
		progressView,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

func (m Model) statusBarView() string {
	w := lipgloss.Width

	dir := styleStatusDir.Render(m.cwd)
	branch := styleStatusBranch.Render(" " + m.gitInfo.Branch)

	// Mode indicator
	modeText := ""
	switch m.mode {
	case ModeWizard:
		modeText = styleStatusText.Render(" [wizard] ")
	case ModeMigration:
		modeText = styleStatusText.Render(" [migrating] ")
	}

	status := ""
	if m.gitInfo.Status == "Dirty" {
		status = styleStatusDirty.Render("Uncommitted Changes")
	} else {
		status = styleStatusClean.Render("All Changes Committed")
	}

	usedWidth := w(dir) + w(branch) + w(modeText) + w(status)
	if usedWidth > m.width {
		usedWidth = m.width
	}

	spacerWidth := m.width - usedWidth
	if spacerWidth < 0 {
		spacerWidth = 0
	}
	spacer := styleStatusBar.Width(spacerWidth).Render("")

	return lipgloss.JoinHorizontal(lipgloss.Top,
		dir,
		branch,
		modeText,
		spacer,
		status,
	)
}

func (m Model) welcomeMessage() string {
	logo := fmt.Sprintf(`
      _            _
   __| |_ __ ___  | |_
  / _' | '_ ' _ \ | __|
 | (_| | | | | | || |_
  \__,_|_| |_| |_| \__|
  Data Migration Tool v%s
`, version.Version)

	welcome := styleTitle.Render(logo)

	body := `
 Welcome to dmt. This tool allows you to safely and
 efficiently migrate data between databases.

 Type /help to see available commands.
`

	tips := lipgloss.NewStyle().Foreground(colorGray).Render(`
 Tip: You can resume an interrupted migration with /resume.
      Hold Shift to select text with mouse.`)

	return welcome + body + tips
}

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
  /wizard               Launch the configuration wizard
  /run [config_file]    Start migration (default: config.yaml)
  /run --profile NAME   Start migration using a saved profile
  /resume [config_file] Resume an interrupted migration
  /resume --profile NAME Resume using a saved profile
  /validate             Validate migration
  /config [config_file] Show configuration details
  /analyze [config_file] Analyze source database and suggest configuration
  /calibrate [config]   Run AI calibration to find optimal config
                        Options: --apply (update config), --sample-size N
  /status [-d]          Show migration status (--detailed for task list)
  /history              Show migration history
  /profile save NAME    Save an encrypted profile
  /profile list         List saved profiles
  /profile delete NAME  Delete a saved profile
  /profile export NAME  Export a profile to a config file
  /logs                 Save session logs to a file
  /clear                Clear screen
  /quit                 Exit application
  !<command>            Run a shell command

Note: You can use @/path/to/file for config files.`
		return func() tea.Msg { return BoxedOutputMsg(help) }

	case "/logs":
		logFile := "session.log"
		err := os.WriteFile(logFile, []byte(m.content.String()), 0644)
		if err != nil {
			return func() tea.Msg { return OutputMsg(fmt.Sprintf("Error saving logs: %v\n", err)) }
		}
		return func() tea.Msg { return OutputMsg(fmt.Sprintf("Logs saved to %s\n", logFile)) }

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

	case "/wizard":
		m.mode = ModeWizard
		m.wizardStep = stepSourceType
		m.textInput.Reset()
		m.textInput.Placeholder = ""

		configFile, profileName := parseConfigArgs(parts)
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
		configFile, profileName := parseConfigArgs(parts)
		return m.runMigrationCmd(configFile, profileName)

	case "/resume":
		// Block if migration already running
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A migration is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		configFile, profileName := parseConfigArgs(parts)
		return m.runResumeCmd(configFile, profileName)

	case "/validate":
		configFile, profileName := parseConfigArgs(parts)
		return m.runValidateCmd(configFile, profileName)

	case "/status":
		configFile, profileName, detailed := parseStatusArgs(parts)
		return m.runStatusCmd(configFile, profileName, detailed)

	case "/history":
		configFile, profileName, runID := parseHistoryArgs(parts)
		return m.runHistoryCmd(configFile, profileName, runID)

	case "/profile":
		return m.handleProfileCommand(parts)

	case "/analyze":
		configFile, profileName := parseConfigArgs(parts)
		return m.runAnalyzeCmd(configFile, profileName)

	case "/calibrate":
		configFile, profileName, applyToConfig, sampleSize := parseCalibrateArgs(parts)
		return m.runCalibrateCmd(configFile, profileName, applyToConfig, sampleSize)

	case "/config":
		configFile, profileName := parseConfigArgs(parts)
		return m.runConfigCmd(configFile, profileName)

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// Migration commands

func (m Model) runMigrationCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		label := configFile
		if profileName != "" {
			label = profileName
		}

		// Load config synchronously to catch errors before spawning goroutine
		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error loading config: %v", err)}
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error initializing: %v", err)}
		}

		p.Send(OutputMsg(fmt.Sprintf("Starting migration with %s\n", label)))

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			ctx, cancel := context.WithCancel(context.Background())
			p.Send(migrationStartedMsg{cancel: cancel})

			// Redirect output
			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error creating pipe: %v", pipeErr)})
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			runErr := orch.Run(ctx)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				if ctx.Err() == context.Canceled {
					p.Send(MigrationDoneMsg{Status: "cancelled", Message: "Migration cancelled"})
				} else {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Migration failed: %v", runErr)})
				}
				return
			}
			p.Send(MigrationDoneMsg{Status: "completed", Message: "Migration completed successfully!"})
		}()

		return nil
	}
}

func (m Model) runResumeCmd(configFile, profileName string) tea.Cmd {
	return func() (result tea.Msg) {
		// Recover from any panics in this function
		defer func() {
			if r := recover(); r != nil {
				result = MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error: %v", r)}
			}
		}()

		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		label := configFile
		if profileName != "" {
			label = profileName
		}

		// Load config synchronously to catch errors before spawning goroutine
		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error loading config: %v", err)}
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error initializing: %v", err)}
		}

		p.Send(OutputMsg(fmt.Sprintf("Resuming migration with %s\n", label)))

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			ctx, cancel := context.WithCancel(context.Background())
			p.Send(migrationStartedMsg{cancel: cancel})

			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error creating pipe: %v", pipeErr)})
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			runErr := orch.Resume(ctx)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				if ctx.Err() == context.Canceled {
					p.Send(MigrationDoneMsg{Status: "cancelled", Message: "Resume cancelled"})
				} else {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Resume failed: %v", runErr)})
				}
				return
			}
			p.Send(MigrationDoneMsg{Status: "completed", Message: "Resume completed successfully!"})
		}()

		return nil
	}
}

func (m Model) runValidateCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			p.Send(OutputMsg(fmt.Sprintf("Validating with %s\n", origin)))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			if err := orch.Validate(context.Background()); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Validation failed: %v\n", err)))
				return
			}
			p.Send(OutputMsg("Validation passed!\n"))
		}()

		return nil
	}
}

func (m Model) runConfigCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		origin := "config: " + configFile
		if profileName != "" {
			origin = "profile: " + profileName
		}

		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error loading %s: %v\n", origin, err))
		}

		return BoxedOutputMsg(cfg.DebugDump())
	}
}

func (m Model) runAnalyzeCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			p.Send(OutputMsg(fmt.Sprintf("Analyzing with %s\n", origin)))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("❌ Error: Both source and target databases must be available for tuning analysis\n")))
				p.Send(OutputMsg(fmt.Sprintf("   %v\n", err)))
				return
			}
			defer orch.Close()

			schema := cfg.Source.Schema
			if schema == "" {
				if cfg.Source.Type == "postgres" {
					schema = "public"
				} else {
					schema = "dbo"
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			suggestions, err := orch.AnalyzeConfig(ctx, schema)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			p.Send(BoxedOutputMsg(suggestions.FormatYAML()))
		}()

		return nil
	}
}

func (m Model) runCalibrateCmd(configFile, profileName string, applyToConfig bool, sampleSize int) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		if applyToConfig && profileName != "" {
			return OutputMsg("Error: --apply requires a file-based config (not a profile)\n")
		}

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			opts := fmt.Sprintf("sample-size=%d", sampleSize)
			if applyToConfig {
				opts += ", apply=true"
			}
			p.Send(OutputMsg(fmt.Sprintf("Starting calibration with %s (%s)\n", origin, opts)))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error creating pipe: %v\n", pipeErr)))
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			result, runErr := orch.Calibrate(ctx, sampleSize)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				p.Send(OutputMsg(fmt.Sprintf("Calibration failed: %v\n", runErr)))
				return
			}

			p.Send(BoxedOutputMsg(result.FormatResultsTable()))
			if result.Recommendation != nil {
				p.Send(OutputMsg("\nRecommended Configuration:\n"))
				p.Send(BoxedOutputMsg(result.FormatYAML()))
				if result.AIReasoning != "" {
					p.Send(OutputMsg(fmt.Sprintf("\nAI Reasoning: %s\n", result.AIReasoning)))
				}

				if applyToConfig {
					if err := result.Recommendation.ApplyToConfigFile(configFile); err != nil {
						p.Send(OutputMsg(fmt.Sprintf("\nFailed to apply config: %v\n", err)))
					} else {
						p.Send(OutputMsg(fmt.Sprintf("\nConfig updated: %s\n", configFile)))
					}
				}
			}
		}()

		return nil
	}
}

func (m Model) runStatusCmd(configFile, profileName string, detailed bool) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			var output string
			if detailed {
				output, err = CaptureToString(orch.ShowDetailedStatus)
			} else {
				output, err = CaptureToString(orch.ShowStatus)
			}
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error showing status: %v\n", err)))
				return
			}
			p.Send(BoxedOutputMsg(output))
		}()

		return nil
	}
}

func (m Model) runHistoryCmd(configFile, profileName, runID string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			var output string
			if runID != "" {
				output, err = CaptureToString(func() error { return orch.ShowRunDetails(runID) })
			} else {
				output, err = CaptureToString(orch.ShowHistory)
			}
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error showing history: %v\n", err)))
				return
			}
			p.Send(BoxedOutputMsg(output))
		}()

		return nil
	}
}

func (m Model) runShellCmd(shellCmd string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cmd := exec.Command("sh", "-c", shellCmd)
			output, err := cmd.CombinedOutput()
			if err != nil {
				p.Send(BoxedOutputMsg(fmt.Sprintf("%s\nError: %v", string(output), err)))
				return
			}
			p.Send(BoxedOutputMsg(string(output)))
		}()

		return nil
	}
}

// Profile commands

func (m Model) handleProfileCommand(parts []string) tea.Cmd {
	if len(parts) < 2 {
		return func() tea.Msg { return OutputMsg("Usage: /profile save|list|delete|export\n") }
	}

	action := parts[1]
	switch action {
	case "list":
		return m.profileListCmd()
	case "save":
		name, configFile := parseProfileSaveArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile save NAME [config_file]\n") }
		}
		return m.profileSaveCmd(name, configFile)
	case "delete":
		if len(parts) < 3 {
			return func() tea.Msg { return OutputMsg("Usage: /profile delete NAME\n") }
		}
		return m.profileDeleteCmd(parts[2])
	case "export":
		name, outFile := parseProfileExportArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile export NAME [output_file]\n") }
		}
		return m.profileExportCmd(name, outFile)
	default:
		return func() tea.Msg { return OutputMsg("Unknown profile command: " + action + "\n") }
	}
}

func (m Model) profileSaveCmd(name, configFile string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := config.Load(configFile)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading config: %v\n", err)))
				return
			}
			if name == "" {
				if cfg.Profile.Name != "" {
					name = cfg.Profile.Name
				} else {
					base := filepath.Base(configFile)
					name = strings.TrimSuffix(base, filepath.Ext(base))
				}
			}
			payload, err := yaml.Marshal(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error encoding config: %v\n", err)))
				return
			}

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.SaveProfile(name, cfg.Profile.Description, payload); err != nil {
				if strings.Contains(err.Error(), "DMT_MASTER_KEY is not set") {
					p.Send(OutputMsg("Error saving profile: DMT_MASTER_KEY is not set. Start the TUI with the env var set.\n"))
					return
				}
				p.Send(OutputMsg(fmt.Sprintf("Error saving profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Saved profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileListCmd() tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			profiles, err := state.ListProfiles()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error listing profiles: %v\n", err)))
				return
			}
			if len(profiles) == 0 {
				p.Send(BoxedOutputMsg("No profiles found"))
				return
			}

			var b strings.Builder
			fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n", "Name", "Description", "Created", "Updated")
			for _, prof := range profiles {
				desc := strings.ReplaceAll(strings.TrimSpace(prof.Description), "\n", " ")
				fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n",
					prof.Name,
					desc,
					prof.CreatedAt.Format("2006-01-02 15:04:05"),
					prof.UpdatedAt.Format("2006-01-02 15:04:05"))
			}
			p.Send(BoxedOutputMsg(b.String()))
		}()

		return nil
	}
}

func (m Model) profileDeleteCmd(name string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.DeleteProfile(name); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error deleting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Deleted profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileExportCmd(name, outFile string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			blob, err := state.GetProfile(name)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading profile: %v\n", err)))
				return
			}
			if err := os.WriteFile(outFile, blob, 0600); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error exporting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Exported profile %q to %s\n", name, outFile)))
		}()

		return nil
	}
}

// Wizard handling

func (m *Model) handleWizardStep(input string) tea.Cmd {
	if input != "" {
		m.appendOutput(styleUserInput.Render("> "+input) + "\n")
		m.textInput.Reset()
	} else {
		m.appendOutput(styleUserInput.Render("  (default)") + "\n")
	}

	if cmd := m.processWizardInput(input); cmd != nil {
		return cmd
	}

	prompt := m.renderWizardPrompt()
	m.appendOutput(prompt)
	return nil
}

func (m *Model) processWizardInput(input string) tea.Cmd {
	switch m.wizardStep {
	case stepSourceType:
		if input != "" {
			m.wizardData.Source.Type = input
		}
		m.wizardStep = stepSourceHost
	case stepSourceHost:
		if input != "" {
			m.wizardData.Source.Host = input
		}
		m.wizardStep = stepSourcePort
	case stepSourcePort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Source.Port)
		}
		m.wizardStep = stepSourceDB
	case stepSourceDB:
		if input != "" {
			m.wizardData.Source.Database = input
		}
		m.wizardStep = stepSourceUser
	case stepSourceUser:
		if input != "" {
			m.wizardData.Source.User = input
		}
		m.wizardStep = stepSourcePass
	case stepSourcePass:
		if input != "" {
			m.wizardData.Source.Password = input
		}
		m.wizardStep = stepSourceSSL
		m.textInput.EchoMode = textinput.EchoNormal
	case stepSourceSSL:
		if input != "" {
			if m.wizardData.Source.Type == "postgres" {
				m.wizardData.Source.SSLMode = input
			} else {
				if strings.ToLower(input) == "y" || strings.ToLower(input) == "yes" || strings.ToLower(input) == "true" {
					m.wizardData.Source.TrustServerCert = true
				} else {
					m.wizardData.Source.TrustServerCert = false
				}
			}
		}
		m.wizardStep = stepTargetType
	case stepTargetType:
		if input != "" {
			m.wizardData.Target.Type = input
		}
		m.wizardStep = stepTargetHost
	case stepTargetHost:
		if input != "" {
			m.wizardData.Target.Host = input
		}
		m.wizardStep = stepTargetPort
	case stepTargetPort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Target.Port)
		}
		m.wizardStep = stepTargetDB
	case stepTargetDB:
		if input != "" {
			m.wizardData.Target.Database = input
		}
		m.wizardStep = stepTargetUser
	case stepTargetUser:
		if input != "" {
			m.wizardData.Target.User = input
		}
		m.wizardStep = stepTargetPass
	case stepTargetPass:
		if input != "" {
			m.wizardData.Target.Password = input
		}
		m.wizardStep = stepTargetSSL
		m.textInput.EchoMode = textinput.EchoNormal
	case stepTargetSSL:
		if input != "" {
			if m.wizardData.Target.Type == "postgres" {
				m.wizardData.Target.SSLMode = input
			} else {
				if strings.ToLower(input) == "y" || strings.ToLower(input) == "yes" || strings.ToLower(input) == "true" {
					m.wizardData.Target.TrustServerCert = true
				} else {
					m.wizardData.Target.TrustServerCert = false
				}
			}
		}
		m.wizardStep = stepWorkers
	case stepWorkers:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Migration.Workers)
		}
		return m.finishWizard()
	}
	return nil
}

func (m *Model) renderWizardPrompt() string {
	var prompt string
	switch m.wizardStep {
	case stepSourceType:
		def := "mssql"
		if m.wizardData.Source.Type != "" {
			def = m.wizardData.Source.Type
		}
		prompt = fmt.Sprintf("Source Type (mssql/postgres) [%s]: ", def)
	case stepSourceHost:
		prompt = fmt.Sprintf("Source Host [%s]: ", m.wizardData.Source.Host)
	case stepSourcePort:
		def := 1433
		if m.wizardData.Source.Port != 0 {
			def = m.wizardData.Source.Port
		}
		prompt = fmt.Sprintf("Source Port [%d]: ", def)
	case stepSourceDB:
		prompt = fmt.Sprintf("Source Database [%s]: ", m.wizardData.Source.Database)
	case stepSourceUser:
		prompt = fmt.Sprintf("Source User [%s]: ", m.wizardData.Source.User)
	case stepSourcePass:
		prompt = "Source Password [******]: "
		m.textInput.EchoMode = textinput.EchoPassword
	case stepSourceSSL:
		if m.wizardData.Source.Type == "postgres" {
			def := "require"
			if m.wizardData.Source.SSLMode != "" {
				def = m.wizardData.Source.SSLMode
			}
			prompt = fmt.Sprintf("Source SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Source.TrustServerCert {
				def = "y"
			}
			prompt = fmt.Sprintf("Trust Source Server Certificate? (y/n) [%s]: ", def)
		}
	case stepTargetType:
		def := "postgres"
		if m.wizardData.Target.Type != "" {
			def = m.wizardData.Target.Type
		}
		prompt = fmt.Sprintf("Target Type (postgres/mssql) [%s]: ", def)
	case stepTargetHost:
		prompt = fmt.Sprintf("Target Host [%s]: ", m.wizardData.Target.Host)
	case stepTargetPort:
		def := 5432
		if m.wizardData.Target.Port != 0 {
			def = m.wizardData.Target.Port
		}
		prompt = fmt.Sprintf("Target Port [%d]: ", def)
	case stepTargetDB:
		prompt = fmt.Sprintf("Target Database [%s]: ", m.wizardData.Target.Database)
	case stepTargetUser:
		prompt = fmt.Sprintf("Target User [%s]: ", m.wizardData.Target.User)
	case stepTargetPass:
		prompt = "Target Password [******]: "
		m.textInput.EchoMode = textinput.EchoPassword
	case stepTargetSSL:
		if m.wizardData.Target.Type == "postgres" {
			def := "require"
			if m.wizardData.Target.SSLMode != "" {
				def = m.wizardData.Target.SSLMode
			}
			prompt = fmt.Sprintf("Target SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Target.TrustServerCert {
				def = "y"
			}
			prompt = fmt.Sprintf("Trust Target Server Certificate? (y/n) [%s]: ", def)
		}
	case stepWorkers:
		def := 8
		if m.wizardData.Migration.Workers != 0 {
			def = m.wizardData.Migration.Workers
		}
		prompt = fmt.Sprintf("Parallel Workers [%d]: ", def)
	}
	return prompt
}

func (m *Model) finishWizard() tea.Cmd {
	return func() tea.Msg {
		if m.wizardData.Source.Type == "" {
			m.wizardData.Source.Type = "mssql"
		}
		if m.wizardData.Target.Type == "" {
			m.wizardData.Target.Type = "postgres"
		}
		if m.wizardData.Migration.Workers == 0 {
			m.wizardData.Migration.Workers = 8
		}

		data, err := yaml.Marshal(m.wizardData)
		if err != nil {
			return WizardFinishedMsg{Err: fmt.Errorf("generating config: %w", err)}
		}

		filename := m.wizardFile
		if filename == "" {
			filename = "config.yaml"
		}

		if err := os.WriteFile(filename, data, 0600); err != nil {
			return WizardFinishedMsg{Err: fmt.Errorf("saving %s: %w", filename, err)}
		}

		return WizardFinishedMsg{Message: fmt.Sprintf("Configuration saved to %s!\nYou can now run the migration with /run @%s", filename, filename)}
	}
}

// Helper functions

func parseConfigArgs(parts []string) (string, string) {
	configFile := "config.yaml"
	profileName := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		if arg == "--profile" && i+1 < len(parts) {
			profileName = parts[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "@") {
			configFile = arg[1:]
		} else {
			configFile = arg
		}
	}

	return configFile, profileName
}

func parseHistoryArgs(parts []string) (string, string, string) {
	configFile := "config.yaml"
	profileName := ""
	runID := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--run":
			if i+1 < len(parts) {
				runID = parts[i+1]
				i++
			}
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, runID
}

func parseStatusArgs(parts []string) (string, string, bool) {
	configFile := "config.yaml"
	profileName := ""
	detailed := false

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--detailed", "-d":
			detailed = true
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, detailed
}

func parseCalibrateArgs(parts []string) (string, string, bool, int) {
	configFile := "config.yaml"
	profileName := ""
	applyToConfig := false
	sampleSize := 10000

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--apply", "-a":
			applyToConfig = true
		case "--sample-size", "-s":
			if i+1 < len(parts) {
				if n, err := fmt.Sscanf(parts[i+1], "%d", &sampleSize); err == nil && n == 1 {
					i++
				}
			}
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else if !strings.HasPrefix(arg, "-") {
				configFile = arg
			}
		}
	}

	return configFile, profileName, applyToConfig, sampleSize
}

func parseProfileSaveArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}

	name := ""
	configFile := "config.yaml"

	if strings.HasPrefix(parts[2], "@") {
		configFile = parts[2][1:]
	} else {
		name = parts[2]
	}

	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			configFile = parts[3][1:]
		} else {
			configFile = parts[3]
		}
	}

	return name, configFile
}

func parseProfileExportArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}
	name := parts[2]
	outFile := "config.yaml"
	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			outFile = parts[3][1:]
		} else {
			outFile = parts[3]
		}
	}
	return name, outFile
}

func loadConfigFromOrigin(configFile, profileName string) (*config.Config, error) {
	if profileName != "" {
		return loadProfileConfig(profileName)
	}
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configFile)
	}
	return config.Load(configFile)
}

func loadProfileConfig(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}

// wrapLine wraps text to fit within width, preserving word boundaries where
// possible. Words longer than width are split at the boundary. Whitespace
// is preserved as separate tokens to maintain formatting.
func wrapLine(line string, width int) string {
	if width <= 0 || len(line) <= width {
		return line
	}

	var result strings.Builder
	currentLine := ""

	words := splitIntoWords(line)
	for _, word := range words {
		if len(currentLine)+len(word) > width {
			if currentLine != "" {
				result.WriteString(currentLine)
				result.WriteString("\n")
			}
			for len(word) > width {
				result.WriteString(word[:width])
				result.WriteString("\n")
				word = word[width:]
			}
			currentLine = word
		} else {
			currentLine += word
		}
	}

	if currentLine != "" {
		result.WriteString(currentLine)
	}

	return result.String()
}

func splitIntoWords(s string) []string {
	var words []string
	var current strings.Builder

	for _, r := range s {
		if unicode.IsSpace(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}

// Start launches the TUI program
func Start() error {
	logging.SetLevel(logging.LevelInfo)

	m := InitialModel()
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	SetProgramRef(p)

	cleanup := CaptureOutput(p)
	defer cleanup()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}
