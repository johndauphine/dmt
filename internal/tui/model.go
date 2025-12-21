package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/orchestrator"
	"gopkg.in/yaml.v3"
)

type sessionMode int

const (
	modeNormal sessionMode = iota
	modeWizard
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

// Model is the main TUI model
type Model struct {
	viewport    viewport.Model
	textInput   textinput.Model
	ready       bool
	gitInfo     GitInfo
	cwd         string
	err         error
	width       int
	height      int
	history     []string
	historyIdx  int
	logBuffer   string // Persistent buffer for logs
	lineBuffer  string // Buffer for incoming partial lines
	suggestions []string // Auto-completion suggestions
	suggestionIdx int    // Currently selected suggestion index
	lastInput   string   // Last input value to prevent unnecessary suggestion regeneration

	// Wizard state
	mode        sessionMode
	step        wizardStep
	wizardData  config.Config
	wizardInput string
	wizardFile  string
}

type commandInfo struct {
	Name        string
	Description string
}

var availableCommands = []commandInfo{
	{"/run", "Start migration (default: config.yaml)"},
	{"/validate", "Validate migration row counts"},
	{"/status", "Show migration status"},
	{"/history", "Show migration history"},
	{"/wizard", "Launch configuration wizard"},
	{"/logs", "Save session logs to file"},
	{"/about", "Show application information"},
	{"/help", "Show available commands"},
	{"/clear", "Clear screen"},
	{"/quit", "Exit application"},
}

// TickMsg is used to update the UI periodically (e.g. for git status)
type TickMsg time.Time

// WizardFinishedMsg indicates the wizard completed (or failed)
type WizardFinishedMsg struct {
	Err     error
	Message string
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
	ti.Placeholder = "Type /help for commands..."
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 20
	ti.Prompt = "❯ "
	ti.PromptStyle = stylePrompt

	cwd, _ := os.Getwd()

	m := Model{
		textInput:  ti,
		gitInfo:    GetGitInfo(),
		cwd:        cwd,
		history:    []string{},
		historyIdx: -1,
	}

	return m
}

// Update handles messages and updates the model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
				// Select suggestion
				if m.suggestionIdx >= 0 && m.suggestionIdx < len(m.suggestions) {
					selection := m.suggestions[m.suggestionIdx]
					// Extract actual value (first word) if it contains description
					completion := strings.Fields(selection)[0]
					
					input := m.textInput.Value()
					
					// File completion logic (@)
					if idx := strings.LastIndex(input, "@"); idx != -1 && (idx == 0 || input[idx-1] == ' ') {
						newValue := input[:idx+1] + completion
						
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break // Fallthrough
						}
						m.textInput.SetValue(newValue)
						m.textInput.SetCursor(len(newValue))
					
					} else if strings.HasPrefix(input, "/") {
						// Command completion logic
						newValue := completion
						
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break // Fallthrough
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
		case tea.KeyCtrlC, tea.KeyEsc:
			return m, tea.Quit
		case tea.KeyEnter:
			value := m.textInput.Value()
			if m.mode == modeWizard {
				return m, m.handleWizardStep(value)
			}
			if value != "" {
				m.logBuffer += styleUserInput.Render("> "+value) + "\n"
				m.viewport.SetContent(m.logBuffer)
				m.viewport.GotoBottom()
				
				m.textInput.Reset()
				m.history = append(m.history, value)
				m.historyIdx = len(m.history)
				return m, m.handleCommand(value)
			}
		case tea.KeyUp:
			if m.historyIdx > 0 {
				m.historyIdx--
				m.textInput.SetValue(m.history[m.historyIdx])
			}
		case tea.KeyDown:
			if m.historyIdx < len(m.history)-1 {
				m.historyIdx++
				m.textInput.SetValue(m.history[m.historyIdx])
			} else {
				m.historyIdx = len(m.history)
				m.textInput.Reset()
			}
		case tea.KeyTab: // Command completion
			if m.mode == modeNormal {
				m.autocompleteCommand()
			}
		}

	case tea.WindowSizeMsg:
		headerHeight := 0
		footerHeight := 7 // Bordered input (3) + Status bar (1) + Separator (1) + Suggestions (1) + Safety (1)
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width-2, msg.Height-verticalMarginHeight) // -2 for scrollbar
			m.viewport.YPosition = headerHeight
			// Initialize log buffer with welcome message
			m.logBuffer = m.welcomeMessage()
			m.viewport.SetContent(m.logBuffer)
			m.ready = true
		} else {
			m.viewport.Width = msg.Width - 2 // -2 for scrollbar
			m.viewport.Height = msg.Height - verticalMarginHeight
		}
		m.width = msg.Width
		m.height = msg.Height
		m.textInput.Width = msg.Width - 4

	case WizardFinishedMsg:
		m.mode = modeNormal
		
		text := msg.Message
		if msg.Err != nil {
			text = styleError.Render("✖ " + msg.Err.Error())
		} else {
			text = styleSuccess.Render("✔ " + text)
		}
		
		m.logBuffer += "\n" + text + "\n"
		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()

	case OutputMsg:
		m.lineBuffer += string(msg)

		// Process complete lines
		for {
			newlineIdx := strings.Index(m.lineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}

			// Extract line
			line := m.lineBuffer[:newlineIdx]
			m.lineBuffer = m.lineBuffer[newlineIdx+1:]

			// Handle carriage returns (simulate line overwrite by taking last part)
			if lastCR := strings.LastIndex(line, "\r"); lastCR != -1 {
				line = line[lastCR+1:]
			}

			// Apply styling
			lowerText := strings.ToLower(line)
			prefix := "  "
			
			isError := strings.Contains(lowerText, "error") || 
				(strings.Contains(lowerText, "fail") && !strings.Contains(lowerText, "0 failed"))
				
			if isError {
				line = styleError.Render(line)
				prefix = styleError.Render("✖ ")
			} else if strings.Contains(lowerText, "success") || strings.Contains(lowerText, "passed") || strings.Contains(lowerText, "complete") {
				line = styleSuccess.Render(line)
				prefix = styleSuccess.Render("✔ ")
			} else {
				line = styleSystemOutput.Render(line)
			}

			// Append to log
			m.logBuffer += prefix + line + "\n"
		}
		
		// Update viewport
		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()

	case TickMsg:
		m.gitInfo = GetGitInfo()
		return m, tickCmd()
	}

	m.textInput, tiCmd = m.textInput.Update(msg)
	
	// Handle auto-completion suggestions
	input := m.textInput.Value()
	if input != m.lastInput {
		m.lastInput = input
		m.suggestions = nil // Reset first
		
		// File completion (@)
		if idx := strings.LastIndex(input, "@"); idx != -1 {
			// Only trigger if @ is start of input or preceded by space
			if idx == 0 || input[idx-1] == ' ' {
				prefix := input[idx+1:]
				matches, err := filepath.Glob(prefix + "*")
				if err == nil {
					// Filter matches to top 15
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
			// Find matching commands
			for _, cmd := range availableCommands {
				if strings.HasPrefix(cmd.Name, input) {
					// Format: "/cmd - Description"
					m.suggestions = append(m.suggestions, fmt.Sprintf("%-10s %s", cmd.Name, cmd.Description))
				}
			}
			if len(m.suggestions) > 0 {
				m.suggestionIdx = 0
			}
		}
	}

	// Only pass messages to viewport if they are NOT Up/Down arrow keys
	// or if we want to allow scrolling via other keys (PageUp/Down works by default in viewport)
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
			// Use the first match
			completion := matches[0]
			newValue := input[:idx+1] + completion
			m.textInput.SetValue(newValue)
			m.textInput.SetCursor(len(newValue))
			m.suggestions = nil
			return
		}
	}

	commands := []string{"/run", "/validate", "/status", "/history", "/wizard", "/logs", "/clear", "/quit", "/help"}
	
	for _, cmd := range commands {
		if strings.HasPrefix(cmd, input) {
			m.textInput.SetValue(cmd)
			// Move cursor to end
			m.textInput.SetCursor(len(cmd))
			return
		}
	}
	
	// DEBUG: If no match
	// m.logBuffer += fmt.Sprintf("No auto-complete match for '%s'\n", input)
	// m.viewport.SetContent(m.logBuffer)
}

// View renders the TUI
func (m Model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	// Calculate scrollbar
	// Viewport height is m.viewport.Height
	// Total content height is len(strings.Split(m.logBuffer, "\n")) - but wait, wordwrap changes line count.
	// m.viewport.TotalLineCount() is accurate.
	
	totalLines := m.viewport.TotalLineCount()
	visibleLines := m.viewport.Height
	scrollPercent := 0.0
	if totalLines > visibleLines {
		scrollPercent = float64(m.viewport.YOffset) / float64(totalLines-visibleLines)
	}

	// Render Scrollbar
	bar := make([]string, visibleLines)
	handleHeight := int(float64(visibleLines) * (float64(visibleLines) / float64(totalLines)))
	if handleHeight < 1 {
		handleHeight = 1
	}
	handleTop := int(scrollPercent * float64(visibleLines-handleHeight))

	for i := 0; i < visibleLines; i++ {
		if totalLines <= visibleLines {
			bar[i] = " " // No scrollbar needed
		} else if i >= handleTop && i < handleTop+handleHeight {
			bar[i] = styleScrollbarHandle.Render("┃")
		} else {
			bar[i] = styleScrollbar.Render("│")
		}
	}

	// Join Viewport and Scrollbar
	// We need to ensure viewport width accounts for scrollbar
	// In WindowSizeMsg, we set viewport.Width = msg.Width.
	// If we add a scrollbar, we should reduce viewport width by 1 or 2.
	// But changing it here is late.
	// Let's assume we can overlay it or just append it?
	// Appending it requires line-by-line join.
	
	vpLines := strings.Split(m.viewport.View(), "\n")
	// Pad or truncate vpLines to match visibleLines
	for len(vpLines) < visibleLines {
		vpLines = append(vpLines, "")
	}
	
	var viewWithBar []string
	for i := 0; i < visibleLines && i < len(vpLines); i++ {
		viewWithBar = append(viewWithBar, vpLines[i] + " " + bar[i])
	}

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

	return fmt.Sprintf("%s\n%s\n%s%s",
		m.viewport.View(),
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

func (m Model) statusBarView() string {
	w := lipgloss.Width

	dir := styleStatusDir.Render(m.cwd)
	branch := styleStatusBranch.Render(" " + m.gitInfo.Branch)

	status := ""
	if m.gitInfo.Status == "Dirty" {
		status = styleStatusDirty.Render("Uncommitted Changes") 
	} else {
		status = styleStatusClean.Render("All Changes Committed") 
	}

	// Calculate remaining width for spacer
	usedWidth := w(dir) + w(branch) + w(status)
	if usedWidth > m.width {
		usedWidth = m.width
	}
	
	spacer := styleStatusBar.
		Width(m.width - usedWidth).
		Render("")

	return lipgloss.JoinHorizontal(lipgloss.Top,
		dir,
		branch,
		spacer,
		status,
	)
}

func (m Model) welcomeMessage() string {
	logo := `
  __  __  _____  _____  _____ _       
 |  \/  |/ ____|/ ____|/ ____| |      
 | \  / | (___ | (___ | |  __| |      
 | |\/| |\___ \ \___ \ | | |_ | |      
 | |  | |____) |____) | |__| | |____  
 |_|  |_|_____/|_____/ \_____|______| 
  PG-MIGRATE INTERACTIVE SHELL
`
	
	welcome := styleTitle.Render(logo)
	
	body := `
 Welcome to the migration engine. This tool allows you to
 safely and efficiently move data between SQL Server and 
 PostgreSQL.
 
 Type /help to see available commands.
`
	
	tips := lipgloss.NewStyle().Foreground(colorGray).Render(`
 Tip: You can resume an interrupted migration with /run.
      Hold Shift to select text with mouse.`)

	return welcome + body + tips
}
func (m *Model) handleCommand(cmdStr string) tea.Cmd {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	
	// Helper to extract config file from args, supporting @/path/to/file syntax
	getConfigFile := func(args []string) string {
		if len(args) > 1 {
			path := args[1]
			if strings.HasPrefix(path, "@") {
				return path[1:]
			}
			return path
		}
		return "config.yaml"
	}

	switch cmd {
	case "/quit", "/exit":
		return tea.Quit

	case "/clear":
		m.logBuffer = m.welcomeMessage()
		m.viewport.SetContent(m.logBuffer)
		return nil

	case "/help":
		help := `
Available Commands:
  /wizard               Launch the configuration wizard
  /run [config_file]    Start migration (default: config.yaml)
  /validate             Validate migration
  /status               Show migration status
  /history              Show migration history
  /logs                 Save session logs to a file for analysis
  /clear                Clear screen
  /quit                 Exit application
  
  Note: You can use @/path/to/file for config files.
`
		return func() tea.Msg { return OutputMsg(help) }

	case "/logs":
		logFile := "session.log"
		err := os.WriteFile(logFile, []byte(m.logBuffer), 0644)
		if err != nil {
			return func() tea.Msg { return OutputMsg(fmt.Sprintf("Error saving logs: %v\n", err)) }
		}
		return func() tea.Msg { return OutputMsg(fmt.Sprintf("Logs saved to %s\n", logFile)) }

	case "/about":
		about := `
  MSSQL-PG-MIGRATE v1.10.0
  
  A high-performance data migration tool for moving data 
  from SQL Server to PostgreSQL (and vice-versa).
  
  Features:
  - Parallel transfer with auto-tuning
  - Resume capability (chunk-level)
  - Data validation
  - Configuration wizard
  
  Built with Go and Bubble Tea.
`
		return func() tea.Msg { return OutputMsg(about) }

	case "/wizard":
		m.mode = modeWizard
		m.step = stepSourceType
		m.textInput.Reset()
		m.textInput.Placeholder = ""
		
		// Determine config file to edit/create
		m.wizardFile = getConfigFile(parts)
		
		// Load existing config if available to use as defaults
		if _, err := os.Stat(m.wizardFile); err == nil {
			if cfg, err := config.Load(m.wizardFile); err == nil {
				m.wizardData = *cfg
				m.logBuffer += fmt.Sprintf("\n--- EDITING CONFIGURATION: %s ---\n", m.wizardFile)
			} else {
				m.logBuffer += fmt.Sprintf("\n--- CONFIGURATION WIZARD: %s ---\n", m.wizardFile)
			}
		} else {
			m.logBuffer += fmt.Sprintf("\n--- CONFIGURATION WIZARD: %s ---\n", m.wizardFile)
		}
		
		// Display first prompt
		prompt := m.renderWizardPrompt()
		m.logBuffer += prompt
		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()
		return nil

	case "/run":
		return m.runMigrationCmd(getConfigFile(parts))
	
	case "/validate":
		return m.runValidateCmd(getConfigFile(parts))

	case "/status":
		return m.runStatusCmd(getConfigFile(parts))
		
	case "/history":
		configFile := "config.yaml"
		runID := ""
		if len(parts) > 1 {
			if parts[1] == "--run" && len(parts) > 2 {
				runID = parts[2]
			} else {
				configFile = getConfigFile(parts)
			}
		}
		return m.runHistoryCmd(configFile, runID)

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// Wrappers for Orchestrator actions

func (m Model) runMigrationCmd(configFile string) tea.Cmd {
	return func() tea.Msg {
		// Output to the view
		out := fmt.Sprintf("Running migration with config: %s\n", configFile)
		
		// Check file existence first for better error
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return OutputMsg(out + "Config file not found. Run /wizard to create one.\n")
		}

		// Load config
		cfg, err := config.Load(configFile)
		if err != nil {
			return OutputMsg(out + fmt.Sprintf("Error loading config: %v\n", err))
		}

		// Create orchestrator
		orch, err := orchestrator.New(cfg)
		if err != nil {
			return OutputMsg(out + fmt.Sprintf("Error initializing orchestrator: %v\n", err))
		}
		defer orch.Close()

		// Run
		if err := orch.Run(context.Background()); err != nil {
			return OutputMsg(out + fmt.Sprintf("Migration failed: %v\n", err))
		}
		
		return OutputMsg(out + "Migration completed successfully!\n")
	}
}

func (m Model) runValidateCmd(configFile string) tea.Cmd {
	return func() tea.Msg {
		out := fmt.Sprintf("Validating with config: %s\n", configFile)
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return OutputMsg(out + "Config file not found. Run /wizard to create one.\n")
		}
		cfg, err := config.Load(configFile)
		if err != nil {
			return OutputMsg(out + fmt.Sprintf("Error: %v\n", err))
		}
		orch, err := orchestrator.New(cfg)
		if err != nil {
			return OutputMsg(out + fmt.Sprintf("Error: %v\n", err))
		}
		defer orch.Close()

		if err := orch.Validate(context.Background()); err != nil {
			return OutputMsg(out + fmt.Sprintf("Validation failed: %v\n", err))
		}
		return OutputMsg(out + "Validation passed!\n")
	}
}

func (m Model) runStatusCmd(configFile string) tea.Cmd {
	return func() tea.Msg {
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return OutputMsg("Config file not found. Run /wizard to create one.\n")
		}
		cfg, err := config.Load(configFile)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		orch, err := orchestrator.New(cfg)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		defer orch.Close()

		// Capture stdout for ShowStatus
		if err := orch.ShowStatus(); err != nil {
			return OutputMsg(fmt.Sprintf("Error showing status: %v\n", err))
		}
		return nil
	}
}

func (m Model) runHistoryCmd(configFile, runID string) tea.Cmd {
	return func() tea.Msg {
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return OutputMsg("Config file not found. Run /wizard to create one.\n")
		}
		cfg, err := config.Load(configFile)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		orch, err := orchestrator.New(cfg)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		defer orch.Close()

		if runID != "" {
			if err := orch.ShowRunDetails(runID); err != nil {
				return OutputMsg(fmt.Sprintf("Error showing run details: %v\n", err))
			}
		} else {
			if err := orch.ShowHistory(); err != nil {
				return OutputMsg(fmt.Sprintf("Error showing history: %v\n", err))
			}
		}
		return nil
	}
}


func (m *Model) processWizardInput(input string) tea.Cmd {
	// Capture input for current step before moving to next
	switch m.step {
	case stepSourceType:
		if input != "" { m.wizardData.Source.Type = input }
		m.step = stepSourceHost
	case stepSourceHost:
		if input != "" { m.wizardData.Source.Host = input }
		m.step = stepSourcePort
	case stepSourcePort:
		if input != "" { fmt.Sscanf(input, "%d", &m.wizardData.Source.Port) }
		m.step = stepSourceDB
	case stepSourceDB:
		if input != "" { m.wizardData.Source.Database = input }
		m.step = stepSourceUser
	case stepSourceUser:
		if input != "" { m.wizardData.Source.User = input }
		m.step = stepSourcePass
	case stepSourcePass:
		if input != "" { m.wizardData.Source.Password = input }
		m.step = stepSourceSSL
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
		m.step = stepTargetType
	case stepTargetType:
		if input != "" { m.wizardData.Target.Type = input }
		m.step = stepTargetHost
	case stepTargetHost:
		if input != "" { m.wizardData.Target.Host = input }
		m.step = stepTargetPort
	case stepTargetPort:
		if input != "" { fmt.Sscanf(input, "%d", &m.wizardData.Target.Port) }
		m.step = stepTargetDB
	case stepTargetDB:
		if input != "" { m.wizardData.Target.Database = input }
		m.step = stepTargetUser
	case stepTargetUser:
		if input != "" { m.wizardData.Target.User = input }
		m.step = stepTargetPass
	case stepTargetPass:
		if input != "" { m.wizardData.Target.Password = input }
		m.step = stepTargetSSL
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
		m.step = stepWorkers
	case stepWorkers:
		if input != "" { fmt.Sscanf(input, "%d", &m.wizardData.Migration.Workers) }
		return m.finishWizard()
	}
	return nil
}

func (m *Model) renderWizardPrompt() string {
	var prompt string
	switch m.step {
	case stepSourceType:
		def := "mssql"
		if m.wizardData.Source.Type != "" { def = m.wizardData.Source.Type }
		prompt = fmt.Sprintf("Source Type (mssql/postgres) [%s]: ", def)
	case stepSourceHost:
		prompt = fmt.Sprintf("Source Host [%s]: ", m.wizardData.Source.Host)
	case stepSourcePort:
		def := 1433
		if m.wizardData.Source.Port != 0 { def = m.wizardData.Source.Port }
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
			if m.wizardData.Source.SSLMode != "" { def = m.wizardData.Source.SSLMode }
			prompt = fmt.Sprintf("Source SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Source.TrustServerCert { def = "y" }
			prompt = fmt.Sprintf("Trust Source Server Certificate? (y/n) [%s]: ", def)
		}
	case stepTargetType:
		def := "postgres"
		if m.wizardData.Target.Type != "" { def = m.wizardData.Target.Type }
		prompt = fmt.Sprintf("Target Type (postgres/mssql) [%s]: ", def)
	case stepTargetHost:
		prompt = fmt.Sprintf("Target Host [%s]: ", m.wizardData.Target.Host)
	case stepTargetPort:
		def := 5432
		if m.wizardData.Target.Port != 0 { def = m.wizardData.Target.Port }
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
			if m.wizardData.Target.SSLMode != "" { def = m.wizardData.Target.SSLMode }
			prompt = fmt.Sprintf("Target SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Target.TrustServerCert { def = "y" }
			prompt = fmt.Sprintf("Trust Target Server Certificate? (y/n) [%s]: ", def)
		}
	case stepWorkers:
		def := 8
		if m.wizardData.Migration.Workers != 0 { def = m.wizardData.Migration.Workers }
		prompt = fmt.Sprintf("Parallel Workers [%d]: ", def)
	}
	return prompt
}

func (m *Model) handleWizardStep(input string) tea.Cmd {
	if input != "" {
		m.logBuffer += styleUserInput.Render("> " + input) + "\n"
		m.viewport.SetContent(m.logBuffer)
		m.textInput.Reset()
	} else {
		// User accepted default
		m.logBuffer += styleUserInput.Render("  (default)") + "\n"
		m.viewport.SetContent(m.logBuffer)
	}

	if cmd := m.processWizardInput(input); cmd != nil {
		return cmd
	}

	// Display prompt for current (new) step
	prompt := m.renderWizardPrompt()
	m.logBuffer += prompt
	m.viewport.SetContent(m.logBuffer)
	m.viewport.GotoBottom()
	return nil
}

func (m *Model) finishWizard() tea.Cmd {
	return func() tea.Msg {
		// Set some sensible defaults if missed
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

		// Write to file
		if err := os.WriteFile(filename, data, 0600); err != nil {
			return WizardFinishedMsg{Err: fmt.Errorf("saving %s: %w", filename, err)}
		}

		return WizardFinishedMsg{Message: fmt.Sprintf("Configuration saved to %s!\nYou can now run the migration with /run @%s", filename, filename)}
	}
}

// Start launches the TUI program
func Start() error {
	m := InitialModel()
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	// Start output capture
	cleanup := CaptureOutput(p)
	defer cleanup()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}