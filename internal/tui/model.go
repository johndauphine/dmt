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
	viewport       viewport.Model
	wizardViewport viewport.Model // Secondary viewport for wizard during migration
	textInput      textinput.Model
	ready          bool
	gitInfo        GitInfo
	cwd            string
	err            error
	width          int
	height         int
	history        []string
	historyIdx     int
	logBuffer      string   // Persistent buffer for logs
	wizardBuffer   string   // Buffer for wizard output (used during split view)
	lineBuffer     string   // Buffer for incoming partial lines
	progressLine   string   // Current progress bar line (updated in-place)
	suggestions    []string // Auto-completion suggestions
	suggestionIdx  int      // Currently selected suggestion index
	lastInput      string   // Last input value to prevent unnecessary suggestion regeneration

	// Migration state
	migrations       map[string]*MigrationInstance // Active migrations by ID
	migrationOrder   []string                      // Order for display (FIFO)
	focusedMigration string                        // Currently focused migration ID

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
	{"/resume", "Resume an interrupted migration"},
	{"/validate", "Validate migration row counts"},
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

// TickMsg is used to update the UI periodically (e.g. for git status)
type TickMsg time.Time

// WizardFinishedMsg indicates the wizard completed (or failed)
type WizardFinishedMsg struct {
	Err     error
	Message string
}

// MigrationDoneMsg signals that a migration has completed
type MigrationDoneMsg struct {
	Output string
}

// BoxedOutputMsg is output that should be displayed in a bordered box
type BoxedOutputMsg string

// MigrationInstance tracks a single running migration
type MigrationInstance struct {
	ID           string             // Unique ID for this migration
	ConfigFile   string             // Config file path
	ProfileName  string             // Profile name (if used)
	Cancel       context.CancelFunc // Cancel function for this migration
	Buffer       string             // Output buffer for this migration
	LineBuffer   string             // Buffer for incoming partial lines
	ProgressLine string             // Current progress bar line
	Status       string             // "running", "completed", "failed", "cancelled"
	StartedAt    time.Time
	Viewport     viewport.Model // Scrollable viewport for this migration
	UserScrolled bool           // True if user manually scrolled (disables auto-scroll)
}

// MigrationOutputMsg routes output to a specific migration
type MigrationOutputMsg struct {
	ID     string
	Output string
}

// MigrationDoneWithIDMsg signals that a specific migration has completed
type MigrationDoneWithIDMsg struct {
	ID     string
	Output string
	Status string // "completed", "failed", "cancelled"
}

// migrationCancels holds cancel functions for each running migration
var migrationCancels = make(map[string]context.CancelFunc)

// hasRunningMigration returns true if any migration is currently running
func (m *Model) hasRunningMigration() bool {
	for _, mi := range m.migrations {
		if mi.Status == "running" {
			return true
		}
	}
	return false
}

// runningMigrationCount returns the number of currently running migrations
func (m *Model) runningMigrationCount() int {
	count := 0
	for _, mi := range m.migrations {
		if mi.Status == "running" {
			count++
		}
	}
	return count
}

// generateMigrationID creates a short unique ID for a migration
func generateMigrationID() string {
	return fmt.Sprintf("m%d", time.Now().UnixNano()%100000)
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

// wrapLine wraps a line of text to fit within the specified width.
// It preserves word boundaries when possible.
func wrapLine(line string, width int) string {
	if width <= 0 || len(line) <= width {
		return line
	}

	var result strings.Builder
	currentLine := ""

	words := splitIntoWords(line)
	for _, word := range words {
		// If adding this word would exceed width
		if len(currentLine)+len(word) > width {
			if currentLine != "" {
				result.WriteString(currentLine)
				result.WriteString("\n")
			}
			// If the word itself is longer than width, split it
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

// splitIntoWords splits text into words while preserving whitespace.
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

	m := Model{
		textInput:      ti,
		gitInfo:        GetGitInfo(),
		cwd:            cwd,
		history:        []string{},
		historyIdx:     -1,
		migrations:     make(map[string]*MigrationInstance),
		migrationOrder: []string{},
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
		case tea.KeyCtrlC:
			// If in wizard mode, cancel wizard first
			if m.mode == modeWizard {
				m.mode = modeNormal
				m.wizardBuffer = ""
				msg := styleSystemOutput.Render("Wizard cancelled") + "\n"
				m.logBuffer += msg
				m.viewport.SetContent(m.logBuffer)
				m.viewport.GotoBottom()
				return m, nil
			}
			// If a migration is focused and running, cancel it
			if m.focusedMigration != "" {
				if cancel, ok := migrationCancels[m.focusedMigration]; ok {
					cancel()
					if mi, ok := m.migrations[m.focusedMigration]; ok {
						mi.Buffer += styleSystemOutput.Render("Cancelling migration... please wait") + "\n"
					}
					return m, nil
				}
			}
			// No active migrations, quit
			if !m.hasRunningMigration() {
				return m, tea.Quit
			}
			return m, nil
		case tea.KeyEsc:
			// Esc also cancels wizard
			if m.mode == modeWizard {
				m.mode = modeNormal
				m.wizardBuffer = ""
				msg := styleSystemOutput.Render("Wizard cancelled") + "\n"
				m.logBuffer += msg
				m.viewport.SetContent(m.logBuffer)
				m.viewport.GotoBottom()
				return m, nil
			}
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

				// Check if this is a migration command and create migration instance
				cmd := strings.Fields(value)[0]
				if cmd == "/run" || cmd == "/resume" {
					parts := strings.Fields(value)
					configFile, profileName := parseConfigArgs(parts)

					// Generate unique ID and create migration instance
					migrationID := generateMigrationID()
					label := configFile
					if profileName != "" {
						label = profileName
					}

					// Calculate viewport size (full screen for tab interface)
					reservedHeight := 10 // tab bar, separator, input, status bar, borders, padding
					vpHeight := m.height - reservedHeight
					if vpHeight < 5 {
						vpHeight = 5
					}
					vp := viewport.New(m.width-4, vpHeight)

					m.migrations[migrationID] = &MigrationInstance{
						ID:          migrationID,
						ConfigFile:  configFile,
						ProfileName: profileName,
						Status:      "running",
						StartedAt:   time.Now(),
						Viewport:    vp,
					}
					m.migrationOrder = append(m.migrationOrder, migrationID)
					m.focusedMigration = migrationID

					// Return the appropriate command with migration ID
					if cmd == "/run" {
						return m, m.runMigrationCmdWithID(configFile, profileName, migrationID, label)
					}
					return m, m.runResumeCmdWithID(configFile, profileName, migrationID, label)
				}

				return m, m.handleCommand(value)
			}
		case tea.KeyTab: // Command completion
			if m.mode == modeNormal {
				m.autocompleteCommand()
			}
		case tea.KeyPgUp:
			// Scroll focused viewport up (console or migration)
			if m.focusedMigration == "console" || m.focusedMigration == "" {
				m.viewport.ScrollUp(m.viewport.Height / 2)
				return m, nil
			}
			if m.focusedMigration != "" {
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.ScrollUp(mi.Viewport.Height / 2)
					mi.UserScrolled = true // Disable auto-scroll
					return m, nil
				}
			}
		case tea.KeyPgDown:
			// Scroll focused viewport down (console or migration)
			if m.focusedMigration == "console" || m.focusedMigration == "" {
				m.viewport.ScrollDown(m.viewport.Height / 2)
				return m, nil
			}
			if m.focusedMigration != "" {
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.ScrollDown(mi.Viewport.Height / 2)
					// Check if at bottom, re-enable auto-scroll
					if mi.Viewport.AtBottom() {
						mi.UserScrolled = false
					}
					return m, nil
				}
			}
		case tea.KeyHome:
			// Scroll to top of focused viewport (console or migration)
			if m.focusedMigration == "console" || m.focusedMigration == "" {
				m.viewport.GotoTop()
				return m, nil
			}
			if m.focusedMigration != "" {
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.GotoTop()
					mi.UserScrolled = true // Disable auto-scroll
					return m, nil
				}
			}
		case tea.KeyEnd:
			// Scroll to bottom of focused viewport (console or migration)
			if m.focusedMigration == "console" || m.focusedMigration == "" {
				m.viewport.GotoBottom()
				return m, nil
			}
			if m.focusedMigration != "" {
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.GotoBottom()
					mi.UserScrolled = false // Re-enable auto-scroll
					return m, nil
				}
			}
		case tea.KeyUp:
			// If migrations are showing and input is empty, scroll viewport
			if len(m.migrationOrder) > 0 && m.textInput.Value() == "" && len(m.suggestions) == 0 {
				if m.focusedMigration == "console" || m.focusedMigration == "" {
					m.viewport.ScrollUp(1)
					return m, nil
				}
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.ScrollUp(1)
					mi.UserScrolled = true
					return m, nil
				}
			}
			// Otherwise fall through to history navigation
			if m.historyIdx > 0 {
				m.historyIdx--
				m.textInput.SetValue(m.history[m.historyIdx])
			}
			return m, nil
		case tea.KeyDown:
			// If migrations are showing and input is empty, scroll viewport
			if len(m.migrationOrder) > 0 && m.textInput.Value() == "" && len(m.suggestions) == 0 {
				if m.focusedMigration == "console" || m.focusedMigration == "" {
					m.viewport.ScrollDown(1)
					return m, nil
				}
				if mi, ok := m.migrations[m.focusedMigration]; ok {
					mi.Viewport.ScrollDown(1)
					if mi.Viewport.AtBottom() {
						mi.UserScrolled = false
					}
					return m, nil
				}
			}
			// Otherwise fall through to history navigation
			if m.historyIdx < len(m.history)-1 {
				m.historyIdx++
				m.textInput.SetValue(m.history[m.historyIdx])
			} else {
				m.historyIdx = len(m.history)
				m.textInput.Reset()
			}
			return m, nil
		}

		// Alt+number keys to switch migration focus
		if len(m.migrationOrder) > 0 && msg.Alt {
			if msg.Type == tea.KeyRunes && len(msg.Runes) == 1 {
				// Alt+0 switches to console
				if msg.Runes[0] == '0' {
					m.focusedMigration = "console"
					return m, nil
				}
				// Alt+1, Alt+2, etc. switch to migration tabs
				num := int(msg.Runes[0] - '1') // '1' -> 0, '2' -> 1, etc.
				if num >= 0 && num < len(m.migrationOrder) {
					m.focusedMigration = m.migrationOrder[num]
					return m, nil
				}
			}
		}

		// Alt+Left/Right to cycle through tabs (console + migrations)
		if len(m.migrationOrder) > 0 {
			if msg.Type == tea.KeyLeft && msg.Alt {
				// Previous tab
				if m.focusedMigration == "console" || m.focusedMigration == "" {
					// From console, go to last migration
					m.focusedMigration = m.migrationOrder[len(m.migrationOrder)-1]
					return m, nil
				}
				// Find current migration and go to previous
				for i, id := range m.migrationOrder {
					if id == m.focusedMigration {
						if i > 0 {
							m.focusedMigration = m.migrationOrder[i-1]
						} else {
							m.focusedMigration = "console"
						}
						return m, nil
					}
				}
			} else if msg.Type == tea.KeyRight && msg.Alt {
				// Next tab
				if m.focusedMigration == "console" || m.focusedMigration == "" {
					// From console, go to first migration
					m.focusedMigration = m.migrationOrder[0]
					return m, nil
				}
				// Find current migration and go to next
				for i, id := range m.migrationOrder {
					if id == m.focusedMigration {
						if i < len(m.migrationOrder)-1 {
							m.focusedMigration = m.migrationOrder[i+1]
						} else {
							m.focusedMigration = "console"
						}
						return m, nil
					}
				}
			}
		}

	case tea.WindowSizeMsg:
		headerHeight := 0
		footerHeight := 7 // Bordered input (3) + Status bar (1) + Separator (1) + Suggestions (1) + Safety (1)
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width-2, msg.Height-verticalMarginHeight) // -2 for scrollbar
			m.viewport.YPosition = headerHeight
			// Initialize wizard viewport (same size, will be resized in split mode)
			m.wizardViewport = viewport.New(msg.Width-2, msg.Height-verticalMarginHeight)
			// Initialize log buffer with welcome message
			m.logBuffer = m.welcomeMessage()
			m.viewport.SetContent(m.logBuffer)
			m.ready = true
		} else {
			m.viewport.Width = msg.Width - 2 // -2 for scrollbar
			m.viewport.Height = msg.Height - verticalMarginHeight
			m.wizardViewport.Width = msg.Width - 2
			m.wizardViewport.Height = msg.Height - verticalMarginHeight
		}
		m.width = msg.Width
		m.height = msg.Height
		m.textInput.Width = msg.Width - 4

		// Resize migration viewports when window changes (full screen for tab interface)
		if len(m.migrationOrder) > 0 {
			reservedHeight := 10 // tab bar, separator, input, status bar, borders, padding
			viewportHeight := msg.Height - reservedHeight
			if viewportHeight < 5 {
				viewportHeight = 5
			}
			for _, id := range m.migrationOrder {
				if mi, ok := m.migrations[id]; ok {
					mi.Viewport.Width = msg.Width - 4
					mi.Viewport.Height = viewportHeight
				}
			}
		}

	case WizardFinishedMsg:
		m.mode = modeNormal

		// Calculate wrap width (viewport width minus prefix and margins)
		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80 // Fallback for uninitialized viewport
		}

		text := msg.Message
		if msg.Err != nil {
			text = wrapLine(msg.Err.Error(), wrapWidth)
			text = styleError.Render("✖ " + text)
		} else {
			text = wrapLine(text, wrapWidth)
			text = styleSuccess.Render("✔ " + text)
		}

		// Clear wizard buffer and add result to main log
		m.wizardBuffer = ""
		m.logBuffer += "\n" + text + "\n"
		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()

	case MigrationDoneMsg:
		// Legacy handler for old-style single migration
		// (kept for backward compatibility with non-multi-migration code paths)
		m.progressLine = "" // Clear any progress bar
		// Process the output the same way as OutputMsg
		m.lineBuffer += msg.Output

		// Calculate wrap width (viewport width minus prefix and margins)
		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80 // Fallback for uninitialized viewport
		}

		for {
			newlineIdx := strings.Index(m.lineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}
			line := m.lineBuffer[:newlineIdx]
			m.lineBuffer = m.lineBuffer[newlineIdx+1:]
			if line == "" {
				continue
			}

			// Wrap long lines before styling
			wrappedLines := strings.Split(wrapLine(line, wrapWidth), "\n")
			for _, wrappedLine := range wrappedLines {
				// Style the line based on content
				var styledLine string
				if strings.Contains(line, "Error") || strings.Contains(line, "failed") || strings.Contains(line, "obsolete") {
					styledLine = styleError.Render("✖ " + wrappedLine)
				} else if strings.Contains(line, "success") || strings.Contains(line, "completed") {
					styledLine = styleSuccess.Render("✔ " + wrappedLine)
				} else {
					styledLine = styleSystemOutput.Render("  " + wrappedLine)
				}
				m.logBuffer += styledLine + "\n"
			}
		}

		// If wizard is still active, merge wizard buffer into main buffer
		if m.mode == modeWizard && m.wizardBuffer != "" {
			m.logBuffer += "\n" + m.wizardBuffer
			m.wizardBuffer = ""
		}

		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()

	case BoxedOutputMsg:
		// Boxed output - wrap the entire output in a bordered box
		output := strings.TrimSpace(string(msg))
		if output == "" {
			break
		}

		// Calculate box width based on viewport
		boxWidth := m.viewport.Width - 4
		if boxWidth < 40 {
			boxWidth = 80
		}

		// Apply the box style to the output
		boxedOutput := styleOutputBox.Width(boxWidth).Render(output)
		m.logBuffer += boxedOutput + "\n"
		m.viewport.SetContent(m.logBuffer)
		m.viewport.GotoBottom()

	case OutputMsg:
		m.lineBuffer += string(msg)

		// Calculate wrap width (viewport width minus prefix and margins)
		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80 // Fallback for uninitialized viewport
		}

		// Process complete lines
		for {
			newlineIdx := strings.Index(m.lineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}

			// Clear progress line when we get a complete line
			m.progressLine = ""

			// Extract line
			line := m.lineBuffer[:newlineIdx]
			m.lineBuffer = m.lineBuffer[newlineIdx+1:]

			// Handle carriage returns (simulate line overwrite by taking last part)
			if lastCR := strings.LastIndex(line, "\r"); lastCR != -1 {
				line = line[lastCR+1:]
			}

			// Wrap long lines before styling
			wrappedLines := strings.Split(wrapLine(line, wrapWidth), "\n")
			for _, wrappedLine := range wrappedLines {
				// Apply styling
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

				// Append to log
				m.logBuffer += prefix + wrappedLine + "\n"
			}
		}

		// Check for progress bar updates (lines with \r but no \n)
		// These are in-place updates like progress bars
		if strings.Contains(m.lineBuffer, "\r") {
			// Extract the part after the last \r as the current progress line
			if lastCR := strings.LastIndex(m.lineBuffer, "\r"); lastCR != -1 {
				m.progressLine = strings.TrimSpace(m.lineBuffer[lastCR+1:])
				// Keep everything before the last \r in case there's more content
				m.lineBuffer = m.lineBuffer[:lastCR+1]
			}
		}

		// Update viewport with log buffer + progress line (but not during wizard)
		content := m.logBuffer
		if m.progressLine != "" && m.mode != modeWizard {
			content += styleSystemOutput.Render("  "+m.progressLine) + "\n"
		}
		m.viewport.SetContent(content)
		m.viewport.GotoBottom()

	case MigrationOutputMsg:
		// Route output to the specific migration's buffer
		mi, ok := m.migrations[msg.ID]
		if !ok {
			break
		}

		mi.LineBuffer += msg.Output

		// Calculate wrap width
		wrapWidth := m.viewport.Width - 6
		if wrapWidth < 20 {
			wrapWidth = 80
		}

		// Process complete lines
		for {
			newlineIdx := strings.Index(mi.LineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}

			mi.ProgressLine = ""
			line := mi.LineBuffer[:newlineIdx]
			mi.LineBuffer = mi.LineBuffer[newlineIdx+1:]

			if lastCR := strings.LastIndex(line, "\r"); lastCR != -1 {
				line = line[lastCR+1:]
			}

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

				mi.Buffer += prefix + wrappedLine + "\n"
			}
		}

		// Handle progress bar updates
		if strings.Contains(mi.LineBuffer, "\r") {
			if lastCR := strings.LastIndex(mi.LineBuffer, "\r"); lastCR != -1 {
				mi.ProgressLine = strings.TrimSpace(mi.LineBuffer[lastCR+1:])
				mi.LineBuffer = mi.LineBuffer[:lastCR+1]
			}
		}

		// Update viewport content
		content := mi.Buffer
		if mi.ProgressLine != "" {
			content += styleSystemOutput.Render("  "+mi.ProgressLine) + "\n"
		}
		mi.Viewport.SetContent(content)
		// Only auto-scroll if user hasn't manually scrolled up
		if !mi.UserScrolled {
			mi.Viewport.GotoBottom()
		}

	case MigrationDoneWithIDMsg:
		// Update the specific migration's status
		mi, ok := m.migrations[msg.ID]
		if ok {
			mi.Status = msg.Status
			mi.ProgressLine = ""
			if msg.Output != "" {
				// Add final message to buffer
				prefix := styleSuccess.Render("✔ ")
				if msg.Status == "failed" || msg.Status == "cancelled" {
					prefix = styleError.Render("✖ ")
				}
				mi.Buffer += prefix + msg.Output
			}
			// Update viewport
			mi.Viewport.SetContent(mi.Buffer)
			// Only auto-scroll if user hasn't manually scrolled up
			if !mi.UserScrolled {
				mi.Viewport.GotoBottom()
			}
		}

		// Remove cancel function
		delete(migrationCancels, msg.ID)

		// Clean up completed migrations from the order list if all are done
		allDone := true
		for _, id := range m.migrationOrder {
			if mi, ok := m.migrations[id]; ok && mi.Status == "running" {
				allDone = false
				break
			}
		}
		if allDone && len(m.migrationOrder) > 0 {
			// Keep the migrations visible but clear the order after a moment
			// For now, just leave them visible
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

	commands := []string{"/run", "/resume", "/validate", "/analyze", "/calibrate", "/status", "/history", "/wizard", "/logs", "/profile", "/clear", "/quit", "/help"}

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

	// Check if we have migrations to display
	if len(m.migrationOrder) > 0 {
		// Check if we also have wizard active
		if m.mode == modeWizard {
			return m.renderMigrationsWithWizard(suggestionsView)
		}
		return m.renderMultiMigrationView(suggestionsView)
	}

	// Normal single viewport view
	viewport := styleViewport.Width(m.viewport.Width + 2).Render(m.viewport.View())
	return fmt.Sprintf("%s\n%s\n%s%s",
		viewport,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

// renderSplitView renders a split screen with migration on top, wizard on bottom
func (m Model) renderSplitView(suggestionsView string) string {
	// Calculate heights for split view
	// Total available height = viewport height
	// Split: 60% for migration, 40% for wizard
	totalHeight := m.viewport.Height
	migrationHeight := (totalHeight * 60) / 100
	wizardHeight := totalHeight - migrationHeight - 1 // -1 for separator

	if migrationHeight < 5 {
		migrationHeight = 5
	}
	if wizardHeight < 5 {
		wizardHeight = 5
	}

	// Create a temporary copy of viewport heights for rendering
	// Render migration viewport (top)
	migrationContent := m.logBuffer
	if m.progressLine != "" {
		migrationContent += styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}

	// Style for the split panes (both with consistent padding)
	migrationStyle := lipgloss.NewStyle().
		Width(m.width-2).
		Height(migrationHeight).
		PaddingLeft(1).
		Border(lipgloss.RoundedBorder(), false, false, true, false). // Bottom border only
		BorderForeground(colorGray)

	wizardStyle := lipgloss.NewStyle().
		Width(m.width - 2).
		Height(wizardHeight).
		PaddingLeft(1)

	// Truncate content to fit heights
	migrationLines := strings.Split(migrationContent, "\n")
	if len(migrationLines) > migrationHeight {
		migrationLines = migrationLines[len(migrationLines)-migrationHeight:]
	}
	migrationView := migrationStyle.Render(strings.Join(migrationLines, "\n"))

	wizardLines := strings.Split(m.wizardBuffer, "\n")
	if len(wizardLines) > wizardHeight {
		wizardLines = wizardLines[len(wizardLines)-wizardHeight:]
	}
	wizardView := wizardStyle.Render(strings.Join(wizardLines, "\n"))

	return fmt.Sprintf("%s\n%s\n%s\n%s%s",
		migrationView,
		wizardView,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

// renderMultiMigrationView renders a full-screen tab interface for migrations
func (m Model) renderMultiMigrationView(suggestionsView string) string {
	numMigrations := len(m.migrationOrder)
	if numMigrations == 0 {
		return m.View() // Fallback to normal view
	}

	// Build tab bar - start with Console tab
	var tabs []string

	// Console tab (tab 0)
	consoleTabLabel := " 0:📋 Console "
	var consoleTabStyle lipgloss.Style
	if m.focusedMigration == "console" || m.focusedMigration == "" {
		consoleTabStyle = lipgloss.NewStyle().
			Background(colorPurple).
			Foreground(colorWhite).
			Bold(true).
			Padding(0, 1)
	} else {
		consoleTabStyle = lipgloss.NewStyle().
			Background(colorGray).
			Foreground(colorWhite).
			Padding(0, 1)
	}
	tabs = append(tabs, consoleTabStyle.Render(consoleTabLabel))

	// Migration tabs (tab 1, 2, 3, ...)
	for i, migrationID := range m.migrationOrder {
		mi, ok := m.migrations[migrationID]
		if !ok {
			continue
		}

		// Determine tab style based on focus and status
		isFocused := migrationID == m.focusedMigration
		var statusIcon string
		var tabStyle lipgloss.Style

		switch mi.Status {
		case "running":
			statusIcon = "►"
		case "completed":
			statusIcon = "✔"
		case "failed", "cancelled":
			statusIcon = "✖"
		}

		// Build tab label
		label := mi.ConfigFile
		if mi.ProfileName != "" {
			label = mi.ProfileName
		}
		// Truncate long labels
		if len(label) > 20 {
			label = label[:17] + "..."
		}
		tabLabel := fmt.Sprintf(" %d:%s %s ", i+1, statusIcon, label) // i+1 because console is tab 0

		if isFocused {
			// Active tab style
			tabStyle = lipgloss.NewStyle().
				Background(colorPurple).
				Foreground(colorWhite).
				Bold(true).
				Padding(0, 1)
		} else {
			// Inactive tab style - color based on status
			var bgColor lipgloss.Color
			switch mi.Status {
			case "completed":
				bgColor = colorGreen
			case "failed", "cancelled":
				bgColor = colorRed
			default:
				bgColor = colorGray
			}
			tabStyle = lipgloss.NewStyle().
				Background(bgColor).
				Foreground(colorWhite).
				Padding(0, 1)
		}

		tabs = append(tabs, tabStyle.Render(tabLabel))
	}

	// Build tab bar
	var tabBar string
	if len(tabs) == 0 {
		tabBar = lipgloss.NewStyle().
			Background(colorGray).
			Foreground(colorWhite).
			Padding(0, 1).
			Render(" No migrations ")
	} else {
		tabBar = lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
	}
	// Add a full-width separator line under the tabs
	separatorStyle := lipgloss.NewStyle().Foreground(colorGray)
	tabBarWithSeparator := tabBar + "\n" + separatorStyle.Render(strings.Repeat("─", m.width-2))

	// Check if console tab is focused
	var content string
	if m.focusedMigration == "console" || m.focusedMigration == "" {
		// Show console (main log buffer)
		reservedHeight := 10
		viewportHeight := m.height - reservedHeight
		if viewportHeight < 5 {
			viewportHeight = 5
		}

		// Update main viewport size if needed
		if m.viewport.Height != viewportHeight || m.viewport.Width != m.width-4 {
			m.viewport.Width = m.width - 4
			m.viewport.Height = viewportHeight
		}

		// Update content to include progress line
		consoleContent := m.logBuffer
		if m.progressLine != "" {
			consoleContent += styleSystemOutput.Render("  "+m.progressLine) + "\n"
		}
		m.viewport.SetContent(consoleContent)

		contentStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorPurple).
			Width(m.width - 2).
			Height(viewportHeight)

		content = contentStyle.Render(m.viewport.View())
	} else {
		// Get the focused migration
		mi, ok := m.migrations[m.focusedMigration]
		if !ok {
			// Fallback to first migration if focused one doesn't exist
			if len(m.migrationOrder) > 0 {
				m.focusedMigration = m.migrationOrder[0]
				mi, ok = m.migrations[m.focusedMigration]
			}
			if !ok {
				// Still not found, fallback to console
				m.focusedMigration = "console"
				return m.renderMultiMigrationView(suggestionsView)
			}
		}

		// Calculate viewport height (full screen minus tab bar, separator, input, status bar, borders)
		// Tab bar (1) + separator (1) + content border (2) + input box (3) + status bar (1) + padding (2) = 10
		reservedHeight := 10 + len(m.suggestions)
		viewportHeight := m.height - reservedHeight
		if viewportHeight < 5 {
			viewportHeight = 5
		}

		// Resize viewport if needed
		if mi.Viewport.Height != viewportHeight || mi.Viewport.Width != m.width-4 {
			mi.Viewport.Width = m.width - 4
			mi.Viewport.Height = viewportHeight
		}

		// Build scroll indicator
		scrollInfo := ""
		if mi.Viewport.TotalLineCount() > mi.Viewport.Height {
			scrollPct := int(mi.Viewport.ScrollPercent() * 100)
			scrollInfo = fmt.Sprintf(" [%d%%]", scrollPct)
		}

		// Build content border with title
		var borderColor lipgloss.Color
		switch mi.Status {
		case "running":
			borderColor = colorPurple
		case "completed":
			borderColor = colorGreen
		case "failed", "cancelled":
			borderColor = colorRed
		default:
			borderColor = colorGray
		}

		contentStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Width(m.width - 2).
			Height(viewportHeight)

		content = contentStyle.Render(mi.Viewport.View())

		// Add scroll info to the right side of the border
		if scrollInfo != "" {
			contentLines := strings.Split(content, "\n")
			if len(contentLines) > 0 {
				firstLine := contentLines[0]
				scrollStyled := lipgloss.NewStyle().Foreground(borderColor).Render(scrollInfo)
				// Insert scroll info near the end of the first line
				insertPos := len(firstLine) - len(scrollInfo) - 3
				if insertPos > 10 {
					contentLines[0] = firstLine[:insertPos] + scrollStyled + firstLine[insertPos+len(scrollInfo):]
				}
				content = strings.Join(contentLines, "\n")
			}
		}
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s%s",
		content,
		tabBarWithSeparator,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

// renderMigrationsWithWizard renders migrations and wizard in split view
func (m Model) renderMigrationsWithWizard(suggestionsView string) string {
	// For now, use a simple approach: migrations on top (70%), wizard on bottom (30%)
	numMigrations := len(m.migrationOrder)
	if numMigrations == 0 {
		return m.renderSplitView(suggestionsView)
	}

	reservedHeight := 4 + len(m.suggestions)
	availableHeight := m.height - reservedHeight

	// Split 70/30 between migrations and wizard
	migrationsHeight := (availableHeight * 70) / 100
	wizardHeight := availableHeight - migrationsHeight - 1

	heightPerMigration := migrationsHeight / numMigrations
	if heightPerMigration < 4 {
		heightPerMigration = 4
	}

	var panes []string
	for _, migrationID := range m.migrationOrder {
		mi, ok := m.migrations[migrationID]
		if !ok {
			continue
		}

		content := mi.Buffer
		if mi.ProgressLine != "" && mi.Status == "running" {
			content += styleSystemOutput.Render("  "+mi.ProgressLine) + "\n"
		}

		isFocused := migrationID == m.focusedMigration
		var borderColor lipgloss.Color
		var statusIcon string

		switch mi.Status {
		case "running":
			statusIcon = "►"
			if isFocused {
				borderColor = colorPurple
			} else {
				borderColor = colorGray
			}
		case "completed":
			statusIcon = "✔"
			borderColor = colorGreen
		default:
			statusIcon = "✖"
			borderColor = colorRed
		}

		label := mi.ConfigFile
		if mi.ProfileName != "" {
			label = mi.ProfileName
		}

		borderStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Width(m.width - 4).
			Height(heightPerMigration - 2).
			PaddingLeft(1)

		lines := strings.Split(content, "\n")
		maxLines := heightPerMigration - 3
		if len(lines) > maxLines {
			lines = lines[len(lines)-maxLines:]
		}

		pane := borderStyle.Render(strings.Join(lines, "\n"))
		paneLines := strings.Split(pane, "\n")
		if len(paneLines) > 0 {
			title := fmt.Sprintf(" %s [%s] %s ", statusIcon, migrationID, label)
			firstLine := paneLines[0]
			if len(firstLine) > len(title)+4 {
				paneLines[0] = firstLine[:2] + title + firstLine[2+len(title):]
			}
			pane = strings.Join(paneLines, "\n")
		}

		panes = append(panes, pane)
	}

	migrationsView := strings.Join(panes, "\n")

	// Wizard pane
	wizardStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(colorBlue).
		Width(m.width - 4).
		Height(wizardHeight - 2).
		PaddingLeft(1)

	wizardLines := strings.Split(m.wizardBuffer, "\n")
	if len(wizardLines) > wizardHeight-3 {
		wizardLines = wizardLines[len(wizardLines)-wizardHeight+3:]
	}
	wizardPane := wizardStyle.Render(strings.Join(wizardLines, "\n"))

	return fmt.Sprintf("%s\n%s\n%s\n%s%s",
		migrationsView,
		wizardPane,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

func (m Model) statusBarView() string {
	w := lipgloss.Width

	dir := styleStatusDir.Render(m.cwd)
	branch := styleStatusBranch.Render(" " + m.gitInfo.Branch)

	// Migration count indicator (only show if migrations exist)
	migCount := ""
	if len(m.migrationOrder) > 0 {
		migCount = styleStatusText.Render(fmt.Sprintf(" [%d migs] ", len(m.migrationOrder)))
	}

	status := ""
	if m.gitInfo.Status == "Dirty" {
		status = styleStatusDirty.Render("Uncommitted Changes")
	} else {
		status = styleStatusClean.Render("All Changes Committed")
	}

	// Calculate remaining width for spacer
	usedWidth := w(dir) + w(branch) + w(migCount) + w(status)
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
		migCount,
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
		m.logBuffer = m.welcomeMessage()
		m.viewport.SetContent(m.logBuffer)
		return nil

	case "/help":
		help := `Available Commands:
  /wizard               Launch the configuration wizard
  /run [config_file]    Start migration (default: config.yaml)
  /run --profile NAME   Start migration using a saved profile
  /resume [config_file] Resume an interrupted migration
  /resume --profile NAME Resume using a saved profile
  /validate             Validate migration
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
		err := os.WriteFile(logFile, []byte(m.logBuffer), 0644)
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
		m.mode = modeWizard
		m.step = stepSourceType
		m.textInput.Reset()
		m.textInput.Placeholder = ""

		// Parse wizard arguments - support --profile or config file
		configFile, profileName := parseConfigArgs(parts)
		m.wizardFile = configFile

		// Choose which buffer to use based on whether migration is running
		var headerMsg string
		var loaded bool

		// Try to load from profile first if specified
		if profileName != "" {
			if cfg, err := loadProfileConfig(profileName); err == nil {
				m.wizardData = *cfg
				m.wizardFile = profileName + ".yaml" // Will save to this file
				headerMsg = fmt.Sprintf("\n--- EDITING PROFILE: %s ---\n", profileName)
				loaded = true
			}
		}

		// If no profile or profile load failed, try config file
		if !loaded {
			if _, err := os.Stat(m.wizardFile); err == nil {
				if cfg, err := config.LoadWithOptions(m.wizardFile, config.LoadOptions{SuppressWarnings: true}); err == nil {
					m.wizardData = *cfg
					headerMsg = fmt.Sprintf("\n--- EDITING CONFIGURATION: %s ---\n", m.wizardFile)
					loaded = true
				}
			}
		}

		// If still not loaded, start fresh wizard
		if !loaded {
			headerMsg = fmt.Sprintf("\n--- CONFIGURATION WIZARD: %s ---\n", m.wizardFile)
		}

		// Display first prompt
		prompt := m.renderWizardPrompt()

		if m.hasRunningMigration() {
			// Use separate wizard buffer for split view
			m.wizardBuffer = headerMsg + prompt
			m.wizardViewport.SetContent(m.wizardBuffer)
			m.wizardViewport.GotoBottom()
		} else {
			// Use main buffer when no migration running
			m.logBuffer += headerMsg + prompt
			m.viewport.SetContent(m.logBuffer)
			m.viewport.GotoBottom()
		}
		return nil

	case "/run", "/resume":
		// These commands are handled earlier in Update() for multi-migration support
		// This case should never be reached, but we keep it as a safety fallback
		return func() tea.Msg {
			return OutputMsg("Error: migration command not properly routed\n")
		}

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

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// Wrappers for Orchestrator actions

// runMigrationCmdWithID runs a migration with output routed to a specific migration instance
func (m Model) runMigrationCmdWithID(configFile, profileName, migrationID, label string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: "Internal error: no program reference\n"}
		}

		// Helper to send output to this migration
		send := func(msg string) {
			p.Send(MigrationOutputMsg{ID: migrationID, Output: msg})
		}

		send(fmt.Sprintf("Starting migration [%s] with %s\n", migrationID, label))

		// Run migration asynchronously in a goroutine to avoid blocking the UI
		go func() {
			// Load config
			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				delete(migrationCancels, migrationID)
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Error loading config: %v\n", err)})
				return
			}

			// Create orchestrator
			orch, err := orchestrator.New(cfg)
			if err != nil {
				delete(migrationCancels, migrationID)
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Error initializing orchestrator: %v\n", err)})
				return
			}
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			// Create cancellable context
			ctx, cancel := context.WithCancel(context.Background())
			migrationCancels[migrationID] = cancel
			defer delete(migrationCancels, migrationID)

			// Redirect stdout/stderr and logging to this migration's output
			r, w, _ := os.Pipe()
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			// Reader goroutine
			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(MigrationOutputMsg{ID: migrationID, Output: string(buf[:n])})
					}
					if err != nil {
						break
					}
				}
			}()

			// Run migration
			runErr := orch.Run(ctx)

			// Restore stdout/stderr and logging
			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done // Wait for reader to finish

			if runErr != nil {
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Migration failed: %v\n", runErr)})
				return
			}
			p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "completed", Output: "Migration completed successfully!\n"})
		}()

		// Return immediately to keep UI responsive
		return nil
	}
}

// runResumeCmdWithID resumes a migration with output routed to a specific migration instance
func (m Model) runResumeCmdWithID(configFile, profileName, migrationID, label string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: "Internal error: no program reference\n"}
		}

		// Helper to send output to this migration
		send := func(msg string) {
			p.Send(MigrationOutputMsg{ID: migrationID, Output: msg})
		}

		send(fmt.Sprintf("Resuming migration [%s] with %s\n", migrationID, label))

		// Run resume asynchronously in a goroutine to avoid blocking the UI
		go func() {
			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				delete(migrationCancels, migrationID)
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Error loading config: %v\n", err)})
				return
			}

			orch, err := orchestrator.New(cfg)
			if err != nil {
				delete(migrationCancels, migrationID)
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Error initializing orchestrator: %v\n", err)})
				return
			}
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			// Create cancellable context
			ctx, cancel := context.WithCancel(context.Background())
			migrationCancels[migrationID] = cancel
			defer delete(migrationCancels, migrationID)

			// Redirect stdout/stderr and logging to this migration's output
			r, w, _ := os.Pipe()
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			// Reader goroutine
			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(MigrationOutputMsg{ID: migrationID, Output: string(buf[:n])})
					}
					if err != nil {
						break
					}
				}
			}()

			// Run resume
			runErr := orch.Resume(ctx)

			// Restore stdout/stderr and logging
			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done // Wait for reader to finish

			if runErr != nil {
				p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "failed", Output: fmt.Sprintf("Resume failed: %v\n", runErr)})
				return
			}
			p.Send(MigrationDoneWithIDMsg{ID: migrationID, Status: "completed", Output: "Resume completed successfully!\n"})
		}()

		// Return immediately to keep UI responsive
		return nil
	}
}

func (m Model) runValidateCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		// Run asynchronously to avoid blocking the UI
		go func() {
			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			out := fmt.Sprintf("Validating with %s\n", origin)
			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(out + fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(out + fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			if err := orch.Validate(context.Background()); err != nil {
				p.Send(OutputMsg(out + fmt.Sprintf("Validation failed: %v\n", err)))
				return
			}
			p.Send(OutputMsg(out + "Validation passed!\n"))
		}()

		return nil
	}
}

func (m Model) runAnalyzeCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		// Run asynchronously to avoid blocking the UI
		go func() {
			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			out := fmt.Sprintf("Analyzing with %s\n", origin)
			p.Send(OutputMsg(out))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			// Create orchestrator with both source and target for complete tuning analysis
			// Both databases must be available for analyze to work
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("❌ Error: Both source and target databases must be available for tuning analysis\n")))
				p.Send(OutputMsg(fmt.Sprintf("   %v\n", err)))
				return
			}
			defer orch.Close()

			// Get schema from config
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

			// Output suggestions in a box
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

		// Check if --apply is valid (requires file-based config, not profile)
		if applyToConfig && profileName != "" {
			return OutputMsg("Error: --apply requires a file-based config (not a profile)\n")
		}

		// Run asynchronously to avoid blocking the UI
		go func() {
			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			opts := fmt.Sprintf("sample-size=%d", sampleSize)
			if applyToConfig {
				opts += ", apply=true"
			}
			out := fmt.Sprintf("Starting calibration with %s (%s)\n", origin, opts)
			p.Send(OutputMsg(out))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			// Create orchestrator
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			// Redirect stdout/stderr for calibration output
			r, w, _ := os.Pipe()
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			// Reader goroutine
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

			// Restore stdout/stderr and logging
			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done // Wait for reader to finish

			if runErr != nil {
				p.Send(OutputMsg(fmt.Sprintf("Calibration failed: %v\n", runErr)))
				return
			}

			// Output the results table and YAML recommendation
			p.Send(BoxedOutputMsg(result.FormatResultsTable()))
			if result.Recommendation != nil {
				p.Send(OutputMsg("\nRecommended Configuration:\n"))
				p.Send(BoxedOutputMsg(result.FormatYAML()))
				if result.AIReasoning != "" {
					p.Send(OutputMsg(fmt.Sprintf("\nAI Reasoning: %s\n", result.AIReasoning)))
				}

				// Apply to config file if requested
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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

			// Capture stdout for ShowStatus or ShowDetailedStatus
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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
				if err != nil {
					p.Send(OutputMsg(fmt.Sprintf("Error showing run details: %v\n", err)))
					return
				}
			} else {
				output, err = CaptureToString(orch.ShowHistory)
				if err != nil {
					p.Send(OutputMsg(fmt.Sprintf("Error showing history: %v\n", err)))
					return
				}
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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

func (m Model) profileSaveCmd(name, configFile string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		// Run asynchronously to avoid blocking the UI
		go func() {
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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

		// Run asynchronously to avoid blocking the UI
		go func() {
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

func (m *Model) processWizardInput(input string) tea.Cmd {
	// Capture input for current step before moving to next
	switch m.step {
	case stepSourceType:
		if input != "" {
			m.wizardData.Source.Type = input
		}
		m.step = stepSourceHost
	case stepSourceHost:
		if input != "" {
			m.wizardData.Source.Host = input
		}
		m.step = stepSourcePort
	case stepSourcePort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Source.Port)
		}
		m.step = stepSourceDB
	case stepSourceDB:
		if input != "" {
			m.wizardData.Source.Database = input
		}
		m.step = stepSourceUser
	case stepSourceUser:
		if input != "" {
			m.wizardData.Source.User = input
		}
		m.step = stepSourcePass
	case stepSourcePass:
		if input != "" {
			m.wizardData.Source.Password = input
		}
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
		if input != "" {
			m.wizardData.Target.Type = input
		}
		m.step = stepTargetHost
	case stepTargetHost:
		if input != "" {
			m.wizardData.Target.Host = input
		}
		m.step = stepTargetPort
	case stepTargetPort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Target.Port)
		}
		m.step = stepTargetDB
	case stepTargetDB:
		if input != "" {
			m.wizardData.Target.Database = input
		}
		m.step = stepTargetUser
	case stepTargetUser:
		if input != "" {
			m.wizardData.Target.User = input
		}
		m.step = stepTargetPass
	case stepTargetPass:
		if input != "" {
			m.wizardData.Target.Password = input
		}
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
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Migration.Workers)
		}
		return m.finishWizard()
	}
	return nil
}

func (m *Model) renderWizardPrompt() string {
	var prompt string
	switch m.step {
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

func (m *Model) handleWizardStep(input string) tea.Cmd {
	// Helper to append to the correct buffer
	appendToBuffer := func(text string) {
		if m.hasRunningMigration() {
			m.wizardBuffer += text
			m.wizardViewport.SetContent(m.wizardBuffer)
			m.wizardViewport.GotoBottom()
		} else {
			m.logBuffer += text
			m.viewport.SetContent(m.logBuffer)
			m.viewport.GotoBottom()
		}
	}

	if input != "" {
		appendToBuffer(styleUserInput.Render("> "+input) + "\n")
		m.textInput.Reset()
	} else {
		// User accepted default
		appendToBuffer(styleUserInput.Render("  (default)") + "\n")
	}

	if cmd := m.processWizardInput(input); cmd != nil {
		return cmd
	}

	// Display prompt for current (new) step
	prompt := m.renderWizardPrompt()
	appendToBuffer(prompt)
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
	// Enable debug logging by default in TUI mode
	logging.SetLevel(logging.LevelDebug)

	m := InitialModel()
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	// Store program reference for migration commands
	SetProgramRef(p)

	// Start output capture for general (non-migration) output
	cleanup := CaptureOutput(p, "")
	defer cleanup()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}
