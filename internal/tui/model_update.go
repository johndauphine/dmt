package tui

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

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
			// Cancel setup wizard
			if m.mode == ModeSetup {
				m.mode = ModeNormal
				m.setupState = nil
				m.appendOutput(styleSystemOutput.Render("Setup cancelled") + "\n")
				return m, nil
			}
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
			if m.mode == ModeSetup {
				m.mode = ModeNormal
				m.setupState = nil
				m.appendOutput(styleSystemOutput.Render("Setup cancelled") + "\n")
				return m, nil
			}
			if m.mode == ModeWizard {
				m.mode = ModeNormal
				m.appendOutput(styleSystemOutput.Render("Wizard cancelled") + "\n")
				return m, nil
			}
			return m, tea.Quit

		case tea.KeyEnter:
			value := m.textInput.Value()
			if m.mode == ModeSetup {
				// Ignore Enter during auto-action steps (connection tests, writes)
				if m.setupState != nil && m.setupState.Prompt().IsAutoAction {
					return m, nil
				}
				return m, safeCmd(m.handleSetupStep(value))
			}
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

		// Clamp width/height to a minimum of 1 so a tiny terminal never
		// produces a negative-sized viewport. The viewport's internal
		// visibleLines() does slice math on Height and panics with
		// "slice bounds out of range" on GotoBottom when Height < 0.
		vpWidth := msg.Width - 2
		if vpWidth < 1 {
			vpWidth = 1
		}
		vpHeight := msg.Height - verticalMarginHeight
		if vpHeight < 1 {
			vpHeight = 1
		}

		if !m.ready {
			m.viewport = viewport.New(vpWidth, vpHeight)
			m.viewport.YPosition = headerHeight
			m.content.WriteString(m.welcomeMessage())
			m.viewport.SetContent(m.content.String())
			// Start anchored at the bottom so AtBottom() returns true and
			// subsequent appendOutput auto-scrolls. Without this, the
			// welcome-message overflow leaves YOffset=0 for the whole
			// session and every new line silently appends off-screen
			// until the user manually scrolls down.
			m.viewport.GotoBottom()
			m.ready = true
		} else {
			// Preserve at-bottom state across resizes — a shrink can leave
			// YOffset stranded above the new max, which would otherwise
			// break auto-follow the same way as the init bug above.
			wasAtBottom := m.viewport.AtBottom()
			m.viewport.Width = vpWidth
			m.viewport.Height = vpHeight
			if wasAtBottom {
				m.viewport.GotoBottom()
			}
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

	case SetupConnTestMsg:
		// Verify step correlation - prevent stale messages from advancing wrong steps
		if m.setupState == nil || msg.Step != m.setupState.CurrentStep {
			break // stale message, ignore
		}
		if msg.Result.Connected {
			m.appendOutput(styleSuccess.Render(fmt.Sprintf("  Connected! (%dms)", msg.Result.LatencyMs)) + "\n")
			m.setupState.Process("")
		} else {
			m.appendOutput(styleError.Render(fmt.Sprintf("  Failed: %s (%dms)", msg.Result.Error, msg.Result.LatencyMs)) + "\n")
			m.setupState.Process(msg.Result.Error)
		}
		return m, safeCmd(m.processSetupAutoSteps())

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

	commands := []string{"/run", "/resume", "/validate", "/analyze", "/status", "/history", "/setup", "/wizard", "/logs", "/profile", "/verbosity", "/explore", "/clear", "/quit", "/help"}

	for _, cmd := range commands {
		if strings.HasPrefix(cmd, input) {
			m.textInput.SetValue(cmd)
			m.textInput.SetCursor(len(cmd))
			return
		}
	}
}
