package tui

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/orchestrator"
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
}

// TickMsg is used to update the UI periodically (e.g. for git status)
type TickMsg time.Time

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
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			return m, tea.Quit
		case tea.KeyEnter:
			value := m.textInput.Value()
			if value != "" {
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
		}

	case tea.WindowSizeMsg:
		headerHeight := 0
		footerHeight := 2 // Input bar + Status bar
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width, msg.Height-verticalMarginHeight)
			m.viewport.YPosition = headerHeight
			m.viewport.SetContent(m.welcomeMessage())
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - verticalMarginHeight
		}
		m.width = msg.Width
		m.height = msg.Height
		m.textInput.Width = msg.Width - 4

	case OutputMsg:
		m.viewport.SetContent(m.viewport.View() + string(msg) + "\n")
		m.viewport.GotoBottom()

	case TickMsg:
		m.gitInfo = GetGitInfo()
		return m, tickCmd()
	}

	m.textInput, tiCmd = m.textInput.Update(msg)
	m.viewport, vpCmd = m.viewport.Update(msg)

	return m, tea.Batch(tiCmd, vpCmd)
}

// View renders the TUI
func (m Model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	return fmt.Sprintf("%s\n%s\n%s",
		m.viewport.View(),
		m.textInput.View(),
		m.statusBarView(),
	)
}

func (m Model) statusBarView() string {
	w := lipgloss.Width

	dir := styleStatusDir.Render(m.cwd)
	branch := styleStatusBranch.Render(" " + m.gitInfo.Branch)

	status := ""
	if m.gitInfo.Status == "Dirty" {
		status = styleStatusDirty.Render("Wait") // Dirty
	} else {
		status = styleStatusClean.Render("Ready") // Clean
	}

	// Calculate remaining width for spacer
	usedWidth := w(dir) + w(branch) + w(status)
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
   MSSQL-PG-MIGRATE
   Interative Shell
`
	help := `
   Type /help to see available commands.
   Type /run to start migration (uses config.yaml).
   Type /quit to exit.
`
	return styleTitle.Render(logo) + help
}

func (m Model) handleCommand(cmdStr string) tea.Cmd {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]

	switch cmd {
	case "/quit", "/exit":
		return tea.Quit

	case "/clear":
		m.viewport.SetContent(m.welcomeMessage())
		return nil

	case "/help":
		help := `
Available Commands:
  /run [config_file]    Start migration (default: config.yaml)
  /validate             Validate migration
  /status               Show migration status
  /history              Show migration history
  /clear                Clear screen
  /quit                 Exit application
`
		return func() tea.Msg { return OutputMsg(help) }

	case "/run":
		configFile := "config.yaml"
		if len(parts) > 1 {
			configFile = parts[1]
		}
		return m.runMigrationCmd(configFile)
	
	case "/validate":
		configFile := "config.yaml"
		if len(parts) > 1 {
			configFile = parts[1]
		}
		return m.runValidateCmd(configFile)

	case "/status":
		configFile := "config.yaml"
		if len(parts) > 1 {
			configFile = parts[1]
		}
		return m.runStatusCmd(configFile)
		
	case "/history":
		configFile := "config.yaml"
		if len(parts) > 1 {
			configFile = parts[1]
		}
		return m.runHistoryCmd(configFile)

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// Wrappers for Orchestrator actions

func (m Model) runMigrationCmd(configFile string) tea.Cmd {
	return func() tea.Msg {
		// Output to the view
		out := fmt.Sprintf("Running migration with config: %s\n", configFile)
		
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
		// This is tricky because ShowStatus prints to stdout directly.
		// Since we have hooked stdout in main.go, this should work automatically!
		if err := orch.ShowStatus(); err != nil {
			return OutputMsg(fmt.Sprintf("Error showing status: %v\n", err))
		}
		return nil
	}
}

func (m Model) runHistoryCmd(configFile string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load(configFile)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		orch, err := orchestrator.New(cfg)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error: %v\n", err))
		}
		defer orch.Close()

		if err := orch.ShowHistory(); err != nil {
			return OutputMsg(fmt.Sprintf("Error showing history: %v\n", err))
		}
		return nil
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
