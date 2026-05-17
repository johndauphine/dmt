package tui

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

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
	if m.progressLine != "" && m.mode != ModeWizard && m.mode != ModeSetup {
		content += styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}
	return content
}

// Start launches the TUI program
func Start() error {
	logging.SetLevel(logging.LevelInfo)

	m := InitialModel()
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	SetProgramRef(p)

	// Register diagnosis handler to format as BoxedOutputMsg
	driver.SetDiagnosisHandler(func(diagnosis *driver.ErrorDiagnosis) {
		p.Send(BoxedOutputMsg(diagnosis.Format()))
	})
	defer driver.SetDiagnosisHandler(nil) // Cleanup on exit

	cleanup := CaptureOutput(p)
	defer cleanup()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}
