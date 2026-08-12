package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/johndauphine/dmt/v5/internal/version"
)

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
	case ModeSetup:
		modeText = styleStatusText.Render(" [setup] ")
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
  Data Migration Tool %s
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
