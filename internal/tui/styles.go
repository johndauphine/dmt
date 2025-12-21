package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	colorBackground = lipgloss.Color("")
	colorSurface    = lipgloss.Color("")
	colorPurple     = lipgloss.Color("#B08CFF")
	colorTeal       = lipgloss.Color("#8FE3FF")
	colorGold       = lipgloss.Color("#E9C07A")
	colorGreen      = lipgloss.Color("#39D98A")
	colorRed        = lipgloss.Color("#FF6B6B")
	colorGray       = lipgloss.Color("#5D6581")
	colorLightGray  = lipgloss.Color("#A8B0C9")
	colorWhite      = lipgloss.Color("#E6E9F5")

	// Base Styles
	styleNormal = lipgloss.NewStyle().
			Foreground(colorWhite)

	// Status Bar Styles
	styleStatusBar = lipgloss.NewStyle().
			Height(1).
			Foreground(colorLightGray)

	styleStatusDir = lipgloss.NewStyle().
			Foreground(colorBackground).
			Background(colorTeal).
			Padding(0, 1).
			Bold(true)

	styleStatusBranch = lipgloss.NewStyle().
				Foreground(colorWhite).
				Background(colorPurple).
				Padding(0, 1)

	styleStatusClean = lipgloss.NewStyle().
				Foreground(colorBackground).
				Background(colorGreen).
				Padding(0, 1)

	styleStatusDirty = lipgloss.NewStyle().
				Foreground(colorBackground).
				Background(colorRed).
				Padding(0, 1)

	styleStatusText = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorGray).
			Padding(0, 1)

	// Viewport Styles
	styleViewport = lipgloss.NewStyle().
			Foreground(colorWhite).
			Padding(0, 1)

	styleTitle = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true).
			MarginBottom(1)

	stylePrompt = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true)

	styleError = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	styleSuccess = lipgloss.NewStyle().
			Foreground(colorGreen).
			Bold(true)

	styleUserInput = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true)

	styleSystemOutput = lipgloss.NewStyle().
				Foreground(colorLightGray)

	styleInputContainer = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorGold).
				Foreground(colorWhite).
				Padding(0, 1)

	styleScrollbar = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), false, false, false, true). // Left border only
			BorderForeground(colorGray).
			Foreground(colorGray)

	styleScrollbarHandle = lipgloss.NewStyle().
				Foreground(colorPurple)

	// Command output box style - subtle left border to indicate command output
	styleOutputBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorPurple).
			Padding(0, 1).
			MarginTop(1).
			MarginBottom(1)
)
