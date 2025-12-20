package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	colorPurple    = lipgloss.Color("#7D56F4")
	colorDarkPurple = lipgloss.Color("#5a3eaf")
	colorGreen     = lipgloss.Color("#04B575")
	colorRed       = lipgloss.Color("#FF4141")
	colorGray      = lipgloss.Color("#626262")
	colorLightGray = lipgloss.Color("#9e9e9e")
	colorWhite     = lipgloss.Color("#FFFFFF")
	colorBlue      = lipgloss.Color("#007BFF")

	// Base Styles
	styleNormal = lipgloss.NewStyle().Foreground(colorWhite)

	// Status Bar Styles
	styleStatusBar = lipgloss.NewStyle().
			Height(1).
			Foreground(colorWhite)

	styleStatusDir = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorBlue).
			Padding(0, 1).
			Bold(true)

	styleStatusBranch = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorPurple).
			Padding(0, 1)

	styleStatusClean = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorGreen).
			Padding(0, 1)

	styleStatusDirty = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorRed).
			Padding(0, 1)

	styleStatusText = lipgloss.NewStyle().
			Foreground(colorWhite).
			Background(colorGray).
			Padding(0, 1)

	// Viewport Styles
	styleViewport = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorPurple).
			Padding(0, 1)
	
	styleTitle = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true).
			MarginBottom(1)
	
	stylePrompt = lipgloss.NewStyle().
			Foreground(colorPurple).
			Bold(true)
)
