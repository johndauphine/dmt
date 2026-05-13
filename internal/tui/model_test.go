package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// Regression: a WindowSizeMsg with Height smaller than the footer (7 rows)
// used to produce a negative viewport height, then panic inside bubbles'
// visibleLines() when GotoBottom() ran on init. Codex flagged this on the
// auto-scroll fix; if it ever regresses we want the test to catch it
// before users hit the panic during a terminal resize.
func TestTinyTerminalDoesNotPanic(t *testing.T) {
	for _, size := range []struct{ w, h int }{
		{80, 5},  // h < footerHeight(7)
		{80, 1},  // pathological min
		{1, 1},   // both at minimum
		{80, 7},  // h == footerHeight (exact-zero viewport before clamp)
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("WindowSizeMsg{%d,%d} panicked: %v", size.w, size.h, r)
				}
			}()
			m := InitialModel()
			m.Update(tea.WindowSizeMsg{Width: size.w, Height: size.h})
		})
	}
}
