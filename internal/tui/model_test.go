package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// Regression: a WindowSizeMsg with Height smaller than the footer (7 rows)
// used to produce a negative viewport height, then panic inside bubbles'
// visibleLines() when GotoBottom() ran on init. Codex flagged this on the
// auto-scroll fix; if it ever regresses we want the test to catch it
// before users hit the panic during a terminal resize.
// runCmd resolves a tea.Cmd to its OutputMsg payload, so tests can assert
// on the user-visible message without spinning up a full tea.Program.
func runCmd(t *testing.T, cmd tea.Cmd) string {
	t.Helper()
	if cmd == nil {
		return ""
	}
	msg := cmd()
	if out, ok := msg.(OutputMsg); ok {
		return string(out)
	}
	if boxed, ok := msg.(BoxedOutputMsg); ok {
		return string(boxed)
	}
	t.Fatalf("unexpected message type %T", msg)
	return ""
}

// #182: /explore on arms a one-shot flag; /explore <mode> sets the
// steady-state ε; /explore off clears both. Bare /explore reports state.
func TestExploreCommandParsing(t *testing.T) {
	m := InitialModel()

	out := runCmd(t, m.handleExploreCommand([]string{"/explore"}))
	if !strings.Contains(out, "next-run probe: off") {
		t.Fatalf("bare /explore should report off initially; got: %q", out)
	}

	out = runCmd(t, m.handleExploreCommand([]string{"/explore", "on"}))
	if !m.exploreArmed {
		t.Fatalf("/explore on should arm; got exploreArmed=%v", m.exploreArmed)
	}
	if !strings.Contains(out, "armed") {
		t.Fatalf("expected armed confirmation, got: %q", out)
	}

	out = runCmd(t, m.handleExploreCommand([]string{"/explore", "HIGH"}))
	if m.exploreMode != "high" {
		t.Fatalf("/explore HIGH (case-insensitive) should set high; got %q", m.exploreMode)
	}
	if !strings.Contains(out, "high") {
		t.Fatalf("expected mode confirmation, got: %q", out)
	}

	out = runCmd(t, m.handleExploreCommand([]string{"/explore"}))
	if !strings.Contains(out, "next-run probe: on") || !strings.Contains(out, "steady-state ε mode: high") {
		t.Fatalf("bare /explore after on+high should report both states; got: %q", out)
	}

	out = runCmd(t, m.handleExploreCommand([]string{"/explore", "off"}))
	if m.exploreArmed || m.exploreMode != "" {
		t.Fatalf("/explore off should clear both; got armed=%v mode=%q", m.exploreArmed, m.exploreMode)
	}
	if !strings.Contains(out, "disarmed") {
		t.Fatalf("expected disarm confirmation, got: %q", out)
	}

	out = runCmd(t, m.handleExploreCommand([]string{"/explore", "garbage"}))
	if !strings.Contains(out, "Unknown") {
		t.Fatalf("unknown arg should error; got: %q", out)
	}
}

func TestTinyTerminalDoesNotPanic(t *testing.T) {
	for _, size := range []struct{ w, h int }{
		{80, 5}, // h < footerHeight(7)
		{80, 1}, // pathological min
		{1, 1},  // both at minimum
		{80, 7}, // h == footerHeight (exact-zero viewport before clamp)
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
