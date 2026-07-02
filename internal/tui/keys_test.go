package tui

import (
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// TestTickDoesNotFetchGitInline pins #559: the 5s tick must not run the blocking
// GetGitInfo() git subprocesses inline in Update() (which freezes the event
// loop). It defers the fetch to a command and only stores the result when a
// gitInfoMsg arrives.
func TestTickDoesNotFetchGitInline(t *testing.T) {
	m := InitialModel()
	sentinel := GitInfo{Branch: "SENTINEL-BRANCH", Status: "SENTINEL"}
	m.gitInfo = sentinel

	newModel, cmd := m.Update(TickMsg(time.Now()))
	m2 := newModel.(Model)

	if m2.gitInfo != sentinel {
		t.Fatalf("TickMsg fetched git inline (gitInfo changed to %+v); it must run in a command", m2.gitInfo)
	}
	if cmd == nil {
		t.Fatal("TickMsg must return a command (next tick + async git fetch)")
	}
}

// TestGitInfoMsgStoresResult confirms the deferred fetch's result is stored.
func TestGitInfoMsgStoresResult(t *testing.T) {
	m := InitialModel()
	newModel, _ := m.Update(gitInfoMsg(GitInfo{Branch: "feature-x", Status: "Dirty"}))
	m2 := newModel.(Model)
	if m2.gitInfo.Branch != "feature-x" || m2.gitInfo.Status != "Dirty" {
		t.Fatalf("gitInfoMsg result not stored: %+v", m2.gitInfo)
	}
}

// TestEscCancelsRunningMigration pins #558: Esc during a running migration must
// cancel it gracefully (context cancel + checkpoint flush), like Ctrl+C, not
// quit the process mid-flight.
func TestEscCancelsRunningMigration(t *testing.T) {
	m := InitialModel()
	m.mode = ModeMigration
	m.migrationStatus = "running"
	cancelled := false
	m.migrationCancel = func() { cancelled = true }

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})

	if !cancelled {
		t.Fatal("Esc during a running migration must call migrationCancel")
	}
	if cmd != nil {
		t.Fatalf("Esc during a running migration must not quit (want nil cmd, got a command that returns %T)", cmd())
	}
}

// TestEscQuitsWhenIdle confirms Esc still quits the TUI when nothing is running.
func TestEscQuitsWhenIdle(t *testing.T) {
	m := InitialModel() // ModeNormal, no migration

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})

	if cmd == nil {
		t.Fatal("Esc when idle should quit")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("Esc when idle should return tea.Quit, got %T", cmd())
	}
}
