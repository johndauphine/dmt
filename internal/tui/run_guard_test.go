package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// TestRunGuardRejectsConcurrentRun pins #557: /run must mark the migration
// running synchronously so a second /run or /resume issued before the async
// migrationStartedMsg round-trips (while connections are still dialing) is
// rejected, rather than starting a second concurrent migration.
func TestRunGuardRejectsConcurrentRun(t *testing.T) {
	m := &Model{}

	// First /run: parses OK and marks running synchronously. The returned
	// command (which would dial DBs) is intentionally not executed.
	if cmd := m.handleCommand("/run config.yaml"); cmd == nil {
		t.Fatal("first /run should return a command")
	}
	if m.migrationStatus != "running" {
		t.Fatalf("first /run must set migrationStatus=running synchronously, got %q", m.migrationStatus)
	}

	// Second /run BEFORE any migrationStartedMsg → must be rejected.
	assertAlreadyRunning(t, "/run", m.handleCommand("/run config.yaml"))
	// /resume while running → also rejected.
	assertAlreadyRunning(t, "/resume", m.handleCommand("/resume"))
}

// TestRunGuardDryRunDoesNotMarkRunning pins that a dry-run never enters the
// running state — it sends neither migrationStartedMsg nor MigrationDoneMsg, so
// marking it running would stick forever and block all future runs (#557).
func TestRunGuardDryRunDoesNotMarkRunning(t *testing.T) {
	m := &Model{}
	m.handleCommand("/run --dry-run config.yaml")
	if m.migrationStatus == "running" {
		t.Fatalf("dry-run must not set migrationStatus=running (would stick forever), got %q", m.migrationStatus)
	}
	// A real /run afterward must still be allowed.
	if cmd := m.handleCommand("/run config.yaml"); cmd != nil {
		msg := cmd()
		if out, ok := msg.(OutputMsg); ok && strings.Contains(string(out), "already running") {
			t.Fatal("a real /run after a dry-run must be allowed, but was rejected as already running")
		}
	}
}

// TestMigrationDoneResetsRunningGuard pins the reset half of #557: an
// early-start failure (or completion) sends MigrationDoneMsg, which must clear
// migrationStatus so future runs aren't permanently locked out.
func TestMigrationDoneResetsRunningGuard(t *testing.T) {
	m := InitialModel()
	m.migrationStatus = "running"

	newModel, _ := m.Update(MigrationDoneMsg{Status: "failed", Message: "early start failure"})
	if got := newModel.(Model).migrationStatus; got == "running" {
		t.Fatalf("MigrationDoneMsg must reset migrationStatus (still %q) so future /run is allowed", got)
	}
}

func assertAlreadyRunning(t *testing.T, cmdName string, cmd tea.Cmd) {
	t.Helper()
	if cmd == nil {
		t.Fatalf("%s while running should return a rejection command", cmdName)
	}
	msg := cmd()
	out, ok := msg.(OutputMsg)
	if !ok {
		t.Fatalf("%s while running should return an OutputMsg rejection, got %T", cmdName, msg)
	}
	if !strings.Contains(string(out), "already running") {
		t.Fatalf("%s rejection = %q, want it to mention 'already running'", cmdName, string(out))
	}
}
