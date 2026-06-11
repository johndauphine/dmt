package tui

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/command"
)

// slashFor maps a parity-registry command path to the slash command(s)
// that implement it in the TUI. A TUISupported path with no mapping
// fails TestTUICommandSurface — the registry and this table must move
// together.
var slashFor = map[string][]string{
	"run":              {"/run"},
	"resume":           {"/resume"},
	"status":           {"/status"},
	"validate":         {"/validate"},
	"diagnose":         {"/diagnose"},
	"history":          {"/history"},
	"profile":          {"/profile"},
	"preflight":        {"/preflight", "/health-check"},
	"analyze":          {"/analyze"},
	"ai":               {"/ai"},
	"init-secrets":     {"/init-secrets"},
	"setup":            {"/setup"},
	"cache":            {"/cache"},
	"cache clear":      {"/cache"},
	"ai config-review": {"/ai"},
	"profile save":     {"/profile"},
	"profile list":     {"/profile"},
	"profile delete":   {"/profile"},
	"profile export":   {"/profile"},
}

// TestTUICommandSurface is the TUI-side half of the #438 parity
// enforcement (#446): every command the registry declares TUISupported
// must be discoverable — present in the autocomplete list and in the
// /help text. The cmd/migrate parity test enforces the CLI side; this
// one stops the TUI from silently dropping a surface the registry
// still advertises.
func TestTUICommandSurface(t *testing.T) {
	autocomplete := map[string]bool{}
	for _, c := range availableCommands {
		autocomplete[c.Name] = true
	}
	help := helpText(t)

	all := append(append([]command.CommandSpec{}, command.Registry...), command.Subcommands...)
	for _, spec := range all {
		if spec.Status != command.TUISupported {
			continue
		}
		names, ok := slashFor[spec.Path]
		if !ok {
			t.Errorf("registry path %q is TUISupported but has no slash-command mapping in slashFor", spec.Path)
			continue
		}
		for _, name := range names {
			if !autocomplete[name] {
				t.Errorf("%s (registry %q) missing from availableCommands autocomplete", name, spec.Path)
			}
			if !strings.Contains(help, name) {
				t.Errorf("%s (registry %q) missing from /help text", name, spec.Path)
			}
		}
	}
}

// TestAutocompleteEntriesAreDispatched: every advertised command must
// be handled — typing a suggested command can never hit the
// unknown-command branch.
func TestAutocompleteEntriesAreDispatched(t *testing.T) {
	// dispatchable mirrors handleCommand's switch labels. Kept as an
	// explicit list (not reflection) so adding an autocomplete entry
	// without a dispatch case fails here.
	dispatched := map[string]bool{}
	for _, name := range []string{
		"/quit", "/exit", "/clear", "/help", "/session", "/init-secrets",
		"/cache", "/logs", "/verbosity", "/explore", "/about", "/setup",
		"/wizard", "/run", "/resume", "/preflight", "/health-check",
		"/validate", "/diagnose", "/status", "/history", "/profile",
		"/analyze", "/ai", "/config",
	} {
		dispatched[name] = true
	}
	for _, c := range availableCommands {
		if !dispatched[c.Name] {
			t.Errorf("autocomplete advertises %s but handleCommand has no case for it", c.Name)
		}
	}
}

// helpText renders the /help output for inspection.
func helpText(t *testing.T) string {
	t.Helper()
	m := &Model{}
	return runCmd(t, m.handleCommand("/help"))
}
