package main

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/command"
	"github.com/urfave/cli/v2"
)

// TestCommandParityRegistry is the #438 enforcement: every CLI command
// and flag must have a registry entry with an explicit TUI disposition,
// and every registry entry must exist on the real CLI tree. New CLI
// surface cannot ship without declaring its TUI story; removed surface
// cannot leave stale registry entries behind.
func TestCommandParityRegistry(t *testing.T) {
	app := newApp()

	// Global flags ↔ registry.
	declaredGlobal := map[string]bool{}
	for _, f := range command.GlobalFlags {
		declaredGlobal[f.Name] = true
	}
	for _, f := range app.Flags {
		name := f.Names()[0]
		if name == "help" || name == "version" {
			continue
		}
		if !declaredGlobal[name] {
			t.Errorf("global flag --%s has no command.GlobalFlags entry — declare its TUI disposition (#438)", name)
		}
		delete(declaredGlobal, name)
	}
	for name := range declaredGlobal {
		t.Errorf("command.GlobalFlags declares --%s but the CLI no longer defines it", name)
	}

	// Commands and subcommands ↔ registry.
	seen := map[string]bool{}
	var walk func(prefix string, cmds []*cli.Command)
	walk = func(prefix string, cmds []*cli.Command) {
		for _, c := range cmds {
			if c.Name == "help" {
				continue
			}
			path := strings.TrimSpace(prefix + " " + c.Name)
			seen[path] = true
			spec, ok := command.Lookup(path)
			if !ok {
				t.Errorf("command %q has no registry entry — declare its TUI disposition (#438)", path)
				continue
			}
			if spec.Status == command.TUIPlanned && spec.Ref == "" {
				t.Errorf("command %q is planned without an issue ref", path)
			}
			for _, f := range c.Flags {
				name := f.Names()[0]
				if name == "help" {
					continue
				}
				if !spec.FlagRegistered(name) {
					t.Errorf("flag --%s on %q has no registry entry (#438)", name, path)
				}
			}
			walk(path, c.Subcommands)
		}
	}
	walk("", app.Commands)

	for _, spec := range append(append([]command.CommandSpec{}, command.Registry...), command.Subcommands...) {
		if !seen[spec.Path] {
			t.Errorf("registry declares %q but the CLI no longer defines it", spec.Path)
		}
	}
}
