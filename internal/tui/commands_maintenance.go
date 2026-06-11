package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/command"
)

// Maintenance commands (#443): secrets bootstrap and type-cache
// management — the CLI's init-secrets and cache clear, without shell
// escape. Both delegate to internal/command so the behavior and the
// wording match the CLI exactly.

// handleInitSecretsCommand implements /init-secrets [--with-ai] [--force].
func handleInitSecretsCommand(parts []string) tea.Cmd {
	pa, err := parseSlashArgs(argSpec{
		command: "/init-secrets",
		bools: map[string]string{
			"--with-ai": "with-ai",
			"--force":   "force",
			"-f":        "force",
		},
	}, parts)
	if err != nil {
		return errOutput(err)
	}
	out, err := command.InitSecrets(pa.bools["force"], pa.bools["with-ai"])
	if err != nil {
		return errOutput(err)
	}
	return func() tea.Msg { return BoxedOutputMsg(out) }
}

// handleCacheCommand implements /cache clear [--ai-only] [--confirm].
// Clearing is destructive (the cache holds resolved type mappings), so
// without --confirm it only states exactly what would be removed.
func handleCacheCommand(parts []string) tea.Cmd {
	if len(parts) < 2 || parts[1] != "clear" {
		return errOutput(fmt.Errorf("/cache: usage: /cache clear [--ai-only] [--confirm]"))
	}
	synthetic := append([]string{"/cache clear"}, parts[2:]...)
	pa, err := parseSlashArgs(argSpec{
		command: "/cache clear",
		bools: map[string]string{
			"--ai-only": "ai-only",
			"--confirm": "confirm",
		},
	}, synthetic)
	if err != nil {
		return errOutput(err)
	}
	aiOnly := pa.bools["ai-only"]
	if !pa.bools["confirm"] {
		return textOutput(fmt.Sprintf(
			"This will remove %s.\nRe-run as /cache clear %s--confirm to proceed.\n",
			command.CacheClearDescription(aiOnly),
			map[bool]string{true: "--ai-only ", false: ""}[aiOnly],
		))
	}
	out, err := command.ClearTypeCache(aiOnly)
	if err != nil {
		return errOutput(err)
	}
	return textOutput(out)
}
