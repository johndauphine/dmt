package tui

import (
	"fmt"
	"strings"
)

// Normalized slash-command argument parsing (#444). Every TUI command
// parses through parseSlashArgs so @config files, positionals, --flag
// VALUE / --flag=VALUE, boolean flags, unknown-flag errors, and
// missing-value errors behave identically across commands. Before this,
// each command had its own loop and an unknown flag silently became the
// config-file positional.

// argSpec declares one command's accepted flags. Keys are the flag
// spellings (including aliases like "-d"); values are the canonical
// name results are stored under.
type argSpec struct {
	command string            // for error messages, e.g. "/status"
	bools   map[string]string // flags that take no value
	strs    map[string]string // flags that take a value
}

// parsedArgs is the normalized result.
type parsedArgs struct {
	positionals []string
	strs        map[string]string
	bools       map[string]bool
}

// parseSlashArgs parses parts[1:] (parts[0] is the command itself).
// "@path" positionals have the "@" stripped — both "@cfg.yaml" and
// "cfg.yaml" mean the same config-file positional, as before.
func parseSlashArgs(spec argSpec, parts []string) (parsedArgs, error) {
	pa := parsedArgs{strs: map[string]string{}, bools: map[string]bool{}}
	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		if !strings.HasPrefix(arg, "-") {
			pa.positionals = append(pa.positionals, strings.TrimPrefix(arg, "@"))
			continue
		}
		name, inlineVal, hasInline := strings.Cut(arg, "=")
		if canonical, ok := spec.bools[name]; ok {
			if hasInline {
				return pa, fmt.Errorf("%s: flag %s does not take a value (see /help)", spec.command, name)
			}
			pa.bools[canonical] = true
			continue
		}
		if canonical, ok := spec.strs[name]; ok {
			if hasInline {
				pa.strs[canonical] = inlineVal
				continue
			}
			if i+1 >= len(parts) {
				return pa, fmt.Errorf("%s: flag %s requires a value (see /help)", spec.command, name)
			}
			i++
			pa.strs[canonical] = parts[i]
			continue
		}
		return pa, fmt.Errorf("%s: unknown flag %s (see /help)", spec.command, name)
	}
	return pa, nil
}

// originFlags are the flags every config-loading command shares.
func originFlags() map[string]string {
	return map[string]string{"--profile": "profile"}
}

// resolveOrigin applies the positional/flag → session-default → built-in
// precedence for the config origin (#444): an explicit positional config
// file or --profile wins; otherwise session defaults apply; otherwise
// the historical "config.yaml" default. A profile (explicit or session)
// suppresses the config-file default exactly as before.
func (m *Model) resolveOrigin(pa parsedArgs) (configFile, profileName string) {
	if len(pa.positionals) > 0 {
		configFile = pa.positionals[len(pa.positionals)-1]
	}
	profileName = pa.strs["profile"]
	if configFile == "" && profileName == "" {
		configFile = m.sessionGet("config")
		profileName = m.sessionGet("profile")
	}
	if configFile == "" {
		configFile = "config.yaml"
	}
	return configFile, profileName
}
