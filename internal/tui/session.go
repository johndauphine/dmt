package tui

import (
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/logging"
)

// Session defaults (#444): sticky per-session values for options that
// are awkward to repeat on every slash command. Commands fall back to
// these when no explicit argument is given. #445 adds the
// observability/output keys to the same table.
var sessionKeys = map[string]string{
	"config":     "default config file for commands that load one",
	"profile":    "default saved profile (overrides config default)",
	"state-file": "YAML state file override (Airflow-style backend, like --state-file)",
	"verbosity":  "log level (applies immediately: debug|info|warn|error)",
}

func (m *Model) sessionGet(key string) string {
	if m.session == nil {
		return ""
	}
	return m.session[key]
}

// handleSessionCommand implements /session [KEY VALUE] | [clear [KEY]].
func (m *Model) handleSessionCommand(parts []string) tea.Cmd {
	if len(parts) == 1 {
		return func() tea.Msg { return BoxedOutputMsg(m.sessionSummary()) }
	}
	if parts[1] == "clear" {
		if len(parts) >= 3 {
			key := parts[2]
			if _, ok := sessionKeys[key]; !ok {
				return errOutput(fmt.Errorf("/session: unknown key %q (see /session)", key))
			}
			delete(m.session, key)
			return textOutput(fmt.Sprintf("Session default %q cleared\n", key))
		}
		m.session = nil
		return textOutput("All session defaults cleared\n")
	}
	if len(parts) < 3 {
		return errOutput(fmt.Errorf("/session: usage: /session [KEY VALUE] or /session clear [KEY]"))
	}
	key, value := parts[1], strings.Join(parts[2:], " ")
	if _, ok := sessionKeys[key]; !ok {
		return errOutput(fmt.Errorf("/session: unknown key %q (see /session)", key))
	}
	if key == "verbosity" {
		level, err := logging.ParseLevel(value)
		if err != nil {
			return errOutput(fmt.Errorf("/session: invalid log level %q (debug|info|warn|error)", value))
		}
		logging.SetLevel(level)
	}
	if m.session == nil {
		m.session = map[string]string{}
	}
	m.session[key] = value
	return textOutput(fmt.Sprintf("Session default set: %s = %s\n", key, value))
}

func (m *Model) sessionSummary() string {
	var b strings.Builder
	b.WriteString("Session defaults (/session KEY VALUE to set, /session clear [KEY] to unset):\n")
	keys := make([]string, 0, len(sessionKeys))
	for k := range sessionKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		val := m.sessionGet(k)
		if val == "" {
			val = "(unset)"
		}
		fmt.Fprintf(&b, "  %-11s %-24s %s\n", k, val, sessionKeys[k])
	}
	return b.String()
}

func textOutput(s string) tea.Cmd {
	return func() tea.Msg { return OutputMsg(s) }
}

func errOutput(err error) tea.Cmd {
	return func() tea.Msg { return OutputMsg("Error: " + err.Error() + "\n") }
}
