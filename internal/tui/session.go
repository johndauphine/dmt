package tui

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/logging"
)

// Session defaults (#444): sticky per-session values for options that
// are awkward to repeat on every slash command. Commands fall back to
// these when no explicit argument is given. The observability and
// audit keys (#445) are read by /run and /resume at start time, like
// the equivalent CLI flags.
var sessionKeys = map[string]string{
	"config":               "default config file for commands that load one",
	"profile":              "default saved profile (overrides config default)",
	"state-file":           "YAML state file override (Airflow-style backend, like --state-file)",
	"verbosity":            "log level (applies immediately: debug|info|warn|error)",
	"log-format":           "log format (applies immediately: text|json)",
	"metrics-addr":         "Prometheus /metrics listen address for runs (e.g. :9090)",
	"otel-endpoint":        "OTLP trace exporter endpoint for runs",
	"audit-dir":            "audit log directory for runs (like --audit-dir)",
	"audit-tamper-evident": "hash-chain the audit log (true|false)",
	"no-audit":             "disable the audit log for runs (true|false)",
}

// sessionValidators reject bad values at /session set time so a typo
// surfaces immediately, not minutes into a run. Validators with side
// effects (verbosity, log-format) also apply the value.
var sessionValidators = map[string]func(value string) error{
	"verbosity": func(value string) error {
		level, err := logging.ParseLevel(value)
		if err != nil {
			return fmt.Errorf("invalid log level %q (debug|info|warn|error)", value)
		}
		logging.SetLevel(level)
		return nil
	},
	"log-format": func(value string) error {
		if value != "text" && value != "json" {
			return fmt.Errorf("invalid log format %q (text|json)", value)
		}
		logging.SetFormat(value)
		return nil
	},
	"audit-tamper-evident": validateBoolValue,
	"no-audit":             validateBoolValue,
	"metrics-addr": func(value string) error {
		if _, _, err := net.SplitHostPort(value); err != nil {
			return fmt.Errorf("invalid metrics address %q (host:port or :port, e.g. :9090)", value)
		}
		return nil
	},
	"otel-endpoint": func(value string) error {
		// Exactly the shape check SetupTracer applies at run start, so
		// a bad endpoint fails here instead of silently disabling
		// tracing. Note a bare "host:port" parses with an empty Host —
		// the scheme is required, matching the runtime check.
		u, err := url.Parse(value)
		if err != nil || u.Host == "" {
			return fmt.Errorf("invalid OTLP endpoint %q (use a URL like http://host:4318)", value)
		}
		return nil
	},
}

func validateBoolValue(value string) error {
	if value != "true" && value != "false" {
		return fmt.Errorf("invalid value %q (true|false)", value)
	}
	return nil
}

func (m *Model) sessionGet(key string) string {
	if m.session == nil {
		return ""
	}
	return m.session[key]
}

// sessionGetBool reads a true|false session key; absent means false.
func (m *Model) sessionGetBool(key string) bool {
	return m.sessionGet(key) == "true"
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
	if validate, ok := sessionValidators[key]; ok {
		if err := validate(value); err != nil {
			return errOutput(fmt.Errorf("/session: %w", err))
		}
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
		fmt.Fprintf(&b, "  %-21s %-20s %s\n", k, val, sessionKeys[k])
	}
	return b.String()
}

// runSessionSettings is the snapshot of run-relevant session keys,
// captured synchronously before the run goroutine starts (#444 race
// rule). applyToConfig mirrors the CLI's IsSet-gated overrides.
type runSessionSettings struct {
	stateFile          string
	metricsAddr        string
	otelEndpoint       string
	auditDir           string
	auditTamperEvident bool
	auditTamperSet     bool
	noAudit            bool
	noAuditSet         bool
}

func (m *Model) captureRunSessionSettings() runSessionSettings {
	return runSessionSettings{
		stateFile:          m.sessionGet("state-file"),
		metricsAddr:        m.sessionGet("metrics-addr"),
		otelEndpoint:       m.sessionGet("otel-endpoint"),
		auditDir:           m.sessionGet("audit-dir"),
		auditTamperEvident: m.sessionGetBool("audit-tamper-evident"),
		auditTamperSet:     m.sessionGet("audit-tamper-evident") != "",
		noAudit:            m.sessionGetBool("no-audit"),
		noAuditSet:         m.sessionGet("no-audit") != "",
	}
}

func textOutput(s string) tea.Cmd {
	return func() tea.Msg { return OutputMsg(s) }
}

func errOutput(err error) tea.Cmd {
	return func() tea.Msg { return OutputMsg("Error: " + err.Error() + "\n") }
}
