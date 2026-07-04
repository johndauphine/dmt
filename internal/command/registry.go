// Package command is the shared command vocabulary between the CLI and
// the TUI (#438, parent epic #437). The Registry is the single list of
// production commands and flags with an explicit TUI disposition; a
// parity test in cmd/migrate enumerates the real urfave/cli tree against
// it, so new CLI surface cannot ship without declaring its TUI story.
// Typed option structs (options.go) are the handler vocabulary the
// per-family parity issues (#439-#445) migrate onto.
package command

// TUIStatus declares how a command or flag is exposed in the TUI.
type TUIStatus string

const (
	// TUISupported: a slash command / TUI flag exists today.
	TUISupported TUIStatus = "supported"
	// TUIPlanned: parity is scheduled; Ref names the issue.
	TUIPlanned TUIStatus = "planned"
	// TUICLIOnly: deliberately CLI-only; Ref states why.
	TUICLIOnly TUIStatus = "cli-only"
)

// FlagSpec declares one flag's TUI disposition. Status/Ref default to
// the owning command's when empty.
type FlagSpec struct {
	Name   string
	Status TUIStatus
	Ref    string
}

// CommandSpec declares one command (or "parent sub" subcommand path).
type CommandSpec struct {
	// Path is the space-joined command path, e.g. "run" or "profile save".
	Path   string
	Status TUIStatus
	// Ref is the issue (planned) or reason (cli-only) backing the status.
	Ref   string
	Flags []FlagSpec
}

// flags is shorthand for a list of plain flag specs inheriting the
// command's disposition.
func flags(names ...string) []FlagSpec {
	out := make([]FlagSpec, len(names))
	for i, n := range names {
		out[i] = FlagSpec{Name: n}
	}
	return out
}

// GlobalFlags declares the app-level flags. Origin/state flags gained
// session-default ergonomics with #444, observability/audit flags with
// #445. JSON/file output modes are deliberately CLI-only: the TUI
// renders structured blocks interactively, and /logs saves the session
// transcript — automation belongs to the CLI.
var GlobalFlags = []FlagSpec{
	{Name: "config", Status: TUISupported}, // @config / positional in TUI
	{Name: "profile", Status: TUISupported},
	{Name: "state-file", Status: TUISupported}, // /session state-file
	{Name: "run-id", Status: TUICLIOnly, Ref: "/diagnose --run covers the TUI need; explicit run IDs are an automation concern"},
	{Name: "output-json", Status: TUICLIOnly, Ref: "automation output; TUI renders structured blocks"},
	{Name: "output-file", Status: TUICLIOnly, Ref: "automation output; /logs saves the session transcript"},
	{Name: "log-format", Status: TUISupported}, // /session log-format
	{Name: "verbosity", Status: TUISupported},  // /verbosity
	{Name: "shutdown-timeout", Status: TUICLIOnly, Ref: "TUI cancels interactively via Ctrl+C"},
	{Name: "progress", Status: TUICLIOnly, Ref: "TUI renders its own progress"},
	{Name: "progress-interval", Status: TUICLIOnly, Ref: "TUI renders its own progress"},
	{Name: "metrics-addr", Status: TUISupported},         // /session metrics-addr
	{Name: "otel-endpoint", Status: TUISupported},        // /session otel-endpoint
	{Name: "audit-dir", Status: TUISupported},            // /session audit-dir
	{Name: "audit-tamper-evident", Status: TUISupported}, // /session audit-tamper-evident
	{Name: "no-audit", Status: TUISupported},             // /session no-audit
	// WebUI launch/config flags (#578). These select and configure a
	// separate front-end, not a TUI slash command, so they are CLI-only
	// with respect to the TUI. The WebUI's own command parity is tracked
	// against the registry separately (#583).
	{Name: "webui", Status: TUICLIOnly, Ref: "launches the WebUI front-end; not a TUI command (#578)"},
	{Name: "webui-addr", Status: TUICLIOnly, Ref: "WebUI bind address (#578)"},
	{Name: "webui-auth-token", Status: TUICLIOnly, Ref: "WebUI auth secret (#578)"},
	{Name: "webui-tls-cert", Status: TUICLIOnly, Ref: "WebUI TLS certificate (#578)"},
	{Name: "webui-tls-key", Status: TUICLIOnly, Ref: "WebUI TLS key (#578)"},
	{Name: "webui-insecure", Status: TUICLIOnly, Ref: "WebUI plaintext remote-bind opt-out (#578)"},
}

// Registry is the authoritative command/flag parity list.
var Registry = []CommandSpec{
	{Path: "run", Status: TUISupported, Flags: []FlagSpec{
		{Name: "source-schema"}, {Name: "target-schema"}, {Name: "workers"},
		{Name: "dry-run", Status: TUISupported},           // /run --dry-run
		{Name: "ai-schema-advisor", Status: TUISupported}, // /run --ai-schema-advisor
		{Name: "explore", Status: TUISupported},           // /explore
		{Name: "skip-preflight", Status: TUISupported},    // /run --skip-preflight
		{Name: "confirm-backup", Status: TUISupported, Ref: "/run --confirm-backup"},
	}},
	{Path: "resume", Status: TUISupported, Flags: []FlagSpec{
		{Name: "force-resume", Status: TUISupported},   // /resume --force-resume
		{Name: "skip-preflight", Status: TUISupported}, // /resume --skip-preflight
	}},
	{Path: "status", Status: TUISupported, Flags: flags("json")},
	{Path: "validate", Status: TUISupported, Flags: []FlagSpec{
		{Name: "ai-triage", Status: TUISupported}, // /validate --ai-triage
		{Name: "timeout"},
		{Name: "json", Status: TUICLIOnly, Ref: "TUI renders the triage block; use the CLI for JSON"},
	}},
	{Path: "diagnose", Status: TUISupported, Flags: []FlagSpec{ // /diagnose
		{Name: "run"}, {Name: "ai-triage"}, {Name: "timeout"},
		{Name: "json", Status: TUICLIOnly, Ref: "TUI renders the triage block; use the CLI for JSON"},
	}},
	{Path: "history", Status: TUISupported, Flags: flags("run")},
	{Path: "profile", Status: TUISupported},
	{Path: "preflight", Status: TUISupported, // /preflight, /health-check
		Flags: flags("skip-preflight", "ai-review")},
	{Path: "analyze", Status: TUISupported, Flags: []FlagSpec{
		{Name: "apply"},
		{Name: "ai-explain", Status: TUISupported}, // /analyze --ai-explain
	}},
	{Path: "ai", Status: TUISupported}, // /ai config-review | runbook
	{Path: "init", Status: TUICLIOnly, Ref: "/wizard and /setup cover interactive init",
		Flags: flags("output", "advanced", "force")},
	{Path: "init-secrets", Status: TUISupported, // /init-secrets
		Flags: flags("force", "with-ai")},
	{Path: "setup", Status: TUISupported, Flags: flags("output", "force")},
	{Path: "cache", Status: TUISupported}, // /cache clear
}

// Subcommands carries the second-level paths; same enforcement.
var Subcommands = []CommandSpec{
	{Path: "profile save", Status: TUISupported, Flags: flags("name")},
	{Path: "profile list", Status: TUISupported},
	{Path: "profile delete", Status: TUISupported, Flags: flags("name")},
	{Path: "profile export", Status: TUISupported, Flags: flags("name", "out")},
	{Path: "ai config-review", Status: TUISupported, Flags: []FlagSpec{ // /ai config-review
		{Name: "request"}, {Name: "timeout"},
		{Name: "state-file"}, // /session state-file
		{Name: "json", Status: TUICLIOnly, Ref: "TUI renders the review block; use the CLI for JSON"},
		{Name: "output-json", Status: TUICLIOnly, Ref: "TUI renders the review block; use the CLI for JSON"},
		{Name: "output-file", Status: TUICLIOnly, Ref: "TUI renders the review block; use the CLI for JSON"},
	}},
	{Path: "ai evals", Status: TUICLIOnly, Ref: "developer/eval harness, no operator workflow",
		Flags: flags("live", "list", "scenario", "timeout", "output-file")},
	{Path: "cache clear", Status: TUISupported, Flags: flags("ai-only")}, // /cache clear --ai-only
}

// Lookup returns the spec for a command path, checking Registry then
// Subcommands. ok=false means the command is unregistered — the parity
// test treats that as a failure.
func Lookup(path string) (CommandSpec, bool) {
	for _, c := range Registry {
		if c.Path == path {
			return c, true
		}
	}
	for _, c := range Subcommands {
		if c.Path == path {
			return c, true
		}
	}
	return CommandSpec{}, false
}

// FlagRegistered reports whether the named flag is declared on the spec.
func (c CommandSpec) FlagRegistered(name string) bool {
	for _, f := range c.Flags {
		if f.Name == name {
			return true
		}
	}
	return false
}
