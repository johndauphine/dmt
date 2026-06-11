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

// GlobalFlags declares the app-level flags. Session-default ergonomics
// for these land with #444; output/observability flags with #445.
var GlobalFlags = []FlagSpec{
	{Name: "config", Status: TUISupported}, // @config / positional in TUI
	{Name: "profile", Status: TUISupported},
	{Name: "state-file", Status: TUISupported},        // /session state-file
	{Name: "run-id", Status: TUIPlanned, Ref: "#441"}, // /diagnose --run covers the operator need
	{Name: "output-json", Status: TUIPlanned, Ref: "#445"},
	{Name: "output-file", Status: TUIPlanned, Ref: "#445"},
	{Name: "log-format", Status: TUIPlanned, Ref: "#445"},       // /session key, lands with the observability batch
	{Name: "verbosity", Status: TUISupported},                   // /verbosity
	{Name: "shutdown-timeout", Status: TUIPlanned, Ref: "#445"}, // /session key, lands with the observability batch
	{Name: "progress", Status: TUICLIOnly, Ref: "TUI renders its own progress"},
	{Name: "progress-interval", Status: TUICLIOnly, Ref: "TUI renders its own progress"},
	{Name: "metrics-addr", Status: TUIPlanned, Ref: "#445"},
	{Name: "otel-endpoint", Status: TUIPlanned, Ref: "#445"},
	{Name: "audit-dir", Status: TUIPlanned, Ref: "#445"},
	{Name: "audit-tamper-evident", Status: TUIPlanned, Ref: "#445"},
	{Name: "no-audit", Status: TUIPlanned, Ref: "#445"},
}

// Registry is the authoritative command/flag parity list.
var Registry = []CommandSpec{
	{Path: "run", Status: TUISupported, Flags: []FlagSpec{
		{Name: "source-schema"}, {Name: "target-schema"}, {Name: "workers"},
		{Name: "dry-run", Status: TUISupported},           // /run --dry-run
		{Name: "ai-schema-advisor", Status: TUISupported}, // /run --ai-schema-advisor
		{Name: "explore", Status: TUISupported},           // /explore
		{Name: "skip-preflight", Status: TUISupported},    // /run --skip-preflight
		{Name: "confirm-backup", Status: TUICLIOnly, Ref: "TUI confirms interactively"},
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
		{Name: "ai-explain", Status: TUIPlanned, Ref: "#442"},
	}},
	{Path: "ai", Status: TUIPlanned, Ref: "#442"},
	{Path: "init", Status: TUICLIOnly, Ref: "/wizard and /setup cover interactive init",
		Flags: flags("output", "advanced", "force")},
	{Path: "init-secrets", Status: TUIPlanned, Ref: "#443",
		Flags: flags("force", "with-ai")},
	{Path: "setup", Status: TUISupported, Flags: flags("output", "force")},
	{Path: "cache", Status: TUIPlanned, Ref: "#443"},
}

// Subcommands carries the second-level paths; same enforcement.
var Subcommands = []CommandSpec{
	{Path: "profile save", Status: TUISupported, Flags: flags("name")},
	{Path: "profile list", Status: TUISupported},
	{Path: "profile delete", Status: TUISupported, Flags: flags("name")},
	{Path: "profile export", Status: TUISupported, Flags: flags("name", "out")},
	{Path: "ai config-review", Status: TUIPlanned, Ref: "#442",
		Flags: flags("request", "timeout", "state-file", "json", "output-json", "output-file")},
	{Path: "ai evals", Status: TUICLIOnly, Ref: "developer/eval harness, no operator workflow",
		Flags: flags("live", "list", "scenario", "timeout", "output-file")},
	{Path: "cache clear", Status: TUIPlanned, Ref: "#443", Flags: flags("ai-only")},
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
