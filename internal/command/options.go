package command

import "time"

// Typed option structs (#438): the shared vocabulary CLI and TUI
// handlers converge on. cmd/migrate binds urfave/cli flags into these;
// the TUI parses slash-command arguments into the same structs. The
// per-family parity issues (#439-#445) migrate handlers onto them; until
// then they define the contract.

// OutputSink is where shared command logic writes results. The CLI
// backs it with stdout/file + exit codes; the TUI renders into the
// conversation pane.
type OutputSink interface {
	// Printf emits human-readable output.
	Printf(format string, args ...any)
	// JSON emits a machine-readable result (the --json/--output-json paths).
	JSON(v any) error
}

// RunOptions mirrors `dmt run` (#439).
type RunOptions struct {
	SourceSchema    string
	TargetSchema    string
	Workers         int
	DryRun          bool
	AISchemaAdvisor bool
	Explore         bool
	SkipPreflight   bool
	ConfirmBackup   bool
}

// ResumeOptions mirrors `dmt resume` (#439).
type ResumeOptions struct {
	ForceResume   bool
	SkipPreflight bool
}

// ValidateOptions mirrors `dmt validate` (#441).
type ValidateOptions struct {
	AITriage bool
	Timeout  time.Duration
	JSON     bool
}

// DiagnoseOptions mirrors `dmt diagnose` (#441).
type DiagnoseOptions struct {
	RunID    string
	AITriage bool
	Timeout  time.Duration
	JSON     bool
}

// PreflightOptions mirrors `dmt preflight` (#440).
type PreflightOptions struct {
	SkipPreflight bool
	AIReview      bool
}

// AnalyzeOptions mirrors `dmt analyze` (#442).
type AnalyzeOptions struct {
	Apply     bool
	AIExplain bool
}

// ProfileOptions mirrors the `dmt profile` subcommands.
type ProfileOptions struct {
	Action      string // save | list | delete | export
	Name        string
	Description string
	Out         string // export destination path (--out)
}

// SetupOptions mirrors `dmt setup` / `dmt init-secrets` (#443).
type SetupOptions struct {
	Output string
	Force  bool
	WithAI bool
}

// CacheOptions mirrors `dmt cache clear` (#443).
type CacheOptions struct {
	AIOnly bool
}
