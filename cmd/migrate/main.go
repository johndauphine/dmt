package main

import (
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/version"

	"github.com/urfave/cli/v2"
)

// main.go builds the CLI app and command tree. Command action implementations
// live in the sibling files named for each command or shared helper area.
func main() {
	app := &cli.App{
		Name:    version.Name,
		Usage:   version.Description,
		Version: version.Version,
		// Suppress urfave/cli's default ExitCoder handling so errors
		// from action funcs propagate back to app.Run and get formatted
		// by dmt's centralized handler below (Error/Exit code/recoverable
		// banner). Without this, any error implementing ExitCode() int
		// (e.g. orchestrator.PartialMigrationError from #248) would be
		// intercepted by cli.HandleExitCoder and bypass our format.
		ExitErrHandler: func(*cli.Context, error) {},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "Path to configuration file",
			},
			&cli.StringFlag{
				Name:  "state-file",
				Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
			},
			&cli.StringFlag{
				Name:  "profile",
				Usage: "Profile name stored in SQLite",
			},
			&cli.StringFlag{
				Name:  "run-id",
				Usage: "Explicit run ID (for Airflow, default: auto-generated UUID)",
			},
			&cli.BoolFlag{
				Name:  "output-json",
				Usage: "Output JSON result to stdout on completion (logs go to stderr)",
			},
			&cli.StringFlag{
				Name:  "output-file",
				Usage: "Write JSON result to file on completion",
			},
			&cli.StringFlag{
				Name:  "log-format",
				Value: "text",
				Usage: "Log format: text or json",
			},
			&cli.StringFlag{
				Name:  "verbosity",
				Value: "info",
				Usage: "Log verbosity level (debug, info, warn, error)",
			},
			&cli.DurationFlag{
				Name:  "shutdown-timeout",
				Value: 60 * time.Second,
				Usage: "Graceful shutdown timeout",
			},
			&cli.BoolFlag{
				Name:  "progress",
				Usage: "Output JSON progress updates to stderr",
			},
			&cli.DurationFlag{
				Name:  "progress-interval",
				Value: 1 * time.Second,
				Usage: "Interval between progress updates",
			},
			&cli.StringFlag{
				Name:  "metrics-addr",
				Usage: "Bind a Prometheus /metrics endpoint at this address (e.g. \":9090\") (#229). Empty = disabled.",
			},
			&cli.StringFlag{
				Name:  "otel-endpoint",
				Usage: "OTLP HTTP endpoint for trace export (e.g. \"http://otel-collector:4318\") (#229). Empty = disabled.",
			},
			&cli.StringFlag{
				Name:  "audit-dir",
				Usage: "Directory for per-run audit logs (#235). Default $HOME/.dmt/audit. NDJSON, append-only during the run, chmod 0444 after.",
			},
			&cli.BoolFlag{
				Name:  "audit-tamper-evident",
				Usage: "Opt into hash-chained audit events (#235). Each event gets seq/prev_hash/hash so retroactive modification is detectable. Required for high-compliance scenarios.",
			},
			&cli.BoolFlag{
				Name:  "no-audit",
				Usage: "Disable the per-run audit log entirely (#235). Only use this if you have another compliance mechanism — the default audit log has zero data-plane impact.",
			},
		},
		Before: func(c *cli.Context) error {
			// Set log level from flag
			level, err := logging.ParseLevel(c.String("verbosity"))
			if err != nil {
				return err
			}
			logging.SetLevel(level)

			// Set log format
			if c.String("log-format") == "json" {
				logging.SetFormat("json")
			}

			// Redirect logs to stderr when JSON output is enabled
			if c.Bool("output-json") || c.String("output-file") != "" {
				logging.SetOutput(os.Stderr)
			}

			return nil
		},
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				// No command provided, launch TUI
				return startTUI(c)
			}
			return cli.ShowAppHelp(c)
		},
		Commands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "Start a new migration",
				Action: runMigration,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "source-schema",
						Value: "dbo",
						Usage: "Source schema name",
					},
					&cli.StringFlag{
						Name:  "target-schema",
						Value: "public",
						Usage: "Target schema name",
					},
					&cli.IntFlag{
						Name:  "workers",
						Value: 8,
						Usage: "Number of parallel workers",
					},
					&cli.BoolFlag{
						Name:  "dry-run",
						Usage: "Preview migration plan without executing",
					},
					&cli.BoolFlag{
						Name:  "ai-schema-advisor",
						Usage: "With --dry-run, ask the configured AI provider for advisory schema drift/evolution guidance (#399)",
					},
					&cli.BoolFlag{
						Name:  "explore",
						Usage: "Force a tuning exploration probe on this run (PR2 #179 activates this; PR1 #175 plumbs the flag)",
					},
					&cli.StringFlag{
						Name:  "skip-preflight",
						Usage: "Comma-separated list of preflight checks to skip, or 'all' to disable preflight (#228). Skip names match Finding.Check (e.g. 'privileges.create_table') or a dotted prefix (e.g. 'privileges'). Use sparingly — preflight catches misconfigurations before they corrupt a long migration.",
					},
					&cli.BoolFlag{
						Name:  "confirm-backup",
						Usage: "Acknowledge that target tables will be dropped (drop_recreate mode). Required when the target schema already contains non-empty tables (#228).",
					},
				},
			},
			{
				Name:   "resume",
				Usage:  "Resume an interrupted migration",
				Action: resumeMigration,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "force-resume",
						Usage: "Force resume even if config has changed",
					},
					&cli.StringFlag{
						Name:  "skip-preflight",
						Usage: "Comma-separated list of preflight checks to skip, or 'all' to disable preflight (#228)",
					},
				},
			},
			{
				Name:   "status",
				Usage:  "Show status of current/last run",
				Action: showStatus,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output status as JSON",
					},
				},
			},
			{
				Name:   "validate",
				Usage:  "Validate row counts between source and target",
				Action: validateMigration,
			},
			{
				Name:  "history",
				Usage: "List all migration runs, or view details of a specific run",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "run",
						Usage: "Show details for a specific run ID",
					},
				},
				Action: showHistory,
			},
			{
				Name:  "profile",
				Usage: "Manage encrypted profiles stored in SQLite",
				Subcommands: []*cli.Command{
					{
						Name:   "save",
						Usage:  "Save a profile from a config file",
						Action: saveProfile,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:    "name",
								Aliases: []string{"n"},
								Usage:   "Profile name (inferred from profile.name or filename if omitted)",
							},
						},
					},
					{
						Name:   "list",
						Usage:  "List saved profiles",
						Action: listProfiles,
					},
					{
						Name:   "delete",
						Usage:  "Delete a saved profile",
						Action: deleteProfile,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "name",
								Aliases:  []string{"n"},
								Required: true,
								Usage:    "Profile name",
							},
						},
					},
					{
						Name:   "export",
						Usage:  "Export a profile to a config file",
						Action: exportProfile,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "name",
								Aliases:  []string{"n"},
								Required: true,
								Usage:    "Profile name",
							},
							&cli.StringFlag{
								Name:    "out",
								Aliases: []string{"o"},
								Value:   "config.yaml",
								Usage:   "Output path for exported config",
							},
						},
					},
				},
			},
			{
				// preflight runs the connection ping AND the driver-side
				// preflight checks introduced in #228. health-check is
				// preserved as an alias so existing Airflow/k8s probes
				// keep working without edits.
				Name:    "preflight",
				Aliases: []string{"health-check"},
				Usage:   "Verify source/target connectivity, privileges, version, encoding, and other preflight checks (#228)",
				Action:  healthCheck,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "skip-preflight",
						Usage: "Comma-separated list of preflight checks to skip, or 'all' to disable preflight",
					},
					&cli.BoolFlag{
						Name:  "ai-review",
						Usage: "Ask the configured AI provider for an advisory readiness review after deterministic preflight (#398)",
					},
				},
			},
			{
				Name:   "analyze",
				Usage:  "Analyze source database and suggest optimal configuration",
				Action: analyzeConfig,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "apply",
						Usage: "Apply AI-tuned parameters to the analyzed config file",
					},
					&cli.BoolFlag{
						Name:  "ai-explain",
						Usage: "Ask the configured AI provider to explain deterministic smartconfig and runtime tuning evidence (#402)",
					},
				},
			},
			{
				Name:   "init",
				Usage:  "Create a new configuration file interactively",
				Action: initConfig,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Value:   "config.yaml",
						Usage:   "Output file path",
					},
					&cli.BoolFlag{
						Name:  "advanced",
						Usage: "Show advanced configuration options",
					},
					&cli.BoolFlag{
						Name:    "force",
						Aliases: []string{"f"},
						Usage:   "Overwrite existing file",
					},
				},
			},
			{
				Name:   "init-secrets",
				Usage:  "Create a secrets file for encryption and notifications (AI is optional, see --with-ai)",
				Action: initSecrets,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "force",
						Aliases: []string{"f"},
						Usage:   "Overwrite existing secrets file",
					},
					&cli.BoolFlag{
						Name:  "with-ai",
						Usage: "Seed the AI provider section (anthropic / openai / gemini / ollama / lmstudio). AI is optional since #167; dmt runs migrations deterministically without it.",
					},
				},
			},
			{
				Name:   "setup",
				Usage:  "Guided setup wizard: secrets, config, connection test, and AI analysis",
				Action: runSetup,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Value:   "config.yaml",
						Usage:   "Output config file path",
					},
					&cli.BoolFlag{
						Name:    "force",
						Aliases: []string{"f"},
						Usage:   "Overwrite existing files",
					},
				},
			},
			{
				// Cache management (#177). Inspect and invalidate the
				// `~/.dmt/type-cache.json` file. Subcommand-only — bare
				// `dmt cache` prints usage.
				Name:  "cache",
				Usage: "Manage the type-mapping cache (~/.dmt/type-cache.json)",
				Subcommands: []*cli.Command{
					{
						Name:   "clear",
						Usage:  "Remove cached type mappings (use --ai-only to keep deterministic entries)",
						Action: cacheClear,
						Flags: []cli.Flag{
							&cli.BoolFlag{
								Name:  "ai-only",
								Usage: "Clear only AI-sourced entries (use after AI model upgrade). Leaves any non-AI entries in place.",
							},
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		code := exitcodes.FromError(err)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if exitcodes.IsRecoverable(code) {
			fmt.Fprintf(os.Stderr, "Exit code %d (%s) - safe to retry\n", code, exitcodes.Description(code))
		} else {
			fmt.Fprintf(os.Stderr, "Exit code %d (%s)\n", code, exitcodes.Description(code))
		}
		os.Exit(code)
	}
}
