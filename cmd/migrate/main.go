package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/setup"
	"github.com/johndauphine/dmt/internal/tui"
	"github.com/johndauphine/dmt/internal/version"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

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
				},
			},
			{
				Name:   "analyze",
				Usage:  "Analyze source database and suggest optimal configuration",
				Action: analyzeConfig,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "apply",
						Usage: "Apply AI-tuned parameters to ~/.secrets/dmt-config.yaml",
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

func startTUI(c *cli.Context) error {
	return tui.Start()
}

func runMigration(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override from flags
	if c.IsSet("source-schema") {
		cfg.Source.Schema = c.String("source-schema")
	}
	if c.IsSet("target-schema") {
		cfg.Target.Schema = c.String("target-schema")
	}
	if c.IsSet("workers") {
		cfg.Migration.Workers = c.Int("workers")
	}
	if c.IsSet("explore") {
		cfg.Migration.Explore = c.Bool("explore")
	}
	// Preflight knobs (#228). The CLI flags override any YAML values so
	// operators can opt out per-invocation without editing config files.
	if c.IsSet("skip-preflight") {
		cfg.Migration.SkipPreflight = []string{c.String("skip-preflight")}
	}
	if c.IsSet("confirm-backup") {
		cfg.Migration.ConfirmBackup = c.Bool("confirm-backup")
	}
	// Audit-log knobs (#235). Same CLI-override-YAML pattern.
	if c.IsSet("audit-dir") {
		cfg.Migration.AuditDir = c.String("audit-dir")
	}
	if c.IsSet("audit-tamper-evident") {
		cfg.Migration.AuditTamperEvident = c.Bool("audit-tamper-evident")
	}
	if c.IsSet("no-audit") {
		cfg.Migration.NoAudit = c.Bool("no-audit")
	}

	// Build orchestrator options
	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
		RunID:     c.String("run-id"),
	}

	// Create orchestrator
	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	// Observability: start Prometheus + OTLP if configured (#229).
	stopObs := setupObservability(c, orch)
	defer stopObs()

	// Set up progress reporter if --progress flag is set
	if c.Bool("progress") {
		reporter := progress.NewJSONReporter(os.Stderr, c.Duration("progress-interval"))
		orch.SetProgressReporter(reporter, c.Duration("progress-interval"))
		defer reporter.Close()
	}

	// Handle dry-run mode
	if c.Bool("dry-run") {
		ctx := context.Background()
		result, err := orch.DryRun(ctx)
		if err != nil {
			return err
		}

		if c.Bool("output-json") || c.String("output-file") != "" {
			data, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal result: %w", err)
			}
			if c.Bool("output-json") {
				fmt.Println(string(data))
			}
			if outputFile := c.String("output-file"); outputFile != "" {
				if err := os.WriteFile(outputFile, data, 0600); err != nil {
					return fmt.Errorf("failed to write output file: %w", err)
				}
			}
			return nil
		}

		// Human-readable output
		printDryRunResult(result)
		return nil
	}

	// Handle graceful shutdown with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupSignalHandler(c, cancel, orch.Close)

	// Run migration
	runErr := orch.Run(ctx)

	// Output JSON result if requested
	if c.Bool("output-json") || c.String("output-file") != "" {
		result, err := orch.GetLastRunResult()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to get run result: %v\n", err)
		} else {
			if runErr != nil {
				result.Error = runErr.Error()
			}
			if err := outputJSON(c, result); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to output JSON: %v\n", err)
			}
		}
	}

	return runErr
}

func resumeMigration(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Preflight skip flag — same plumbing as runMigration. confirm-backup
	// is not on resume because resume implies an in-progress migration
	// whose target was already created or staged (#228).
	if c.IsSet("skip-preflight") {
		cfg.Migration.SkipPreflight = []string{c.String("skip-preflight")}
	}
	// Audit knobs on resume — operators may want to opt out the audit
	// on a resume specifically (e.g. resuming a job whose audit-dir
	// has moved). (#235)
	if c.IsSet("audit-dir") {
		cfg.Migration.AuditDir = c.String("audit-dir")
	}
	if c.IsSet("audit-tamper-evident") {
		cfg.Migration.AuditTamperEvident = c.Bool("audit-tamper-evident")
	}
	if c.IsSet("no-audit") {
		cfg.Migration.NoAudit = c.Bool("no-audit")
	}

	opts := orchestrator.Options{
		StateFile:   c.String("state-file"),
		ForceResume: c.Bool("force-resume"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	// Observability: identical setup to runMigration (#229).
	stopObs := setupObservability(c, orch)
	defer stopObs()

	// Set up progress reporter if --progress flag is set
	if c.Bool("progress") {
		reporter := progress.NewJSONReporter(os.Stderr, c.Duration("progress-interval"))
		orch.SetProgressReporter(reporter, c.Duration("progress-interval"))
		defer reporter.Close()
	}

	// Handle graceful shutdown with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupSignalHandler(c, cancel, orch.Close)

	runErr := orch.Resume(ctx)

	// Output JSON result if requested
	if c.Bool("output-json") || c.String("output-file") != "" {
		result, err := orch.GetLastRunResult()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to get run result: %v\n", err)
		} else {
			if runErr != nil {
				result.Error = runErr.Error()
			}
			if err := outputJSON(c, result); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to output JSON: %v\n", err)
			}
		}
	}

	return runErr
}

func showStatus(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	// JSON output
	if c.Bool("json") {
		result, err := orch.GetStatusResult()
		if err != nil {
			// Return empty status for no active migration
			emptyResult := &orchestrator.StatusResult{
				Status: "no_active_migration",
			}
			data, _ := json.MarshalIndent(emptyResult, "", "  ")
			fmt.Println(string(data))
			return nil
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal status: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}

	return orch.ShowStatus()
}

func validateMigration(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	return orch.Validate(context.Background())
}

func showHistory(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	// If --run flag is provided, show details for that specific run
	if runID := c.String("run"); runID != "" {
		return orch.ShowRunDetails(runID)
	}

	return orch.ShowHistory()
}

// getConfigPath returns the config path from the context lineage.
// Because --config has a non-empty default ("config.yaml"), we use IsSet
// to find the first context where the user explicitly provided the flag.
// This ensures "dmt -c X run" and "dmt run -c X" both work correctly.
func getConfigPath(c *cli.Context) (path string, isSet bool) {
	for _, ctx := range c.Lineage() {
		if ctx != nil && ctx.IsSet("config") {
			return ctx.String("config"), true
		}
	}
	return "config.yaml", false
}

func loadConfigWithOrigin(c *cli.Context) (*config.Config, string, string, error) {
	profileName := c.String("profile")
	if profileName != "" {
		cfg, err := loadProfileConfig(profileName)
		return cfg, profileName, "", err
	}

	configPath, configIsSet := getConfigPath(c)
	if _, err := os.Stat(configPath); os.IsNotExist(err) && !configIsSet {
		return nil, "", "", fmt.Errorf("configuration file not found: %s", configPath)
	}
	cfg, err := config.Load(configPath)
	return cfg, "", configPath, err
}

func loadProfileConfig(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}

func saveProfile(c *cli.Context) error {
	configPath, _ := getConfigPath(c)
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	name := c.String("name")
	if name == "" {
		if cfg.Profile.Name != "" {
			name = cfg.Profile.Name
		} else {
			base := filepath.Base(configPath)
			name = strings.TrimSuffix(base, filepath.Ext(base))
		}
	}
	payload, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return err
	}
	defer state.Close()

	if err := state.SaveProfile(name, cfg.Profile.Description, payload); err != nil {
		if strings.Contains(err.Error(), "DMT_MASTER_KEY is not set") {
			return fmt.Errorf("DMT_MASTER_KEY is not set; set it before saving profiles")
		}
		return err
	}
	fmt.Printf("Saved profile %q\n", name)
	return nil
}

func listProfiles(c *cli.Context) error {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return err
	}
	defer state.Close()

	profiles, err := state.ListProfiles()
	if err != nil {
		return err
	}
	if len(profiles) == 0 {
		fmt.Println("No profiles found")
		return nil
	}
	fmt.Printf("%-20s %-40s %-20s %-20s\n", "Name", "Description", "Created", "Updated")
	for _, p := range profiles {
		desc := strings.ReplaceAll(strings.TrimSpace(p.Description), "\n", " ")
		fmt.Printf("%-20s %-40s %-20s %-20s\n",
			p.Name,
			desc,
			p.CreatedAt.Format("2006-01-02 15:04:05"),
			p.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func deleteProfile(c *cli.Context) error {
	name := c.String("name")
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return err
	}
	defer state.Close()

	if err := state.DeleteProfile(name); err != nil {
		return err
	}
	fmt.Printf("Deleted profile %q\n", name)
	return nil
}

func exportProfile(c *cli.Context) error {
	name := c.String("name")
	outPath := c.String("out")

	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, blob, 0600); err != nil {
		return err
	}
	fmt.Printf("Exported profile %q to %s\n", name, outPath)
	return nil
}

// outputJSON writes the migration result as JSON to stdout and/or a file
func outputJSON(c *cli.Context, result *orchestrator.MigrationResult) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	// Write to stdout if --output-json flag is set
	if c.Bool("output-json") {
		fmt.Println(string(data))
	}

	// Write to file if --output-file flag is set
	if outputFile := c.String("output-file"); outputFile != "" {
		if err := os.WriteFile(outputFile, data, 0600); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
	}

	return nil
}

// setupSignalHandler sets up graceful shutdown with timeout for Airflow/Kubernetes.
// Exit codes:
//   - 5 (Cancelled): Normal signal-based shutdown, safe to retry
//   - Timeout or double-signal also exits with 5 (still user-initiated cancellation)
//
// The cleanup function is called before forced exit to close database connections
// and other resources. This ensures we're a good citizen and don't leave orphaned
// connections in the database.
func setupSignalHandler(c *cli.Context, cancel context.CancelFunc, cleanup func()) {
	shutdownTimeout := c.Duration("shutdown-timeout")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Guard against concurrent cleanup from timer and second signal racing.
	// AfterFunc.Stop() doesn't wait for an in-flight callback, so both paths
	// could invoke forceCleanupAndExit simultaneously without this guard.
	var exitOnce sync.Once

	go func() {
		sig := <-sigCh
		sigName := "SIGINT"
		if sig == syscall.SIGTERM {
			sigName = "SIGTERM"
		}
		fmt.Fprintf(os.Stderr, "\nReceived %s. Shutting down gracefully (timeout: %s)...\n", sigName, shutdownTimeout)
		fmt.Fprintln(os.Stderr, "Saving checkpoint and allowing in-progress transfers to complete...")
		cancel()

		// Start shutdown timer
		shutdownTimer := time.AfterFunc(shutdownTimeout, func() {
			exitOnce.Do(func() {
				fmt.Fprintln(os.Stderr, "Shutdown timeout reached, forcing exit...")
				forceCleanupAndExit(cleanup)
			})
		})

		// Wait for second signal for immediate exit
		<-sigCh
		shutdownTimer.Stop()
		exitOnce.Do(func() {
			fmt.Fprintln(os.Stderr, "Second signal received, forcing immediate exit...")
			forceCleanupAndExit(cleanup)
		})
	}()
}

// forceCleanupAndExit closes database connections with a deadline and exits.
// Runs cleanup in a goroutine with a short timeout to avoid blocking on
// connections held by stalled operations (e.g. pgxpool.Close waits for
// acquired connections to be released, which never happens if a COPY is stalled).
// A brief sleep after cleanup allows the OS TCP stack to send FIN/RST packets
// so the database server can clean up its connection state.
func forceCleanupAndExit(cleanup func()) {
	fmt.Fprintln(os.Stderr, "Closing database connections...")
	if cleanup != nil {
		done := make(chan struct{})
		go func() {
			cleanup()
			close(done)
		}()
		select {
		case <-done:
			// Cleanup completed
		case <-time.After(5 * time.Second):
			fmt.Fprintln(os.Stderr, "Cleanup timed out, forcing exit...")
		}
	}
	// Brief pause to let OS flush TCP FIN/RST packets to database servers
	time.Sleep(100 * time.Millisecond)
	fmt.Fprintf(os.Stderr, "Exit code %d (%s) - safe to retry\n", exitcodes.Cancelled, exitcodes.Description(exitcodes.Cancelled))
	os.Exit(exitcodes.Cancelled)
}

// healthCheck implements `dmt preflight` (and its legacy `health-check`
// alias). Tests connections and runs the driver-side preflight checks
// introduced in #228.
func healthCheck(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if c.IsSet("skip-preflight") {
		cfg.Migration.SkipPreflight = []string{c.String("skip-preflight")}
	}

	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := orch.HealthCheck(ctx)
	if err != nil {
		return err
	}

	// Output JSON or human-readable based on --output-json flag
	if c.Bool("output-json") || c.String("output-file") != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal result: %w", err)
		}
		if c.Bool("output-json") {
			fmt.Println(string(data))
		}
		if outputFile := c.String("output-file"); outputFile != "" {
			if err := os.WriteFile(outputFile, data, 0600); err != nil {
				return fmt.Errorf("failed to write output file: %w", err)
			}
		}
		if !result.Healthy {
			// Use the same exit-code policy as the human-readable path
			// (Copilot review): JSON consumers (Airflow, CI) need ConfigError
			// for misconfiguration vs ConnectionError for ping failure so
			// retry policy can branch correctly.
			return preflightExitError(result)
		}
		return nil
	}

	// Human-readable output
	fmt.Println("\nHealth Check Results:")
	fmt.Printf("  Source (%s): %s (%dms)\n",
		result.SourceDBType,
		boolToStatus(result.SourceConnected),
		result.SourceLatencyMs)
	if result.SourceError != "" {
		fmt.Printf("    Error: %s\n", result.SourceError)
	}
	if result.SourceConnected && result.SourceTableCount > 0 {
		fmt.Printf("    Tables: %d\n", result.SourceTableCount)
	}

	fmt.Printf("  Target (%s): %s (%dms)\n",
		result.TargetDBType,
		boolToStatus(result.TargetConnected),
		result.TargetLatencyMs)
	if result.TargetError != "" {
		fmt.Printf("    Error: %s\n", result.TargetError)
	}

	if len(result.PreFlightFindings) > 0 {
		fmt.Println("\n  Preflight findings:")
		for _, f := range result.PreFlightFindings {
			fmt.Printf("    [%s] %s/%s: %s\n", f.Severity, f.Side, f.Check, f.Message)
			if f.Remedy != "" {
				fmt.Printf("      remedy: %s\n", f.Remedy)
			}
		}
	}

	fmt.Printf("\n  Overall: %s\n", boolToHealthy(result.Healthy))

	if !result.Healthy {
		return preflightExitError(result)
	}
	return nil
}

// preflightExitError classifies a failed preflight result into an exit code:
//   - ConfigError when at least one error-severity finding remains
//     (misconfigured environment — non-recoverable, operator must fix it).
//   - ConnectionError when the ping itself failed (network/credentials —
//     potentially recoverable; retry policy can backoff).
//
// The message counts only error-severity findings so the operator isn't
// misled by warn/info findings inflating the failure count (Copilot review).
func preflightExitError(result *orchestrator.HealthCheckResult) error {
	if result.PreFlightAborted {
		errorCount := 0
		for _, f := range result.PreFlightFindings {
			if f.Severity == driver.SeverityError {
				errorCount++
			}
		}
		return exitcodes.NewExitError(fmt.Errorf("preflight failed: %d blocking finding(s)", errorCount), exitcodes.ConfigError)
	}
	return exitcodes.NewExitError(fmt.Errorf("preflight failed"), exitcodes.ConnectionError)
}

func analyzeConfig(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Try full orchestrator first (with both source and target)
	orch, err := orchestrator.New(cfg)
	if err != nil {
		// Full connection failed - try source-only mode for analyze
		logging.Warn("Cannot connect to both databases: %v", err)
		logging.Info("Attempting source-only analysis...")

		orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{SourceOnly: true})
		if err != nil {
			return fmt.Errorf("cannot connect to source database: %w", err)
		}
		logging.Info("Connected to source database")
		logging.Warn("Target database unavailable - tuning recommendations may be less accurate")
	}
	defer orch.Close()

	// Get schema from config (default to dbo/public based on source type)
	schema := cfg.Source.Schema
	if schema == "" {
		if cfg.Source.Type == "postgres" {
			schema = "public"
		} else {
			schema = "dbo"
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Run smart config analysis
	suggestions, err := orch.AnalyzeConfig(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to analyze config: %w", err)
	}

	// Output suggestions
	fmt.Println(suggestions.FormatYAML())

	// Apply AI-tuned parameters to secrets file if requested
	if c.Bool("apply") {
		if err := applyTuningToSecrets(suggestions); err != nil {
			return fmt.Errorf("failed to apply tuning: %w", err)
		}
		fmt.Printf("\n✓ Applied AI-tuned parameters to %s\n", secrets.GetSecretsPath())
	}

	return nil
}

// applyTuningToSecrets writes AI-tuned parameters to the secrets file.
func applyTuningToSecrets(suggestions *driver.SmartConfigSuggestions) error {
	updates := &secrets.Config{
		MigrationDefaults: secrets.MigrationDefaults{
			Workers:              suggestions.Workers,
			MaxSourceConnections: suggestions.MaxSourceConnections,
			MaxTargetConnections: suggestions.MaxTargetConnections,
			MaxMemoryMB:          suggestions.EstimatedMemMB,
			ReadAheadBuffers:     suggestions.ReadAheadBuffers,
			WriteAheadWriters:    suggestions.WriteAheadWriters,
			ParallelReaders:      suggestions.ParallelReaders,
			CheckpointFrequency:  suggestions.CheckpointFrequency,
			MaxRetries:           suggestions.MaxRetries,
		},
	}

	return secrets.Save(updates)
}

func boolToStatus(connected bool) string {
	if connected {
		return "OK"
	}
	return "FAILED"
}

func boolToHealthy(healthy bool) string {
	if healthy {
		return "HEALTHY"
	}
	return "UNHEALTHY"
}

// initConfig runs the CLI wizard to create a config file
func initConfig(c *cli.Context) error {
	outputPath := c.String("output")
	advanced := c.Bool("advanced")
	force := c.Bool("force")

	// Check if file exists (unless --force)
	if !force {
		if _, err := os.Stat(outputPath); err == nil {
			return fmt.Errorf("file %s already exists (use --force to overwrite)", outputPath)
		}
	}

	cfg, err := runCLIWizard(advanced)
	if err != nil {
		return err
	}

	// Marshal and write
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("generating config: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	fmt.Printf("Configuration saved to %s\n", outputPath)
	return nil
}

// initSecrets creates a secrets file for API keys and encryption
func initSecrets(c *cli.Context) error {
	force := c.Bool("force")
	withAI := c.Bool("with-ai")

	// Ensure secrets directory exists
	secretsDir, err := secrets.EnsureSecretsDir()
	if err != nil {
		return fmt.Errorf("creating secrets directory: %w", err)
	}

	secretsPath := secrets.GetSecretsPath()

	// Check if file exists (unless --force)
	if !force {
		if _, err := os.Stat(secretsPath); err == nil {
			return fmt.Errorf("secrets file %s already exists (use --force to overwrite)", secretsPath)
		}
	}

	// Default template is AI-free; --with-ai seeds the AI provider
	// section. AI is optional in dmt (since #167) — the deterministic
	// type mapper, error diagnosis catalog, and DB tuning analyzer all
	// run without it.
	var template string
	if withAI {
		template = secrets.GenerateTemplateWithAI()
	} else {
		template = secrets.GenerateTemplate()
	}

	// Write with secure permissions
	if err := os.WriteFile(secretsPath, []byte(template), 0600); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	fmt.Printf("Secrets file created: %s\n", secretsPath)
	fmt.Printf("Directory: %s (permissions: 0700)\n", secretsDir)
	fmt.Println("\nNext steps:")
	fmt.Println("1. Set encryption.master_key for profile encryption:")
	fmt.Println("   Generate with: openssl rand -base64 32")
	if withAI {
		fmt.Println("2. Edit the file to add your AI provider API key (required for AI features to work; leave blank to skip a provider)")
		fmt.Println("3. You're ready to run `dmt run --config config.yaml`")
	} else {
		fmt.Println("2. You're ready to run `dmt run --config config.yaml`")
		fmt.Println("\nAI features are OPTIONAL. To opt in later, APPEND an ai: section to")
		fmt.Println("the file manually — do NOT run --force --with-ai, which would overwrite")
		fmt.Println("any master_key / slack webhook values you set above.")
	}
	fmt.Println("\nIMPORTANT: Keep this file secure and never commit it to version control!")

	return nil
}

// CLI prompt helpers

func cliPrompt(reader *bufio.Reader, label, defaultValue string) string {
	if defaultValue != "" {
		fmt.Printf("%s [%s]: ", label, defaultValue)
	} else {
		fmt.Printf("%s: ", label)
	}
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultValue
	}
	return input
}

func cliPromptInt(reader *bufio.Reader, label string, defaultValue int) int {
	result := cliPrompt(reader, label, fmt.Sprintf("%d", defaultValue))
	if val, err := fmt.Sscanf(result, "%d", &defaultValue); err != nil || val != 1 {
		return defaultValue
	}
	return defaultValue
}

func cliPromptBool(reader *bufio.Reader, label string, defaultValue bool) bool {
	defStr := "n"
	if defaultValue {
		defStr = "y"
	}
	result := strings.ToLower(cliPrompt(reader, label+" (y/n)", defStr))
	return result == "y" || result == "yes"
}

func cliPromptPassword(label string) string {
	fmt.Printf("%s: ", label)
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // newline after hidden input
	if err != nil {
		return ""
	}
	return string(password)
}

func cliPromptChoice(reader *bufio.Reader, label string, choices []string, defaultValue string) string {
	for {
		result := cliPrompt(reader, label, defaultValue)
		for _, choice := range choices {
			if result == choice {
				return result
			}
		}
		fmt.Printf("  Invalid choice. Options: %s\n", strings.Join(choices, ", "))
	}
}

// runCLIWizard runs an interactive CLI wizard to create a config
func runCLIWizard(advanced bool) (*config.Config, error) {
	reader := bufio.NewReader(os.Stdin)

	cfg := &config.Config{}

	dbTypes := []string{"mssql", "postgres"}
	targetModes := []string{"drop_recreate", "upsert"}

	fmt.Println("\n=== Source Database ===")
	cfg.Source.Type = cliPromptChoice(reader, "Database type (mssql/postgres)", dbTypes, "mssql")
	cfg.Source.Host = cliPrompt(reader, "Host", "localhost")
	defaultPort := 1433
	if cfg.Source.Type == "postgres" {
		defaultPort = 5432
	}
	cfg.Source.Port = cliPromptInt(reader, "Port", defaultPort)
	cfg.Source.Database = cliPrompt(reader, "Database name", "")
	cfg.Source.User = cliPrompt(reader, "Username", "sa")
	cfg.Source.Password = cliPromptPassword("Password")
	cfg.Source.Schema = cliPrompt(reader, "Schema", "dbo")

	fmt.Println("\n=== Target Database ===")
	cfg.Target.Type = cliPromptChoice(reader, "Database type (mssql/postgres)", dbTypes, "postgres")
	cfg.Target.Host = cliPrompt(reader, "Host", "localhost")
	defaultPort = 5432
	if cfg.Target.Type == "mssql" {
		defaultPort = 1433
	}
	cfg.Target.Port = cliPromptInt(reader, "Port", defaultPort)
	cfg.Target.Database = cliPrompt(reader, "Database name", "")
	cfg.Target.User = cliPrompt(reader, "Username", "postgres")
	cfg.Target.Password = cliPromptPassword("Password")
	cfg.Target.Schema = cliPrompt(reader, "Schema", "public")

	fmt.Println("\n=== Migration Settings ===")
	cfg.Migration.TargetMode = cliPromptChoice(reader, "Target mode (drop_recreate/upsert)", targetModes, "drop_recreate")
	cfg.Migration.CreateIndexes = cliPromptBool(reader, "Create indexes", false)
	cfg.Migration.CreateForeignKeys = cliPromptBool(reader, "Create foreign keys", false)

	if advanced {
		fmt.Println("\n=== Advanced Settings ===")
		cfg.Migration.Workers = cliPromptInt(reader, "Workers", 6)
		cfg.Migration.ChunkSize = cliPromptInt(reader, "Chunk size", 100000)
	}

	return cfg, nil
}

// runSetup runs the unified setup wizard
func runSetup(c *cli.Context) error {
	reader := bufio.NewReader(os.Stdin)
	state := setup.NewState()

	// Seed Slack webhook from the global secrets file so edit mode shows
	// the current value as Enter-to-keep. Capture Original too so a
	// failed write can revert without leaving a misleading "configured"
	// hint.
	loadedWebhook := setup.LoadExistingSlackWebhook()
	state.SlackWebhook = loadedWebhook
	state.SlackWebhookOriginal = loadedWebhook

	if c.IsSet("output") {
		state.ConfigPath = c.String("output")
	}
	state.Force = c.Bool("force")

	// If the target config already exists, load it so the wizard offers
	// edit-vs-new and seeds each prompt with the user's prior values.
	// LoadRaw (not Load) — placeholders like ${env:PASS} must survive a
	// re-save so the wizard never writes resolved secrets to disk.
	if _, err := os.Stat(state.ConfigPath); err == nil {
		if cfg, err := config.LoadRaw(state.ConfigPath); err == nil {
			state.Config = *cfg
			state.CurrentStep = setup.StepEditOrNew
			state.EditMode = true
		}
	}

	fmt.Println("\n=== DMT Setup Wizard ===")

	for state.CurrentStep != setup.StepDone {
		info := state.Prompt()

		// Display section header
		if info.SectionHeader != "" {
			fmt.Printf("\n=== %s ===\n", info.SectionHeader)
		}

		if info.IsAutoAction {
			var result string
			switch state.CurrentStep {
			case setup.StepCheckSecrets:
				result = setup.CheckExistingSecrets()
				if result == "has_ai" {
					fmt.Println("  AI provider already configured, skipping AI setup")
				}

			case setup.StepSourceConnTest, setup.StepTargetConnTest:
				fmt.Printf("  %s\n", info.Text)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				// Resolve ${env:...} / ${file:...} placeholders for the side
				// being tested only — an unrelated missing template on the
				// other side must not poison this test. state.Config keeps
				// the raw form so a re-save preserves placeholders.
				var connResult *setup.ConnTestResult
				if state.CurrentStep == setup.StepSourceConnTest {
					conn := state.SourceConnConfig()
					connResult = setup.TestConnection(ctx,
						conn.Source.Type, conn.Source.Host,
						conn.Source.Port, conn.Source.Database,
						conn.Source.User, conn.Source.Password,
						conn.Source.DSNOptions())
				} else {
					conn := state.TargetConnConfig()
					connResult = setup.TestConnection(ctx,
						conn.Target.Type, conn.Target.Host,
						conn.Target.Port, conn.Target.Database,
						conn.Target.User, conn.Target.Password,
						conn.Target.DSNOptions())
				}
				cancel()

				if connResult.Connected {
					fmt.Printf("  Connected! (%dms)\n", connResult.LatencyMs)
					result = ""
				} else {
					fmt.Printf("  Failed: %s (%dms)\n", connResult.Error, connResult.LatencyMs)
					result = connResult.Error
				}

			case setup.StepWriteSecrets:
				if err := state.WriteSecretsFile(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else {
					fmt.Printf("  Secrets saved to %s\n", secrets.GetSecretsPath())
				}

			case setup.StepWriteSlackSecret:
				if err := state.WriteSlackSecret(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else if state.SlackWebhook == "" {
					fmt.Println("  Slack webhook cleared")
				} else {
					fmt.Printf("  Slack webhook saved to %s\n", secrets.GetSecretsPath())
				}

			case setup.StepWriteConfig:
				// Check if file exists (unless --force)
				if !state.Force {
					if _, err := os.Stat(state.ConfigPath); err == nil {
						fmt.Printf("  File %s already exists. Overwrite? (y/n) [n]: ", state.ConfigPath)
						answer, _ := reader.ReadString('\n')
						if strings.ToLower(strings.TrimSpace(answer)) != "y" {
							state.CurrentStep = setup.StepConfigPath
							continue
						}
					}
				}
				if err := state.WriteConfigFile(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else {
					fmt.Printf("  Configuration saved to %s\n", state.ConfigPath)
				}
			}

			if errMsg := state.Process(result); errMsg != "" {
				fmt.Printf("  Error: %s\n", errMsg)
			}
			continue
		}

		// User input step - loop until valid
		for {
			var input string
			if info.IsMasked {
				input = cliPromptPassword(info.Text)
			} else {
				input = cliPrompt(reader, info.Text, info.Default)
			}

			if errMsg := state.Process(input); errMsg != "" {
				fmt.Printf("  %s\n", errMsg)
				continue
			}
			break
		}
	}

	fmt.Println("\nSetup complete!")

	// Final summary — distinguish "AI: not configured (optional)" from
	// "AI: configured" so users see explicitly that the AI-free path is
	// supported rather than a degraded mode (#174).
	if state.AIConfigured {
		fmt.Println("  AI: configured")
	} else {
		fmt.Println("  AI: not configured (optional; to enable later, append an ai: section to ~/.secrets/dmt-config.yaml — see README § Optional AI Enhancements)")
	}

	// Run AI analysis if requested
	if state.RunAnalysis {
		fmt.Println("\nRunning AI analysis...")
		cfg, err := config.Load(state.ConfigPath)
		if err != nil {
			fmt.Printf("Error loading config for analysis: %v\n", err)
			return nil
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			// Try source-only
			logging.Warn("Cannot connect to both databases: %v", err)
			orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{SourceOnly: true})
			if err != nil {
				fmt.Printf("Cannot connect for analysis: %v\n", err)
				return nil
			}
		}
		defer orch.Close()

		schema := cfg.Source.Schema
		if schema == "" {
			if cfg.Source.Type == "postgres" {
				schema = "public"
			} else {
				schema = "dbo"
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		suggestions, err := orch.AnalyzeConfig(ctx, schema)
		if err != nil {
			fmt.Printf("Analysis failed: %v\n", err)
			return nil
		}

		fmt.Println(suggestions.FormatYAML())

		answer := cliPrompt(reader, "Apply tuning to secrets? (y/n)", "n")
		if strings.ToLower(answer) == "y" {
			if err := applyTuningToSecrets(suggestions); err != nil {
				fmt.Printf("Error applying tuning: %v\n", err)
			} else {
				fmt.Printf("Applied AI-tuned parameters to %s\n", secrets.GetSecretsPath())
			}
		}
	}

	return nil
}

// printDryRunResult prints the dry-run result in human-readable format
func printDryRunResult(r *orchestrator.DryRunResult) {
	fmt.Println("\n=== Migration Preview (Dry Run) ===")
	fmt.Printf("Source: %s (%s)\n", r.SourceType, r.SourceSchema)
	fmt.Printf("Target: %s (%s)\n", r.TargetType, r.TargetSchema)
	fmt.Printf("Mode: %s\n", r.TargetMode)
	fmt.Println()

	fmt.Printf("%-30s %12s %12s %15s\n", "Table", "Rows", "Partitions", "Pagination")
	fmt.Println(strings.Repeat("-", 75))
	for _, t := range r.Tables {
		fmt.Printf("%-30s %12d %12d %15s\n",
			t.Name, t.RowCount, t.Partitions, t.PaginationMethod)
	}
	fmt.Println(strings.Repeat("-", 75))
	fmt.Printf("%-30s %12d\n", "TOTAL", r.TotalRows)
	fmt.Println()

	fmt.Printf("Workers: %d\n", r.Workers)
	fmt.Printf("Chunk Size: %d\n", r.ChunkSize)
	fmt.Printf("Estimated Memory: ~%d MB\n", r.EstimatedMemMB)
}

// cacheClear implements `dmt cache clear [--ai-only]` (#177). With no
// flag it removes the entire ~/.dmt/type-cache.json file; with --ai-only
// it preserves any non-AI entries (today: none — but the format is
// designed to support deterministic-tagged entries in future).
func cacheClear(c *cli.Context) error {
	cacheFile := driver.DefaultCacheFilePath()
	if c.Bool("ai-only") {
		cleared, err := driver.ClearAICacheEntries(cacheFile)
		if err != nil {
			return fmt.Errorf("clearing AI cache entries: %w", err)
		}
		fmt.Printf("Cleared %d AI cache entries from %s\n", cleared, cacheFile)
		return nil
	}

	if err := os.Remove(cacheFile); err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("No cache file to clear at %s\n", cacheFile)
			return nil
		}
		return fmt.Errorf("removing cache file: %w", err)
	}
	fmt.Printf("Removed cache file %s\n", cacheFile)
	return nil
}
