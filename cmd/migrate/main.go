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
	"syscall"
	"time"

	"github.com/johndauphine/dmt/internal/calibration"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/secrets"
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
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
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
					&cli.StringFlag{
						Name:  "state-file",
						Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
					},
					&cli.BoolFlag{
						Name:  "dry-run",
						Usage: "Preview migration plan without executing",
					},
				},
			},
			{
				Name:   "resume",
				Usage:  "Resume an interrupted migration",
				Action: resumeMigration,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
					&cli.StringFlag{
						Name:  "state-file",
						Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
					},
					&cli.BoolFlag{
						Name:  "force-resume",
						Usage: "Force resume even if config has changed",
					},
				},
			},
			{
				Name:   "status",
				Usage:  "Show status of current/last run",
				Action: showStatus,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
					&cli.StringFlag{
						Name:  "state-file",
						Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
					},
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
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
					&cli.StringFlag{
						Name:  "state-file",
						Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
					},
				},
			},
			{
				Name:  "history",
				Usage: "List all migration runs, or view details of a specific run",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
					&cli.StringFlag{
						Name:  "run",
						Usage: "Show details for a specific run ID",
					},
					&cli.StringFlag{
						Name:  "state-file",
						Usage: "Use YAML state file instead of SQLite (for Airflow/headless)",
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
							&cli.StringFlag{
								Name:    "config",
								Aliases: []string{"c"},
								Value:   "config.yaml",
								Usage:   "Path to configuration file",
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
				Name:   "health-check",
				Usage:  "Test database connections",
				Action: healthCheck,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
				},
			},
			{
				Name:   "analyze",
				Usage:  "Analyze source database and suggest optimal configuration",
				Action: analyzeConfig,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
				},
			},
			{
				Name:   "calibrate",
				Usage:  "Run calibration tests to find optimal configuration using AI",
				Action: runCalibration,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
						Usage:   "Configuration file path",
					},
					&cli.StringFlag{
						Name:  "profile",
						Usage: "Profile name stored in SQLite",
					},
					&cli.IntFlag{
						Name:  "sample-size",
						Value: 10000,
						Usage: "Number of rows per table for calibration",
					},
					&cli.StringSliceFlag{
						Name:  "tables",
						Usage: "Specific tables to use (default: auto-select)",
					},
					&cli.StringFlag{
						Name:  "output",
						Usage: "Write recommended config to YAML file",
					},
					&cli.BoolFlag{
						Name:    "yes",
						Aliases: []string{"y"},
						Usage:   "Skip confirmation prompt",
					},
					&cli.BoolFlag{
						Name:  "apply",
						Usage: "Update the source config file with recommended values",
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
				Usage:  "Create a secrets file for API keys and encryption",
				Action: initSecrets,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "force",
						Aliases: []string{"f"},
						Usage:   "Overwrite existing secrets file",
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

	// Build orchestrator options
	opts := orchestrator.Options{
		StateFile: getStateFile(c),
		RunID:     c.String("run-id"),
	}

	// Create orchestrator
	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

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
	setupSignalHandler(c, cancel)

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

	opts := orchestrator.Options{
		StateFile:   getStateFile(c),
		ForceResume: c.Bool("force-resume"),
	}

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	// Set up progress reporter if --progress flag is set
	if c.Bool("progress") {
		reporter := progress.NewJSONReporter(os.Stderr, c.Duration("progress-interval"))
		orch.SetProgressReporter(reporter, c.Duration("progress-interval"))
		defer reporter.Close()
	}

	// Handle graceful shutdown with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupSignalHandler(c, cancel)

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
		StateFile: getStateFile(c),
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
		StateFile: getStateFile(c),
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
		StateFile: getStateFile(c),
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

// getStateFile returns the state file path from the context.
// Checks both command-level and global flags.
func getStateFile(c *cli.Context) string {
	// Check command-level flag first, then walk up the context lineage
	for _, ctx := range c.Lineage() {
		if ctx == nil {
			continue
		}
		if sf := ctx.String("state-file"); sf != "" {
			return sf
		}
	}
	return ""
}

func loadConfigWithOrigin(c *cli.Context) (*config.Config, string, string, error) {
	profileName := c.String("profile")
	if profileName != "" {
		cfg, err := loadProfileConfig(profileName)
		return cfg, profileName, "", err
	}

	configPath := c.String("config")
	if _, err := os.Stat(configPath); os.IsNotExist(err) && !c.IsSet("config") {
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
	configPath := c.String("config")
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
func setupSignalHandler(c *cli.Context, cancel context.CancelFunc) {
	shutdownTimeout := c.Duration("shutdown-timeout")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

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
			fmt.Fprintln(os.Stderr, "Shutdown timeout reached, forcing exit...")
			fmt.Fprintf(os.Stderr, "Exit code %d (%s) - safe to retry\n", exitcodes.Cancelled, exitcodes.Description(exitcodes.Cancelled))
			os.Exit(exitcodes.Cancelled)
		})

		// Wait for second signal for immediate exit
		<-sigCh
		shutdownTimer.Stop()
		fmt.Fprintln(os.Stderr, "Second signal received, forcing immediate exit...")
		fmt.Fprintf(os.Stderr, "Exit code %d (%s) - safe to retry\n", exitcodes.Cancelled, exitcodes.Description(exitcodes.Cancelled))
		os.Exit(exitcodes.Cancelled)
	}()
}

// healthCheck tests database connections
func healthCheck(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	opts := orchestrator.Options{
		StateFile: getStateFile(c),
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
			return fmt.Errorf("health check failed")
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

	fmt.Printf("\n  Overall: %s\n", boolToHealthy(result.Healthy))

	if !result.Healthy {
		return fmt.Errorf("health check failed")
	}
	return nil
}

func analyzeConfig(c *cli.Context) error {
	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create source-only orchestrator (no target connection needed)
	opts := orchestrator.Options{
		SourceOnly: true,
	}
	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
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

	return nil
}

func runCalibration(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Check if --apply is valid (requires file-based config, not profile)
	if c.Bool("apply") && configPath == "" {
		return fmt.Errorf("--apply requires a file-based config (not a profile): use -c config.yaml")
	}
	_ = profileName // unused for now

	// Confirmation prompt unless --yes is specified
	if !c.Bool("yes") {
		fmt.Println("\nCalibration will:")
		fmt.Println("  - Run 5 test migrations against your databases")
		fmt.Println("  - Generate significant read load on source")
		fmt.Println("  - Generate write load on target (temp schema)")
		fmt.Println("  - Take approximately 2-5 minutes")
		fmt.Print("\nContinue? [y/N]: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))
		if input != "y" && input != "yes" {
			fmt.Println("Calibration cancelled.")
			return nil
		}
	}

	// Get drivers
	sourceDriver, err := driver.Get(cfg.Source.Type)
	if err != nil {
		return fmt.Errorf("unknown source database type: %s", cfg.Source.Type)
	}
	targetDriver, err := driver.Get(cfg.Target.Type)
	if err != nil {
		return fmt.Errorf("unknown target database type: %s", cfg.Target.Type)
	}

	// Create pools
	const minCalibrationConns = 10
	maxConns := cfg.Migration.Workers * 2
	if maxConns < minCalibrationConns {
		maxConns = minCalibrationConns
	}

	sourcePool, err := pool.NewSourcePool(&cfg.Source, maxConns)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourcePool.Close()

	// Get AI type mapper (required for target pool, optional for calibration analysis)
	var aiMapper *driver.AITypeMapper
	aiMapper, err = driver.NewAITypeMapperFromSecrets()
	if err != nil {
		logging.Warn("AI not configured: %v (calibration will use best-observed configuration)", err)
	}

	// Use AI mapper as type mapper, or create a minimal one if no AI
	var typeMapper driver.TypeMapper
	if aiMapper != nil {
		typeMapper = aiMapper
	} else {
		// Fall back to getting default type mapper
		typeMapper, err = driver.GetAITypeMapper()
		if err != nil {
			return fmt.Errorf("no type mapper available - configure AI provider or run 'dmt init-secrets': %w", err)
		}
	}

	targetPool, err := pool.NewTargetPool(&cfg.Target, maxConns, cfg.Migration.ChunkSize, cfg.Source.Type, typeMapper)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetPool.Close()

	// Create calibrator
	cal := calibration.NewCalibrator(
		cfg,
		sourcePool,
		targetPool,
		sourceDriver,
		targetDriver,
		calibration.CalibratorOptions{
			SampleSize: c.Int("sample-size"),
			Tables:     c.StringSlice("tables"),
			Depth:      calibration.DepthQuick,
			AIMapper:   aiMapper,
		},
	)

	// Run calibration
	ctx := context.Background()
	result, err := cal.Run(ctx)
	if err != nil {
		return fmt.Errorf("calibration failed: %w", err)
	}

	// Display results
	fmt.Println("\n" + result.FormatResultsTable())
	fmt.Println(result.FormatRecommendation())

	// Handle output options
	if c.Bool("apply") {
		// Update the source config file with recommended values
		if err := applyCalibrationToConfig(configPath, result.Recommendation); err != nil {
			return fmt.Errorf("failed to apply calibration: %w", err)
		}
		fmt.Printf("\nConfig updated: %s\n", configPath)
	} else if outputPath := c.String("output"); outputPath != "" {
		yamlContent := result.FormatYAML()
		if err := os.WriteFile(outputPath, []byte(yamlContent), 0600); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		fmt.Printf("\nRecommended config written to: %s\n", outputPath)
	} else {
		fmt.Println("\nRecommended YAML config:")
		fmt.Println(result.FormatYAML())
	}

	return nil
}

// applyCalibrationToConfig updates an existing config file with AI-recommended values.
// It preserves the existing structure, comments, and non-migration settings.
func applyCalibrationToConfig(configPath string, rec *calibration.RecommendedConfig) error {
	if rec == nil {
		return fmt.Errorf("no recommendation available")
	}

	// Read existing file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse as yaml.Node to preserve structure and comments
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Find or create the migration section
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return fmt.Errorf("invalid YAML structure")
	}

	docContent := root.Content[0]
	if docContent.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping at root")
	}

	// Find migration key
	var migrationNode *yaml.Node
	for i := 0; i < len(docContent.Content)-1; i += 2 {
		if docContent.Content[i].Value == "migration" {
			migrationNode = docContent.Content[i+1]
			break
		}
	}

	// Create migration section if it doesn't exist
	if migrationNode == nil {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: "migration"}
		migrationNode = &yaml.Node{Kind: yaml.MappingNode}
		docContent.Content = append(docContent.Content, keyNode, migrationNode)
	}

	// Helper to set a value in the migration mapping
	setMigrationValue := func(key string, value interface{}) {
		// Look for existing key
		for i := 0; i < len(migrationNode.Content)-1; i += 2 {
			if migrationNode.Content[i].Value == key {
				// Update existing value
				migrationNode.Content[i+1].Value = fmt.Sprintf("%v", value)
				return
			}
		}
		// Add new key-value pair
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key}
		valueNode := &yaml.Node{Kind: yaml.ScalarNode, Value: fmt.Sprintf("%v", value)}
		migrationNode.Content = append(migrationNode.Content, keyNode, valueNode)
	}

	// Apply recommended values
	setMigrationValue("chunk_size", rec.ChunkSize)
	setMigrationValue("workers", rec.Workers)
	setMigrationValue("read_ahead_buffers", rec.ReadAheadBuffers)

	if rec.ParallelReaders > 0 {
		setMigrationValue("parallel_readers", rec.ParallelReaders)
	}
	if rec.WriteAheadWriters > 0 {
		setMigrationValue("write_ahead_writers", rec.WriteAheadWriters)
	}
	if rec.PacketSize > 0 {
		setMigrationValue("mssql_packet_size", rec.PacketSize)
	}
	if rec.MaxPartitions > 0 {
		setMigrationValue("max_partitions", rec.MaxPartitions)
	}
	if rec.MaxSourceConnections > 0 {
		setMigrationValue("max_source_connections", rec.MaxSourceConnections)
	}
	if rec.MaxTargetConnections > 0 {
		setMigrationValue("max_target_connections", rec.MaxTargetConnections)
	}
	if rec.LargeTableThreshold > 0 {
		setMigrationValue("large_table_threshold", rec.LargeTableThreshold)
	}
	if rec.MSSQLRowsPerBatch > 0 {
		setMigrationValue("mssql_rows_per_batch", rec.MSSQLRowsPerBatch)
	}
	if rec.UpsertMergeChunkSize > 0 {
		setMigrationValue("upsert_merge_chunk_size", rec.UpsertMergeChunkSize)
	}

	// Marshal back to YAML
	var buf strings.Builder
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(&root); err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}
	encoder.Close()

	// Write back to file
	if err := os.WriteFile(configPath, []byte(buf.String()), 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
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

	// Generate template
	template := secrets.GenerateTemplate()

	// Write with secure permissions
	if err := os.WriteFile(secretsPath, []byte(template), 0600); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	fmt.Printf("Secrets file created: %s\n", secretsPath)
	fmt.Printf("Directory: %s (permissions: 0700)\n", secretsDir)
	fmt.Println("\nNext steps:")
	fmt.Println("1. Edit the file to add your AI provider API key")
	fmt.Println("2. Set encryption.master_key for profile encryption:")
	fmt.Println("   Generate with: openssl rand -base64 32")
	fmt.Println("\nIMPORTANT: Keep this file secure and never commit it to version control!")

	return nil
}

// runCLIWizard runs an interactive CLI wizard to create a config
func runCLIWizard(advanced bool) (*config.Config, error) {
	reader := bufio.NewReader(os.Stdin)

	prompt := func(label, defaultValue string) string {
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

	promptInt := func(label string, defaultValue int) int {
		result := prompt(label, fmt.Sprintf("%d", defaultValue))
		if val, err := fmt.Sscanf(result, "%d", &defaultValue); err != nil || val != 1 {
			return defaultValue
		}
		return defaultValue
	}

	promptBool := func(label string, defaultValue bool) bool {
		defStr := "n"
		if defaultValue {
			defStr = "y"
		}
		result := strings.ToLower(prompt(label+" (y/n)", defStr))
		return result == "y" || result == "yes"
	}

	promptPassword := func(label string) string {
		fmt.Printf("%s: ", label)
		password, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Println() // newline after hidden input
		if err != nil {
			return ""
		}
		return string(password)
	}

	promptChoice := func(label string, choices []string, defaultValue string) string {
		for {
			result := prompt(label, defaultValue)
			for _, choice := range choices {
				if result == choice {
					return result
				}
			}
			fmt.Printf("  Invalid choice. Options: %s\n", strings.Join(choices, ", "))
		}
	}

	cfg := &config.Config{}

	dbTypes := []string{"mssql", "postgres"}
	targetModes := []string{"drop_recreate", "upsert"}

	fmt.Println("\n=== Source Database ===")
	cfg.Source.Type = promptChoice("Database type (mssql/postgres)", dbTypes, "mssql")
	cfg.Source.Host = prompt("Host", "localhost")
	defaultPort := 1433
	if cfg.Source.Type == "postgres" {
		defaultPort = 5432
	}
	cfg.Source.Port = promptInt("Port", defaultPort)
	cfg.Source.Database = prompt("Database name", "")
	cfg.Source.User = prompt("Username", "sa")
	cfg.Source.Password = promptPassword("Password")
	cfg.Source.Schema = prompt("Schema", "dbo")

	fmt.Println("\n=== Target Database ===")
	cfg.Target.Type = promptChoice("Database type (mssql/postgres)", dbTypes, "postgres")
	cfg.Target.Host = prompt("Host", "localhost")
	defaultPort = 5432
	if cfg.Target.Type == "mssql" {
		defaultPort = 1433
	}
	cfg.Target.Port = promptInt("Port", defaultPort)
	cfg.Target.Database = prompt("Database name", "")
	cfg.Target.User = prompt("Username", "postgres")
	cfg.Target.Password = promptPassword("Password")
	cfg.Target.Schema = prompt("Schema", "public")

	fmt.Println("\n=== Migration Settings ===")
	cfg.Migration.TargetMode = promptChoice("Target mode (drop_recreate/upsert)", targetModes, "drop_recreate")
	cfg.Migration.CreateIndexes = promptBool("Create indexes", false)
	cfg.Migration.CreateForeignKeys = promptBool("Create foreign keys", false)

	if advanced {
		fmt.Println("\n=== Advanced Settings ===")
		cfg.Migration.Workers = promptInt("Workers", 6)
		cfg.Migration.ChunkSize = promptInt("Chunk size", 100000)
	}

	return cfg, nil
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
