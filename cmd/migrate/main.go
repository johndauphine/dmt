package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/orchestrator"
	"github.com/johndauphine/mssql-pg-migrate/internal/tui"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var version = "dev"

func main() {
	app := &cli.App{
		Name:    "mssql-pg-migrate",
		Usage:   "High-performance MSSQL to PostgreSQL migration",
		Version: version,
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
				},
			},
			{
				Name:   "resume",
				Usage:  "Resume an interrupted migration",
				Action: resumeMigration,
				Flags: []cli.Flag{
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
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

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted. Saving checkpoint...")
		cancel()
	}()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted. Saving checkpoint...")
		cancel()
	}()

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
		if strings.Contains(err.Error(), "MSSQL_PG_MIGRATE_MASTER_KEY is not set") {
			return fmt.Errorf("MSSQL_PG_MIGRATE_MASTER_KEY is not set; set it before saving profiles")
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
