package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
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
								Name:     "name",
								Aliases:  []string{"n"},
								Required: true,
								Usage:    "Profile name",
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
		fmt.Println("\nInterrupted. Saving checkpoint...")
		cancel()
	}()

	// Run migration
	return orch.Run(ctx)
}

func resumeMigration(c *cli.Context) error {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nInterrupted. Saving checkpoint...")
		cancel()
	}()

	return orch.Resume(ctx)
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
	// First check command-level flag
	if sf := c.String("state-file"); sf != "" {
		return sf
	}
	// Fall back to global flag
	return c.String("state-file")
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
