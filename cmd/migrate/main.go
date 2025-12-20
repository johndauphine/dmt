package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/orchestrator"
	"github.com/urfave/cli/v2"
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
				},
			},
			{
				Name:   "resume",
				Usage:  "Resume an interrupted migration",
				Action: resumeMigration,
			},
			{
				Name:   "status",
				Usage:  "Show status of current/last run",
				Action: showStatus,
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runMigration(c *cli.Context) error {
	cfg, err := config.Load(c.String("config"))
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

	// Create orchestrator
	orch, err := orchestrator.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

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
	cfg, err := config.Load(c.String("config"))
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	orch, err := orchestrator.New(cfg)
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
	cfg, err := config.Load(c.String("config"))
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	orch, err := orchestrator.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	return orch.ShowStatus()
}

func validateMigration(c *cli.Context) error {
	cfg, err := config.Load(c.String("config"))
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	orch, err := orchestrator.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	return orch.Validate(context.Background())
}

func showHistory(c *cli.Context) error {
	cfg, err := config.Load(c.String("config"))
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	orch, err := orchestrator.New(cfg)
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
