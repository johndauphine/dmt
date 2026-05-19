package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/progress"

	"github.com/urfave/cli/v2"
)

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

	outputOrPrintMigrationResult(c, orch, runErr)

	return runErr
}
