package main

import (
	"context"
	"fmt"
	"os"

	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/progress"

	"github.com/urfave/cli/v2"
)

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

	outputOrPrintMigrationResult(c, orch, runErr)

	return runErr
}
