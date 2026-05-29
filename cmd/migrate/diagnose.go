package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func diagnoseMigration(c *cli.Context) error {
	if c.Bool("json") {
		logging.SetOutput(os.Stderr)
	}

	cfg, _, _, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	opts := orchestrator.Options{
		StateFile: c.String("state-file"),
	}

	orch, err := orchestrator.NewDiagnosticsWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	commandCtx := c.Context
	if commandCtx == nil {
		commandCtx = context.Background()
	}
	timeout := c.Duration("timeout")
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(commandCtx, timeout)
	defer cancel()

	review, err := orch.DiagnoseRun(ctx, c.String("run"), c.Bool("ai-triage"))
	if err != nil {
		return err
	}
	if c.Bool("json") {
		return printTriageReviewJSON(review)
	}
	printTriageReview(review)
	return nil
}
