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

func validateMigration(c *cli.Context) error {
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

	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	defer orch.Close()

	commandCtx := c.Context
	if commandCtx == nil {
		commandCtx = context.Background()
	}
	if c.Bool("ai-triage") {
		result, validateErr := orch.ValidateDetailed(commandCtx)
		timeout := c.Duration("timeout")
		if timeout <= 0 {
			timeout = 90 * time.Second
		}
		reviewCtx, cancel := context.WithTimeout(commandCtx, timeout)
		defer cancel()
		review := orch.ReviewValidationWithAI(reviewCtx, result, validateErr)
		if review != nil {
			if c.Bool("json") {
				if err := printTriageReviewJSON(review); err != nil {
					return err
				}
			} else {
				printTriageReview(review)
			}
		}
		return validateErr
	}

	return orch.Validate(commandCtx)
}
