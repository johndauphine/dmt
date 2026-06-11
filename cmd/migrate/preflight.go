package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmt/internal/command"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

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

	commandCtx := c.Context
	if commandCtx == nil {
		commandCtx = context.Background()
	}

	ctx, cancel := context.WithTimeout(commandCtx, 30*time.Second)
	defer cancel()

	result, err := orch.HealthCheck(ctx)
	if err != nil {
		return err
	}
	if c.Bool("ai-review") {
		reviewCtx, reviewCancel := context.WithTimeout(commandCtx, 90*time.Second)
		defer reviewCancel()
		result.AIPreflightReview = orch.ReviewPreflightWithAI(reviewCtx, result)
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

	// Human-readable output -- same renderer the TUI uses (#440).
	fmt.Print(command.FormatHealthCheckResult(result))

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
