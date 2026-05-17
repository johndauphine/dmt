package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

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
