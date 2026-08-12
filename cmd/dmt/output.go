package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/johndauphine/dmt/v5/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func outputJSON(c *cli.Context, result *orchestrator.MigrationResult) error {
	return outputJSONValue(c, result)
}

func outputJSONValue(c *cli.Context, result any) error {
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
//
// The cleanup function is called before forced exit to close database connections
// and other resources. This ensures we're a good citizen and don't leave orphaned
