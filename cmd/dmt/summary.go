package main

import (
	"fmt"
	"os"

	"github.com/johndauphine/dmt/v5/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func printMigrationSummary(result *orchestrator.MigrationResult) {
	fmt.Print(orchestrator.FormatMigrationSummary(result))
}

func outputOrPrintMigrationResult(c *cli.Context, orch *orchestrator.Orchestrator, runErr error) {
	result, err := orch.GetLastRunResult()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to get run result: %v\n", err)
		return
	}
	if runErr != nil {
		result.Error = runErr.Error()
	}

	if c.Bool("output-json") || c.String("output-file") != "" {
		if err := outputJSON(c, result); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to output JSON: %v\n", err)
		}
		return
	}
	printMigrationSummary(result)
}
