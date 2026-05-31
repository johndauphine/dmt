package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/urfave/cli/v2"
)

func aiAdvisoryEvals(c *cli.Context) error {
	if c.Bool("list") {
		for _, scenario := range aicopilot.AdvisoryEvalScenarios() {
			fmt.Printf("%s\t%s\t%s\n", scenario.ID, scenario.Kind, scenario.Description)
		}
		return nil
	}
	if !c.Bool("live") {
		return fmt.Errorf("live AI evals are opt-in; rerun with --live after confirming provider/model credentials")
	}
	client, err := driver.NewAITypeMapperFromSecrets()
	if err != nil {
		return err
	}
	timeout := c.Duration("timeout")
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(c.Context, timeout)
	defer cancel()
	report, err := aicopilot.RunAdvisoryEvals(ctx, client, aicopilot.AdvisoryEvalOptions{ScenarioIDs: c.StringSlice("scenario")})
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal advisory eval report: %w", err)
	}
	if outputFile := c.String("output-file"); outputFile != "" {
		if err := os.WriteFile(outputFile, append(data, '\n'), 0600); err != nil {
			return fmt.Errorf("write advisory eval report: %w", err)
		}
	} else {
		fmt.Println(string(data))
	}
	if !report.Passed {
		return fmt.Errorf("AI advisory evals failed")
	}
	return nil
}
