package main

import (
	"encoding/json"
	"fmt"

	"github.com/johndauphine/dmt/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func showStatus(c *cli.Context) error {
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

	// JSON output
	if c.Bool("json") {
		result, err := orch.GetStatusResult()
		if err != nil {
			// Return empty status for no active migration
			emptyResult := &orchestrator.StatusResult{
				Status: "no_active_migration",
			}
			data, _ := json.MarshalIndent(emptyResult, "", "  ")
			fmt.Println(string(data))
			return nil
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal status: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}

	return orch.ShowStatus()
}
