package main

import (
	"fmt"

	"github.com/johndauphine/dmt/v5/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func showHistory(c *cli.Context) error {
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

	// If --run flag is provided, show details for that specific run
	if runID := c.String("run"); runID != "" {
		return orch.ShowRunDetails(runID)
	}

	return orch.ShowHistory()
}

// getConfigPath returns the config path from the context lineage.
// Because --config has a non-empty default ("config.yaml"), we use IsSet
// to find the first context where the user explicitly provided the flag.
