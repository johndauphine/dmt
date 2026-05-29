package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"

	"github.com/urfave/cli/v2"
)

func analyzeConfig(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	if c.Bool("apply") && configPath == "" {
		return fmt.Errorf("analyze --apply requires a config file; profile %q cannot be updated in place", profileName)
	}

	// Try full orchestrator first (with both source and target)
	orch, err := orchestrator.New(cfg)
	if err != nil {
		// Full connection failed - try source-only mode for analyze
		logging.Warn("Cannot connect to both databases: %v", err)
		logging.Info("Attempting source-only analysis...")

		orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{SourceOnly: true})
		if err != nil {
			return fmt.Errorf("cannot connect to source database: %w", err)
		}
		logging.Info("Connected to source database")
		logging.Warn("Target database unavailable - tuning recommendations may be less accurate")
	}
	defer orch.Close()

	// Get schema from config (default to dbo/public based on source type)
	schema := cfg.Source.Schema
	if schema == "" {
		if cfg.Source.Type == "postgres" {
			schema = "public"
		} else {
			schema = "dbo"
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Run smart config analysis
	suggestions, err := orch.AnalyzeConfig(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to analyze config: %w", err)
	}

	var explanation *aicopilot.PerformanceExplanation
	if c.Bool("ai-explain") {
		reviewCtx, reviewCancel := context.WithTimeout(commandContext(c), 90*time.Second)
		defer reviewCancel()
		explanation = orch.ExplainPerformanceWithAI(reviewCtx, suggestions)
	}

	// Output suggestions
	fmt.Println(suggestions.FormatYAML())
	printAIPerformanceExplanation(explanation)

	// Apply AI-tuned parameters to the analyzed config file if requested.
	if c.Bool("apply") {
		if err := config.ApplyTuningToConfigFile(configPath, suggestions); err != nil {
			return fmt.Errorf("failed to apply tuning: %w", err)
		}
		fmt.Printf("\n✓ Applied AI-tuned parameters to %s\n", configPath)
	}

	return nil
}

func commandContext(c *cli.Context) context.Context {
	if c.Context != nil {
		return c.Context
	}
	return context.Background()
}

func printAIPerformanceExplanation(explanation *aicopilot.PerformanceExplanation) {
	if explanation == nil {
		return
	}
	fmt.Println("\nAI performance explanation:")
	fmt.Printf("  Status: %s\n", explanation.Status)
	if explanation.Provider != "" {
		fmt.Printf("  Provider: %s", explanation.Provider)
		if explanation.Model != "" {
			fmt.Printf(" / %s", explanation.Model)
		}
		fmt.Println()
	}
	if explanation.Summary != "" {
		fmt.Printf("  Summary: %s\n", explanation.Summary)
	}
	if explanation.Error != "" {
		fmt.Printf("  Error: %s\n", logging.Scrub(explanation.Error))
	}
	if len(explanation.Findings) > 0 {
		fmt.Println("  AI advisory findings:")
		for _, finding := range explanation.Findings {
			category := finding.Category
			if category == "" {
				category = "other"
			}
			fmt.Printf("    - [%s] %s: %s\n", strings.ToUpper(category), finding.Knob, finding.Rationale)
			if len(finding.Evidence) > 0 {
				fmt.Println("      evidence:")
				for _, evidence := range finding.Evidence {
					fmt.Printf("        - %s\n", evidence)
				}
			}
			if finding.NextAction != "" {
				fmt.Printf("      next: %s\n", finding.NextAction)
			}
		}
	}
	if len(explanation.Notes) > 0 {
		fmt.Println("  Notes:")
		for _, note := range explanation.Notes {
			fmt.Printf("    - %s\n", note)
		}
	}
}
