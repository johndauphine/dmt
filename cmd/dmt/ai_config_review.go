package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/command"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"

	"github.com/urfave/cli/v2"
)

func aiConfigReview(c *cli.Context) error {
	if aiConfigReviewJSONRequested(c) || aiConfigReviewOutputFile(c) != "" {
		logging.SetOutput(os.Stderr)
	}

	cfg, profileName, configPath, err := loadConfigWithOrigin(c)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	payload := aicopilot.BuildConfigReviewPayload(cfg, aicopilot.ConfigReviewOptions{
		OperatorRequest: c.String("request"),
		ConfigPath:      configPath,
		ProfileName:     profileName,
		StateFile:       aiConfigReviewString(c, "state-file"),
	})

	var review *aicopilot.ConfigReview
	client := driver.GetAIMapper()
	if aicopilot.IsNilTextClient(client) {
		review = aicopilot.UnavailableConfigReview("no AI provider configured in secrets", payload)
	} else {
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

		generated, err := aicopilot.GenerateConfigReview(ctx, client, payload)
		if err != nil {
			review = aicopilot.ErrorConfigReview(client.ProviderName(), client.Model(), err, payload)
			logging.WarnEvent("AI config review failed",
				"provider", client.ProviderName(),
				"model", client.Model(),
				"error", review.Error,
			)
		} else {
			review = generated
		}
	}

	return outputAIConfigReview(c, review)
}

func outputAIConfigReview(c *cli.Context, review *aicopilot.ConfigReview) error {
	jsonRequested := aiConfigReviewJSONRequested(c)
	outputFile := aiConfigReviewOutputFile(c)
	if jsonRequested || outputFile != "" {
		data, err := json.MarshalIndent(review, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal AI config review: %w", err)
		}
		if jsonRequested {
			fmt.Println(string(data))
		}
		if outputFile != "" {
			if err := os.WriteFile(outputFile, data, 0600); err != nil {
				return fmt.Errorf("failed to write output file: %w", err)
			}
			if !jsonRequested {
				printAIConfigReview(review)
				fmt.Printf("\nWrote AI config review JSON to %s\n", outputFile)
			} else {
				fmt.Fprintf(os.Stderr, "Wrote AI config review JSON to %s\n", outputFile)
			}
		}
		return nil
	}

	printAIConfigReview(review)
	return nil
}

func aiConfigReviewJSONRequested(c *cli.Context) bool {
	return aiConfigReviewBool(c, "json") || aiConfigReviewBool(c, "output-json")
}

func aiConfigReviewOutputFile(c *cli.Context) string {
	return aiConfigReviewString(c, "output-file")
}

func aiConfigReviewString(c *cli.Context, name string) string {
	for _, ctx := range c.Lineage() {
		if ctx == nil || !flagExplicitlySet(ctx, name) {
			continue
		}
		return ctx.String(name)
	}
	return c.String(name)
}

func aiConfigReviewBool(c *cli.Context, name string) bool {
	for _, ctx := range c.Lineage() {
		if ctx == nil || !flagExplicitlySet(ctx, name) {
			continue
		}
		return ctx.Bool(name)
	}
	return c.Bool(name)
}

func flagExplicitlySet(c *cli.Context, name string) bool {
	set := false
	for _, flagName := range c.LocalFlagNames() {
		if flagName == name {
			set = c.IsSet(name)
			break
		}
	}
	return set
}

// printAIConfigReview renders via the shared CLI/TUI formatter (#442).
func printAIConfigReview(review *aicopilot.ConfigReview) {
	fmt.Print(command.FormatConfigReview(review))
}
