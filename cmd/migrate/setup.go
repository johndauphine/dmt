package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/setup"

	"github.com/urfave/cli/v2"
)

func runSetup(c *cli.Context) error {
	reader := bufio.NewReader(os.Stdin)
	state := setup.NewState()

	// Seed Slack webhook from the global secrets file so edit mode shows
	// the current value as Enter-to-keep. Capture Original too so a
	// failed write can revert without leaving a misleading "configured"
	// hint.
	loadedWebhook := setup.LoadExistingSlackWebhook()
	state.SlackWebhook = loadedWebhook
	state.SlackWebhookOriginal = loadedWebhook

	if c.IsSet("output") {
		state.ConfigPath = c.String("output")
	}
	state.Force = c.Bool("force")

	// If the target config already exists, load it so the wizard offers
	// edit-vs-new and seeds each prompt with the user's prior values.
	// LoadRaw (not Load) — placeholders like ${env:PASS} must survive a
	// re-save so the wizard never writes resolved secrets to disk.
	if _, err := os.Stat(state.ConfigPath); err == nil {
		if cfg, err := config.LoadRaw(state.ConfigPath); err == nil {
			state.Config = *cfg
			state.CurrentStep = setup.StepEditOrNew
			state.EditMode = true
		}
	}

	fmt.Println("\n=== DMT Setup Wizard ===")

	for state.CurrentStep != setup.StepDone {
		info := state.Prompt()

		// Display section header
		if info.SectionHeader != "" {
			fmt.Printf("\n=== %s ===\n", info.SectionHeader)
		}

		if info.IsAutoAction {
			var result string
			switch state.CurrentStep {
			case setup.StepCheckSecrets:
				result = setup.CheckExistingSecrets()
				if result == "has_ai" {
					fmt.Println("  AI provider already configured, skipping AI setup")
				}

			case setup.StepSourceConnTest, setup.StepTargetConnTest:
				fmt.Printf("  %s\n", info.Text)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				// Resolve ${env:...} / ${file:...} placeholders for the side
				// being tested only — an unrelated missing template on the
				// other side must not poison this test. state.Config keeps
				// the raw form so a re-save preserves placeholders.
				var connResult *setup.ConnTestResult
				if state.CurrentStep == setup.StepSourceConnTest {
					conn := state.SourceConnConfig()
					connResult = setup.TestConnection(ctx,
						conn.Source.Type, conn.Source.Host,
						conn.Source.Port, conn.Source.Database,
						conn.Source.User, conn.Source.Password,
						conn.Source.DSNOptions())
				} else {
					conn := state.TargetConnConfig()
					connResult = setup.TestConnection(ctx,
						conn.Target.Type, conn.Target.Host,
						conn.Target.Port, conn.Target.Database,
						conn.Target.User, conn.Target.Password,
						conn.Target.DSNOptions())
				}
				cancel()

				if connResult.Connected {
					fmt.Printf("  Connected! (%dms)\n", connResult.LatencyMs)
					result = ""
				} else {
					fmt.Printf("  Failed: %s (%dms)\n", connResult.Error, connResult.LatencyMs)
					result = connResult.Error
				}

			case setup.StepWriteSecrets:
				if err := state.WriteSecretsFile(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else {
					fmt.Printf("  Secrets saved to %s\n", secrets.GetSecretsPath())
				}

			case setup.StepWriteSlackSecret:
				if err := state.WriteSlackSecret(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else if state.SlackWebhook == "" {
					fmt.Println("  Slack webhook cleared")
				} else {
					fmt.Printf("  Slack webhook saved to %s\n", secrets.GetSecretsPath())
				}

			case setup.StepWriteConfig:
				// Check if file exists (unless --force)
				if !state.Force {
					if _, err := os.Stat(state.ConfigPath); err == nil {
						fmt.Printf("  File %s already exists. Overwrite? (y/n) [n]: ", state.ConfigPath)
						answer, _ := reader.ReadString('\n')
						if strings.ToLower(strings.TrimSpace(answer)) != "y" {
							state.CurrentStep = setup.StepConfigPath
							continue
						}
					}
				}
				if err := state.WriteConfigFile(); err != nil {
					result = err.Error()
					fmt.Printf("  Error: %s\n", result)
				} else {
					fmt.Printf("  Configuration saved to %s\n", state.ConfigPath)
				}
			}

			if errMsg := state.Process(result); errMsg != "" {
				fmt.Printf("  Error: %s\n", errMsg)
			}
			continue
		}

		// User input step - loop until valid
		for {
			var input string
			if info.IsMasked {
				input = cliPromptPassword(info.Text)
			} else {
				input = cliPrompt(reader, info.Text, info.Default)
			}

			if errMsg := state.Process(input); errMsg != "" {
				fmt.Printf("  %s\n", errMsg)
				continue
			}
			break
		}
	}

	fmt.Println("\nSetup complete!")

	// Final summary — distinguish "AI: not configured (optional)" from
	// "AI: configured" so users see explicitly that the AI-free path is
	// supported rather than a degraded mode (#174).
	if state.AIConfigured {
		fmt.Println("  AI: configured")
	} else {
		fmt.Println("  AI: not configured (optional; to enable later, append an ai: section to ~/.secrets/dmt-config.yaml — see README § Optional AI Enhancements)")
	}

	// Run AI analysis if requested
	if state.RunAnalysis {
		fmt.Println("\nRunning AI analysis...")
		cfg, err := config.Load(state.ConfigPath)
		if err != nil {
			fmt.Printf("Error loading config for analysis: %v\n", err)
			return nil
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			// Try source-only
			logging.Warn("Cannot connect to both databases: %v", err)
			orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{SourceOnly: true})
			if err != nil {
				fmt.Printf("Cannot connect for analysis: %v\n", err)
				return nil
			}
		}
		defer orch.Close()

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

		suggestions, err := orch.AnalyzeConfig(ctx, schema)
		if err != nil {
			fmt.Printf("Analysis failed: %v\n", err)
			return nil
		}

		fmt.Println(suggestions.FormatYAML())

		answer := cliPrompt(reader, "Apply tuning to config file? (y/n)", "n")
		if strings.ToLower(answer) == "y" {
			if err := config.ApplyTuningToConfigFile(state.ConfigPath, suggestions); err != nil {
				fmt.Printf("Error applying tuning: %v\n", err)
			} else {
				fmt.Printf("Applied AI-tuned parameters to %s\n", state.ConfigPath)
			}
		}
	}

	return nil
}
