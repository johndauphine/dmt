package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/secrets"

	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

func initConfig(c *cli.Context) error {
	outputPath := c.String("output")
	advanced := c.Bool("advanced")
	force := c.Bool("force")

	// Check if file exists (unless --force)
	if !force {
		if _, err := os.Stat(outputPath); err == nil {
			return fmt.Errorf("file %s already exists (use --force to overwrite)", outputPath)
		}
	}

	cfg, err := runCLIWizard(advanced)
	if err != nil {
		return err
	}

	// Marshal and write
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("generating config: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	fmt.Printf("Configuration saved to %s\n", outputPath)
	return nil
}

// initSecrets creates a secrets file for API keys and encryption
func initSecrets(c *cli.Context) error {
	force := c.Bool("force")
	withAI := c.Bool("with-ai")

	// Ensure secrets directory exists
	secretsDir, err := secrets.EnsureSecretsDir()
	if err != nil {
		return fmt.Errorf("creating secrets directory: %w", err)
	}

	secretsPath := secrets.GetSecretsPath()

	// Check if file exists (unless --force)
	if !force {
		if _, err := os.Stat(secretsPath); err == nil {
			return fmt.Errorf("secrets file %s already exists (use --force to overwrite)", secretsPath)
		}
	}

	// Default template is AI-free; --with-ai seeds the AI provider
	// section. AI is optional in dmt (since #167) — the deterministic
	// type mapper, error diagnosis catalog, and DB tuning analyzer all
	// run without it.
	var template string
	if withAI {
		template = secrets.GenerateTemplateWithAI()
	} else {
		template = secrets.GenerateTemplate()
	}

	// Write with secure permissions
	if err := os.WriteFile(secretsPath, []byte(template), 0600); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	fmt.Printf("Secrets file created: %s\n", secretsPath)
	fmt.Printf("Directory: %s (permissions: 0700)\n", secretsDir)
	fmt.Println("\nNext steps:")
	fmt.Println("1. Set encryption.master_key for profile encryption:")
	fmt.Println("   Generate with: openssl rand -base64 32")
	if withAI {
		fmt.Println("2. Edit the file to add your AI provider API key (required for AI features to work; leave blank to skip a provider)")
		fmt.Println("3. You're ready to run `dmt run --config config.yaml`")
	} else {
		fmt.Println("2. You're ready to run `dmt run --config config.yaml`")
		fmt.Println("\nAI features are OPTIONAL. To opt in later, APPEND an ai: section to")
		fmt.Println("the file manually — do NOT run --force --with-ai, which would overwrite")
		fmt.Println("any master_key / slack webhook values you set above.")
	}
	fmt.Println("\nIMPORTANT: Keep this file secure and never commit it to version control!")

	return nil
}

// CLI prompt helpers

func cliPrompt(reader *bufio.Reader, label, defaultValue string) string {
	if defaultValue != "" {
		fmt.Printf("%s [%s]: ", label, defaultValue)
	} else {
		fmt.Printf("%s: ", label)
	}
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultValue
	}
	return input
}

func cliPromptInt(reader *bufio.Reader, label string, defaultValue int) int {
	result := cliPrompt(reader, label, fmt.Sprintf("%d", defaultValue))
	if val, err := fmt.Sscanf(result, "%d", &defaultValue); err != nil || val != 1 {
		return defaultValue
	}
	return defaultValue
}

func cliPromptBool(reader *bufio.Reader, label string, defaultValue bool) bool {
	defStr := "n"
	if defaultValue {
		defStr = "y"
	}
	result := strings.ToLower(cliPrompt(reader, label+" (y/n)", defStr))
	return result == "y" || result == "yes"
}

func cliPromptPassword(label string) string {
	fmt.Printf("%s: ", label)
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // newline after hidden input
	if err != nil {
		return ""
	}
	return string(password)
}

func cliPromptChoice(reader *bufio.Reader, label string, choices []string, defaultValue string) string {
	for {
		result := cliPrompt(reader, label, defaultValue)
		for _, choice := range choices {
			if result == choice {
				return result
			}
		}
		fmt.Printf("  Invalid choice. Options: %s\n", strings.Join(choices, ", "))
	}
}

// runCLIWizard runs an interactive CLI wizard to create a config
func runCLIWizard(advanced bool) (*config.Config, error) {
	reader := bufio.NewReader(os.Stdin)

	cfg := &config.Config{}

	dbTypes := []string{"mssql", "postgres"}
	targetModes := []string{"drop_recreate", "upsert"}

	fmt.Println("\n=== Source Database ===")
	cfg.Source.Type = cliPromptChoice(reader, "Database type (mssql/postgres)", dbTypes, "mssql")
	cfg.Source.Host = cliPrompt(reader, "Host", "localhost")
	defaultPort := 1433
	if cfg.Source.Type == "postgres" {
		defaultPort = 5432
	}
	cfg.Source.Port = cliPromptInt(reader, "Port", defaultPort)
	cfg.Source.Database = cliPrompt(reader, "Database name", "")
	cfg.Source.User = cliPrompt(reader, "Username", "sa")
	cfg.Source.Password = cliPromptPassword("Password")
	cfg.Source.Schema = cliPrompt(reader, "Schema", "dbo")

	fmt.Println("\n=== Target Database ===")
	cfg.Target.Type = cliPromptChoice(reader, "Database type (mssql/postgres)", dbTypes, "postgres")
	cfg.Target.Host = cliPrompt(reader, "Host", "localhost")
	defaultPort = 5432
	if cfg.Target.Type == "mssql" {
		defaultPort = 1433
	}
	cfg.Target.Port = cliPromptInt(reader, "Port", defaultPort)
	cfg.Target.Database = cliPrompt(reader, "Database name", "")
	cfg.Target.User = cliPrompt(reader, "Username", "postgres")
	cfg.Target.Password = cliPromptPassword("Password")
	cfg.Target.Schema = cliPrompt(reader, "Schema", "public")

	fmt.Println("\n=== Migration Settings ===")
	cfg.Migration.TargetMode = cliPromptChoice(reader, "Target mode (drop_recreate/upsert)", targetModes, "drop_recreate")
	createIndexes := cliPromptBool(reader, "Create indexes", true)
	cfg.Migration.CreateIndexes = &createIndexes
	createForeignKeys := cliPromptBool(reader, "Create foreign keys", true)
	cfg.Migration.CreateForeignKeys = &createForeignKeys

	if advanced {
		fmt.Println("\n=== Advanced Settings ===")
		cfg.Migration.Workers = cliPromptInt(reader, "Workers", 6)
		cfg.Migration.ChunkSize = cliPromptInt(reader, "Chunk size", 100000)
	}

	return cfg, nil
}
