// Secrets-file bootstrap shared by the CLI and the TUI (#443). The
// template choice and the next-steps guidance must read identically on
// both surfaces, so the whole action lives here.
package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/johndauphine/dmt/internal/secrets"
)

// InitSecrets creates ~/.secrets/dmt-config.yaml and returns the
// human-readable result text. The default template is AI-free; withAI
// seeds the AI provider section. AI is optional in dmt (since #167) —
// the deterministic type mapper, error diagnosis catalog, and DB
// tuning analyzer all run without it. Refuses to overwrite an existing
// file unless force is set.
func InitSecrets(force, withAI bool) (string, error) {
	secretsDir, err := secrets.EnsureSecretsDir()
	if err != nil {
		return "", fmt.Errorf("creating secrets directory: %w", err)
	}

	secretsPath := secrets.GetSecretsPath()

	if !force {
		if _, err := os.Stat(secretsPath); err == nil {
			return "", fmt.Errorf("secrets file %s already exists (use --force to overwrite)", secretsPath)
		}
	}

	var template string
	if withAI {
		template = secrets.GenerateTemplateWithAI()
	} else {
		template = secrets.GenerateTemplate()
	}

	if err := os.WriteFile(secretsPath, []byte(template), 0600); err != nil {
		return "", fmt.Errorf("writing secrets file: %w", err)
	}
	// Drop the package-level cache so a session that already called
	// secrets.Load() (e.g. an earlier /run with no secrets file) sees
	// the new file immediately — mirrors secrets.Save (codex).
	secrets.Reset()

	var b strings.Builder
	fmt.Fprintf(&b, "Secrets file created: %s\n", secretsPath)
	fmt.Fprintf(&b, "Directory: %s (permissions: 0700)\n", secretsDir)
	b.WriteString("\nNext steps:\n")
	b.WriteString("1. Set encryption.master_key for profile encryption:\n")
	b.WriteString("   Generate with: openssl rand -base64 32\n")
	if withAI {
		b.WriteString("2. Edit the file to add your AI provider API key (required for AI features to work; leave blank to skip a provider)\n")
		b.WriteString("3. You're ready to run `dmt run --config config.yaml`\n")
	} else {
		b.WriteString("2. You're ready to run `dmt run --config config.yaml`\n")
		b.WriteString("\nAI features are OPTIONAL. To opt in later, APPEND an ai: section to\n")
		b.WriteString("the file manually — do NOT run --force --with-ai, which would overwrite\n")
		b.WriteString("any master_key / slack webhook values you set above.\n")
	}
	b.WriteString("\nIMPORTANT: Keep this file secure and never commit it to version control!\n")
	return b.String(), nil
}
