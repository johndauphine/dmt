package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"regexp"
)

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	return LoadWithOptions(path, LoadOptions{})
}

// LoadWithOptions reads configuration from a YAML file with options.
func LoadWithOptions(path string, opts LoadOptions) (*Config, error) {
	// Check file permissions before reading (warns if insecure)
	if warning := checkFilePermissions(path); warning != "" && !opts.SuppressWarnings {
		fmt.Fprint(os.Stderr, warning)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	return LoadBytes(data)
}

// expandYAMLTemplates expands ${file:path} and ${env:VAR} templates in YAML string.
// Also supports legacy ${VAR} syntax for backward compatibility.
// This runs before YAML parsing to allow templates in any field.
//
// Security note: File paths are not restricted - users should only use trusted paths
// like /run/secrets/ for Docker secrets. Avoid user-controlled paths.
func expandYAMLTemplates(yamlStr string) (string, error) {
	// Pattern to match ${file:path}, ${env:VAR}, or ${VAR} in YAML
	// - file: allows any path characters (user responsibility to use safe paths)
	// - env: restricted to valid env var names [A-Za-z_][A-Za-z0-9_]*
	// - legacy ${VAR}: also restricted to valid env var names
	pattern := regexp.MustCompile(`\$\{file:([^}]+)\}|\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

	var firstErr error
	result := pattern.ReplaceAllStringFunc(yamlStr, func(match string) string {
		// If we've already encountered an error, leave subsequent matches unchanged
		if firstErr != nil {
			return match
		}

		expanded, err := expandTemplateValue(match)
		if err != nil {
			firstErr = err
			return match // Keep original on error
		}
		return expanded
	})

	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}

// Expand resolves a single ${env:...}, ${file:...}, or legacy ${VAR}
// template in s. Non-template strings are returned unchanged. Pair with
// LoadRaw: callers who need a resolved single field (e.g. just the
// source-side password for a connection test) can scope expansion to
// one value at a time instead of rolling up the whole config.
func Expand(s string) (string, error) {
	return expandTemplateValue(s)
}

// LoadRaw reads a config YAML for *editing* — no secret-template expansion,
// no defaults application, no validation. Placeholders like ${env:DB_PASSWORD}
// and ${file:/run/secrets/x} survive as literal strings so the setup wizard
// can re-marshal them back to disk without exposing resolved secrets.
//
// This is intentionally narrow: the migration runtime must always use Load /
// LoadWithOptions so that templates resolve before connection attempts.
//
// Known limitations (each tracked as a follow-up):
//   - Templated non-string scalars (e.g. `source.port: ${env:DB_PORT}` or
//     `migration.workers: ${env:DMT_WORKERS}`) fail YAML unmarshal here
//     because the int/bool field cannot hold the placeholder string. The
//     wizard's edit path then falls back to fresh-setup mode for such
//     configs. Fixing requires yaml.Node-level expansion of non-string
//     fields while preserving placeholders for string secrets.
func LoadRaw(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return &cfg, nil
}

// LoadBytes reads configuration from YAML bytes.
func LoadBytes(data []byte) (*Config, error) {
	// Expand templates (${file:path}, ${env:VAR}, and legacy ${VAR} syntax)
	expanded, err := expandYAMLTemplates(string(data))
	if err != nil {
		return nil, fmt.Errorf("expanding templates: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Apply defaults
	if err := cfg.applyDefaults(); err != nil {
		return nil, fmt.Errorf("applying defaults: %w", err)
	}

	// Validate
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// DefaultDataDir returns the default data directory for state storage.
func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".dmt")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}
