package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
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

// Expand resolves a single ${env:...}, ${file:...}, or legacy ${VAR}
// template in s. Non-template strings are returned unchanged. Pair with
// LoadRaw: callers who need a resolved single field (e.g. just the
// source-side password for a connection test) can scope expansion to
// one value at a time instead of rolling up the whole config.
func Expand(s string) (string, error) {
	return expandTemplateValue(s)
}

// LoadRaw reads a config YAML for *editing* — no defaults application and no
// validation. Placeholders in string fields like ${env:DB_PASSWORD} and
// ${file:/run/secrets/x} survive as literal strings so the setup wizard can
// re-marshal them back to disk without exposing resolved secrets.
// Placeholders in non-string scalar fields are expanded just enough for typed
// unmarshal to succeed.
//
// This is intentionally narrow: the migration runtime must always use Load /
// LoadWithOptions so that templates resolve before connection attempts.
func LoadRaw(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if err := expandRawNonStringTemplates(&node); err != nil {
		return nil, fmt.Errorf("expanding non-string templates: %w", err)
	}

	var cfg Config
	if err := node.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}
	return &cfg, nil
}

// LoadBytes reads configuration from YAML bytes.
func LoadBytes(data []byte) (*Config, error) {
	// Parse first, then expand ${file:}/${env:}/${VAR} templates per-scalar on
	// the node tree — NOT as raw text substitution over the document. A secret
	// value is stored as a literal scalar node value, so characters like '#',
	// newlines, or ':' inside a secret are preserved verbatim and cannot
	// truncate the value (YAML comment) or inject config structure (#552).
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	var cfg Config
	if node.Kind != 0 {
		if err := expandAllTemplates(&node); err != nil {
			return nil, fmt.Errorf("expanding templates: %w", err)
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("parsing config: %w", err)
		}
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
