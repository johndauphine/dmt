package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Template patterns for secret expansion:
// - filePattern: ${file:/path/to/file} - any path characters allowed
// - envPattern: ${env:VAR_NAME} - valid env var names only [A-Za-z_][A-Za-z0-9_]*
// - legacyEnvPattern: ${VAR_NAME} - legacy shorthand for ${env:VAR_NAME}
var filePattern = regexp.MustCompile(`^\$\{file:(.+)\}$`)
var envPattern = regexp.MustCompile(`^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)
var legacyEnvPattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

// expandTemplateValue expands template patterns in a string value.
// Supported patterns:
//   - ${file:/path/to/file} - reads value from file (trimmed of whitespace)
//   - ${env:VAR_NAME} - reads value from environment variable (explicit)
//   - ${VAR_NAME} - reads value from environment variable (legacy shorthand)
//   - Any other value is returned as-is (cleartext password)
//
// Returns the expanded value and any error encountered.
func expandTemplateValue(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	// Check for ${file:...} pattern
	if matches := filePattern.FindStringSubmatch(value); matches != nil {
		filePath := expandTilde(matches[1])
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("reading secret from file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	// Check for ${env:VAR_NAME} pattern (explicit, restricted to valid env var names)
	if matches := envPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - allows optional env vars
		// but may cause silent auth failures if variable name is misspelled.
		return os.Getenv(matches[1]), nil
	}

	// Check for legacy ${VAR_NAME} pattern (shorthand for ${env:VAR_NAME})
	if matches := legacyEnvPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - matches ${env:VAR} behavior.
		// This allows optional env vars but may cause silent auth failures
		// if the variable name is misspelled. Use explicit ${env:VAR} for clarity.
		return os.Getenv(matches[1]), nil
	}

	// Not a template pattern - return as-is (cleartext)
	return value, nil
}
