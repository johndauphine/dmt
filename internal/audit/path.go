package audit

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveFilePath returns the on-disk path of the audit file for a
// given (dir, runID) pair without creating anything. Callers use this
// to pre-check whether a file pre-exists (e.g. `dmt resume` wants to
// distinguish "reopening prior audit" from "creating fresh"). Mirrors
// the path New() will use internally.
func ResolveFilePath(dir, runID string) (string, error) {
	resolved, err := resolveDir(dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, runID+".ndjson"), nil
}

// resolveDir returns the absolute audit directory, defaulting to
// $HOME/.dmt/audit when dir is empty. Tilde expansion is handled
// because operators pass --audit-dir=~/audit and the shell may or
// may not expand it depending on how dmt was invoked.
func resolveDir(dir string) (string, error) {
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("audit: resolve $HOME: %w", err)
		}
		return filepath.Join(home, ".dmt", "audit"), nil
	}
	if len(dir) >= 2 && dir[:2] == "~/" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("audit: expand tilde: %w", err)
		}
		return filepath.Join(home, dir[2:]), nil
	}
	return dir, nil
}
