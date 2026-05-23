package secrets

import (
	"fmt"
	"os"
	"runtime"
)

// ValidateFilePermissions enforces dmt's secrets-file permission policy.
// On Unix-like systems, files must not grant any group or world permissions.
func ValidateFilePermissions(path string) error {
	// os.Stat follows symlinks, so this validates the final file's mode.
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("checking secret file permissions for %s: %w", path, err)
	}

	// POSIX group/world mode bits do not reliably represent Windows ACLs.
	if runtime.GOOS == "windows" {
		return nil
	}

	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		return fmt.Errorf("secret file %s has insecure permissions (%04o): "+
			"group/world permissions are not allowed for file-based secrets. Run: chmod %03o %s",
			path, mode, SecureFileMode, path)
	}
	return nil
}
