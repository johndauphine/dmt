package config

import (
	"github.com/johndauphine/dmt/internal/driver"
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
	_ "github.com/johndauphine/dmt/internal/driver/sqlite"
	"os"
	"path/filepath"
	"strings"
)

// canonicalDriverName returns the canonical driver name for a given type.
// For example, "pg" -> "postgres", "sqlserver" -> "mssql".
// Uses the driver registry for lookup.
func canonicalDriverName(dbType string) string {
	if d, err := driver.Get(dbType); err == nil {
		return d.Name()
	}
	return dbType
}

// isValidDriverType returns true if the type is a valid driver type or alias.
// Uses the driver registry for validation.
func isValidDriverType(dbType string) bool {
	return driver.IsRegistered(dbType)
}

// availableDriverTypes returns a list of supported driver types.
// Uses the driver registry to get available drivers.
func availableDriverTypes() []string {
	return driver.Available()
}

// isFileBasedDriver returns true when the driver connects to a file
// rather than a network endpoint, in which case `host` is meaningless
// and the connection identity is carried on `database` (the path).
//
// Today the only file-based driver is sqlite (registered as primary
// name "sqlite" with aliases "sqlite3" / "sqlitedb" — see
// internal/driver/sqlite/driver.go Aliases()). The alias forms are
// handled correctly here because canonicalDriverName resolves through
// driver.Get(...).Name(), which collapses aliases to the canonical
// primary name before comparison.
//
// Used by validate() to skip the `host is required` check for file
// drivers — without this, a config like `type: sqlite, database:
// ./foo.db` would error at load time even though sqlite has no host.
func isFileBasedDriver(dbType string) bool {
	return canonicalDriverName(dbType) == "sqlite"
}

// expandTilde expands ~ or ~/ at the start of a path to the user's home directory
func expandTilde(path string) string {
	if path == "" {
		return path
	}
	if path == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}
