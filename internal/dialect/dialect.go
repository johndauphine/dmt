// Package dialect provides database-specific SQL generation.
//
// Deprecated: This package is deprecated. Use the driver package instead.
// The Dialect interface and implementations are now in internal/driver and
// internal/driver/{mssql,postgres}/dialect.go respectively.
//
// All types and functions in this package are thin wrappers that delegate
// to the driver package for backward compatibility.
package dialect

import (
	"github.com/johndauphine/dmt/internal/driver"
	// Import driver packages to register dialects
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
)

// Dialect is an alias for driver.Dialect.
// Deprecated: Use driver.Dialect instead.
type Dialect = driver.Dialect

// DateFilter is an alias for driver.DateFilter.
// Deprecated: Use driver.DateFilter instead.
type DateFilter = driver.DateFilter

// GetDialect returns the appropriate dialect for the given database type.
// Deprecated: Use driver.GetDialect instead.
func GetDialect(dbType string) Dialect {
	return driver.GetDialect(dbType)
}
