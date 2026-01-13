// Package postgres provides the PostgreSQL driver implementation.
// It registers itself with the driver registry on import.
package postgres

import (
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

func init() {
	driver.Register(&Driver{})
}

// Driver implements driver.Driver for PostgreSQL databases.
type Driver struct{}

// Name returns the primary driver name.
func (d *Driver) Name() string {
	return "postgres"
}

// Aliases returns alternative names for this driver.
func (d *Driver) Aliases() []string {
	return []string{"postgresql", "pg"}
}

// Defaults returns the default configuration values for PostgreSQL.
func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  5432,
		Schema:                "public",
		SSLMode:               "require", // Secure default
		WriteAheadWriters:     2,         // Minimum, scaled with cores
		ScaleWritersWithCores: true,      // COPY handles parallelism well
	}
}

// Dialect returns the PostgreSQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new PostgreSQL reader.
func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new PostgreSQL writer.
func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}

// TypeMapper returns the PostgreSQL type mapper.
func (d *Driver) TypeMapper() driver.TypeMapper {
	return &TypeMapper{}
}
