// Package mysql provides the MySQL/MariaDB driver implementation.
// It registers itself with the driver registry on import.
package mysql

import (
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

func init() {
	driver.Register(&Driver{})
}

// Driver implements driver.Driver for MySQL/MariaDB databases.
type Driver struct{}

// Name returns the primary driver name.
func (d *Driver) Name() string {
	return "mysql"
}

// Aliases returns alternative names for this driver.
func (d *Driver) Aliases() []string {
	return []string{"mariadb", "maria"}
}

// Defaults returns the default configuration values for MySQL.
func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  3306,
		Schema:                "", // MySQL uses database name, not schema
		SSLMode:               "preferred",
		WriteAheadWriters:     2,
		ScaleWritersWithCores: true,
	}
}

// Dialect returns the MySQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new MySQL reader.
func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new MySQL writer.
func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}
