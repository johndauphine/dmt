// Package postgres provides the PostgreSQL driver implementation.
// It registers itself with the driver registry on import.
package postgres

import (
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
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

// Dialect returns the PostgreSQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new PostgreSQL reader.
func (d *Driver) NewReader(cfg *config.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new PostgreSQL writer.
func (d *Driver) NewWriter(cfg *config.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}

// TypeMapper returns the PostgreSQL type mapper.
func (d *Driver) TypeMapper() driver.TypeMapper {
	return &TypeMapper{}
}
