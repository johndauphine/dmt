package mssql

import (
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

func init() {
	driver.Register(&Driver{})
}

// Driver implements driver.Driver for Microsoft SQL Server.
type Driver struct{}

// Name returns the primary driver name.
func (d *Driver) Name() string {
	return "mssql"
}

// Aliases returns alternative names for the driver.
func (d *Driver) Aliases() []string {
	return []string{"sqlserver", "sql-server"}
}

// Dialect returns the MSSQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new MSSQL reader.
func (d *Driver) NewReader(cfg *config.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new MSSQL writer.
func (d *Driver) NewWriter(cfg *config.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}

// TypeMapper returns the MSSQL type mapper.
func (d *Driver) TypeMapper() driver.TypeMapper {
	return &TypeMapper{}
}
