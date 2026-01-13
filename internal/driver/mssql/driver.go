package mssql

import (
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
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

// Defaults returns the default configuration values for MSSQL.
func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  1433,
		Schema:                "dbo",
		Encrypt:               true,  // Secure default
		PacketSize:            32767, // 32KB max - significantly improves read/write throughput
		WriteAheadWriters:     2,     // Conservative due to TABLOCK bulk insert serialization
		ScaleWritersWithCores: false, // More writers = more contention with TABLOCK
	}
}

// Dialect returns the MSSQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new MSSQL reader.
func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new MSSQL writer.
func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}

// TypeMapper returns the MSSQL type mapper.
func (d *Driver) TypeMapper() driver.TypeMapper {
	return &TypeMapper{}
}
