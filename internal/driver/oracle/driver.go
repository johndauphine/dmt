package oracle

import (
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

func init() {
	driver.Register(&Driver{})
}

// Driver implements driver.Driver for Oracle Database.
type Driver struct{}

// Name returns the primary driver name.
func (d *Driver) Name() string {
	return "oracle"
}

// Aliases returns alternative names for this driver.
func (d *Driver) Aliases() []string {
	return []string{"ora", "oracledb"}
}

// Defaults returns the default configuration values.
func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  1521,
		Schema:                "", // Oracle uses username as default schema
		SSLMode:               "",
		WriteAheadWriters:     4, // More parallel writers for better throughput
		ScaleWritersWithCores: true,
	}
}

// Dialect returns the Oracle SQL dialect.
func (d *Driver) Dialect() driver.Dialect {
	return &Dialect{}
}

// NewReader creates a new Oracle reader.
func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new Oracle writer.
func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}
