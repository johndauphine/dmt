package mssql

import (
	"context"
	"database/sql"

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
		WriteAheadWriters:     2,     // Base value, scales with cores when ScaleWritersWithCores=true
		ScaleWritersWithCores: true,  // Parallel BCP without TABLOCK

		// Unmeasured — smartconfig falls back to a conservative 10 MB
		// default. A target-side throughput sweep (separate issue) will
		// populate this once we have data. See #166.
		OptimumBulkChunkBytes: 0,
	}
}

// HardChunkLimit returns 0 — TDS BCP has no per-chunk frame limit that
// matters at our chunk sizes. Memory budget is the only cap.
func (d *Driver) HardChunkLimit(avgRowBytes int64) int {
	return 0
}

// ProbeTarget returns an empty TargetProbe — MSSQL doesn't have a
// runtime-probed value that affects chunk_size today. Method exists
// to satisfy the Driver interface (#166).
func (d *Driver) ProbeTarget(_ context.Context, _ *sql.DB) driver.TargetProbe {
	return driver.TargetProbe{}
}

// PreFlight runs MSSQL preflight checks (#228). Implementation in preflight.go.
func (d *Driver) PreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return preFlight(ctx, db, req)
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
