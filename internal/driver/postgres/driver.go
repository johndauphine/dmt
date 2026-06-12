// Package postgres provides the PostgreSQL driver implementation.
// It registers itself with the driver registry on import.
package postgres

import (
	"context"
	"database/sql"

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

		// 25 MB per chunk — measured plateau on M5 Pro 48GB SO2010
		// mssql→pg fixture (#164 n=15 sweep, statistically equivalent
		// to 87896 with cleaner tail behavior). See docs/BENCHMARKS.md
		// "Chunk Size vs Memory-Fit Ceiling".
		OptimumBulkChunkBytes: 25_000_000,
	}
}

// HardChunkLimit returns 0 — PG's COPY protocol has no fixed per-chunk
// frame limit. Memory budget is the only cap.
func (d *Driver) HardChunkLimit(avgRowBytes int64) int {
	return 0
}

// ProbeTarget returns an empty TargetProbe — PG has no runtime-probed
// values that affect chunk_size today. Method exists to satisfy the
// Driver interface (#166); future probes (e.g. shared_buffers-derived
// cap) could populate fields here.
func (d *Driver) ProbeTarget(_ context.Context, _ *sql.DB) driver.TargetProbe {
	return driver.TargetProbe{}
}

// PreFlight runs PostgreSQL preflight checks (#228). Implementation lives
// in preflight.go to keep this entry point lean.
func (d *Driver) PreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return preFlight(ctx, db, req)
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
