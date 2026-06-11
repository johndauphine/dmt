// Package sqlite provides a SQLite driver implementation for dmt.
// It registers itself with the driver registry on import.
//
// SQLite is a file-based, single-writer database. The driver exists
// primarily to support testing dmt without external database servers —
// fixtures live in .db files and round-trip through the same pipeline
// the production drivers use. It uses the pure-Go modernc.org/sqlite
// driver (already a dependency for the checkpoint store).
package sqlite

import (
	"context"
	"database/sql"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// NOTE (#191 phase 1): this package no longer registers itself — the
// catalog-driven generic engine registers "sqlite" from
// internal/driver/generic/catalogs/sqlite.yaml. The hand-written
// implementation remains as the differential-test oracle until the
// benchmark-gated cleanup removes it.

// Driver implements driver.Driver for SQLite.
type Driver struct{}

// Name returns the primary driver name.
func (d *Driver) Name() string { return "sqlite" }

// Aliases returns alternative names for this driver.
func (d *Driver) Aliases() []string { return []string{"sqlite3", "sqlitedb"} }

// Defaults returns the default configuration values for SQLite.
// SQLite is file-based with a single-writer constraint: parallel writers
// would just serialize on the file lock without any throughput benefit.
func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  0,
		Schema:                "",
		SSLMode:               "",
		WriteAheadWriters:     1,
		ScaleWritersWithCores: false,
		// 10 MB chunks are fine for the test scenarios this driver is
		// designed for. The single-writer constraint dominates throughput
		// before chunk size matters.
		OptimumBulkChunkBytes: 0,
	}
}

// HardChunkLimit caps the rows per multi-row INSERT to stay under
// SQLite's compile-time SQLITE_MAX_VARIABLE_NUMBER (32766 since 3.32).
// With one placeholder per cell, the row count is bounded by
// floor(32766 / numCols). avgRowBytes is unused — SQLite has no
// packet-size analog. Returns 0 (memory-budget-only) when we can't
// reason about column count from row bytes alone.
func (d *Driver) HardChunkLimit(avgRowBytes int64) int {
	return 0
}

// ProbeTarget is a no-op for SQLite — no runtime tunables.
func (d *Driver) ProbeTarget(ctx context.Context, db *sql.DB) driver.TargetProbe {
	return driver.TargetProbe{}
}

// PreFlight moved to the generic engine with the registration (#191):
// the battery lives in internal/driver/generic/preflight_sqlite.go and
// is selected by the catalog. The oracle keeps a minimal stub so the
// type still satisfies driver.Driver for differential tests.
func (d *Driver) PreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{})
}

// Dialect returns the SQLite dialect.
func (d *Driver) Dialect() driver.Dialect { return &Dialect{} }

// NewReader creates a new SQLite reader.
func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(cfg, maxConns)
}

// NewWriter creates a new SQLite writer.
func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(cfg, maxConns, opts)
}
