package generic

import (
	"context"
	"database/sql"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// Driver implements driver.Driver from a catalog — the factory the
// registry hands out. One implementation, N catalogs.
type Driver struct {
	cat *Catalog
}

var _ driver.Driver = (*Driver)(nil)

// NewDriver wraps a validated catalog.
func NewDriver(cat *Catalog) *Driver { return &Driver{cat: cat} }

func (d *Driver) Name() string      { return d.cat.Name }
func (d *Driver) Aliases() []string { return d.cat.Aliases }

func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  d.cat.Connection.DefaultPort,
		Schema:                d.cat.Defaults.Schema,
		SSLMode:               d.cat.Defaults.SSLMode,
		WriteAheadWriters:     d.cat.Defaults.WriteAheadWriters,
		ScaleWritersWithCores: d.cat.Defaults.ScaleWritersWithCores,
		OptimumBulkChunkBytes: int64(d.cat.Defaults.OptimumBulkChunkBytes),
	}
}

// HardChunkLimit: no phase-1 catalog has a packet-size analog; 0 means
// memory-budget-only. Becomes a catalog field when an engine needs it
// (mysql max_allowed_packet in phase 3).
func (d *Driver) HardChunkLimit(avgRowBytes int64) int { return 0 }

// ProbeTarget: no phase-1 catalog has runtime tunables to probe.
func (d *Driver) ProbeTarget(ctx context.Context, db *sql.DB) driver.TargetProbe {
	return driver.TargetProbe{}
}

// PreFlight runs the catalog's named preflight battery, or the shared
// minimal battery when the catalog declares none.
func (d *Driver) PreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if s := d.cat.Preflight; s != "" {
		return preflightStrategies[s](ctx, db, req)
	}
	return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{})
}

func (d *Driver) Dialect() driver.Dialect { return NewDialect(d.cat) }

func (d *Driver) NewReader(cfg *dbconfig.SourceConfig, maxConns int) (driver.Reader, error) {
	return NewReader(d.cat, cfg, maxConns)
}

func (d *Driver) NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (driver.Writer, error) {
	return NewWriter(d.cat, cfg, maxConns, opts)
}

// init registers every embedded catalog with the driver registry. A
// catalog that fails to load or validate is a programming error caught
// by the package's own tests; panicking here matches the registry's
// duplicate-registration policy.
func init() {
	for _, name := range []string{"sqlite", "clickhouse", "mysql", "postgres"} {
		cat, err := LoadCatalog(name)
		if err != nil {
			panic("generic: embedded catalog " + name + ": " + err.Error())
		}
		driver.Register(NewDriver(cat))
	}
}
