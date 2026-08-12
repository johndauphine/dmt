package generic

import (
	"context"
	"database/sql"

	"github.com/johndauphine/dmt/v5/internal/dbconfig"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/driver/shared"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

// Driver implements driver.Driver from a catalog — the factory the
// registry hands out. One implementation, N catalogs.
type Driver struct {
	cat *Catalog
}

var (
	_ driver.Driver              = (*Driver)(nil)
	_ driver.SchemaStatsProvider = (*Driver)(nil)
)

// NewDriver wraps a validated catalog.
func NewDriver(cat *Catalog) *Driver { return &Driver{cat: cat} }

func (d *Driver) Name() string      { return d.cat.Name }
func (d *Driver) Aliases() []string { return d.cat.Aliases }

func (d *Driver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{
		Port:                  d.cat.Connection.DefaultPort,
		Portless:              d.cat.Connection.Portless,
		Schema:                d.cat.Defaults.Schema,
		SSLMode:               d.cat.Defaults.SSLMode,
		Encrypt:               d.cat.Defaults.Encrypt,
		PacketSize:            d.cat.Defaults.PacketSize,
		WriteAheadWriters:     d.cat.Defaults.WriteAheadWriters,
		ScaleWritersWithCores: d.cat.Defaults.ScaleWritersWithCores,
		OptimumBulkChunkBytes: int64(d.cat.Defaults.OptimumBulkChunkBytes),
	}
}

// SchemaStatsReader returns the reader explicitly selected by this catalog.
// Every catalog-backed engine shares the same concrete Driver type, so support
// must be reported through catalog data rather than an interface assertion.
func (d *Driver) SchemaStatsReader() (driver.SchemaStatsReader, bool) {
	if !d.cat.Capabilities.SchemaStats {
		return nil, false
	}
	switch d.cat.SchemaStats.Strategy {
	case "query":
		return driver.NewQuerySchemaStatsReader(
			d.cat.SchemaStats.TableStats,
			d.cat.SchemaStats.DateColumns,
		), true
	case "sqlite":
		return newSQLiteSchemaStatsReader(d.cat), true
	case "none":
		return nil, false
	default:
		// Catalog validation rejects this path. Keep the runtime fallback
		// conservative for hand-built Catalog values used by callers/tests.
		return nil, false
	}
}

// HardChunkLimit: no phase-1 catalog has a packet-size analog; 0 means
// memory-budget-only. Becomes a catalog field when an engine needs it
// (mysql max_allowed_packet in phase 3).
func (d *Driver) HardChunkLimit(avgRowBytes int64) int { return 0 }

// ProbeTarget queries the target for runtime values that affect
// chunk_size selection when the catalog declares a probe. Failures
// degrade gracefully — an empty TargetProbe falls back to the
// memory-budget-only chunk size. (The hand-written mysql driver probed
// @@max_allowed_packet; the #516 flip silently lost it until the
// oracle-removal audit.)
func (d *Driver) ProbeTarget(ctx context.Context, db *sql.DB) driver.TargetProbe {
	var probe driver.TargetProbe
	q := d.cat.Queries.ProbeMaxAllowedPacket
	if q == "" || db == nil {
		return probe
	}
	var packet int64
	if err := db.QueryRowContext(ctx, q).Scan(&packet); err != nil {
		logging.Debug("%s: max-packet probe failed: %v (continuing without packet-aware chunk cap)", d.cat.Name, err)
		return probe
	}
	probe.MaxAllowedPacket = packet
	logging.Debug("%s: probed max packet = %d bytes (%.1f MB)", d.cat.Name, packet, float64(packet)/1024/1024)
	return probe
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
	for _, name := range []string{"sqlite", "clickhouse", "mysql", "postgres", "mssql"} {
		cat, err := LoadCatalog(name)
		if err != nil {
			panic("generic: embedded catalog " + name + ": " + err.Error())
		}
		driver.Register(NewDriver(cat))
	}
}
