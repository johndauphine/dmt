// Package mysql provides the MySQL/MariaDB driver implementation.
// It registers itself with the driver registry on import.
package mysql

import (
	"context"
	"database/sql"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
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
		Port:   3306,
		Schema: "", // MySQL uses database name, not schema
		// "require" maps to tls=true (require + verify) in the
		// dialect; operators who knowingly want downgradeable TLS
		// must explicitly set ssl_mode: preferred. (#252)
		SSLMode:               "require",
		WriteAheadWriters:     2,
		ScaleWritersWithCores: true,

		// Unmeasured — smartconfig falls back to a conservative 10 MB
		// default. A target-side throughput sweep (separate issue) will
		// populate this once we have data. See #166.
		OptimumBulkChunkBytes: 0,
	}
}

// HardChunkLimit returns 0 — MySQL's row-count cap depends on the
// server's @@max_allowed_packet, which requires a live DB connection
// to probe. Callers with a connection should use ProbeTarget +
// HardChunkLimitFromPacket; this synchronous method has no way to
// observe the packet size and must return 0 (#166).
func (d *Driver) HardChunkLimit(avgRowBytes int64) int {
	return 0
}

// HardChunkLimitFromPacket computes the row-count cap from a probed
// @@max_allowed_packet value. The 0.8 safety margin leaves headroom
// for per-chunk protocol overhead (column metadata, escapes, the
// VALUES wrapper). Zero inputs return zero (caller falls back to the
// memory-budget cap). When a single row exceeds 80% of the packet,
// returns 1 — one row per chunk is the smallest meaningful unit and
// is still strictly below the protocol limit; falling back to 0
// would disable packet protection exactly when it matters most
// (Codex review on #166).
func HardChunkLimitFromPacket(maxAllowedPacket, avgRowBytes int64) int {
	if maxAllowedPacket <= 0 || avgRowBytes <= 0 {
		return 0
	}
	const safetyMarginPct = 80
	safeBytes := maxAllowedPacket * safetyMarginPct / 100
	limit := int(safeBytes / avgRowBytes)
	if limit < 1 {
		limit = 1
	}
	return limit
}

// ProbeTarget queries the MySQL server for runtime values that affect
// chunk_size selection. Today: @@max_allowed_packet. Failures degrade
// gracefully — an empty TargetProbe is returned and smartconfig falls
// back to the memory-budget-only chunk size, which still keeps dmt
// running (just without the protocol-aware safety net).
func (d *Driver) ProbeTarget(ctx context.Context, db *sql.DB) driver.TargetProbe {
	var probe driver.TargetProbe
	if db == nil {
		return probe
	}
	var packet int64
	if err := db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&packet); err != nil {
		logging.Debug("mysql: @@max_allowed_packet probe failed: %v (continuing without packet-aware chunk cap)", err)
		return probe
	}
	probe.MaxAllowedPacket = packet
	logging.Debug("mysql: probed @@max_allowed_packet = %d bytes (%.1f MB)",
		packet, float64(packet)/1024/1024)
	return probe
}

// PreFlight runs MySQL preflight checks (#228). Implementation in preflight.go.
func (d *Driver) PreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return preFlight(ctx, db, req)
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
