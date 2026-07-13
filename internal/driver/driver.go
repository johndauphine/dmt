// Package driver provides pluggable database driver abstractions.
// Each database (PostgreSQL, MSSQL, MySQL, etc.) implements the Driver interface
// to provide all database-specific functionality in one cohesive unit.
package driver

import (
	"context"
	"database/sql"

	"github.com/johndauphine/dmt/internal/dbconfig"
)

// DriverDefaults contains default values for a database driver.
// Used by config.applyDefaults() to set sensible defaults for each database type.
type DriverDefaults struct {
	// Port is the default port (e.g., 5432 for PostgreSQL, 1433 for MSSQL).
	Port int

	// Schema is the default schema (e.g., "public" for PostgreSQL, "dbo" for MSSQL).
	Schema string

	// SSLMode is the default SSL mode for PostgreSQL-style connections.
	SSLMode string

	// Encrypt is the default encryption setting for MSSQL-style connections.
	Encrypt bool

	// PacketSize is the default TDS packet size (MSSQL only, 0 means driver default).
	PacketSize int

	// WriteAheadWriters is the default number of parallel writers for this target.
	// Used when this database is the target of a migration.
	WriteAheadWriters int

	// ScaleWritersWithCores indicates whether WriteAheadWriters should scale with CPU cores.
	// If true, config.applyDefaults() will calculate: max(cores/4, WriteAheadWriters)
	// If false, WriteAheadWriters is used as-is.
	// MSSQL: true (parallel BCP without TABLOCK)
	// PostgreSQL: true (COPY handles parallelism well)
	// MySQL: true (multi-value INSERT parallelism)
	ScaleWritersWithCores bool

	// OptimumBulkChunkBytes is the empirically-measured "sweet spot" size
	// in bytes per bulk-insert chunk for this target's bulk path. The
	// smartconfig anchor for chunk_size is computed as
	// OptimumBulkChunkBytes / avg_row_bytes (then clamped to memory budget
	// and HardChunkLimit). 0 signals "unmeasured for this target" — callers
	// fall back to a conservative 10 MB default rather than picking 50000
	// rows blindly. See issue #166.
	OptimumBulkChunkBytes int64
}

// Driver represents a pluggable database driver that provides all
// database-specific functionality in one cohesive unit.
//
// To add a new database:
// 1. Create a package under internal/driver/<dbname>/
// 2. Implement the Driver interface
// 3. Register via init(): driver.Register(&MyDriver{})
type Driver interface {
	// Name returns the primary driver name (e.g., "mssql", "postgres", "mysql").
	Name() string

	// Aliases returns alternative names for this driver.
	// For example, postgres might have aliases ["postgresql", "pg"].
	Aliases() []string

	// Defaults returns the default configuration values for this driver.
	// Used by config.applyDefaults() to avoid hardcoding database-specific defaults.
	Defaults() DriverDefaults

	// Dialect returns the SQL dialect for this database.
	Dialect() Dialect

	// NewReader creates a new Reader for this database type.
	NewReader(cfg *dbconfig.SourceConfig, maxConns int) (Reader, error)

	// NewWriter creates a new Writer for this database type.
	NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts WriterOptions) (Writer, error)

	// HardChunkLimit returns the largest chunk_size (in rows) the target
	// can accept regardless of memory budget — typically driven by a
	// protocol-level packet/frame limit. Returns 0 when no protocol limit
	// applies (memory budget is the only cap). For probe-driven values
	// (e.g. MySQL @@max_allowed_packet), callers should consult
	// ProbeTarget and compute the limit from TargetProbe.MaxAllowedPacket
	// — this synchronous method has no DB access and must return 0 for
	// drivers whose limits depend on runtime probes.
	HardChunkLimit(avgRowBytes int64) int

	// ProbeTarget runs target-side probes that need a live DB
	// connection (#166). MySQL probes @@max_allowed_packet; PG and
	// MSSQL don't need probes today and return an empty TargetProbe.
	// Probe failures degrade gracefully — callers see a zero-valued
	// field and fall back to the memory-budget-only chunk size.
	ProbeTarget(ctx context.Context, db *sql.DB) TargetProbe

	// PreFlight runs the driver's preflight checks against the supplied
	// connection (#228). The orchestrator invokes this before any
	// schema-extraction or DDL work so misconfigured environments fail
	// fast with actionable findings rather than silently after minutes
	// of partial work. Drivers should return findings in stable order.
	// The empty-slice return is the OK signal.
	PreFlight(ctx context.Context, db *sql.DB, req PreFlightRequest) []PreFlightFinding
}

// ConnectionPoolResizer is the optional capability for applying a new live
// database connection limit after schema-aware tuning has finalized the
// effective configuration. The returned value is the maximum the engine
// actually accepted; callers must treat it as authoritative because engines
// with stricter concurrency contracts may clamp the requested value.
type ConnectionPoolResizer interface {
	ResizeConnectionPool(maxConns int) int
}

// TargetProbe carries runtime-probed values from the target database
// that affect chunk_size selection. Zero values mean "not probed" or
// "not applicable for this driver"; callers should treat any field
// as advisory and never as required.
type TargetProbe struct {
	// MaxAllowedPacket is MySQL's per-statement byte cap from
	// SELECT @@max_allowed_packet. Other drivers leave this zero.
	// Smartconfig uses this to compute HardChunkLimit as
	// (MaxAllowedPacket * 0.8) / avg_row_bytes — the 0.8 safety
	// margin accounts for protocol overhead per chunk (escapes,
	// column metadata, etc.).
	MaxAllowedPacket int64
}

// WriterOptions contains options for creating a Writer.
type WriterOptions struct {
	// BatchSize is the default number of rows per bulk insert batch.
	// Each driver uses this value for its native bulk operations:
	// - MSSQL: bulk.Options.RowsPerBatch
	// - MySQL: multi-value INSERT batch size
	// - PostgreSQL: not used (COPY protocol determines batch size dynamically)
	// This default can be overridden per-call via WriteBatchOptions.BatchSize.
	BatchSize int

	// SourceType is the source database type (for cross-engine type handling).
	SourceType string

	// TypeMapper is the AI-powered type mapper for database type conversions.
	// This is required for all migrations.
	TypeMapper TypeMapper
}
