// Package driver provides pluggable database driver abstractions.
// Each database (PostgreSQL, MSSQL, MySQL, etc.) implements the Driver interface
// to provide all database-specific functionality in one cohesive unit.
package driver

import (
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
	// applies (memory budget is the only cap). Probe-driven values (e.g.
	// MySQL @@max_allowed_packet) land in a follow-up to #166.
	HardChunkLimit(avgRowBytes int64) int
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
