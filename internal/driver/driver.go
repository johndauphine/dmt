// Package driver provides pluggable database driver abstractions.
// Each database (PostgreSQL, MSSQL, MySQL, etc.) implements the Driver interface
// to provide all database-specific functionality in one cohesive unit.
package driver

import (
	"github.com/johndauphine/mssql-pg-migrate/internal/dbconfig"
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

	// TypeMapper returns the type mapper for converting to/from this database's types.
	TypeMapper() TypeMapper
}

// WriterOptions contains options for creating a Writer.
type WriterOptions struct {
	// RowsPerBatch is the number of rows per bulk insert batch.
	RowsPerBatch int

	// SourceType is the source database type (for cross-engine type handling).
	SourceType string
}
