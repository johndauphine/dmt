package pool

import (
	"fmt"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"

	// Import driver packages to trigger init() registration
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/oracle"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
)

// NewSourcePool creates a source pool based on the configuration type.
// Uses the driver registry to create the appropriate Reader implementation.
// Adding a new database driver requires no changes to this function.
func NewSourcePool(cfg *config.SourceConfig, maxConns int) (SourcePool, error) {
	// Normalize empty type to default
	dbType := cfg.Type
	if dbType == "" {
		dbType = "mssql" // Default to MSSQL for backward compatibility
	}

	// Get the driver from the registry
	d, err := driver.Get(dbType)
	if err != nil {
		return nil, fmt.Errorf("unsupported source type: %s (available: %v)", dbType, driver.Available())
	}

	// Create the reader using the driver's factory method
	// This is truly pluggable - no switch statement needed
	return d.NewReader((*dbconfig.SourceConfig)(cfg), maxConns)
}

// NewTargetPool creates a target pool based on the configuration type.
// Uses the driver registry to create the appropriate Writer implementation.
// Adding a new database driver requires no changes to this function.
//
// Parameters:
//   - cfg: Target database configuration
//   - maxConns: Maximum number of connections in the pool
//   - mssqlRowsPerBatch: Rows per batch for MSSQL bulk operations
//   - oracleBatchSize: Oracle godror.Batch limit
//   - sourceType: Source database type for cross-engine type handling
//   - typeMapper: AI type mapper for database type conversions (required)
func NewTargetPool(cfg *config.TargetConfig, maxConns int, mssqlRowsPerBatch int, oracleBatchSize int, sourceType string, typeMapper driver.TypeMapper) (TargetPool, error) {
	// Normalize empty type to default
	dbType := cfg.Type
	if dbType == "" {
		dbType = "postgres" // Default to PostgreSQL for backward compatibility
	}

	// Get the driver from the registry
	d, err := driver.Get(dbType)
	if err != nil {
		return nil, fmt.Errorf("unsupported target type: %s (available: %v)", dbType, driver.Available())
	}

	// Create the writer using the driver's factory method
	// This is truly pluggable - no switch statement needed
	opts := driver.WriterOptions{
		RowsPerBatch:    mssqlRowsPerBatch,
		OracleBatchSize: oracleBatchSize,
		SourceType:      sourceType,
		TypeMapper:      typeMapper,
	}
	return d.NewWriter((*dbconfig.TargetConfig)(cfg), maxConns, opts)
}
