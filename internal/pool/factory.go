package pool

import (
	"fmt"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/target"

	// Import driver packages to trigger init() registration
	_ "github.com/johndauphine/mssql-pg-migrate/internal/driver/mssql"
	_ "github.com/johndauphine/mssql-pg-migrate/internal/driver/postgres"
)

// NewSourcePool creates a source pool based on the configuration type.
// Uses the driver registry to validate the database type, then creates
// the appropriate source pool implementation.
func NewSourcePool(cfg *config.SourceConfig, maxConns int) (SourcePool, error) {
	// Normalize empty type to default
	dbType := cfg.Type
	if dbType == "" {
		dbType = "mssql" // Default to MSSQL for backward compatibility
	}

	// Validate driver exists in registry
	_, err := driver.Get(dbType)
	if err != nil {
		return nil, fmt.Errorf("unsupported source type: %s (available: %v)", dbType, driver.Available())
	}

	// Use existing implementations for backward compatibility
	switch dbType {
	case "mssql", "sqlserver", "sql-server":
		return source.NewPool(cfg, maxConns)
	case "postgres", "postgresql", "pg":
		return source.NewPgxSourcePool(cfg, maxConns)
	default:
		return nil, fmt.Errorf("no implementation for source type: %s", dbType)
	}
}

// NewTargetPool creates a target pool based on the configuration type.
// Uses the driver registry to validate the database type, then creates
// the appropriate target pool implementation.
// mssqlRowsPerBatch is only used for MSSQL targets (ignored for PostgreSQL)
// sourceType indicates the source database type ("mssql" or "postgres") for DDL generation
func NewTargetPool(cfg *config.TargetConfig, maxConns int, mssqlRowsPerBatch int, sourceType string) (TargetPool, error) {
	// Normalize empty type to default
	dbType := cfg.Type
	if dbType == "" {
		dbType = "postgres" // Default to PostgreSQL for backward compatibility
	}

	// Validate driver exists in registry
	_, err := driver.Get(dbType)
	if err != nil {
		return nil, fmt.Errorf("unsupported target type: %s (available: %v)", dbType, driver.Available())
	}

	// Use existing implementations for backward compatibility
	switch dbType {
	case "postgres", "postgresql", "pg":
		return target.NewPool(cfg, maxConns, sourceType)
	case "mssql", "sqlserver", "sql-server":
		return target.NewMSSQLPool(cfg, maxConns, mssqlRowsPerBatch, sourceType)
	default:
		return nil, fmt.Errorf("no implementation for target type: %s", dbType)
	}
}
