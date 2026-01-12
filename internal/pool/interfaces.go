// Package pool provides type aliases for database pool interfaces.
// All interfaces are now defined in the driver package.
package pool

import (
	"context"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// SourcePool is an alias for driver.Reader for backward compatibility.
// It represents a source database connection pool for reading data.
type SourcePool = driver.Reader

// TargetPool is an alias for driver.Writer for backward compatibility.
// It represents a target database connection pool for writing data.
type TargetPool = driver.Writer

// BulkWriter is an alias for a subset of driver.Writer that can write batches.
// This is kept for backward compatibility with the transfer package.
type BulkWriter interface {
	WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error
}

// Re-export driver types for convenience
type (
	// TableOptions contains options for table creation.
	TableOptions = driver.TableOptions

	// WriteBatchOptions configures a bulk write operation.
	WriteBatchOptions = driver.WriteBatchOptions

	// UpsertBatchOptions configures an upsert operation.
	UpsertBatchOptions = driver.UpsertBatchOptions

	// ReadOptions configures how to read data from a table.
	ReadOptions = driver.ReadOptions

	// Batch represents a batch of rows read from the source.
	Batch = driver.Batch

	// DateFilter specifies a filter on a date/timestamp column.
	DateFilter = driver.DateFilter
)
