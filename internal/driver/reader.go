package driver

import (
	"context"
	"database/sql"
	"time"

	"github.com/johndauphine/dmt/internal/stats"
)

// IncrementalDateReader is the optional capability of locating a
// date-updated column and reading its high watermark (#476) — the
// source side of incremental date-filtered sync. A source engine
// without it degrades to full sync with one warning; date tracking is
// a sync optimization, not a correctness requirement.
type IncrementalDateReader interface {
	GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool)
	GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error)
}

// Reader represents a database reader that can stream data from source tables.
// This is the "Producer" in the Reader -> Queue -> Writer pipeline.
type Reader interface {
	// Connection management
	Close() error
	DB() *sql.DB

	// Schema operations
	ExtractSchema(ctx context.Context, schema string) ([]Table, error)
	LoadIndexes(ctx context.Context, t *Table) error
	LoadForeignKeys(ctx context.Context, t *Table) error
	LoadCheckConstraints(ctx context.Context, t *Table) error

	// Metadata
	GetRowCount(ctx context.Context, schema, table string) (int64, error)     // Tries fast first, falls back to exact
	GetRowCountFast(ctx context.Context, schema, table string) (int64, error) // Fast approximate count from system statistics
	// GetRowCountExact returns the exact row count via COUNT(*).
	// strictConsistency=true asks MSSQL drivers to drop the
	// `WITH (NOLOCK)` hint so the count is read-committed rather
	// than dirty (#253). Other drivers don't have NOLOCK semantics
	// and ignore the flag.
	GetRowCountExact(ctx context.Context, schema, table string, strictConsistency bool) (int64, error)
	GetPartitionBoundaries(ctx context.Context, t *Table, numPartitions int) ([]Partition, error)

	// Pool info
	MaxConns() int
	DBType() string
	PoolStats() stats.PoolStats
}
