package pool

import (
	"context"
	"database/sql"

	"github.com/johndauphine/mssql-pg-migrate/internal/source"
)

// SourcePool defines the interface for source database operations
type SourcePool interface {
	// Connection management
	Close() error

	// Database access
	DB() *sql.DB

	// Schema extraction
	ExtractSchema(ctx context.Context, schema string) ([]source.Table, error)

	// Metadata
	GetPartitionBoundaries(ctx context.Context, t *source.Table, numPartitions int) ([]source.Partition, error)
	LoadIndexes(ctx context.Context, t *source.Table) error
	LoadForeignKeys(ctx context.Context, t *source.Table) error
	LoadCheckConstraints(ctx context.Context, t *source.Table) error

	// Row count (for validation)
	GetRowCount(ctx context.Context, schema, table string) (int64, error)

	// Date-based incremental sync
	// GetDateColumnInfo checks if any of the candidate columns exist as a temporal type
	// Returns the first matching column name, its data type, and whether a match was found
	GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool)

	// Pool info
	MaxConns() int
	DBType() string // "mssql" or "postgres"
}

// TargetPool defines the interface for target database operations
type TargetPool interface {
	// Connection management
	Close()
	Ping(ctx context.Context) error

	// Schema operations
	CreateSchema(ctx context.Context, schema string) error
	CreateTable(ctx context.Context, t *source.Table, targetSchema string) error
	CreateTableWithOptions(ctx context.Context, t *source.Table, targetSchema string, unlogged bool) error
	DropTable(ctx context.Context, schema, table string) error
	TruncateTable(ctx context.Context, schema, table string) error
	TableExists(ctx context.Context, schema, table string) (bool, error)
	SetTableLogged(ctx context.Context, schema, table string) error

	// Constraint operations
	CreatePrimaryKey(ctx context.Context, t *source.Table, targetSchema string) error
	CreateIndex(ctx context.Context, t *source.Table, idx *source.Index, targetSchema string) error
	CreateForeignKey(ctx context.Context, t *source.Table, fk *source.ForeignKey, targetSchema string) error
	CreateCheckConstraint(ctx context.Context, t *source.Table, chk *source.CheckConstraint, targetSchema string) error
	HasPrimaryKey(ctx context.Context, schema, table string) (bool, error)

	// Data operations
	GetRowCount(ctx context.Context, schema, table string) (int64, error)
	ResetSequence(ctx context.Context, schema string, t *source.Table) error

	// UpsertChunkWithWriter performs high-performance upsert using staging tables with writer isolation.
	// This method uses per-writer staging tables to avoid contention between parallel writers:
	// - PostgreSQL: Uses TEMP tables + COPY + INSERT...ON CONFLICT with IS DISTINCT FROM
	// - MSSQL: Uses #temp tables + bulk insert + MERGE WITH (TABLOCK)
	// writerID identifies the writer goroutine (0, 1, 2, ...) for staging table isolation
	// partitionID is optional and used when intra-table partitioning is enabled
	// colTypes contains the data types for each column (used to skip geography/geometry from change detection)
	// colSRIDs contains the SRID for each column (used for geography/geometry conversion in cross-engine migrations)
	UpsertChunkWithWriter(ctx context.Context, schema, table string, cols []string, colTypes []string, colSRIDs []int, pkCols []string, rows [][]any, writerID int, partitionID *int) error

	// Pool info
	MaxConns() int
	DBType() string // "mssql" or "postgres"
}

// BulkWriter defines the interface for bulk data writing operations
// This is used by the transfer package for writing chunks of data
type BulkWriter interface {
	WriteChunk(ctx context.Context, schema, table string, cols []string, rows [][]any) error
}
