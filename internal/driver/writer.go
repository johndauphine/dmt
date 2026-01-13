package driver

import (
	"context"

	"github.com/johndauphine/data-transfer-tool/internal/stats"
)

// Writer represents a database writer that can write data to target tables.
// This is the "Consumer" in the Reader -> Queue -> Writer pipeline.
type Writer interface {
	// Connection management
	Close()
	Ping(ctx context.Context) error

	// Schema operations
	CreateSchema(ctx context.Context, schema string) error
	CreateTable(ctx context.Context, t *Table, targetSchema string) error
	CreateTableWithOptions(ctx context.Context, t *Table, targetSchema string, opts TableOptions) error
	DropTable(ctx context.Context, schema, table string) error
	TruncateTable(ctx context.Context, schema, table string) error
	TableExists(ctx context.Context, schema, table string) (bool, error)
	SetTableLogged(ctx context.Context, schema, table string) error

	// Constraint operations
	CreatePrimaryKey(ctx context.Context, t *Table, targetSchema string) error
	CreateIndex(ctx context.Context, t *Table, idx *Index, targetSchema string) error
	CreateForeignKey(ctx context.Context, t *Table, fk *ForeignKey, targetSchema string) error
	CreateCheckConstraint(ctx context.Context, t *Table, chk *CheckConstraint, targetSchema string) error
	HasPrimaryKey(ctx context.Context, schema, table string) (bool, error)

	// Data operations
	GetRowCount(ctx context.Context, schema, table string) (int64, error)
	ResetSequence(ctx context.Context, schema string, t *Table) error

	// Bulk write - for drop_recreate mode
	WriteBatch(ctx context.Context, opts WriteBatchOptions) error

	// Upsert - for upsert mode with per-writer isolation
	UpsertBatch(ctx context.Context, opts UpsertBatchOptions) error

	// Raw SQL execution for cleanup and special operations
	// Returns the number of rows affected and any error.
	ExecRaw(ctx context.Context, query string, args ...any) (int64, error)

	// Raw SQL query for single row results (e.g., EXISTS checks)
	// dest should be a pointer to the value to scan into
	QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error

	// Pool info
	MaxConns() int
	DBType() string
	PoolStats() stats.PoolStats
}

// TableOptions contains options for table creation.
type TableOptions struct {
	// Unlogged creates an unlogged table (PostgreSQL only, for performance).
	Unlogged bool
}

// WriteBatchOptions configures a bulk write operation.
type WriteBatchOptions struct {
	// Schema is the target schema.
	Schema string

	// Table is the target table name.
	Table string

	// Columns is the list of columns to write.
	Columns []string

	// Rows is the data to write.
	Rows [][]any
}

// UpsertBatchOptions configures an upsert operation.
type UpsertBatchOptions struct {
	// Schema is the target schema.
	Schema string

	// Table is the target table name.
	Table string

	// Columns is the list of columns to upsert.
	Columns []string

	// ColumnTypes contains the data types for each column.
	ColumnTypes []string

	// ColumnSRIDs contains the SRID for spatial columns (0 for non-spatial).
	ColumnSRIDs []int

	// PKColumns is the list of primary key columns for conflict detection.
	PKColumns []string

	// Rows is the data to upsert.
	Rows [][]any

	// WriterID identifies this writer for per-writer staging table isolation.
	WriterID int

	// PartitionID identifies the partition being written (for cleanup).
	PartitionID *int
}
