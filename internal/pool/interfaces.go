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

	// Pool info
	MaxConns() int
	DBType() string // "mssql" or "postgres"
}

// TargetPool defines the interface for target database operations
type TargetPool interface {
	// Connection management
	Close()

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

	// Data operations
	GetRowCount(ctx context.Context, schema, table string) (int64, error)
	ResetSequence(ctx context.Context, schema string, t *source.Table) error

	// Pool info
	MaxConns() int
	DBType() string // "mssql" or "postgres"
}

// BulkWriter defines the interface for bulk data writing operations
// This is used by the transfer package for writing chunks of data
type BulkWriter interface {
	WriteChunk(ctx context.Context, schema, table string, cols []string, rows [][]any) error
}
