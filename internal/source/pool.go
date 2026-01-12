package source

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/dialect"
	"github.com/johndauphine/mssql-pg-migrate/internal/stats"
	"github.com/johndauphine/mssql-pg-migrate/internal/util"
	_ "github.com/microsoft/go-mssqldb"
)

// mssqlDialect is the shared dialect instance for MSSQL operations
var mssqlDialect = dialect.GetDialect("mssql")

// PoolStats contains connection pool statistics
type PoolStats struct {
	MaxOpenConnections int   // Maximum number of open connections
	OpenConnections    int   // Current number of open connections
	InUse              int   // Connections currently in use
	Idle               int   // Connections currently idle
	WaitCount          int64 // Total number of connections waited for
	WaitDuration       int64 // Total wait time in milliseconds
}

// Pool manages a pool of MSSQL connections
type Pool struct {
	db           *sql.DB
	config       *config.SourceConfig
	maxConns     int
	strategy     *MSSQLStrategy
	schemaLoader *SchemaLoader
}

// NewPool creates a new MSSQL connection pool
func NewPool(cfg *config.SourceConfig, maxConns int) (*Pool, error) {
	// Build DSN using dialect to eliminate duplication with target pool
	dsn := mssqlDialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	strategy := NewMSSQLStrategy()
	return &Pool{
		db:           db,
		config:       cfg,
		maxConns:     maxConns,
		strategy:     strategy,
		schemaLoader: NewSchemaLoader(db, strategy),
	}, nil
}

// Close closes all connections in the pool
func (p *Pool) Close() error {
	return p.db.Close()
}

// DB returns the underlying database connection
func (p *Pool) DB() *sql.DB {
	return p.db
}

// Stats returns current connection pool statistics
func (p *Pool) Stats() PoolStats {
	stats := p.db.Stats()
	return PoolStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:              stats.InUse,
		Idle:               stats.Idle,
		WaitCount:          stats.WaitCount,
		WaitDuration:       stats.WaitDuration.Milliseconds(),
	}
}

// MaxConns returns the configured maximum connections
func (p *Pool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *Pool) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics in the unified format.
func (p *Pool) PoolStats() stats.PoolStats {
	dbStats := p.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// GetRowCount returns the row count for a table
func (p *Pool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", mssqlDialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ExtractSchema extracts table metadata from the source database
func (p *Pool) ExtractSchema(ctx context.Context, schema string) ([]Table, error) {
	rows, err := p.db.QueryContext(ctx, p.strategy.GetTablesQuery(), p.strategy.BindTableParams(schema, "")...)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Get columns
		if err := p.loadColumns(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading columns for %s: %w", t.FullName(), err)
		}

		// Get primary key
		if err := p.loadPrimaryKey(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading PK for %s: %w", t.FullName(), err)
		}

		// Get row count
		if err := p.loadRowCount(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading row count for %s: %w", t.FullName(), err)
		}

		// Estimate row size from system statistics
		t.EstimatedRowSize = EstimateRowSizeFromStats(ctx, p.db, t.Schema, t.Name)

		tables = append(tables, t)
	}

	return tables, nil
}

func (p *Pool) loadColumns(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadColumns(ctx, t)
}

func (p *Pool) loadPrimaryKey(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadPrimaryKey(ctx, t)
}

func (p *Pool) loadRowCount(ctx context.Context, t *Table) error {
	// Use sys.partitions for fast approximate count
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`

	return p.db.QueryRowContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name)).Scan(&t.RowCount)
}

// GetPartitionBoundaries calculates NTILE boundaries for a large table
func (p *Pool) GetPartitionBoundaries(ctx context.Context, t *Table, numPartitions int) ([]Partition, error) {
	if !t.HasSinglePK() {
		return nil, fmt.Errorf("partitioning requires single-column PK")
	}

	pkCol := t.PrimaryKey[0]

	query := fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s,
				   NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM %s
		)
		SELECT partition_id,
			   MIN(%s) as min_pk,
			   MAX(%s) as max_pk,
			   COUNT(*) as row_count
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, mssqlDialect.QuoteIdentifier(pkCol), numPartitions, mssqlDialect.QuoteIdentifier(pkCol), mssqlDialect.QualifyTable(t.Schema, t.Name), mssqlDialect.QuoteIdentifier(pkCol), mssqlDialect.QuoteIdentifier(pkCol))

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []Partition
	for rows.Next() {
		var part Partition
		part.TableName = t.FullName()
		if err := rows.Scan(&part.PartitionID, &part.MinPK, &part.MaxPK, &part.RowCount); err != nil {
			return nil, err
		}
		partitions = append(partitions, part)
	}

	return partitions, nil
}

// LoadIndexes loads all non-PK indexes for a table
func (p *Pool) LoadIndexes(ctx context.Context, t *Table) error {
	rows, err := p.db.QueryContext(ctx, p.strategy.GetIndexesQuery(), p.strategy.BindIndexParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx Index
		var typeDesc, colsStr, includeStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &typeDesc, &colsStr, &includeStr); err != nil {
			return err
		}
		idx.IsClustered = typeDesc == "CLUSTERED"
		idx.Columns = util.SplitCSV(colsStr)
		if includeStr != "" {
			idx.IncludeCols = util.SplitCSV(includeStr)
		}
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table
func (p *Pool) LoadForeignKeys(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadForeignKeys(ctx, t)
}

// LoadCheckConstraints loads all check constraints for a table
func (p *Pool) LoadCheckConstraints(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadCheckConstraints(ctx, t)
}

// GetDateColumnInfo checks if any of the candidate columns exist as a temporal type
// Returns the first matching column name, its data type, and whether a match was found
func (p *Pool) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	return p.schemaLoader.GetDateColumnInfo(ctx, schema, table, candidates)
}
