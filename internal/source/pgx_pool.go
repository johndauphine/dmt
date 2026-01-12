package source

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/dialect"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/util"
)

// pgDialect is the shared dialect instance for PostgreSQL operations
var pgDialect = dialect.GetDialect("postgres")

// PgxSourcePool manages a pool of PostgreSQL source connections using pgx.
type PgxSourcePool struct {
	pool         *pgxpool.Pool
	sqlDB        *sql.DB // Wrapper for compatibility with SourcePool interface
	config       *config.SourceConfig
	maxConns     int
	strategy     *PostgresStrategy
	schemaLoader *SchemaLoader
}

// NewPgxSourcePool creates a new PostgreSQL source connection pool using pgx.
func NewPgxSourcePool(cfg *config.SourceConfig, maxConns int) (*PgxSourcePool, error) {
	// Build connection string with proper URL encoding for special characters in password
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:   "/" + cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", cfg.SSLMode)
	u.RawQuery = q.Encode()

	poolConfig, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("parsing connection config: %w", err)
	}

	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = int32(maxConns / 4)
	if poolConfig.MinConns < 1 {
		poolConfig.MinConns = 1
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Create sql.DB wrapper for compatibility
	// Use the same connection string for both pools to ensure consistency
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("creating sql.DB wrapper: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	logging.Info("Connected to PostgreSQL source (pgx): %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	strategy := NewPostgresStrategy()
	return &PgxSourcePool{
		pool:         pool,
		sqlDB:        db,
		config:       cfg,
		maxConns:     maxConns,
		strategy:     strategy,
		schemaLoader: NewSchemaLoader(db, strategy),
	}, nil
}

// Close closes all connections in the pool
func (p *PgxSourcePool) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	if p.sqlDB != nil {
		return p.sqlDB.Close()
	}
	return nil
}

// DB returns the underlying sql.DB for compatibility
func (p *PgxSourcePool) DB() *sql.DB {
	return p.sqlDB
}

// MaxConns returns the configured maximum connections
func (p *PgxSourcePool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *PgxSourcePool) DBType() string {
	return "postgres"
}

// GetRowCount returns the row count for a table
func (p *PgxSourcePool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", pgDialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ExtractSchema extracts table metadata from the PostgreSQL source database
func (p *PgxSourcePool) ExtractSchema(ctx context.Context, schema string) ([]Table, error) {
	rows, err := p.pool.Query(ctx, p.strategy.GetTablesQuery(), p.strategy.BindTableParams(schema, "")...)
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
		t.EstimatedRowSize = EstimateRowSizeFromStatsPostgres(ctx, p.sqlDB, t.Schema, t.Name)

		tables = append(tables, t)
	}

	return tables, nil
}

func (p *PgxSourcePool) loadColumns(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadColumns(ctx, t)
}

func (p *PgxSourcePool) loadPrimaryKey(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadPrimaryKey(ctx, t)
}

func (p *PgxSourcePool) loadRowCount(ctx context.Context, t *Table) error {
	// Use pg_stat_user_tables for fast approximate count
	query := `
		SELECT COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2
	`

	err := p.pool.QueryRow(ctx, query, t.Schema, t.Name).Scan(&t.RowCount)
	if err != nil || t.RowCount == 0 {
		// Fall back to COUNT(*) if stats not available or show 0
		exactQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pgDialect.QualifyTable(t.Schema, t.Name))
		return p.pool.QueryRow(ctx, exactQuery).Scan(&t.RowCount)
	}
	return nil
}

// GetPartitionBoundaries calculates NTILE boundaries for a large table
func (p *PgxSourcePool) GetPartitionBoundaries(ctx context.Context, t *Table, numPartitions int) ([]Partition, error) {
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
	`, pgDialect.QuoteIdentifier(pkCol), numPartitions, pgDialect.QuoteIdentifier(pkCol), pgDialect.QualifyTable(t.Schema, t.Name), pgDialect.QuoteIdentifier(pkCol), pgDialect.QuoteIdentifier(pkCol))

	rows, err := p.pool.Query(ctx, query)
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
func (p *PgxSourcePool) LoadIndexes(ctx context.Context, t *Table) error {
	rows, err := p.pool.Query(ctx, p.strategy.GetIndexesQuery(), p.strategy.BindIndexParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx Index
		var colsStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsClustered, &colsStr); err != nil {
			return err
		}
		idx.Columns = util.SplitCSV(colsStr)
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table
func (p *PgxSourcePool) LoadForeignKeys(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadForeignKeys(ctx, t)
}

// LoadCheckConstraints loads all check constraints for a table
func (p *PgxSourcePool) LoadCheckConstraints(ctx context.Context, t *Table) error {
	return p.schemaLoader.LoadCheckConstraints(ctx, t)
}

// GetDateColumnInfo checks if any of the candidate columns exist as a temporal type
// Returns the first matching column name, its data type, and whether a match was found
func (p *PgxSourcePool) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	return p.schemaLoader.GetDateColumnInfo(ctx, schema, table, candidates)
}
