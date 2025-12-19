package source

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	_ "github.com/microsoft/go-mssqldb"
)

// Pool manages a pool of MSSQL connections
type Pool struct {
	db     *sql.DB
	config *config.SourceConfig
	mu     sync.RWMutex
}

// NewPool creates a new MSSQL connection pool
func NewPool(cfg *config.SourceConfig, maxConns int) (*Pool, error) {
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &Pool{db: db, config: cfg}, nil
}

// Close closes all connections in the pool
func (p *Pool) Close() error {
	return p.db.Close()
}

// DB returns the underlying database connection
func (p *Pool) DB() *sql.DB {
	return p.db
}

// ExtractSchema extracts table metadata from the source database
func (p *Pool) ExtractSchema(ctx context.Context, schema string) ([]Table, error) {
	query := `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA = @schema
		ORDER BY t.TABLE_NAME
	`

	rows, err := p.db.QueryContext(ctx, query, sql.Named("schema", schema))
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

		tables = append(tables, t)
	}

	return tables, nil
}

func (p *Pool) loadColumns(ctx context.Context, t *Table) error {
	query := `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			ISNULL(CHARACTER_MAXIMUM_LENGTH, 0),
			ISNULL(NUMERIC_PRECISION, 0),
			ISNULL(NUMERIC_SCALE, 0),
			CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
			COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity'),
			ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION
	`

	rows, err := p.db.QueryContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.DataType, &c.MaxLength, &c.Precision,
			&c.Scale, &c.IsNullable, &c.IsIdentity, &c.OrdinalPos); err != nil {
			return err
		}
		t.Columns = append(t.Columns, c)
	}

	return nil
}

func (p *Pool) loadPrimaryKey(ctx context.Context, t *Table) error {
	query := `
		SELECT c.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND c.TABLE_NAME = tc.TABLE_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		  AND tc.TABLE_SCHEMA = @schema
		  AND tc.TABLE_NAME = @table
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := p.db.QueryContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}

	return nil
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
			SELECT [%s],
				   NTILE(%d) OVER (ORDER BY [%s]) as partition_id
			FROM [%s].[%s]
		)
		SELECT partition_id,
			   MIN([%s]) as min_pk,
			   MAX([%s]) as max_pk,
			   COUNT(*) as row_count
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, pkCol, numPartitions, pkCol, t.Schema, t.Name, pkCol, pkCol)

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []Partition
	for rows.Next() {
		var p Partition
		p.TableName = t.FullName()
		if err := rows.Scan(&p.PartitionID, &p.MinPK, &p.MaxPK, &p.RowCount); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}

	return partitions, nil
}
