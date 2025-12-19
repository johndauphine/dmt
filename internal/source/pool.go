package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	_ "github.com/microsoft/go-mssqldb"
)

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
	db       *sql.DB
	config   *config.SourceConfig
	maxConns int
}

// NewPool creates a new MSSQL connection pool
func NewPool(cfg *config.SourceConfig, maxConns int) (*Pool, error) {
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

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

	return &Pool{db: db, config: cfg, maxConns: maxConns}, nil
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

	// Populate PKColumns with full column metadata
	for _, pkCol := range t.PrimaryKey {
		for _, col := range t.Columns {
			if col.Name == pkCol {
				t.PKColumns = append(t.PKColumns, col)
				break
			}
		}
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
	query := `
		SELECT
			i.name AS index_name,
			i.is_unique,
			i.type_desc,
			STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
			ISNULL(STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ',')
				WITHIN GROUP (ORDER BY ic.key_ordinal), '') AS include_columns
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		JOIN sys.tables tb ON i.object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema
		  AND tb.name = @table
		  AND i.is_primary_key = 0  -- Exclude PK
		  AND i.type > 0  -- Exclude heaps
		GROUP BY i.name, i.is_unique, i.type_desc
		ORDER BY i.name
	`

	rows, err := p.db.QueryContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name))
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
		idx.Columns = splitCSV(colsStr)
		if includeStr != "" {
			idx.IncludeCols = splitCSV(includeStr)
		}
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table
func (p *Pool) LoadForeignKeys(ctx context.Context, t *Table) error {
	query := `
		SELECT
			fk.name AS fk_name,
			STRING_AGG(pc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS parent_columns,
			rs.name AS ref_schema,
			rt.name AS ref_table,
			STRING_AGG(rc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns,
			fk.delete_referential_action_desc,
			fk.update_referential_action_desc
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
		JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
		JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
		JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
		JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
		JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
		WHERE ps.name = @schema AND pt.name = @table
		GROUP BY fk.name, rs.name, rt.name, fk.delete_referential_action_desc, fk.update_referential_action_desc
		ORDER BY fk.name
	`

	rows, err := p.db.QueryContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var fk ForeignKey
		var colsStr, refColsStr string
		if err := rows.Scan(&fk.Name, &colsStr, &fk.RefSchema, &fk.RefTable, &refColsStr, &fk.OnDelete, &fk.OnUpdate); err != nil {
			return err
		}
		fk.Columns = splitCSV(colsStr)
		fk.RefColumns = splitCSV(refColsStr)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}

	return nil
}

// LoadCheckConstraints loads all check constraints for a table
func (p *Pool) LoadCheckConstraints(ctx context.Context, t *Table) error {
	query := `
		SELECT
			cc.name,
			cc.definition
		FROM sys.check_constraints cc
		JOIN sys.tables tb ON cc.parent_object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema AND tb.name = @table
		  AND cc.is_disabled = 0
		ORDER BY cc.name
	`

	rows, err := p.db.QueryContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var chk CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}

	return nil
}

// splitCSV splits a comma-separated string into a slice
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
