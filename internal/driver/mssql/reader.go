package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/stats"
	"github.com/johndauphine/mssql-pg-migrate/internal/util"
	_ "github.com/microsoft/go-mssqldb"
)

// Reader implements driver.Reader for SQL Server.
type Reader struct {
	db       *sql.DB
	config   *config.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new SQL Server reader.
func NewReader(cfg *config.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

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

	logging.Info("Connected to MSSQL source: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	return &Reader{
		db:       db,
		config:   cfg,
		maxConns: maxConns,
		dialect:  dialect,
	}, nil
}

// Close closes all connections.
func (r *Reader) Close() error {
	return r.db.Close()
}

// DB returns the underlying sql.DB for compatibility.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExtractSchema extracts table metadata from the source database.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA = @schema
		ORDER BY t.TABLE_NAME
	`, sql.Named("schema", schema))
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var tables []driver.Table
	for rows.Next() {
		var t driver.Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Get columns
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading columns for %s: %w", t.FullName(), err)
		}

		// Get primary key
		if err := r.loadPrimaryKey(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading PK for %s: %w", t.FullName(), err)
		}

		// Get row count
		if err := r.loadRowCount(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading row count for %s: %w", t.FullName(), err)
		}

		tables = append(tables, t)
	}

	return tables, nil
}

func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
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
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	t.Columns = nil
	for rows.Next() {
		var col driver.Column
		var isNullable, isIdentity int
		if err := rows.Scan(&col.Name, &col.DataType, &col.MaxLength, &col.Precision, &col.Scale, &isNullable, &isIdentity, &col.OrdinalPos); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}
		col.IsNullable = isNullable == 1
		col.IsIdentity = isIdentity == 1
		t.Columns = append(t.Columns, col)
	}

	return nil
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
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
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying primary key: %w", err)
	}
	defer rows.Close()

	t.PrimaryKey = nil
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return fmt.Errorf("scanning PK column: %w", err)
		}
		t.PrimaryKey = append(t.PrimaryKey, colName)
	}

	return nil
}

func (r *Reader) loadRowCount(ctx context.Context, t *driver.Table) error {
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`

	return r.db.QueryRowContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name)).Scan(&t.RowCount)
}

// LoadIndexes loads all non-PK indexes for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
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
		  AND i.is_primary_key = 0
		  AND i.type > 0
		GROUP BY i.name, i.is_unique, i.type_desc
		ORDER BY i.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var typeDesc, colsStr, includeStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &typeDesc, &colsStr, &includeStr); err != nil {
			return err
		}
		idx.IsClustered = typeDesc == "CLUSTERED"
		idx.Columns = util.SplitCSV(colsStr)
		if includeStr != "" {
			idx.Include = util.SplitCSV(includeStr)
		}
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	// Note: The Table type in driver package doesn't have a ForeignKeys field.
	// This is handled separately by the orchestrator.
	return nil
}

// LoadCheckConstraints loads all check constraints for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	// Note: The Table type in driver package doesn't have a CheckConstraints field.
	// This is handled separately by the orchestrator.
	return nil
}

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// GetPartitionBoundaries calculates NTILE boundaries for a large table.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) != 1 {
		return nil, fmt.Errorf("partitioning requires single-column PK")
	}

	pkCol := t.PrimaryKey[0]
	query := r.dialect.PartitionBoundariesQuery(pkCol, t.Schema, t.Name, numPartitions)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []driver.Partition
	for rows.Next() {
		var part driver.Partition
		part.TableName = t.FullName()
		if err := rows.Scan(&part.PartitionID, &part.MinPK, &part.MaxPK, &part.RowCount); err != nil {
			return nil, err
		}
		partitions = append(partitions, part)
	}

	return partitions, nil
}

// GetDateColumnInfo checks if any of the candidate columns exist as a temporal type.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	validTypes := r.dialect.ValidDateTypes()

	for _, candidate := range candidates {
		var colType string
		err := r.db.QueryRowContext(ctx, r.dialect.DateColumnQuery(),
			sql.Named("schema", schema),
			sql.Named("table", table),
			sql.Named("column", candidate)).Scan(&colType)

		if err == nil && validTypes[colType] {
			return candidate, colType, true
		}
	}

	return "", "", false
}

// ReadTable reads data from a table and returns batches via a channel.
func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	batches := make(chan driver.Batch, 4) // Buffer a few batches

	go func() {
		defer close(batches)

		// Build column list
		cols := r.dialect.ColumnListForSelect(opts.Columns, opts.ColumnTypes, opts.TargetDBType)
		tableHint := r.dialect.TableHint(opts.StrictConsistency)

		// Determine pagination strategy
		if opts.Partition != nil && opts.Partition.MinPK != nil {
			r.readKeysetPagination(ctx, batches, opts, cols, tableHint)
		} else if opts.Partition != nil && opts.Partition.StartRow > 0 {
			r.readRowNumberPagination(ctx, batches, opts, cols, tableHint)
		} else {
			r.readFullTable(ctx, batches, opts, cols, tableHint)
		}
	}()

	return batches, nil
}

func (r *Reader) readKeysetPagination(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols, tableHint string) {
	pkCol := opts.Table.PrimaryKey[0]
	lastPK := opts.Partition.MinPK
	maxPK := opts.Partition.MaxPK

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		queryStart := time.Now()
		query := r.dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, tableHint, maxPK != nil, opts.DateFilter)

		args := []any{sql.Named("limit", opts.ChunkSize)}
		args = append(args, r.dialect.BuildKeysetArgs(lastPK, maxPK, opts.DateFilter)...)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("keyset query: %w", err), Done: true}
			return
		}

		batch, newLastPK, err := r.scanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- driver.Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.LastKey = newLastPK

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}

		lastPK = newLastPK

		// Check if we've reached the end
		if maxPK != nil {
			if cmp := compareKeys(lastPK, maxPK); cmp >= 0 {
				batch.Done = true
			}
		}
		if len(batch.Rows) < opts.ChunkSize {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}
	}
}

func (r *Reader) readRowNumberPagination(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols, tableHint string) {
	orderBy := r.dialect.ColumnList(opts.Table.PrimaryKey)
	startRow := opts.Partition.StartRow
	endRow := opts.Partition.EndRow

	currentRow := startRow

	for currentRow < endRow {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		batchEnd := currentRow + int64(opts.ChunkSize)
		if batchEnd > endRow {
			batchEnd = endRow
		}

		queryStart := time.Now()
		query := r.dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, tableHint)
		args := r.dialect.BuildRowNumberArgs(currentRow, batchEnd)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("row_number query: %w", err), Done: true}
			return
		}

		batch, _, err := r.scanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- driver.Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.RowNum = currentRow

		currentRow = batchEnd

		if currentRow >= endRow || len(batch.Rows) == 0 {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}
	}
}

func (r *Reader) readFullTable(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols, tableHint string) {
	queryStart := time.Now()
	query := fmt.Sprintf("SELECT %s FROM %s %s", cols, r.dialect.QualifyTable(opts.Table.Schema, opts.Table.Name), tableHint)

	rows, err := r.db.QueryContext(ctx, query)
	queryTime := time.Since(queryStart)

	if err != nil {
		batches <- driver.Batch{Error: fmt.Errorf("full read query: %w", err), Done: true}
		return
	}
	defer rows.Close()

	for {
		batch := driver.Batch{
			Stats: driver.BatchStats{QueryTime: queryTime},
		}

		scanStart := time.Now()
		for i := 0; i < opts.ChunkSize && rows.Next(); i++ {
			row := make([]any, len(opts.Columns))
			ptrs := make([]any, len(opts.Columns))
			for j := range row {
				ptrs[j] = &row[j]
			}
			if err := rows.Scan(ptrs...); err != nil {
				batches <- driver.Batch{Error: err, Done: true}
				return
			}
			batch.Rows = append(batch.Rows, row)
		}
		batch.Stats.ScanTime = time.Since(scanStart)

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}

		if len(batch.Rows) < opts.ChunkSize {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}

		queryTime = 0 // Only first batch has query time
	}
}

func (r *Reader) scanRows(rows *sql.Rows, numCols int) (driver.Batch, any, error) {
	batch := driver.Batch{}
	scanStart := time.Now()

	var lastPK any
	for rows.Next() {
		row := make([]any, numCols)
		ptrs := make([]any, numCols)
		for j := range row {
			ptrs[j] = &row[j]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return batch, nil, fmt.Errorf("scanning row: %w", err)
		}
		batch.Rows = append(batch.Rows, row)
		lastPK = row[0] // Assume first column is PK for keyset
	}

	batch.Stats.ScanTime = time.Since(scanStart)
	batch.Stats.ReadEnd = time.Now()

	return batch, lastPK, rows.Err()
}

// compareKeys compares two primary key values.
func compareKeys(a, b any) int {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int32:
		if vb, ok := b.(int32); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}
