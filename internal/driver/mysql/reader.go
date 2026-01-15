package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader for MySQL/MariaDB.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new MySQL reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	if db.Stats().MaxOpenConnections > 0 && maxConns/4 < 1 {
		db.SetMaxIdleConns(1)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Detect MySQL vs MariaDB
	var version string
	db.QueryRow("SELECT VERSION()").Scan(&version)
	dbType := "MySQL"
	if strings.Contains(strings.ToLower(version), "mariadb") {
		dbType = "MariaDB"
	}

	logging.Info("Connected to %s source: %s:%d/%s", dbType, cfg.Host, cfg.Port, cfg.Database)

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

// DB returns the underlying sql.DB.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mysql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mysql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExtractSchema extracts table metadata from the database.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	tables := []driver.Table{}

	// In MySQL, schema = database name
	dbName := schema
	if dbName == "" {
		dbName = r.config.Database
	}

	// Get tables
	rows, err := r.db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`, dbName)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t driver.Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Load columns
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, err
		}

		// Load primary key
		if err := r.loadPrimaryKey(ctx, &t); err != nil {
			return nil, err
		}

		// Populate PKColumns with full column metadata
		t.PopulatePKColumns()

		// Get row count
		count, err := r.GetRowCount(ctx, t.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s: %v", t.Name, err)
		}
		t.RowCount = count

		tables = append(tables, t)
	}

	return tables, rows.Err()
}

func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			COALESCE(CHARACTER_MAXIMUM_LENGTH, 0),
			COALESCE(NUMERIC_PRECISION, 0),
			COALESCE(NUMERIC_SCALE, 0),
			CASE WHEN IS_NULLABLE = 'YES' THEN true ELSE false END,
			CASE WHEN EXTRA LIKE '%auto_increment%' THEN true ELSE false END,
			ORDINAL_POSITION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying columns for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var c driver.Column
		if err := rows.Scan(&c.Name, &c.DataType, &c.MaxLength, &c.Precision, &c.Scale,
			&c.IsNullable, &c.IsIdentity, &c.OrdinalPos); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}
		t.Columns = append(t.Columns, c)
	}
	return rows.Err()
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying primary key for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return fmt.Errorf("scanning pk column: %w", err)
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}
	return rows.Err()
}

// LoadIndexes loads index metadata for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			INDEX_NAME,
			NOT NON_UNIQUE AS is_unique,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
		GROUP BY INDEX_NAME, NON_UNIQUE
		ORDER BY INDEX_NAME
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var columns string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &columns); err != nil {
			return err
		}
		idx.Columns = strings.Split(columns, ",")
		t.Indexes = append(t.Indexes, idx)
	}
	return rows.Err()
}

// LoadForeignKeys loads foreign key metadata for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			kcu.CONSTRAINT_NAME,
			GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS columns,
			kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME,
			GROUP_CONCAT(kcu.REFERENCED_COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS ref_columns,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		GROUP BY kcu.CONSTRAINT_NAME, kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
		ORDER BY kcu.CONSTRAINT_NAME
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk driver.ForeignKey
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefSchema, &fk.RefTable, &refColumns,
			&fk.OnUpdate, &fk.OnDelete); err != nil {
			return err
		}
		fk.Columns = strings.Split(columns, ",")
		fk.RefColumns = strings.Split(refColumns, ",")
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return rows.Err()
}

// LoadCheckConstraints loads check constraint metadata for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	// MySQL 8.0.16+ and MariaDB 10.2.1+ support check constraints
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			CONSTRAINT_NAME,
			CHECK_CLAUSE
		FROM information_schema.CHECK_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ?
		AND CONSTRAINT_NAME IN (
			SELECT CONSTRAINT_NAME
			FROM information_schema.TABLE_CONSTRAINTS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'CHECK'
		)
	`, t.Schema, t.Schema, t.Name)
	if err != nil {
		// Check constraints not supported in older versions
		logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var chk driver.CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}
	return rows.Err()
}

// ReadTable reads data from a table and returns batches via a channel.
func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	batches := make(chan driver.Batch, 4)

	go func() {
		defer close(batches)

		cols := r.dialect.ColumnListForSelect(opts.Columns, opts.ColumnTypes, opts.TargetDBType)

		if opts.Partition != nil && opts.Partition.MinPK != nil {
			r.readKeysetPagination(ctx, batches, opts, cols)
		} else if opts.Partition != nil && opts.Partition.StartRow > 0 {
			r.readRowNumberPagination(ctx, batches, opts, cols)
		} else {
			r.readFullTable(ctx, batches, opts, cols)
		}
	}()

	return batches, nil
}

func (r *Reader) readKeysetPagination(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	pkCol := opts.Table.PrimaryKey[0]
	lastPK := opts.Partition.MinPK
	maxPK := opts.Partition.MaxPK

	var dateFilter *driver.DateFilter
	if opts.DateFilter != nil {
		dateFilter = opts.DateFilter
	}

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		queryStart := time.Now()
		hasMaxPK := maxPK != nil
		query := r.dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, "", hasMaxPK, dateFilter)
		args := r.dialect.BuildKeysetArgs(lastPK, maxPK, opts.ChunkSize, hasMaxPK, dateFilter)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
			return
		}

		batch, newLastPK, err := driver.ScanRows(rows, len(opts.Columns))
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

		if maxPK != nil {
			if cmp := driver.CompareKeys(lastPK, maxPK); cmp >= 0 {
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

func (r *Reader) readRowNumberPagination(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
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

		batchSize := opts.ChunkSize
		if currentRow+int64(batchSize) > endRow {
			batchSize = int(endRow - currentRow)
		}

		queryStart := time.Now()
		query := r.dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, "")
		args := r.dialect.BuildRowNumberArgs(currentRow, batchSize)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
			return
		}

		batch, _, err := driver.ScanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- driver.Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.RowNum = currentRow

		currentRow += int64(len(batch.Rows))

		if currentRow >= endRow || len(batch.Rows) == 0 {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}
	}
}

func (r *Reader) readFullTable(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	queryStart := time.Now()
	query := fmt.Sprintf("SELECT %s FROM %s", cols, r.dialect.QualifyTable(opts.Table.Schema, opts.Table.Name))

	rows, err := r.db.QueryContext(ctx, query)
	queryTime := time.Since(queryStart)

	if err != nil {
		batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
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

		queryTime = 0
	}
}

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err = r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx,
		`SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// GetPartitionBoundaries returns partition boundaries for parallel processing.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}

	pkCol := t.PrimaryKey[0]
	qPK := r.dialect.QuoteIdentifier(pkCol)
	qualifiedTable := r.dialect.QualifyTable(t.Schema, t.Name)

	// MySQL 8.0+ supports window functions
	query := fmt.Sprintf(`
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM %s
		) AS numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, qPK, qPK, qPK, numPartitions, qPK, qualifiedTable)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying partition boundaries: %w", err)
	}
	defer rows.Close()

	var partitions []driver.Partition
	for rows.Next() {
		var p driver.Partition
		p.TableName = t.Name
		if err := rows.Scan(&p.PartitionID, &p.MinPK, &p.MaxPK, &p.RowCount); err != nil {
			return nil, fmt.Errorf("scanning partition: %w", err)
		}
		partitions = append(partitions, p)
	}

	return partitions, rows.Err()
}

// GetDateColumnInfo returns information about a date column for incremental sync.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt string
		err := r.db.QueryRowContext(ctx,
			`SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
			schema, table, col).Scan(&dt)

		if err == nil {
			validTypes := r.dialect.ValidDateTypes()
			if validTypes[strings.ToLower(dt)] {
				return col, dt, true
			}
		}
	}
	return "", "", false
}

// SampleColumnValues retrieves sample values from a column for AI type mapping context.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}

	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT CAST(%s AS CHAR) AS sample_val
		FROM %s
		WHERE %s IS NOT NULL
		LIMIT ?
	`, r.dialect.QuoteIdentifier(column), r.dialect.QualifyTable(schema, table), r.dialect.QuoteIdentifier(column))

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling column %s: %w", column, err)
	}
	defer rows.Close()

	var samples []string
	for rows.Next() {
		var val sql.NullString
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("scanning sample value: %w", err)
		}
		if val.Valid {
			samples = append(samples, val.String)
		}
	}

	return samples, rows.Err()
}

// SampleRows retrieves sample rows from a table for AI type mapping context.
func (r *Reader) SampleRows(ctx context.Context, schema, table string, columns []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 5
	}

	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	var quotedCols []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quotedCols = append(quotedCols, fmt.Sprintf("CAST(%s AS CHAR)", r.dialect.QuoteIdentifier(col)))
	}

	query := fmt.Sprintf(`SELECT %s FROM %s LIMIT ?`,
		strings.Join(quotedCols, ", "),
		r.dialect.QualifyTable(schema, table))

	result, err := driver.SampleRowsHelper(ctx, r.db, query, columns, limit, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling rows from %s: %w", table, err)
	}
	return result, nil
}
