//go:build cgo

package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/godror/godror"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader for Oracle Database.
type Reader struct {
	db            *sql.DB
	config        *dbconfig.SourceConfig
	maxConns      int
	dialect       *Dialect
	oracleVersion string
}

// NewReader creates a new Oracle reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	idleConns := maxConns / 4
	if idleConns < 1 {
		idleConns = 1
	}
	db.SetMaxIdleConns(idleConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Detect Oracle version
	var version string
	if err := db.QueryRow("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1").Scan(&version); err != nil {
		// May not have access to V$VERSION (requires SELECT privilege)
		version = "Oracle (version unknown)"
	}

	logging.Debug("Connected to Oracle source: %s:%d/%s (%s)", cfg.Host, cfg.Port, cfg.Database, version)

	return &Reader{
		db:            db,
		config:        cfg,
		maxConns:      maxConns,
		dialect:       dialect,
		oracleVersion: version,
	}, nil
}

// Close closes all connections.
func (r *Reader) Close() error {
	return r.db.Close()
}

// DB returns the underlying database connection.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the maximum number of connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "oracle"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "oracle",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExtractSchema extracts the schema for all tables.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	tables := []driver.Table{}

	// Normalize schema name (Oracle stores in uppercase by default)
	schemaName := strings.ToUpper(schema)
	if schemaName == "" {
		schemaName = strings.ToUpper(r.config.User)
	}

	// Get tables from ALL_TABLES
	rows, err := r.db.QueryContext(ctx, `
		SELECT OWNER, TABLE_NAME
		FROM ALL_TABLES
		WHERE OWNER = :1
		AND SECONDARY = 'N'
		AND NESTED = 'NO'
		AND DROPPED = 'NO'
		ORDER BY TABLE_NAME
	`, schemaName)
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

		// Populate PKColumns with full metadata
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
			NVL(CHAR_LENGTH, NVL(DATA_LENGTH, 0)) AS max_length,
			NVL(DATA_PRECISION, 0) AS data_precision,
			NVL(DATA_SCALE, 0) AS data_scale,
			CASE WHEN NULLABLE = 'Y' THEN 1 ELSE 0 END AS is_nullable,
			CASE WHEN IDENTITY_COLUMN = 'YES' THEN 1 ELSE 0 END AS is_identity,
			COLUMN_ID AS ordinal_position
		FROM ALL_TAB_COLUMNS
		WHERE OWNER = :1 AND TABLE_NAME = :2
		ORDER BY COLUMN_ID
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying columns for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var c driver.Column
		var isNullable, isIdentity int
		if err := rows.Scan(&c.Name, &c.DataType, &c.MaxLength, &c.Precision, &c.Scale,
			&isNullable, &isIdentity, &c.OrdinalPos); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}
		c.IsNullable = isNullable == 1
		c.IsIdentity = isIdentity == 1

		// Normalize Oracle type names
		c.DataType = normalizeOracleType(c.DataType)

		t.Columns = append(t.Columns, c)
	}
	return rows.Err()
}

// normalizeOracleType normalizes Oracle type names
func normalizeOracleType(dataType string) string {
	return strings.ToUpper(strings.TrimSpace(dataType))
}

// normalizeOracleValues converts Oracle-specific types to standard Go types.
// This ensures all target databases receive compatible types.
func normalizeOracleValues(rows [][]any) {
	for i := range rows {
		for j := range rows[i] {
			switch val := rows[i][j].(type) {
			case godror.Number:
				// Oracle NUMBER type - convert to string to preserve precision
				// Target databases can parse the string into their native numeric types
				rows[i][j] = val.String()
			}
		}
	}
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT cc.COLUMN_NAME
		FROM ALL_CONSTRAINTS c
		JOIN ALL_CONS_COLUMNS cc ON c.OWNER = cc.OWNER
			AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
		WHERE c.OWNER = :1
			AND c.TABLE_NAME = :2
			AND c.CONSTRAINT_TYPE = 'P'
		ORDER BY cc.POSITION
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

// LoadIndexes loads indexes for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			i.INDEX_NAME,
			i.UNIQUENESS,
			LISTAGG(ic.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) AS columns
		FROM ALL_INDEXES i
		JOIN ALL_IND_COLUMNS ic ON i.OWNER = ic.INDEX_OWNER
			AND i.INDEX_NAME = ic.INDEX_NAME
		WHERE i.TABLE_OWNER = :1
			AND i.TABLE_NAME = :2
			AND i.INDEX_TYPE != 'LOB'
			AND NOT EXISTS (
				SELECT 1 FROM ALL_CONSTRAINTS c
				WHERE c.OWNER = i.OWNER AND c.CONSTRAINT_NAME = i.INDEX_NAME AND c.CONSTRAINT_TYPE = 'P'
			)
		GROUP BY i.INDEX_NAME, i.UNIQUENESS
		ORDER BY i.INDEX_NAME
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var uniqueness, columns string
		if err := rows.Scan(&idx.Name, &uniqueness, &columns); err != nil {
			return err
		}
		idx.IsUnique = (uniqueness == "UNIQUE")
		idx.Columns = strings.Split(columns, ",")
		t.Indexes = append(t.Indexes, idx)
	}
	return rows.Err()
}

// LoadForeignKeys loads foreign keys for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			c.CONSTRAINT_NAME,
			LISTAGG(cc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY cc.POSITION) AS columns,
			rc.TABLE_NAME AS ref_table,
			LISTAGG(rcc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY rcc.POSITION) AS ref_columns,
			c.DELETE_RULE
		FROM ALL_CONSTRAINTS c
		JOIN ALL_CONS_COLUMNS cc ON c.OWNER = cc.OWNER
			AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
		JOIN ALL_CONSTRAINTS rc ON c.R_OWNER = rc.OWNER
			AND c.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		JOIN ALL_CONS_COLUMNS rcc ON rc.OWNER = rcc.OWNER
			AND rc.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
			AND cc.POSITION = rcc.POSITION
		WHERE c.OWNER = :1
			AND c.TABLE_NAME = :2
			AND c.CONSTRAINT_TYPE = 'R'
		GROUP BY c.CONSTRAINT_NAME, rc.TABLE_NAME, c.DELETE_RULE
		ORDER BY c.CONSTRAINT_NAME
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk driver.ForeignKey
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefTable, &refColumns, &fk.OnDelete); err != nil {
			return err
		}
		fk.Columns = strings.Split(columns, ",")
		fk.RefColumns = strings.Split(refColumns, ",")
		fk.RefSchema = t.Schema   // Same schema
		fk.OnUpdate = "NO ACTION" // Oracle doesn't track ON UPDATE
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return rows.Err()
}

// LoadCheckConstraints loads check constraints for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT CONSTRAINT_NAME, SEARCH_CONDITION
		FROM ALL_CONSTRAINTS
		WHERE OWNER = :1
			AND TABLE_NAME = :2
			AND CONSTRAINT_TYPE = 'C'
			AND GENERATED = 'USER NAME'
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying check constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chk driver.CheckConstraint
		var definition sql.NullString
		if err := rows.Scan(&chk.Name, &definition); err != nil {
			return err
		}
		if definition.Valid {
			chk.Definition = definition.String
			// Filter out system-generated NOT NULL constraints
			if !strings.Contains(strings.ToUpper(chk.Definition), "IS NOT NULL") {
				t.CheckConstraints = append(t.CheckConstraints, chk)
			}
		}
	}
	return rows.Err()
}

// ReadTable reads data from a table with pagination support.
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
	hasMaxPK := maxPK != nil

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		query := r.dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, "", hasMaxPK, opts.DateFilter)
		args := r.dialect.BuildKeysetArgs(lastPK, maxPK, opts.ChunkSize, hasMaxPK, opts.DateFilter)

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("keyset query: %w", err), Done: true}
			return
		}

		batch, newLastPK, err := driver.ScanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("scanning rows: %w", err), Done: true}
			return
		}

		// Normalize Oracle-specific types for target compatibility
		normalizeOracleValues(batch.Rows)

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}

		lastPK = newLastPK

		// Check if we've reached the end
		if hasMaxPK && driver.CompareKeys(lastPK, maxPK) >= 0 {
			batch.Done = true
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
	rowNum := opts.Partition.StartRow - 1 // Convert 1-based to 0-based
	endRow := opts.Partition.EndRow

	orderBy := r.dialect.ColumnList(opts.Table.PrimaryKey)
	if orderBy == "" {
		orderBy = r.dialect.QuoteIdentifier(opts.Columns[0])
	}

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		query := r.dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, "", nil)
		limit := opts.ChunkSize
		if endRow > 0 && rowNum+int64(limit) > endRow {
			limit = int(endRow - rowNum)
		}
		args := r.dialect.BuildRowNumberArgs(rowNum, limit, nil)

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("row number query: %w", err), Done: true}
			return
		}

		batch, _, err := driver.ScanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("scanning rows: %w", err), Done: true}
			return
		}

		// Normalize Oracle-specific types for target compatibility
		normalizeOracleValues(batch.Rows)

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}

		rowNum += int64(len(batch.Rows))
		batch.RowNum = rowNum

		// Check if we've reached the end
		if endRow > 0 && rowNum >= endRow {
			batch.Done = true
		}
		if len(batch.Rows) < limit {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}
	}
}

func (r *Reader) readFullTable(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	qualifiedTable := r.dialect.QualifyTable(opts.Table.Schema, opts.Table.Name)

	query := fmt.Sprintf("SELECT %s FROM %s", cols, qualifiedTable)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		batches <- driver.Batch{Error: fmt.Errorf("full table query: %w", err), Done: true}
		return
	}
	defer rows.Close()

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		batch := driver.Batch{Rows: make([][]any, 0, opts.ChunkSize)}

		for len(batch.Rows) < opts.ChunkSize && rows.Next() {
			row := make([]any, len(opts.Columns))
			scanDest := make([]any, len(opts.Columns))
			for i := range row {
				scanDest[i] = &row[i]
			}
			if err := rows.Scan(scanDest...); err != nil {
				batches <- driver.Batch{Error: fmt.Errorf("scanning row: %w", err), Done: true}
				return
			}
			batch.Rows = append(batch.Rows, row)
		}

		if err := rows.Err(); err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("iterating rows: %w", err), Done: true}
			return
		}

		// Normalize Oracle-specific types for target compatibility
		normalizeOracleValues(batch.Rows)

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
	}
}

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}
	return r.GetRowCountExact(ctx, schema, table)
}

// GetRowCountFast returns an approximate row count using statistics.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count sql.NullInt64
	err := r.db.QueryRowContext(ctx,
		`SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`,
		strings.ToUpper(schema), strings.ToUpper(table)).Scan(&count)
	if err != nil || !count.Valid {
		return 0, err
	}
	return count.Int64, nil
}

// GetRowCountExact returns the exact row count using COUNT(*).
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// GetPartitionBoundaries returns partition boundaries for parallel reading.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key for partitioning", t.Name)
	}

	pkCol := t.PrimaryKey[0]
	query := r.dialect.PartitionBoundariesQuery(pkCol, t.Schema, t.Name, numPartitions)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("partition query: %w", err)
	}
	defer rows.Close()

	var partitions []driver.Partition
	for rows.Next() {
		var p driver.Partition
		var minPK, maxPK any
		if err := rows.Scan(&p.PartitionID, &minPK, &maxPK, &p.RowCount); err != nil {
			return nil, fmt.Errorf("scanning partition: %w", err)
		}
		p.MinPK = minPK
		p.MaxPK = maxPK
		partitions = append(partitions, p)
	}

	return partitions, rows.Err()
}

// GetDateColumnInfo returns information about a date column.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt string
		err := r.db.QueryRowContext(ctx, r.dialect.DateColumnQuery(),
			strings.ToUpper(schema), strings.ToUpper(table), strings.ToUpper(col)).Scan(&dt)
		if err == nil {
			if r.dialect.ValidDateTypes()[strings.ToUpper(dt)] {
				return col, dt, true
			}
		}
	}
	return "", "", false
}

// SampleColumnValues samples values from a column for AI type mapping.
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
		SELECT DISTINCT TO_CHAR(%s) AS sample_val
		FROM %s
		WHERE %s IS NOT NULL
		AND ROWNUM <= :1
	`, r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
		r.dialect.QuoteIdentifier(column))

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

// SampleRows samples multiple columns at once.
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
		quotedCols = append(quotedCols, fmt.Sprintf("TO_CHAR(%s) AS %s",
			r.dialect.QuoteIdentifier(col), r.dialect.QuoteIdentifier(col)))
	}

	query := fmt.Sprintf(`SELECT %s FROM %s WHERE ROWNUM <= :1`,
		strings.Join(quotedCols, ", "),
		r.dialect.QualifyTable(schema, table))

	return driver.SampleRowsHelper(ctx, r.db, query, columns, limit, limit)
}
