package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

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

		// Compute Go heap cost per row from column metadata (static baseline)
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Override with actual avg row sizes from database statistics when available.
	r.applyActualRowSizes(ctx, dbName, tables)

	return tables, nil
}

// applyActualRowSizes queries information_schema.TABLES for actual average row
// sizes and overrides the static estimate when the DB reports a larger value.
func (r *Reader) applyActualRowSizes(ctx context.Context, dbName string, tables []driver.Table) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT TABLE_NAME, IFNULL(AVG_ROW_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	`, dbName)
	if err != nil {
		logging.Debug("Failed to query actual row sizes: %v", err)
		return
	}
	defer rows.Close()

	sizeMap := make(map[string]int64)
	for rows.Next() {
		var name string
		var avgSize int64
		if err := rows.Scan(&name, &avgSize); err != nil {
			continue
		}
		if avgSize > 0 {
			sizeMap[name] = avgSize
		}
	}

	for i := range tables {
		if dbSize, ok := sizeMap[tables[i].Name]; ok && dbSize > tables[i].EstimatedRowSize {
			logging.Debug("Table %s: using DB avg row size %d bytes (static estimate was %d)",
				tables[i].Name, dbSize, tables[i].EstimatedRowSize)
			tables[i].EstimatedRowSize = dbSize
		}
	}
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
			COALESCE(COLUMN_DEFAULT, ''),
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
			&c.IsNullable, &c.IsIdentity, &c.DefaultValue, &c.OrdinalPos); err != nil {
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
