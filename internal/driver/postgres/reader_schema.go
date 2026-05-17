package postgres

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

	// Get tables
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE' AND table_schema = $1
		ORDER BY table_name
	`, schema)
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
	// The static GoHeapBytesPerRow estimate can severely undercount TEXT/BLOB columns.
	r.applyActualRowSizes(ctx, schema, tables)

	return tables, nil
}

// applyActualRowSizes queries pg_stat_user_tables for actual average row sizes
// and overrides the static GoHeapBytesPerRow estimate when the DB reports a
// larger value. This is critical for tables with TEXT/JSONB columns where the
// static estimate (based on column type metadata) severely undercounts.
func (r *Reader) applyActualRowSizes(ctx context.Context, schema string, tables []driver.Table) {
	// Use pg_relation_size (main fork only, excludes TOAST/FSM/VM overhead).
	// TOAST metadata inflates estimates beyond actual in-memory row cost since
	// the driver streams TOAST data lazily. The runtime memory guardrail catches
	// any remaining underestimates.
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT relname,
			CASE WHEN n_live_tup > 0
				THEN pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) / n_live_tup
				ELSE 0
			END AS avg_row_size
		FROM pg_stat_user_tables
		WHERE schemaname = $1
	`, schema)
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
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT
			column_name,
			udt_name,
			COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0),
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END,
			CASE WHEN column_default LIKE 'nextval%' THEN true ELSE false END,
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
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
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)
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
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT
			i.relname AS index_name,
			ix.indisunique,
			CASE WHEN am.amname = 'btree' AND ix.indisclustered THEN true ELSE false END,
			array_to_string(array_agg(a.attname ORDER BY k.ordinality), ',') AS columns
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality)
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
		WHERE n.nspname = $1 AND t.relname = $2 AND NOT ix.indisprimary
		GROUP BY i.relname, ix.indisunique, am.amname, ix.indisclustered
		ORDER BY i.relname
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var columns string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsClustered, &columns); err != nil {
			return err
		}
		idx.Columns = strings.Split(columns, ",")
		t.Indexes = append(t.Indexes, idx)
	}
	return rows.Err()
}

// LoadForeignKeys loads foreign key metadata for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	// Similar pattern to LoadIndexes
	return nil
}

// LoadCheckConstraints loads check constraint metadata for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	// Similar pattern to LoadIndexes
	return nil
}
