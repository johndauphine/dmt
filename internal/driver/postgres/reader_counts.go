package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return shared.RowCountWithFallback(
		func() (int64, error) { return r.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return r.GetRowCountExact(ctx, schema, table, false) },
	)
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.sqlDB.QueryRowContext(ctx,
		`SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// Postgres has no NOLOCK equivalent (uses MVCC); strictConsistency
// is accepted for interface symmetry and ignored here.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, r.sqlDB, r.dialect, schema, table)
}

// GetPartitionBoundaries returns partition boundaries for parallel processing.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}

	pkCol := t.PrimaryKey[0]
	query := fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM %s
		)
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*)
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, r.dialect.QuoteIdentifier(pkCol), numPartitions, r.dialect.QuoteIdentifier(pkCol),
		r.dialect.QualifyTable(t.Schema, t.Name),
		r.dialect.QuoteIdentifier(pkCol), r.dialect.QuoteIdentifier(pkCol))

	return shared.QueryPartitionBoundaries(ctx, r.sqlDB, query, t.Name)
}

// GetDateColumnInfo returns information about a date column for incremental sync.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt string
		err := r.sqlDB.QueryRowContext(ctx,
			`SELECT udt_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
			schema, table, col).Scan(&dt)

		if err == nil {
			validTypes := r.dialect.ValidDateTypes()
			if validTypes[dt] {
				return col, dt, true
			}
		}
	}
	return "", "", false
}

// GetMaxDateColumnValue returns the current source-side high watermark for a
// date column. A nil value means the table has no non-NULL values in that
// column yet.
func (r *Reader) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
	)

	var raw any
	if err := r.sqlDB.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}
