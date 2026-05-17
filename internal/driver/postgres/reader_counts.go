package postgres

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
)

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err = r.sqlDB.QueryRowContext(ctx, query).Scan(&count)
	return count, err
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
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err := r.sqlDB.QueryRowContext(ctx, query).Scan(&count)
	return count, err
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

	rows, err := r.sqlDB.QueryContext(ctx, query)
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
