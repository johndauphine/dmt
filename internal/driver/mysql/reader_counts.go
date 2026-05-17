package mysql

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
)

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
// MySQL has no NOLOCK equivalent (uses MVCC); strictConsistency is
// accepted for interface symmetry and ignored here.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
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
