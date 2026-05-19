package sqlite

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return r.GetRowCountExact(ctx, schema, table, false)
}

// GetRowCountFast returns an approximate row count. SQLite has no stats
// catalog; falls back to COUNT(*).
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	return r.GetRowCountExact(ctx, schema, table, false)
}

// GetRowCountExact returns the exact row count via COUNT(*). The
// strictConsistency flag has no meaning in SQLite (MVCC-like via WAL).
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, r.db, r.dialect, schema, table)
}

// GetPartitionBoundaries returns a single partition spanning the full PK
// range, regardless of the requested partition count. SQLite has no
// parallelism benefit from splitting a table (single-writer / single
// shared file).
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}
	pkCol := t.PrimaryKey[0]
	qPK := r.dialect.QuoteIdentifier(pkCol)
	qualifiedTable := r.dialect.QualifyTable(t.Schema, t.Name)

	row := r.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s", qPK, qPK, qualifiedTable))

	var p driver.Partition
	p.TableName = t.Name
	p.PartitionID = 1
	p.IsFirstPartition = true
	if err := row.Scan(&p.MinPK, &p.MaxPK, &p.RowCount); err != nil {
		return nil, fmt.Errorf("scanning partition boundaries: %w", err)
	}
	return []driver.Partition{p}, nil
}
