package shared

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

// QueryPartitionBoundaries executes a partition-boundary query that returns
// partition_id, min_pk, max_pk, and row_count columns.
func QueryPartitionBoundaries(ctx context.Context, db SQLQuerier, query, tableName string, args ...any) ([]driver.Partition, error) {
	if db == nil {
		return nil, fmt.Errorf("query partition boundaries: db is nil")
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying partition boundaries: %w", err)
	}
	defer rows.Close()

	return ScanPartitionBoundaries(rows, tableName)
}

// ScanPartitionBoundaries scans partition_id, min_pk, max_pk, and row_count
// rows into driver.Partition values for one table.
func ScanPartitionBoundaries(rows *sql.Rows, tableName string) ([]driver.Partition, error) {
	if rows == nil {
		return nil, fmt.Errorf("scan partition boundaries: rows is nil")
	}

	var partitions []driver.Partition
	for rows.Next() {
		var partition driver.Partition
		partition.TableName = tableName
		if err := rows.Scan(&partition.PartitionID, &partition.MinPK, &partition.MaxPK, &partition.RowCount); err != nil {
			return nil, fmt.Errorf("scanning partition: %w", err)
		}
		partitions = append(partitions, partition)
	}

	return partitions, rows.Err()
}
