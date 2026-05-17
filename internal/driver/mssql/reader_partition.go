package mssql

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) != 1 {
		return nil, fmt.Errorf("partitioning requires single-column PK")
	}

	pkCol := r.dialect.QuoteIdentifier(t.PrimaryKey[0])
	qualifiedTable := r.dialect.QualifyTable(t.Schema, t.Name)

	// Get MIN and MAX of the primary key (uses index, very fast)
	var minPK, maxPK int64
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WITH (NOLOCK)", pkCol, pkCol, qualifiedTable)
	err := r.db.QueryRowContext(ctx, query).Scan(&minPK, &maxPK)
	if err != nil {
		return nil, fmt.Errorf("getting MIN/MAX: %w", err)
	}

	// Get approximate row count from stats
	rowCount, _ := r.GetRowCountFast(ctx, t.Schema, t.Name)
	if rowCount == 0 {
		rowCount = maxPK - minPK + 1 // Fallback estimate
	}

	// Calculate even partition boundaries
	rangeSize := maxPK - minPK + 1
	partitionSize := rangeSize / int64(numPartitions)
	rowsPerPartition := rowCount / int64(numPartitions)

	var partitions []driver.Partition
	for i := 0; i < numPartitions; i++ {
		start := minPK + int64(i)*partitionSize
		end := minPK + int64(i+1)*partitionSize - 1
		if i == numPartitions-1 {
			end = maxPK // Last partition takes the remainder
		}

		partitions = append(partitions, driver.Partition{
			TableName:   t.FullName(),
			PartitionID: i + 1,
			MinPK:       start,
			MaxPK:       end,
			RowCount:    rowsPerPartition,
		})
	}

	logging.Debug("  %s: %d partitions via MIN/MAX (range %d-%d)", t.Name, numPartitions, minPK, maxPK)
	return partitions, nil
}
