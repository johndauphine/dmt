package transfer

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/pool"
)

// writeChunkGeneric writes a chunk of data using the appropriate target pool
func writeChunkGeneric(ctx context.Context, tgtPool pool.TargetPool, schema, table string, cols, colTypes []string, rows [][]any, batchSize int, orderCols ...string) error {
	return tgtPool.WriteBatch(ctx, pool.WriteBatchOptions{
		Schema:       schema,
		Table:        table,
		Columns:      cols,
		ColumnTypes:  colTypes,
		Rows:         rows,
		BatchSize:    batchSize,
		OrderColumns: orderCols,
	})
}

// writeChunkIdempotent writes a chunk in idempotent-on-duplicate mode used by
// ROW_NUMBER resume (#227). The driver-specific WriteBatch implementation
// switches to its insert-only path (staging + INSERT...ON CONFLICT DO NOTHING
// for PG/MSSQL, INSERT ... ON DUPLICATE KEY UPDATE pk = pk for MySQL) so a
// replayed chunk is a silent no-op for already-committed rows.
func writeChunkIdempotent(ctx context.Context, tgtPool pool.TargetPool, schema, table string,
	cols, colTypes, pkCols []string, rows [][]any, writerID int, partitionID *int, batchSize int) error {
	return tgtPool.WriteBatch(ctx, pool.WriteBatchOptions{
		Schema:          schema,
		Table:           table,
		Columns:         cols,
		ColumnTypes:     colTypes,
		Rows:            rows,
		BatchSize:       batchSize,
		IdempotentOnDup: true,
		PKColumns:       pkCols,
		WriterID:        writerID,
		PartitionID:     partitionID,
	})
}

// writeChunkUpsertWithWriter writes a chunk using high-performance staging table approach.
// This uses per-writer staging tables for isolation and better parallelism:
// - PostgreSQL: TEMP table + COPY + INSERT...ON CONFLICT
// - MSSQL: #temp table + bulk insert + MERGE WITH (TABLOCK)
// colTypes is passed to skip geography/geometry from change detection in MSSQL MERGE
// colSRIDs is passed for geography/geometry SRID in STGeomFromText conversion (PG→MSSQL)
func writeChunkUpsertWithWriter(ctx context.Context, tgtPool pool.TargetPool, schema, table string,
	cols []string, colTypes []string, colSRIDs []int, pkCols []string, rows [][]any, writerID int, partitionID *int, batchSize int) error {
	// Upsert is an optional capability (#476). The orchestrator rejects
	// target_mode=upsert against incapable engines at construction, so
	// this assertion is a backstop with a clear message, not a hot-path
	// concern (a type assertion is ~ns against a network write).
	up, ok := tgtPool.(driver.Upserter)
	if !ok {
		return fmt.Errorf("target engine %s does not support upsert", tgtPool.DBType())
	}
	return up.UpsertBatch(ctx, pool.UpsertBatchOptions{
		Schema:      schema,
		Table:       table,
		Columns:     cols,
		ColumnTypes: colTypes,
		ColumnSRIDs: colSRIDs,
		PKColumns:   pkCols,
		Rows:        rows,
		BatchSize:   batchSize,
		WriterID:    writerID,
		PartitionID: partitionID,
	})
}
