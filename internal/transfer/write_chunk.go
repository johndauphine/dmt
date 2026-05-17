package transfer

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/dmt/internal/pool"
)

func writeChunk(ctx context.Context, pgPool *pgxpool.Pool, schema, table string, cols []string, rows [][]any) error {
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Disable statement timeout for this operation
	_, err = conn.Exec(ctx, "SET statement_timeout = 0")
	if err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}

	// Use COPY for bulk insert
	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{schema, table},
		cols,
		pgx.CopyFromRows(rows),
	)

	return err
}

// writeChunkGeneric writes a chunk of data using the appropriate target pool
func writeChunkGeneric(ctx context.Context, tgtPool pool.TargetPool, schema, table string, cols []string, rows [][]any, batchSize int, orderCols ...string) error {
	return tgtPool.WriteBatch(ctx, pool.WriteBatchOptions{
		Schema:       schema,
		Table:        table,
		Columns:      cols,
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
	cols, pkCols []string, rows [][]any, writerID int, partitionID *int, batchSize int) error {
	return tgtPool.WriteBatch(ctx, pool.WriteBatchOptions{
		Schema:          schema,
		Table:           table,
		Columns:         cols,
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
	return tgtPool.UpsertBatch(ctx, pool.UpsertBatchOptions{
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

// ValidateBinaryData ensures binary data is properly formatted
func ValidateBinaryData(data []byte) []byte {
	if data == nil || len(data) == 0 {
		return nil
	}
	return data
}

// FormatBytea formats binary data for PostgreSQL bytea column
func FormatBytea(data []byte) string {
	if data == nil || len(data) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("\\x")
	buf.WriteString(hex.EncodeToString(data))
	return buf.String()
}
