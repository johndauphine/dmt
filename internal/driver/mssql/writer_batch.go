package mssql

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	mssql "github.com/microsoft/go-mssqldb"
)

// WriteBatch writes a batch of rows using TDS bulk copy.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	// Resume-safe path for ROW_NUMBER-paged tables: stage + insert-only MERGE
	// so replayed rows that already exist become silent no-ops (#227).
	if opts.IdempotentOnDup {
		return w.writeBatchIdempotent(ctx, opts)
	}

	fullTableName := fmt.Sprintf("[%s].[%s]", opts.Schema, opts.Table)

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}
	defer conn.Close()

	// Set lock timeout to prevent indefinite waits on row/page locks during
	// parallel bulk inserts. 5 minutes is generous for bulk operations.
	// Without this, concurrent writers or uncommitted transactions can
	// block each other indefinitely.
	if _, err := conn.ExecContext(ctx, "SET LOCK_TIMEOUT 300000"); err != nil {
		return fmt.Errorf("setting lock timeout: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	err = conn.Raw(func(driverConn any) error {
		mssqlConn, ok := driverConn.(*mssql.Conn)
		if !ok {
			return fmt.Errorf("expected *mssql.Conn, got %T", driverConn)
		}

		// Sub-batch rows to avoid accumulating too much data in the TDS
		// session buffer between CreateBulkContext and Done().
		// Use per-call BatchSize, then writer default, then fallback.
		// Per-call BatchSize overrides writer default (from target.chunk_size config).
		batchRows := opts.BatchSize
		if batchRows <= 0 {
			batchRows = w.defaultBatchSize
		}
		if batchRows <= 0 {
			return fmt.Errorf("batch size not configured: set chunk_size in config or enable AI tuning")
		}
		for start := 0; start < len(opts.Rows); start += batchRows {
			end := start + batchRows
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]

			bulk := mssqlConn.CreateBulkContext(ctx, fullTableName, opts.Columns)
			// No TABLOCK — enables parallel BCP writers per table.
			// TABLOCK serializes writes but enables minimal logging.
			// Without it, writes are fully logged but parallelizable.
			bulk.Options.Tablock = false
			bulk.Options.RowsPerBatch = len(subBatch)
			if len(opts.OrderColumns) > 0 {
				orderHints := make([]string, len(opts.OrderColumns))
				for i, col := range opts.OrderColumns {
					orderHints[i] = fmt.Sprintf("[%s] ASC", col)
				}
				bulk.Options.Order = orderHints
			}

			for _, row := range subBatch {
				if err := bulk.AddRow(convertRowForBulkCopy(row, opts.ColumnTypes)); err != nil {
					return fmt.Errorf("adding row: %w", err)
				}
			}

			rowsAffected, err := bulk.Done()
			if err != nil {
				return fmt.Errorf("finalizing bulk insert: %w", err)
			}

			if rowsAffected != int64(len(subBatch)) {
				return fmt.Errorf("bulk insert: expected %d rows, got %d", len(subBatch), rowsAffected)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("bulk copy: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing bulk copy: %w", err)
	}

	return nil
}
