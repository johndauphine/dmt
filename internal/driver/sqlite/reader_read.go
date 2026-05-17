package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

// ReadTable streams rows from a table via a channel.
func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	batches := make(chan driver.Batch, 4)

	go func() {
		defer close(batches)

		cols := r.dialect.ColumnListForSelect(opts.Columns, opts.ColumnTypes, opts.TargetDBType)

		if opts.Partition != nil && opts.Partition.MinPK != nil {
			r.readKeyset(ctx, batches, opts, cols)
		} else if opts.Partition != nil && opts.Partition.StartRow > 0 {
			r.readRowNumber(ctx, batches, opts, cols)
		} else {
			r.readFullTable(ctx, batches, opts, cols)
		}
	}()

	return batches, nil
}

func (r *Reader) readKeyset(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	pkCol := opts.Table.PrimaryKey[0]
	lastPK := opts.Partition.MinPK
	maxPK := opts.Partition.MaxPK
	var dateFilter *driver.DateFilter
	if opts.DateFilter != nil {
		dateFilter = opts.DateFilter
	}

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		queryStart := time.Now()
		hasMaxPK := maxPK != nil
		query := r.dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, "", hasMaxPK, dateFilter)
		args := r.dialect.BuildKeysetArgs(lastPK, maxPK, opts.ChunkSize, hasMaxPK, dateFilter)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
			return
		}

		batch, newLastPK, err := driver.ScanRows(rows, len(opts.Columns))
		rows.Close()
		if err != nil {
			batches <- driver.Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.LastKey = newLastPK

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}

		lastPK = newLastPK

		if maxPK != nil {
			if cmp := driver.CompareKeys(lastPK, maxPK); cmp >= 0 {
				batch.Done = true
			}
		}
		if len(batch.Rows) < opts.ChunkSize {
			batch.Done = true
		}

		batches <- batch
		if batch.Done {
			return
		}
	}
}

func (r *Reader) readRowNumber(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	orderBy := r.dialect.ColumnList(opts.Table.PrimaryKey)
	startRow := opts.Partition.StartRow
	endRow := opts.Partition.EndRow
	currentRow := startRow

	for currentRow < endRow {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		batchSize := opts.ChunkSize
		if currentRow+int64(batchSize) > endRow {
			batchSize = int(endRow - currentRow)
		}

		queryStart := time.Now()
		query := r.dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, "", nil)
		args := r.dialect.BuildRowNumberArgs(currentRow, batchSize, nil)

		rows, err := r.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
			return
		}

		batch, _, err := driver.ScanRows(rows, len(opts.Columns))
		rows.Close()
		if err != nil {
			batches <- driver.Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.RowNum = currentRow
		currentRow += int64(len(batch.Rows))

		if currentRow >= endRow || len(batch.Rows) == 0 {
			batch.Done = true
		}

		batches <- batch
		if batch.Done {
			return
		}
	}
}

func (r *Reader) readFullTable(ctx context.Context, batches chan<- driver.Batch, opts driver.ReadOptions, cols string) {
	queryStart := time.Now()
	query := fmt.Sprintf("SELECT %s FROM %s", cols, r.dialect.QualifyTable(opts.Table.Schema, opts.Table.Name))

	rows, err := r.db.QueryContext(ctx, query)
	queryTime := time.Since(queryStart)
	if err != nil {
		batches <- driver.Batch{Error: fmt.Errorf("query error: %w", err), Done: true}
		return
	}
	defer rows.Close()

	for {
		batch := driver.Batch{Stats: driver.BatchStats{QueryTime: queryTime}}
		scanStart := time.Now()
		for i := 0; i < opts.ChunkSize && rows.Next(); i++ {
			row := make([]any, len(opts.Columns))
			ptrs := make([]any, len(opts.Columns))
			for j := range row {
				ptrs[j] = &row[j]
			}
			if err := rows.Scan(ptrs...); err != nil {
				batches <- driver.Batch{Error: err, Done: true}
				return
			}
			batch.Rows = append(batch.Rows, row)
		}
		batch.Stats.ScanTime = time.Since(scanStart)

		if len(batch.Rows) == 0 {
			batch.Done = true
			batches <- batch
			return
		}
		if len(batch.Rows) < opts.ChunkSize {
			batch.Done = true
		}

		batches <- batch
		if batch.Done {
			return
		}
		queryTime = 0
	}
}
