package shared

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

// StreamConfig contains the shared database/sql read-streaming dependencies.
type StreamConfig struct {
	DB                       SQLQuerier
	Dialect                  driver.Dialect
	Buffer                   int
	TableHint                string
	KeysetQueryErrorLabel    string
	RowNumberQueryErrorLabel string
	FullTableQueryErrorLabel string
}

// StreamTable reads table rows into batches using the dialect's query builders.
func StreamTable(ctx context.Context, cfg StreamConfig, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("stream table: db is nil")
	}
	if cfg.Dialect == nil {
		return nil, fmt.Errorf("stream table: dialect is nil")
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 4
	}

	batches := make(chan driver.Batch, cfg.Buffer)
	go func() {
		defer close(batches)

		cols := cfg.Dialect.ColumnListForSelect(opts.Columns, opts.ColumnTypes, opts.TargetDBType)
		if opts.Partition != nil && opts.Partition.MinPK != nil {
			streamKeyset(ctx, batches, cfg, opts, cols)
		} else if opts.Partition != nil && opts.Partition.StartRow > 0 {
			streamRowNumber(ctx, batches, cfg, opts, cols)
		} else {
			streamFullTable(ctx, batches, cfg, opts, cols)
		}
	}()

	return batches, nil
}

func streamKeyset(ctx context.Context, batches chan<- driver.Batch, cfg StreamConfig, opts driver.ReadOptions, cols string) {
	pkCol := opts.Table.PrimaryKey[0]
	lastPK := opts.Partition.MinPK
	maxPK := opts.Partition.MaxPK

	for {
		select {
		case <-ctx.Done():
			batches <- driver.Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		queryStart := time.Now()
		hasMaxPK := maxPK != nil
		query := cfg.Dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, cfg.TableHint, hasMaxPK, opts.DateFilter)
		args := cfg.Dialect.BuildKeysetArgs(lastPK, maxPK, opts.ChunkSize, hasMaxPK, opts.DateFilter)

		rows, err := cfg.DB.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			batches <- driver.Batch{Error: streamQueryError(cfg.KeysetQueryErrorLabel, err), Done: true}
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

func streamRowNumber(ctx context.Context, batches chan<- driver.Batch, cfg StreamConfig, opts driver.ReadOptions, cols string) {
	orderBy := cfg.Dialect.ColumnList(opts.Table.PrimaryKey)
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
		query := cfg.Dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, cfg.TableHint, opts.DateFilter)
		args := cfg.Dialect.BuildRowNumberArgs(currentRow, batchSize, opts.DateFilter)

		rows, err := cfg.DB.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			batches <- driver.Batch{Error: streamQueryError(cfg.RowNumberQueryErrorLabel, err), Done: true}
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

func streamFullTable(ctx context.Context, batches chan<- driver.Batch, cfg StreamConfig, opts driver.ReadOptions, cols string) {
	queryStart := time.Now()
	query := fmt.Sprintf("SELECT %s FROM %s", cols, cfg.Dialect.QualifyTable(opts.Table.Schema, opts.Table.Name))
	if cfg.TableHint != "" {
		query += " " + cfg.TableHint
	}

	rows, err := cfg.DB.QueryContext(ctx, query)
	queryTime := time.Since(queryStart)
	if err != nil {
		batches <- driver.Batch{Error: streamQueryError(cfg.FullTableQueryErrorLabel, err), Done: true}
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

func streamQueryError(label string, err error) error {
	if label == "" {
		label = "query error"
	}
	return fmt.Errorf("%s: %w", label, err)
}
