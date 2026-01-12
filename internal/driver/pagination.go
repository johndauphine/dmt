package driver

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Querier is an interface for executing database queries.
// Both *sql.DB and *sql.Conn implement this interface.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// PaginationDialect defines the methods needed for pagination query building.
type PaginationDialect interface {
	ColumnList(cols []string) string
	QualifyTable(schema, table string) string
	BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *DateFilter) string
	BuildKeysetArgs(lastPK, maxPK any, dateFilter *DateFilter) []any
	BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string
	BuildRowNumberArgs(startRow, endRow int64) []any
}

// PaginatedReader provides common pagination loop logic for database readers.
// Embed this in driver-specific readers to reuse pagination functionality.
type PaginatedReader struct {
	db      Querier
	dialect PaginationDialect
}

// NewPaginatedReader creates a new paginated reader with the given database and dialect.
func NewPaginatedReader(db Querier, dialect PaginationDialect) *PaginatedReader {
	return &PaginatedReader{
		db:      db,
		dialect: dialect,
	}
}

// ReadKeysetPagination performs keyset-based pagination reading.
// This is the most efficient method for tables with sequential integer primary keys.
func (p *PaginatedReader) ReadKeysetPagination(
	ctx context.Context,
	batches chan<- Batch,
	opts ReadOptions,
	cols string,
	tableHint string,
) {
	pkCol := opts.Table.PrimaryKey[0]
	lastPK := opts.Partition.MinPK
	maxPK := opts.Partition.MaxPK

	for {
		select {
		case <-ctx.Done():
			batches <- Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		queryStart := time.Now()
		query := p.dialect.BuildKeysetQuery(cols, pkCol, opts.Table.Schema, opts.Table.Name, tableHint, maxPK != nil, opts.DateFilter)
		args := p.dialect.BuildKeysetArgs(lastPK, maxPK, opts.DateFilter)

		// Add chunk size to args (varies by dialect)
		args = append([]any{opts.ChunkSize}, args...)

		rows, err := p.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- Batch{Error: fmt.Errorf("keyset query: %w", err), Done: true}
			return
		}

		batch, newLastPK, err := ScanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- Batch{Error: err, Done: true}
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

		// Check if we've reached the end
		if maxPK != nil {
			if cmp := CompareKeys(lastPK, maxPK); cmp >= 0 {
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

// ReadRowNumberPagination performs ROW_NUMBER-based pagination reading.
// Use this for tables with composite or non-sequential primary keys.
func (p *PaginatedReader) ReadRowNumberPagination(
	ctx context.Context,
	batches chan<- Batch,
	opts ReadOptions,
	cols string,
	tableHint string,
) {
	orderBy := p.dialect.ColumnList(opts.Table.PrimaryKey)
	startRow := opts.Partition.StartRow
	endRow := opts.Partition.EndRow

	currentRow := startRow

	for currentRow < endRow {
		select {
		case <-ctx.Done():
			batches <- Batch{Error: ctx.Err(), Done: true}
			return
		default:
		}

		batchEnd := currentRow + int64(opts.ChunkSize)
		if batchEnd > endRow {
			batchEnd = endRow
		}

		queryStart := time.Now()
		query := p.dialect.BuildRowNumberQuery(cols, orderBy, opts.Table.Schema, opts.Table.Name, tableHint)
		args := p.dialect.BuildRowNumberArgs(currentRow, batchEnd)

		rows, err := p.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)

		if err != nil {
			batches <- Batch{Error: fmt.Errorf("row_number query: %w", err), Done: true}
			return
		}

		batch, _, err := ScanRows(rows, len(opts.Columns))
		rows.Close()

		if err != nil {
			batches <- Batch{Error: err, Done: true}
			return
		}

		batch.Stats.QueryTime = queryTime
		batch.RowNum = currentRow

		currentRow = batchEnd

		if currentRow >= endRow || len(batch.Rows) == 0 {
			batch.Done = true
		}

		batches <- batch

		if batch.Done {
			return
		}
	}
}

// ReadFullTable reads an entire table without pagination.
// Use this for small tables that fit in memory.
func (p *PaginatedReader) ReadFullTable(
	ctx context.Context,
	batches chan<- Batch,
	opts ReadOptions,
	cols string,
	tableHint string,
) {
	queryStart := time.Now()
	query := fmt.Sprintf("SELECT %s FROM %s %s", cols, p.dialect.QualifyTable(opts.Table.Schema, opts.Table.Name), tableHint)

	rows, err := p.db.QueryContext(ctx, query)
	queryTime := time.Since(queryStart)

	if err != nil {
		batches <- Batch{Error: fmt.Errorf("full table query: %w", err), Done: true}
		return
	}
	defer rows.Close()

	for {
		batch := Batch{
			Stats: BatchStats{QueryTime: queryTime},
		}

		scanStart := time.Now()
		for i := 0; i < opts.ChunkSize && rows.Next(); i++ {
			row := make([]any, len(opts.Columns))
			ptrs := make([]any, len(opts.Columns))
			for j := range row {
				ptrs[j] = &row[j]
			}
			if err := rows.Scan(ptrs...); err != nil {
				batches <- Batch{Error: err, Done: true}
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

		queryTime = 0 // Only first batch has query time
	}
}

// ScanRows scans all rows from a result set into a batch.
// Returns the batch, the last primary key value (for keyset pagination), and any error.
func ScanRows(rows *sql.Rows, numCols int) (Batch, any, error) {
	batch := Batch{}
	scanStart := time.Now()

	var lastPK any
	for rows.Next() {
		row := make([]any, numCols)
		ptrs := make([]any, numCols)
		for j := range row {
			ptrs[j] = &row[j]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return batch, nil, fmt.Errorf("scanning row: %w", err)
		}
		batch.Rows = append(batch.Rows, row)
		lastPK = row[0] // Assume first column is PK for keyset
	}

	batch.Stats.ScanTime = time.Since(scanStart)
	batch.Stats.ReadEnd = time.Now()

	return batch, lastPK, rows.Err()
}

// CompareKeys compares two primary key values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareKeys(a, b any) int {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int32:
		if vb, ok := b.(int32); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	// Cannot compare, assume equal to avoid infinite loops
	return 0
}
