package shared

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLQuerier is the database/sql query surface shared sampling helpers need.
type SQLQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// QuerySampleColumnValues executes a single-column sample query and returns
// non-NULL string values in result order.
func QuerySampleColumnValues(ctx context.Context, db SQLQuerier, query string, args ...any) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("query sample column values: db is nil")
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return ScanSampleColumnValues(rows)
}

// ScanSampleColumnValues scans one nullable string column from rows.
func ScanSampleColumnValues(rows *sql.Rows) ([]string, error) {
	if rows == nil {
		return nil, fmt.Errorf("scan sample column values: rows is nil")
	}

	var samples []string
	for rows.Next() {
		var value sql.NullString
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scanning sample value: %w", err)
		}
		if value.Valid {
			samples = append(samples, value.String)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading samples: %w", err)
	}
	return samples, nil
}

// QuerySampleRows executes a multi-column sample query and returns sample
// values keyed by source column name.
func QuerySampleRows(
	ctx context.Context,
	db SQLQuerier,
	query string,
	columns []string,
	capacity int,
	args ...any,
) (map[string][]string, error) {
	if db == nil {
		return nil, fmt.Errorf("query sample rows: db is nil")
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return ScanSampleRows(rows, columns, capacity)
}

// ScanSampleRows scans nullable string columns into a column-name keyed sample
// map. NULL and empty strings are omitted to keep mapper context compact.
func ScanSampleRows(rows *sql.Rows, columns []string, capacity int) (map[string][]string, error) {
	if rows == nil {
		return nil, fmt.Errorf("scan sample rows: rows is nil")
	}
	if capacity < 0 {
		capacity = 0
	}

	result := make(map[string][]string, len(columns))
	for _, column := range columns {
		result[column] = make([]string, 0, capacity)
	}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		for i, column := range columns {
			if values[i].Valid && values[i].String != "" {
				result[column] = append(result[column], values[i].String)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
