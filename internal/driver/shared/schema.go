package shared

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

// QueryStandardColumns executes a column metadata query that returns the
// standard driver.Column row shape used by information_schema-backed readers.
func QueryStandardColumns(ctx context.Context, db SQLQuerier, query string, tableName string, args ...any) ([]driver.Column, error) {
	if db == nil {
		return nil, fmt.Errorf("querying columns for %s: db is nil", tableName)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	return ScanStandardColumns(rows)
}

// ScanStandardColumns scans rows that contain:
//
//	name, data_type, max_length, precision, scale,
//	is_nullable, is_identity, default_value, ordinal_position
//
// This deliberately shares only the result-shape plumbing. Concrete readers
// still own the catalog SQL and any engine-specific normalization needed to
// produce this shape.
func ScanStandardColumns(rows *sql.Rows) ([]driver.Column, error) {
	if rows == nil {
		return nil, fmt.Errorf("scan standard columns: rows is nil")
	}

	var columns []driver.Column
	for rows.Next() {
		var column driver.Column
		if err := rows.Scan(
			&column.Name,
			&column.DataType,
			&column.MaxLength,
			&column.Precision,
			&column.Scale,
			&column.IsNullable,
			&column.IsIdentity,
			&column.DefaultValue,
			&column.OrdinalPos,
		); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		columns = append(columns, column)
	}

	return columns, rows.Err()
}
