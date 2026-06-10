package transfer

import (
	"database/sql"
)

// scanRows scans database rows into a slice of values, applying the
// source dialect's per-column value converters (#477). convs comes from
// Dialect.ValueConverters, resolved once per transfer; convIdx must hold
// the indexes of non-nil entries so pass-through tables skip the loop
// entirely (see buildConvIdx).
func scanRows(rows *sql.Rows, numCols int, convs []func(any) any, convIdx []int) ([][]any, any, error) {
	// Result slice grows as needed; we primarily optimize by reusing the pointers slice per row.
	var result [][]any
	var lastPK any

	// Reuse pointers slice to avoid allocation per row
	ptrs := make([]any, numCols)

	for rows.Next() {
		row := make([]any, numCols)
		for i := range row {
			ptrs[i] = &row[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}

		for _, i := range convIdx {
			row[i] = convs[i](row[i])
		}

		result = append(result, row)
	}

	if len(result) > 0 {
		// lastPK is derived after the loop from the last row (first column assumed to be PK)
		lastPK = result[len(result)-1][0]
	}

	return result, lastPK, rows.Err()
}

// buildConvIdx returns the indexes of columns that actually convert.
func buildConvIdx(convs []func(any) any) []int {
	idx := make([]int, 0, len(convs))
	for i, c := range convs {
		if c != nil {
			idx = append(idx, i)
		}
	}
	return idx
}
