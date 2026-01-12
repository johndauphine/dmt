package driver

import (
	"database/sql"
	"fmt"
	"time"
)

// ScanRows scans all rows from a result set into a batch.
// Returns the batch, the last primary key value (for keyset pagination), and any error.
//
// IMPORTANT: This function assumes that the primary key column is the FIRST column
// in the SELECT list. The caller must ensure this when building the query.
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
		lastPK = row[0] // First column is PK for keyset pagination
	}

	batch.Stats.ScanTime = time.Since(scanStart)
	batch.Stats.ReadEnd = time.Now()

	return batch, lastPK, rows.Err()
}

// CompareKeys compares two primary key values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
//
// Supported types: int64, int32, int, float64, string.
// For mismatched or unsupported types, returns 0 (equal) to avoid infinite loops.
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
