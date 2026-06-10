package transfer

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// scanRows scans database rows into a slice of values with proper type handling.
func scanRows(rows *sql.Rows, cols, colTypes []string) ([][]any, any, error) {
	numCols := len(cols)
	// Result slice grows as needed; we primarily optimize by reusing the pointers slice per row.
	var result [][]any
	var lastPK any

	// Column types are fixed for the chunk, so conversion is resolved once
	// per scan instead of a per-value string switch (#466) — at pipeline
	// throughput that switch ran millions of times per second, and most
	// columns fall through it unchanged. convIdx holds only the columns
	// that actually need conversion so pass-through tables skip the loop
	// entirely.
	convs := buildValueConverters(colTypes)
	convIdx := make([]int, 0, len(convs))
	for i, c := range convs {
		if c != nil {
			convIdx = append(convIdx, i)
		}
	}

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

// buildValueConverters resolves each column's type to a conversion func.
// A nil entry means the column's values pass through unchanged — the
// common case. The conversions themselves are unchanged from the old
// per-value processValue switch; only the dispatch moved.
func buildValueConverters(colTypes []string) []func(any) any {
	convs := make([]func(any) any, len(colTypes))
	for i, ct := range colTypes {
		convs[i] = converterForType(ct)
	}
	return convs
}

// converterForType handles type conversions for PostgreSQL compatibility.
func converterForType(colType string) func(any) any {
	switch colType {
	case "binary", "varbinary", "image":
		return convertBinaryValue
	case "uniqueidentifier":
		return convertUniqueIdentifierValue
	case "bit":
		return convertBitValue
	case "datetime", "datetime2", "smalldatetime", "datetimeoffset":
		return convertDateTimeValue
	default:
		return nil
	}
}

// convertBinaryValue converts binary data for bytea targets.
func convertBinaryValue(val any) any {
	switch v := val.(type) {
	case []byte:
		if len(v) == 0 {
			return nil
		}
		return v // pgx handles []byte directly
	}
	return val
}

// convertUniqueIdentifierValue handles SQL Server GUID conversion.
func convertUniqueIdentifierValue(val any) any {
	switch v := val.(type) {
	case []byte:
		if len(v) == 16 {
			// SQL Server GUID to PostgreSQL UUID
			return formatUUID(v)
		}
		return string(v)
	case string:
		return v
	}
	return val
}

// convertBitValue converts bit to boolean.
func convertBitValue(val any) any {
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case int:
		return v != 0
	}
	return val
}

// convertDateTimeValue ensures proper timestamp format, mapping SQL Server's
// pre-year-1 sentinel datetimes to NULL.
func convertDateTimeValue(val any) any {
	switch v := val.(type) {
	case time.Time:
		// Handle SQL Server minimum datetime (1753-01-01)
		if v.Year() < 1 {
			return nil
		}
		return v
	}
	return val
}

// formatUUID converts SQL Server GUID bytes to UUID string
func formatUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	// SQL Server stores GUIDs in mixed-endian format
	// Convert to standard UUID format
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], // time_low (reversed)
		b[5], b[4], // time_mid (reversed)
		b[7], b[6], // time_hi_and_version (reversed)
		b[8], b[9], // clock_seq
		b[10], b[11], b[12], b[13], b[14], b[15]) // node
}
