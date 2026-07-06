package transfer

import (
	"database/sql"
	"time"
	"unsafe"
)

// perRowGoOverhead is the fixed Go heap cost of one scanned row independent
// of its values: the []any slice header plus one interface slot per column.
// It mirrors driver.Table.GoHeapBytesPerRow's accounting so the semaphore
// budget (#617) and the static row-size estimate speak the same units.
const (
	sizeofSliceHeaderBytes = int64(unsafe.Sizeof([]any{}))        // 24
	sizeofIfaceSlotBytes   = int64(2 * unsafe.Sizeof(uintptr(0))) // 16
	sizeofStringHeaderB    = int64(unsafe.Sizeof(""))             // 16
	sizeofByteSliceHeaderB = int64(unsafe.Sizeof([]byte{}))       // 24
	sizeofTimeB            = int64(unsafe.Sizeof(time.Time{}))
	sizeofScalarB          = int64(8)
)

// scanRows scans database rows into a slice of values, applying the
// source dialect's per-column value converters (#477). convs comes from
// Dialect.ValueConverters, resolved once per transfer; convIdx must hold
// the indexes of non-nil entries so pass-through tables skip the loop
// entirely (see buildConvIdx).
//
// The second return is the chunk's measured Go heap size — computed in the
// same value walk the converters already do, so it costs nothing extra —
// used to charge the shared in-flight memory budget (#617) against actual
// row sizes rather than a static estimate.
func scanRows(rows *sql.Rows, numCols int, convs []func(any) any, convIdx []int) ([][]any, int64, error) {
	// Result slice grows as needed; we primarily optimize by reusing the pointers slice per row.
	var result [][]any
	var bytes int64
	rowOverhead := sizeofSliceHeaderBytes + int64(numCols)*sizeofIfaceSlotBytes

	// Reuse pointers slice to avoid allocation per row
	ptrs := make([]any, numCols)

	for rows.Next() {
		row := make([]any, numCols)
		for i := range row {
			ptrs[i] = &row[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, 0, err
		}

		for _, i := range convIdx {
			row[i] = convs[i](row[i])
		}

		bytes += rowOverhead
		for _, v := range row {
			bytes += goValueSize(v)
		}

		result = append(result, row)
	}

	return result, bytes, rows.Err()
}

// goValueSize estimates the heap bytes a scanned cell value occupies, using
// the actual length of variable-width values (unlike the static per-column
// estimate). Kept deliberately simple — the budget only needs to track the
// dominant string/[]byte content plus a flat scalar cost.
func goValueSize(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return sizeofStringHeaderB + int64(len(x))
	case []byte:
		return sizeofByteSliceHeaderB + int64(len(x))
	case time.Time:
		return sizeofTimeB
	default:
		return sizeofScalarB
	}
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
