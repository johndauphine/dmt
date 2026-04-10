package driver

import (
	"context"
	"io"
)

// CopyBinaryOptions configures a PostgreSQL binary COPY operation.
// Used by the PG→PG fast path to stream raw binary COPY bytes directly
// between source and target, bypassing row decoding/encoding in Go.
type CopyBinaryOptions struct {
	// Schema is the table schema.
	Schema string

	// Table is the table name.
	Table string

	// Columns lists the columns to COPY, in order.
	// Must match between source and target for a valid binary relay.
	Columns []string

	// Where is an optional WHERE clause (without the "WHERE" keyword).
	// Used to scope reads to a keyset partition. Must use literal values
	// or be pre-rendered — no parameter placeholders are passed through.
	Where string
}

// BinaryCopyReader is an optional capability implemented by Readers that
// support streaming raw binary COPY output. When both the source Reader and
// target Writer implement this and BinaryCopyWriter respectively, the
// transfer pipeline can skip row-by-row scan/encode and pipe raw COPY bytes
// directly between the two connections.
//
// Implementations should execute:
//
//	COPY (SELECT <cols> FROM <schema>.<table> [WHERE <where>]) TO STDOUT (FORMAT BINARY)
//
// and write the raw bytes to w. The returned count is the number of rows
// COPIED (from the CommandTag), not the number of bytes.
type BinaryCopyReader interface {
	CopyBinaryTo(ctx context.Context, w io.Writer, opts CopyBinaryOptions) (int64, error)
}

// BinaryCopyWriter is an optional capability implemented by Writers that
// support ingesting raw binary COPY input from an io.Reader. It is the
// counterpart to BinaryCopyReader for the PG→PG fast path.
//
// Implementations should execute:
//
//	COPY <schema>.<table> (<cols>) FROM STDIN (FORMAT BINARY)
//
// wrapping the COPY in a transaction so a mid-stream failure rolls back
// cleanly. The returned count is the number of rows COPIED.
type BinaryCopyWriter interface {
	CopyBinaryFrom(ctx context.Context, r io.Reader, opts CopyBinaryOptions) (int64, error)
}
