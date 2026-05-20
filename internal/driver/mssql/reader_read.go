package mssql

import (
	"context"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	return shared.StreamTable(ctx, shared.StreamConfig{
		DB:                       r.db,
		Dialect:                  r.dialect,
		Buffer:                   4,
		TableHint:                r.dialect.TableHint(opts.StrictConsistency),
		KeysetQueryErrorLabel:    "keyset query",
		RowNumberQueryErrorLabel: "row_number query",
		FullTableQueryErrorLabel: "full read query",
	}, opts)
}
