package sqlite

import (
	"context"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// ReadTable streams rows from a table via a channel.
func (r *Reader) ReadTable(ctx context.Context, opts driver.ReadOptions) (<-chan driver.Batch, error) {
	return shared.StreamTable(ctx, shared.StreamConfig{
		DB:      r.db,
		Dialect: r.dialect,
		Buffer:  4,
	}, opts)
}
