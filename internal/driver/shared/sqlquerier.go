package shared

import (
	"context"
	"database/sql"
)

// SQLQuerier is the database/sql query surface the shared helpers need.
// (Lived in sampling.go until #476 deleted the dead sampling feature.)
type SQLQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
