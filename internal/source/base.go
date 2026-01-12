package source

import (
	"context"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
)

// QueryRow represents a single row result interface
type QueryRow interface {
	Scan(dest ...interface{}) error
}

// Rows represents multiple row results interface
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
}

// QueryExecutor abstracts different query interfaces (db.QueryContext vs pool.Query)
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) QueryRow
}

// BaseSourcePool provides common functionality for all source pool implementations
type BaseSourcePool struct {
	config   *config.SourceConfig
	maxConns int
}

// NewBaseSourcePool creates a new base source pool
func NewBaseSourcePool(cfg *config.SourceConfig, maxConns int) *BaseSourcePool {
	return &BaseSourcePool{
		config:   cfg,
		maxConns: maxConns,
	}
}

// MaxConns returns the configured maximum connections
func (b *BaseSourcePool) MaxConns() int {
	return b.maxConns
}

// GetConfig returns the source configuration
func (b *BaseSourcePool) GetConfig() *config.SourceConfig {
	return b.config
}
