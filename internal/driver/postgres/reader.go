package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader for PostgreSQL using pgx.
type Reader struct {
	pool     *pgxpool.Pool
	sqlDB    *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new PostgreSQL reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// pgx's ParseConfig / pgxpool error messages can include the raw
	// DSN (password and all) when the conninfo string fails to parse —
	// see #231. ScrubError replaces the password with [REDACTED] so
	// the underlying error stays useful for diagnosis (port, sslmode,
	// option name) without leaking credentials into operator logs.
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("parsing connection config: %w", err))
	}

	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = int32(maxConns / 4)
	if poolConfig.MinConns < 1 {
		poolConfig.MinConns = 1
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("creating pool: %w", err))
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	// Create sql.DB wrapper for compatibility
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		pool.Close()
		return nil, logging.ScrubError(fmt.Errorf("creating sql.DB wrapper: %w", err))
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	logging.Debug("Connected to PostgreSQL source: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	return &Reader{
		pool:     pool,
		sqlDB:    db,
		config:   cfg,
		maxConns: maxConns,
		dialect:  dialect,
	}, nil
}

// Close closes all connections.
// Reset() is called first to immediately close idle connections and mark acquired
// connections for destruction, preventing Close() from blocking on stalled operations.
func (r *Reader) Close() error {
	if r.pool != nil {
		r.pool.Reset()
		r.pool.Close()
	}
	if r.sqlDB != nil {
		return r.sqlDB.Close()
	}
	return nil
}

// DB returns the underlying sql.DB for compatibility.
func (r *Reader) DB() *sql.DB {
	return r.sqlDB
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "postgres"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	poolStats := r.pool.Stat()
	return stats.PoolStats{
		DBType:      "postgres",
		MaxConns:    int(poolStats.MaxConns()),
		ActiveConns: int(poolStats.AcquiredConns()),
		IdleConns:   int(poolStats.IdleConns()),
		WaitCount:   poolStats.EmptyAcquireCount(),
		WaitTimeMs:  0,
	}
}
