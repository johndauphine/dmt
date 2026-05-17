package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // SQLite driver (pure Go, no cgo)

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader for SQLite.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new SQLite reader. The file path is read from
// cfg.Database (SQLite has no host/port/user/password). Empty database
// opens an in-memory DB.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite connection: %w", err)
	}

	// SQLite tolerates multiple read connections fine. We cap at maxConns
	// to honor the caller's pool sizing.
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	if maxConns/4 < 1 {
		db.SetMaxIdleConns(1)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging sqlite database: %w", err)
	}

	logging.Debug("Connected to SQLite source: %s", cfg.Database)

	return &Reader{
		db:       db,
		config:   cfg,
		maxConns: maxConns,
		dialect:  dialect,
	}, nil
}

// Close closes the connection pool.
func (r *Reader) Close() error { return r.db.Close() }

// DB returns the underlying sql.DB.
func (r *Reader) DB() *sql.DB { return r.db }

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int { return r.maxConns }

// DBType returns the database type.
func (r *Reader) DBType() string { return "sqlite" }

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "sqlite",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}
