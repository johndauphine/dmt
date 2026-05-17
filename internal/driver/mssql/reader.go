package mssql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	_ "github.com/microsoft/go-mssqldb"
)

// Reader implements driver.Reader for SQL Server.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new SQL Server reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// go-mssqldb sometimes echoes the DSN in its open/ping errors —
	// the message can include the password embedded in the URL form.
	// ScrubError replaces it with [REDACTED] before the error reaches
	// a log aggregator (#231).
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("opening connection: %w", err))
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	// Check database compatibility level - require 140+ for STRING_AGG support
	compatLevel, err := getCompatibilityLevel(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("checking database compatibility level: %w", err)
	}
	if compatLevel < 140 {
		db.Close()
		return nil, fmt.Errorf("database compatibility level 140+ required (found %d). Run: ALTER DATABASE [%s] SET COMPATIBILITY_LEVEL = 160", compatLevel, cfg.Database)
	}

	logging.Debug("Connected to MSSQL source: %s:%d/%s (compat level %d)", cfg.Host, cfg.Port, cfg.Database, compatLevel)

	return &Reader{
		db:       db,
		config:   cfg,
		maxConns: maxConns,
		dialect:  dialect,
	}, nil
}

// Close closes all connections.
func (r *Reader) Close() error {
	return r.db.Close()
}

// DB returns the underlying sql.DB for compatibility.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}
