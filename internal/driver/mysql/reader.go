package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// Reader implements driver.Reader for MySQL/MariaDB.
type Reader struct {
	db       *sql.DB
	config   *dbconfig.SourceConfig
	maxConns int
	dialect  *Dialect
}

// NewReader creates a new MySQL reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// go-sql-driver/mysql wraps the DSN into its open/ping errors —
	// the user:password@tcp(...) form is the load-bearing leak vector
	// for #231. ScrubError replaces the password with [REDACTED]
	// before the error reaches a log aggregator.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("opening connection: %w", err))
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	if db.Stats().MaxOpenConnections > 0 && maxConns/4 < 1 {
		db.SetMaxIdleConns(1)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	// Detect MySQL vs MariaDB
	var version string
	db.QueryRow("SELECT VERSION()").Scan(&version)
	dbType := "MySQL"
	if strings.Contains(strings.ToLower(version), "mariadb") {
		dbType = "MariaDB"
	}

	logging.Debug("Connected to %s source: %s:%d/%s", dbType, cfg.Host, cfg.Port, cfg.Database)

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

// DB returns the underlying sql.DB.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mysql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mysql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}
