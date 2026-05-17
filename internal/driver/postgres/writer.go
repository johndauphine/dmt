package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
)

// PostgreSQL writer code is split by responsibility:
// identifiers.go, writer_context.go, writer_ddl.go, writer_rowcount.go,
// writer_copy.go, writer_upsert.go, and writer_raw.go.

// Writer implements driver.Writer for PostgreSQL.
type Writer struct {
	pool               *pgxpool.Pool
	config             *dbconfig.TargetConfig
	maxConns           int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI
	cachedDB           *sql.DB                      // Cached database/sql wrapper for tuning analysis
	copyBatchBytes     int                          // Max bytes per CopyFrom call (derived from TCP buffer size)
}

// NewWriter creates a new PostgreSQL writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	// pgx errors here can quote the raw DSN - see #231. ScrubError
	// strips the password before the message ever reaches a log
	// aggregator.
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("parsing dsn: %w", err))
	}

	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(maxConns / 4)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("creating pool: %w", err))
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
	}

	logging.Debug("Connected to PostgreSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		pool.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		pool.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	// Log mapper initialization. Type-switch to surface which mapper
	// is in use - useful for debugging when AI fallback is/isn't
	// firing (#170).
	driver.LogTypeMapperInit(opts.TypeMapper)

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	w := &Writer{
		pool:               pool,
		config:             cfg,
		maxConns:           maxConns,
		sourceType:         opts.SourceType,
		dialect:            dialect,
		typeMapper:         opts.TypeMapper,
		tableMapper:        tableMapper,
		finalizationMapper: finalizationMapper,
		copyBatchBytes:     probeCopyBatchBytes(pool),
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// Close closes all connections.
// Reset() is called first to immediately close idle connections and mark acquired
// connections for destruction. This prevents Close() from blocking indefinitely
// when a connection is held by a stalled operation (e.g. a COPY waiting for data).
func (w *Writer) Close() {
	if w.cachedDB != nil {
		w.cachedDB.Close()
	}
	w.pool.Reset()
	w.pool.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.pool.Ping(ctx)
}

// DB returns a database/sql connection for tuning analysis.
// The connection is cached and reused across calls to avoid resource leaks.
func (w *Writer) DB() *sql.DB {
	if w.cachedDB == nil {
		// Create stdlib connector from pool config (only once)
		w.cachedDB = stdlib.OpenDBFromPool(w.pool)
	}
	return w.cachedDB
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "postgres"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	poolStats := w.pool.Stat()
	return stats.PoolStats{
		DBType:      "postgres",
		MaxConns:    int(poolStats.MaxConns()),
		ActiveConns: int(poolStats.AcquiredConns()),
		IdleConns:   int(poolStats.IdleConns()),
		WaitCount:   poolStats.EmptyAcquireCount(),
		WaitTimeMs:  0,
	}
}
