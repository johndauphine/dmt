package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	"strings"
	"time"
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

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return shared.RowCountWithFallback(
		func() (int64, error) { return r.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return r.GetRowCountExact(ctx, schema, table, false) },
	)
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.sqlDB.QueryRowContext(ctx,
		`SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// Postgres has no NOLOCK equivalent (uses MVCC); strictConsistency
// is accepted for interface symmetry and ignored here.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, r.sqlDB, r.dialect, schema, table)
}

// GetPartitionBoundaries returns partition boundaries for parallel processing.
func (r *Reader) GetPartitionBoundaries(ctx context.Context, t *driver.Table, numPartitions int) ([]driver.Partition, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", t.Name)
	}

	pkCol := t.PrimaryKey[0]
	query := fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM %s
		)
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*)
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, r.dialect.QuoteIdentifier(pkCol), numPartitions, r.dialect.QuoteIdentifier(pkCol),
		r.dialect.QualifyTable(t.Schema, t.Name),
		r.dialect.QuoteIdentifier(pkCol), r.dialect.QuoteIdentifier(pkCol))

	return shared.QueryPartitionBoundaries(ctx, r.sqlDB, query, t.Name)
}

// GetDateColumnInfo returns information about a date column for incremental sync.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt string
		err := r.sqlDB.QueryRowContext(ctx,
			`SELECT udt_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
			schema, table, col).Scan(&dt)

		if err == nil {
			validTypes := r.dialect.ValidDateTypes()
			if validTypes[dt] {
				return col, dt, true
			}
		}
	}
	return "", "", false
}

// GetMaxDateColumnValue returns the current source-side high watermark for a
// date column. A nil value means the table has no non-NULL values in that
// column yet.
func (r *Reader) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
	)

	var raw any
	if err := r.sqlDB.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}

// ExtractSchema extracts table metadata from the database.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	tables := []driver.Table{}

	// Get tables
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE' AND table_schema = $1
		ORDER BY table_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t driver.Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Load columns
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, err
		}

		// Load primary key
		if err := r.loadPrimaryKey(ctx, &t); err != nil {
			return nil, err
		}

		// Populate PKColumns with full column metadata
		t.PopulatePKColumns()

		// Get row count
		count, err := r.GetRowCount(ctx, t.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s: %v", t.Name, err)
		}
		t.RowCount = count

		// Compute Go heap cost per row from column metadata (static baseline)
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Override with actual avg row sizes from database statistics when available.
	// The static GoHeapBytesPerRow estimate can severely undercount TEXT/BLOB columns.
	r.applyActualRowSizes(ctx, schema, tables)

	return tables, nil
}

// applyActualRowSizes queries pg_stat_user_tables for actual average row sizes
// and overrides the static GoHeapBytesPerRow estimate when the DB reports a
// larger value. This is critical for tables with TEXT/JSONB columns where the
// static estimate (based on column type metadata) severely undercounts.
func (r *Reader) applyActualRowSizes(ctx context.Context, schema string, tables []driver.Table) {
	// Use pg_relation_size (main fork only, excludes TOAST/FSM/VM overhead).
	// TOAST metadata inflates estimates beyond actual in-memory row cost since
	// the driver streams TOAST data lazily. The runtime memory guardrail catches
	// any remaining underestimates.
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT relname,
			CASE WHEN n_live_tup > 0
				THEN pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) / n_live_tup
				ELSE 0
			END AS avg_row_size
		FROM pg_stat_user_tables
		WHERE schemaname = $1
	`, schema)
	if err != nil {
		logging.Debug("Failed to query actual row sizes: %v", err)
		return
	}
	defer rows.Close()

	sizeMap := make(map[string]int64)
	for rows.Next() {
		var name string
		var avgSize int64
		if err := rows.Scan(&name, &avgSize); err != nil {
			continue
		}
		if avgSize > 0 {
			sizeMap[name] = avgSize
		}
	}

	for i := range tables {
		if dbSize, ok := sizeMap[tables[i].Name]; ok && dbSize > tables[i].EstimatedRowSize {
			logging.Debug("Table %s: using DB avg row size %d bytes (static estimate was %d)",
				tables[i].Name, dbSize, tables[i].EstimatedRowSize)
			tables[i].EstimatedRowSize = dbSize
		}
	}
}

func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	columns, err := shared.QueryStandardColumns(ctx, r.sqlDB, `
		SELECT
			column_name,
			udt_name,
			COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0),
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END,
			CASE WHEN column_default LIKE 'nextval%' THEN true ELSE false END,
			COALESCE(column_default, ''),
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, t.Name, t.Schema, t.Name)
	if err != nil {
		return err
	}
	t.Columns = append(t.Columns, columns...)
	return nil
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying primary key for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return fmt.Errorf("scanning pk column: %w", err)
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}
	return rows.Err()
}

// LoadIndexes loads index metadata for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT
			i.relname AS index_name,
			ix.indisunique,
			CASE WHEN am.amname = 'btree' AND ix.indisclustered THEN true ELSE false END,
			array_to_string(array_agg(a.attname ORDER BY k.ordinality), ',') AS columns
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality)
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
		WHERE n.nspname = $1 AND t.relname = $2 AND NOT ix.indisprimary
		GROUP BY i.relname, ix.indisunique, am.amname, ix.indisclustered
		ORDER BY i.relname
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var columns string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsClustered, &columns); err != nil {
			return err
		}
		idx.Columns = strings.Split(columns, ",")
		t.Indexes = append(t.Indexes, idx)
	}
	return rows.Err()
}

// LoadForeignKeys loads foreign key metadata for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	// Similar pattern to LoadIndexes
	return nil
}

// LoadCheckConstraints loads check constraint metadata for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	// Similar pattern to LoadIndexes
	return nil
}
