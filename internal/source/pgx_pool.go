package source

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/dialect"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/util"
)

// pgDialect is the shared dialect instance for PostgreSQL operations
var pgDialect = dialect.GetDialect("postgres")

// PgxSourcePool manages a pool of PostgreSQL source connections using pgx.
type PgxSourcePool struct {
	pool     *pgxpool.Pool
	sqlDB    *sql.DB // Wrapper for compatibility with SourcePool interface
	config   *config.SourceConfig
	maxConns int
}

// NewPgxSourcePool creates a new PostgreSQL source connection pool using pgx.
func NewPgxSourcePool(cfg *config.SourceConfig, maxConns int) (*PgxSourcePool, error) {
	// Build connection string with proper URL encoding for special characters in password
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:   "/" + cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", cfg.SSLMode)
	u.RawQuery = q.Encode()

	poolConfig, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("parsing connection config: %w", err)
	}

	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = int32(maxConns / 4)
	if poolConfig.MinConns < 1 {
		poolConfig.MinConns = 1
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Create sql.DB wrapper for compatibility
	// Use the same connection string for both pools to ensure consistency
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("creating sql.DB wrapper: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	logging.Info("Connected to PostgreSQL source (pgx): %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	return &PgxSourcePool{
		pool:     pool,
		sqlDB:    db,
		config:   cfg,
		maxConns: maxConns,
	}, nil
}

// Close closes all connections in the pool
func (p *PgxSourcePool) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	if p.sqlDB != nil {
		return p.sqlDB.Close()
	}
	return nil
}

// DB returns the underlying sql.DB for compatibility
func (p *PgxSourcePool) DB() *sql.DB {
	return p.sqlDB
}

// MaxConns returns the configured maximum connections
func (p *PgxSourcePool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *PgxSourcePool) DBType() string {
	return "postgres"
}

// GetRowCount returns the row count for a table
func (p *PgxSourcePool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", pgDialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// ExtractSchema extracts table metadata from the PostgreSQL source database
func (p *PgxSourcePool) ExtractSchema(ctx context.Context, schema string) ([]Table, error) {
	query := `
		SELECT
			table_schema,
			table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema = $1
		ORDER BY table_name
	`

	rows, err := p.pool.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Get columns
		if err := p.loadColumns(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading columns for %s: %w", t.FullName(), err)
		}

		// Get primary key
		if err := p.loadPrimaryKey(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading PK for %s: %w", t.FullName(), err)
		}

		// Get row count
		if err := p.loadRowCount(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading row count for %s: %w", t.FullName(), err)
		}

		// Estimate row size from system statistics
		t.EstimatedRowSize = EstimateRowSizeFromStatsPostgres(ctx, p.sqlDB, t.Schema, t.Name)

		tables = append(tables, t)
	}

	return tables, nil
}

func (p *PgxSourcePool) loadColumns(ctx context.Context, t *Table) error {
	query := `
		SELECT
			column_name,
			udt_name,
			COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0),
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END,
			CASE WHEN column_default LIKE 'nextval%' THEN true ELSE false END,
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := p.pool.Query(ctx, query, t.Schema, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.DataType, &c.MaxLength, &c.Precision,
			&c.Scale, &c.IsNullable, &c.IsIdentity, &c.OrdinalPos); err != nil {
			return err
		}
		t.Columns = append(t.Columns, c)
	}

	return nil
}

func (p *PgxSourcePool) loadPrimaryKey(ctx context.Context, t *Table) error {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE i.indisprimary
		  AND n.nspname = $1
		  AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)
	`

	rows, err := p.pool.Query(ctx, query, t.Schema, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}

	// Populate PKColumns with full column metadata
	t.PopulatePKColumns()

	return nil
}

func (p *PgxSourcePool) loadRowCount(ctx context.Context, t *Table) error {
	// Use pg_stat_user_tables for fast approximate count
	query := `
		SELECT COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2
	`

	err := p.pool.QueryRow(ctx, query, t.Schema, t.Name).Scan(&t.RowCount)
	if err != nil || t.RowCount == 0 {
		// Fall back to COUNT(*) if stats not available or show 0
		exactQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pgDialect.QualifyTable(t.Schema, t.Name))
		return p.pool.QueryRow(ctx, exactQuery).Scan(&t.RowCount)
	}
	return nil
}

// GetPartitionBoundaries calculates NTILE boundaries for a large table
func (p *PgxSourcePool) GetPartitionBoundaries(ctx context.Context, t *Table, numPartitions int) ([]Partition, error) {
	if !t.HasSinglePK() {
		return nil, fmt.Errorf("partitioning requires single-column PK")
	}

	pkCol := t.PrimaryKey[0]

	query := fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s,
				   NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM %s
		)
		SELECT partition_id,
			   MIN(%s) as min_pk,
			   MAX(%s) as max_pk,
			   COUNT(*) as row_count
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, pgDialect.QuoteIdentifier(pkCol), numPartitions, pgDialect.QuoteIdentifier(pkCol), pgDialect.QualifyTable(t.Schema, t.Name), pgDialect.QuoteIdentifier(pkCol), pgDialect.QuoteIdentifier(pkCol))

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []Partition
	for rows.Next() {
		var part Partition
		part.TableName = t.FullName()
		if err := rows.Scan(&part.PartitionID, &part.MinPK, &part.MaxPK, &part.RowCount); err != nil {
			return nil, err
		}
		partitions = append(partitions, part)
	}

	return partitions, nil
}

// LoadIndexes loads all non-PK indexes for a table
func (p *PgxSourcePool) LoadIndexes(ctx context.Context, t *Table) error {
	query := `
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
		WHERE n.nspname = $1
		  AND t.relname = $2
		  AND NOT ix.indisprimary
		GROUP BY i.relname, ix.indisunique, am.amname, ix.indisclustered
		ORDER BY i.relname
	`

	rows, err := p.pool.Query(ctx, query, t.Schema, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx Index
		var colsStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsClustered, &colsStr); err != nil {
			return err
		}
		idx.Columns = util.SplitCSV(colsStr)
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table
func (p *PgxSourcePool) LoadForeignKeys(ctx context.Context, t *Table) error {
	query := `
		SELECT
			c.conname AS fk_name,
			array_to_string(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), ',') AS parent_columns,
			nf.nspname AS ref_schema,
			tf.relname AS ref_table,
			array_to_string(array_agg(af.attname ORDER BY array_position(c.confkey, af.attnum)), ',') AS ref_columns,
			CASE c.confdeltype
				WHEN 'a' THEN 'NO_ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET_NULL'
				WHEN 'd' THEN 'SET_DEFAULT'
			END AS on_delete,
			CASE c.confupdtype
				WHEN 'a' THEN 'NO_ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET_NULL'
				WHEN 'd' THEN 'SET_DEFAULT'
			END AS on_update
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_class tf ON tf.oid = c.confrelid
		JOIN pg_namespace nf ON nf.oid = tf.relnamespace
		CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS ka(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ka.attnum
		CROSS JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS kf(attnum, ord)
		JOIN pg_attribute af ON af.attrelid = tf.oid AND af.attnum = kf.attnum AND ka.ord = kf.ord
		WHERE c.contype = 'f'
		  AND n.nspname = $1
		  AND t.relname = $2
		GROUP BY c.conname, nf.nspname, tf.relname, c.confdeltype, c.confupdtype
		ORDER BY c.conname
	`

	rows, err := p.pool.Query(ctx, query, t.Schema, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var fk ForeignKey
		var colsStr, refColsStr string
		if err := rows.Scan(&fk.Name, &colsStr, &fk.RefSchema, &fk.RefTable, &refColsStr, &fk.OnDelete, &fk.OnUpdate); err != nil {
			return err
		}
		fk.Columns = util.SplitCSV(colsStr)
		fk.RefColumns = util.SplitCSV(refColsStr)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}

	return nil
}

// LoadCheckConstraints loads all check constraints for a table
func (p *PgxSourcePool) LoadCheckConstraints(ctx context.Context, t *Table) error {
	query := `
		SELECT
			c.conname,
			pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE c.contype = 'c'
		  AND n.nspname = $1
		  AND t.relname = $2
		ORDER BY c.conname
	`

	rows, err := p.pool.Query(ctx, query, t.Schema, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var chk CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}

	return nil
}

// GetDateColumnInfo checks if any of the candidate columns exist as a temporal type
// Returns the first matching column name, its data type, and whether a match was found
func (p *PgxSourcePool) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	if len(candidates) == 0 {
		return "", "", false
	}

	// Valid PostgreSQL temporal types for date-based incremental sync
	validTypes := map[string]bool{
		"timestamp":   true,
		"timestamptz": true,
		"date":        true,
		// Internal type names (udt_name)
		"timestamp without time zone": true,
		"timestamp with time zone":    true,
	}

	// Check each candidate in order
	for _, candidate := range candidates {
		query := `
			SELECT udt_name
			FROM information_schema.columns
			WHERE table_schema = $1
			  AND table_name = $2
			  AND column_name = $3
		`
		var dt string
		err := p.pool.QueryRow(ctx, query, schema, table, candidate).Scan(&dt)

		if err == nil && validTypes[strings.ToLower(dt)] {
			return candidate, dt, true
		}
	}

	return "", "", false
}
