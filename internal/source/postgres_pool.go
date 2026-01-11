package source

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresPool manages a pool of PostgreSQL source connections
type PostgresPool struct {
	db       *sql.DB
	config   *config.SourceConfig
	maxConns int
}

// NewPostgresPool creates a new PostgreSQL source connection pool
func NewPostgresPool(cfg *config.SourceConfig, maxConns int) (*PostgresPool, error) {
	// URL-encode values that may contain special characters to prevent DSN injection
	// Use QueryEscape for user/password/database to encode special characters consistently
	encodedUser := url.QueryEscape(cfg.User)
	encodedPass := url.QueryEscape(cfg.Password)
	encodedDB := url.QueryEscape(cfg.Database)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		encodedUser, encodedPass, cfg.Host, cfg.Port, encodedDB, cfg.SSLMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &PostgresPool{db: db, config: cfg, maxConns: maxConns}, nil
}

// Close closes all connections in the pool
func (p *PostgresPool) Close() error {
	return p.db.Close()
}

// DB returns the underlying database connection
func (p *PostgresPool) DB() *sql.DB {
	return p.db
}

// MaxConns returns the configured maximum connections
func (p *PostgresPool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *PostgresPool) DBType() string {
	return "postgres"
}

// GetRowCount returns the row count for a table
func (p *PostgresPool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifyPGTable(schema, table))).Scan(&count)
	return count, err
}

// ExtractSchema extracts table metadata from the PostgreSQL source database
func (p *PostgresPool) ExtractSchema(ctx context.Context, schema string) ([]Table, error) {
	query := `
		SELECT
			table_schema,
			table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema = $1
		ORDER BY table_name
	`

	rows, err := p.db.QueryContext(ctx, query, schema)
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
		t.EstimatedRowSize = EstimateRowSizeFromStatsPostgres(ctx, p.db, t.Schema, t.Name)

		tables = append(tables, t)
	}

	return tables, nil
}

func (p *PostgresPool) loadColumns(ctx context.Context, t *Table) error {
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

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
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

	// Load SRID for any spatial columns
	if err := p.loadSpatialSRIDs(ctx, t); err != nil {
		return err
	}

	return nil
}

// loadSpatialSRIDs queries PostGIS metadata to get SRID for geography/geometry columns.
// If PostGIS is not installed or columns are not registered, SRID remains 0 (unset).
func (p *PostgresPool) loadSpatialSRIDs(ctx context.Context, t *Table) error {
	// Find spatial columns
	var spatialCols []string
	for _, c := range t.Columns {
		lowerType := strings.ToLower(c.DataType)
		if lowerType == "geography" || lowerType == "geometry" {
			spatialCols = append(spatialCols, c.Name)
		}
	}

	if len(spatialCols) == 0 {
		return nil
	}

	// Query PostGIS geometry_columns view (covers both geometry and geography)
	// This view exists when PostGIS is installed and tables have spatial columns
	query := `
		SELECT f_geometry_column, srid
		FROM geometry_columns
		WHERE f_table_schema = $1 AND f_table_name = $2
		UNION ALL
		SELECT f_geography_column, srid
		FROM geography_columns
		WHERE f_table_schema = $1 AND f_table_name = $2
	`

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
	if err != nil {
		// PostGIS may not be installed, or views may not exist
		// This is not an error - SRID will remain 0 (default)
		return nil
	}
	defer rows.Close()

	// Build SRID map
	sridMap := make(map[string]int)
	for rows.Next() {
		var colName string
		var srid int
		if err := rows.Scan(&colName, &srid); err != nil {
			continue
		}
		sridMap[strings.ToLower(colName)] = srid
	}

	// Update columns with SRID
	for i := range t.Columns {
		if srid, ok := sridMap[strings.ToLower(t.Columns[i].Name)]; ok {
			t.Columns[i].SRID = srid
		}
	}

	return nil
}

func (p *PostgresPool) loadPrimaryKey(ctx context.Context, t *Table) error {
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

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
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
	for _, pkCol := range t.PrimaryKey {
		for _, col := range t.Columns {
			if col.Name == pkCol {
				t.PKColumns = append(t.PKColumns, col)
				break
			}
		}
	}

	return nil
}

func (p *PostgresPool) loadRowCount(ctx context.Context, t *Table) error {
	// Use pg_stat_user_tables for fast approximate count
	query := `
		SELECT COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2
	`

	err := p.db.QueryRowContext(ctx, query, t.Schema, t.Name).Scan(&t.RowCount)
	if err != nil || t.RowCount == 0 {
		// Fall back to COUNT(*) if stats not available or show 0
		exactQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifyPGTable(t.Schema, t.Name))
		return p.db.QueryRowContext(ctx, exactQuery).Scan(&t.RowCount)
	}
	return nil
}

// GetPartitionBoundaries calculates NTILE boundaries for a large table
func (p *PostgresPool) GetPartitionBoundaries(ctx context.Context, t *Table, numPartitions int) ([]Partition, error) {
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
	`, quotePGIdent(pkCol), numPartitions, quotePGIdent(pkCol), qualifyPGTable(t.Schema, t.Name), quotePGIdent(pkCol), quotePGIdent(pkCol))

	rows, err := p.db.QueryContext(ctx, query)
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
func (p *PostgresPool) LoadIndexes(ctx context.Context, t *Table) error {
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

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
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
		idx.Columns = splitCSV(colsStr)
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// LoadForeignKeys loads all foreign keys for a table
func (p *PostgresPool) LoadForeignKeys(ctx context.Context, t *Table) error {
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

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
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
		fk.Columns = splitCSV(colsStr)
		fk.RefColumns = splitCSV(refColsStr)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}

	return nil
}

// LoadCheckConstraints loads all check constraints for a table
func (p *PostgresPool) LoadCheckConstraints(ctx context.Context, t *Table) error {
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

	rows, err := p.db.QueryContext(ctx, query, t.Schema, t.Name)
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
func (p *PostgresPool) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
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
		err := p.db.QueryRowContext(ctx, query, schema, table, candidate).Scan(&dt)

		if err == nil && validTypes[strings.ToLower(dt)] {
			return candidate, dt, true
		}
	}

	return "", "", false
}

// splitCSV splits a comma-separated string into a slice (duplicated from pool.go for independence)
func splitCSVPG(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func quotePGIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func qualifyPGTable(schema, table string) string {
	return quotePGIdent(schema) + "." + quotePGIdent(table)
}
