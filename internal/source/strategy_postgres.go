package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/dialect"
)

// PostgresStrategy implements SQLStrategy for PostgreSQL
type PostgresStrategy struct {
	BaseStrategy
	dialect dialect.Dialect
}

// NewPostgresStrategy creates a new PostgreSQL strategy
func NewPostgresStrategy() *PostgresStrategy {
	return &PostgresStrategy{
		BaseStrategy: BaseStrategy{DBType: "postgres"},
		dialect:      dialect.GetDialect("postgres"),
	}
}

// GetTablesQuery returns the query to fetch tables
func (s *PostgresStrategy) GetTablesQuery() string {
	return `
		SELECT
			table_schema,
			table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema = $1
		ORDER BY table_name
	`
}

// GetColumnsQuery returns the query to fetch columns
func (s *PostgresStrategy) GetColumnsQuery() string {
	return `
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
}

// GetPrimaryKeyQuery returns the query to fetch primary keys
func (s *PostgresStrategy) GetPrimaryKeyQuery() string {
	return `
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
}

// GetIndexesQuery returns the query to fetch indexes
func (s *PostgresStrategy) GetIndexesQuery() string {
	return `
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
}

// GetForeignKeysQuery returns the query to fetch foreign keys
func (s *PostgresStrategy) GetForeignKeysQuery() string {
	return `
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
}

// GetCheckConstraintsQuery returns the query to fetch check constraints
func (s *PostgresStrategy) GetCheckConstraintsQuery() string {
	return `
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
}

// GetPartitionBoundariesQuery returns the query to fetch partition boundaries
func (s *PostgresStrategy) GetPartitionBoundariesQuery(pkCol string, numPartitions int) string {
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s,
				   NTILE(%d) OVER (ORDER BY %s) as partition_id
			FROM [TABLE_NAME_PLACEHOLDER]
		)
		SELECT partition_id,
			   MIN(%s) as min_pk,
			   MAX(%s) as max_pk,
			   COUNT(*) as row_count
		FROM numbered
		GROUP BY partition_id
		ORDER BY partition_id
	`, s.dialect.QuoteIdentifier(pkCol), numPartitions, s.dialect.QuoteIdentifier(pkCol),
		s.dialect.QuoteIdentifier(pkCol), s.dialect.QuoteIdentifier(pkCol))
}

// GetRowCountQuery returns the query to fetch row count
func (s *PostgresStrategy) GetRowCountQuery(schema, table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", s.dialect.QualifyTable(schema, table))
}

// GetDateColumnQuery returns the query to check if a column is a date type
func (s *PostgresStrategy) GetDateColumnQuery() string {
	return `
		SELECT udt_name
		FROM information_schema.columns
		WHERE table_schema = $1
		  AND table_name = $2
		  AND column_name = $3
	`
}

// IsValidDateType checks if a data type is a temporal type in PostgreSQL
func (s *PostgresStrategy) IsValidDateType(dataType string) bool {
	validTypes := map[string]bool{
		"timestamp":                   true,
		"timestamptz":                 true,
		"date":                        true,
		"timestamp without time zone": true,
		"timestamp with time zone":    true,
	}
	return validTypes[strings.ToLower(dataType)]
}

// EstimateRowSize estimates the row size using PostgreSQL statistics
func (s *PostgresStrategy) EstimateRowSize(ctx context.Context, db *sql.DB, schema, table string) int64 {
	return EstimateRowSizeFromStatsPostgres(ctx, db, schema, table)
}

// BindTableParams returns parameters for table query
func (s *PostgresStrategy) BindTableParams(schema, table string) []interface{} {
	return []interface{}{schema}
}

// BindColumnParams returns parameters for columns query
func (s *PostgresStrategy) BindColumnParams(schema, table string) []interface{} {
	return []interface{}{schema, table}
}

// BindPKParams returns parameters for primary key query
func (s *PostgresStrategy) BindPKParams(schema, table string) []interface{} {
	return []interface{}{schema, table}
}

// BindIndexParams returns parameters for indexes query
func (s *PostgresStrategy) BindIndexParams(schema, table string) []interface{} {
	return []interface{}{schema, table}
}

// BindFKParams returns parameters for foreign keys query
func (s *PostgresStrategy) BindFKParams(schema, table string) []interface{} {
	return []interface{}{schema, table}
}

// BindCheckParams returns parameters for check constraints query
func (s *PostgresStrategy) BindCheckParams(schema, table string) []interface{} {
	return []interface{}{schema, table}
}

// BindDateColumnParams returns parameters for date column query
func (s *PostgresStrategy) BindDateColumnParams(schema, table, column string) []interface{} {
	return []interface{}{schema, table, column}
}

// BindPartitionParams returns parameters for partition query
func (s *PostgresStrategy) BindPartitionParams(schema, table string, numPartitions int) []interface{} {
	return []interface{}{}
}
