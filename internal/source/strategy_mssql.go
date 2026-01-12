package source

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/mssql-pg-migrate/internal/dialect"
)

// MSSQLStrategy implements SQLStrategy for Microsoft SQL Server
type MSSQLStrategy struct {
	BaseStrategy
	dialect dialect.Dialect
}

// NewMSSQLStrategy creates a new MSSQL strategy
func NewMSSQLStrategy() *MSSQLStrategy {
	return &MSSQLStrategy{
		BaseStrategy: BaseStrategy{DBType: "mssql"},
		dialect:      dialect.GetDialect("mssql"),
	}
}

// GetTablesQuery returns the query to fetch tables
func (s *MSSQLStrategy) GetTablesQuery() string {
	return `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA = @schema
		ORDER BY t.TABLE_NAME
	`
}

// GetColumnsQuery returns the query to fetch columns
func (s *MSSQLStrategy) GetColumnsQuery() string {
	return `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			ISNULL(CHARACTER_MAXIMUM_LENGTH, 0),
			ISNULL(NUMERIC_PRECISION, 0),
			ISNULL(NUMERIC_SCALE, 0),
			CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
			COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity'),
			ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION
	`
}

// GetPrimaryKeyQuery returns the query to fetch primary keys
func (s *MSSQLStrategy) GetPrimaryKeyQuery() string {
	return `
		SELECT c.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND c.TABLE_NAME = tc.TABLE_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		  AND tc.TABLE_SCHEMA = @schema
		  AND tc.TABLE_NAME = @table
		ORDER BY c.ORDINAL_POSITION
	`
}

// GetIndexesQuery returns the query to fetch indexes
func (s *MSSQLStrategy) GetIndexesQuery() string {
	return `
		SELECT
			i.name AS index_name,
			i.is_unique,
			i.type_desc,
			STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
			ISNULL(STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ',')
				WITHIN GROUP (ORDER BY ic.key_ordinal), '') AS include_columns
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		JOIN sys.tables tb ON i.object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema
		  AND tb.name = @table
		  AND i.is_primary_key = 0
		  AND i.type > 0
		GROUP BY i.name, i.is_unique, i.type_desc
		ORDER BY i.name
	`
}

// GetForeignKeysQuery returns the query to fetch foreign keys
func (s *MSSQLStrategy) GetForeignKeysQuery() string {
	return `
		SELECT
			fk.name AS fk_name,
			STRING_AGG(pc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS parent_columns,
			rs.name AS ref_schema,
			rt.name AS ref_table,
			STRING_AGG(rc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns,
			fk.delete_referential_action_desc,
			fk.update_referential_action_desc
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
		JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
		JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
		JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
		JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
		JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
		WHERE ps.name = @schema AND pt.name = @table
		GROUP BY fk.name, rs.name, rt.name, fk.delete_referential_action_desc, fk.update_referential_action_desc
		ORDER BY fk.name
	`
}

// GetCheckConstraintsQuery returns the query to fetch check constraints
func (s *MSSQLStrategy) GetCheckConstraintsQuery() string {
	return `
		SELECT
			cc.name,
			cc.definition
		FROM sys.check_constraints cc
		JOIN sys.tables tb ON cc.parent_object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema AND tb.name = @table
		  AND cc.is_disabled = 0
		ORDER BY cc.name
	`
}

// GetPartitionBoundariesQuery returns the query to fetch partition boundaries
func (s *MSSQLStrategy) GetPartitionBoundariesQuery(pkCol string, numPartitions int) string {
	return fmt.Sprintf(`
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
	`, s.dialect.QuoteIdentifier(pkCol), numPartitions, s.dialect.QuoteIdentifier(pkCol),
		"[TABLE_NAME_PLACEHOLDER]", s.dialect.QuoteIdentifier(pkCol), s.dialect.QuoteIdentifier(pkCol))
}

// GetRowCountQuery returns the query to fetch row count
func (s *MSSQLStrategy) GetRowCountQuery(schema, table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", s.dialect.QualifyTable(schema, table))
}

// GetDateColumnQuery returns the query to check if a column is a date type
func (s *MSSQLStrategy) GetDateColumnQuery() string {
	return `
		SELECT DATA_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema
		  AND TABLE_NAME = @table
		  AND COLUMN_NAME = @column
	`
}

// IsValidDateType checks if a data type is a temporal type in MSSQL
func (s *MSSQLStrategy) IsValidDateType(dataType string) bool {
	validTypes := map[string]bool{
		"datetime":       true,
		"datetime2":      true,
		"smalldatetime":  true,
		"date":           true,
		"datetimeoffset": true,
	}
	return validTypes[dataType]
}

// EstimateRowSize estimates the row size using MSSQL statistics
func (s *MSSQLStrategy) EstimateRowSize(ctx context.Context, db *sql.DB, schema, table string) int64 {
	// Use MSSQL row size estimation
	return EstimateRowSizeFromStats(ctx, db, schema, table)
}

// BindTableParams returns parameters for table query
func (s *MSSQLStrategy) BindTableParams(schema, table string) []interface{} {
	return []interface{}{sql.Named("schema", schema)}
}

// BindColumnParams returns parameters for columns query
func (s *MSSQLStrategy) BindColumnParams(schema, table string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
	}
}

// BindPKParams returns parameters for primary key query
func (s *MSSQLStrategy) BindPKParams(schema, table string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
	}
}

// BindIndexParams returns parameters for indexes query
func (s *MSSQLStrategy) BindIndexParams(schema, table string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
	}
}

// BindFKParams returns parameters for foreign keys query
func (s *MSSQLStrategy) BindFKParams(schema, table string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
	}
}

// BindCheckParams returns parameters for check constraints query
func (s *MSSQLStrategy) BindCheckParams(schema, table string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
	}
}

// BindDateColumnParams returns parameters for date column query
func (s *MSSQLStrategy) BindDateColumnParams(schema, table, column string) []interface{} {
	return []interface{}{
		sql.Named("schema", schema),
		sql.Named("table", table),
		sql.Named("column", column),
	}
}

// BindPartitionParams returns parameters for partition query
func (s *MSSQLStrategy) BindPartitionParams(schema, table string, numPartitions int) []interface{} {
	return []interface{}{}
}
