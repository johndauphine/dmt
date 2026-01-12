package source

import (
	"context"
	"database/sql"
)

// SQLStrategy defines database-specific SQL queries and operations
type SQLStrategy interface {
	// DDL Query Builders
	GetTablesQuery() string
	GetColumnsQuery() string
	GetPrimaryKeyQuery() string
	GetIndexesQuery() string
	GetForeignKeysQuery() string
	GetCheckConstraintsQuery() string
	GetPartitionBoundariesQuery(pkCol string, numPartitions int) string
	GetRowCountQuery(schema, table string) string
	GetDateColumnQuery() string

	// Type Checking
	IsValidDateType(dataType string) bool

	// Row Size Estimation
	EstimateRowSize(ctx context.Context, db *sql.DB, schema, table string) int64

	// SQL Parameter binding (return args for parameterized query)
	BindTableParams(schema, table string) []interface{}
	BindColumnParams(schema, table string) []interface{}
	BindPKParams(schema, table string) []interface{}
	BindIndexParams(schema, table string) []interface{}
	BindFKParams(schema, table string) []interface{}
	BindCheckParams(schema, table string) []interface{}
	BindDateColumnParams(schema, table, column string) []interface{}
	BindPartitionParams(schema, table string, numPartitions int) []interface{}
}

// BaseStrategy provides common functionality for SQL strategies
type BaseStrategy struct {
	DBType string
}

// IsValidDateType checks if a data type is a temporal type
// Base implementation returns false; subtypes override
func (s *BaseStrategy) IsValidDateType(dataType string) bool {
	return false
}
