package driver

import (
	"strings"
	"time"
)

// DateFilter specifies a filter on a date/timestamp column for incremental sync.
// This is defined here to avoid circular imports with the reader package.
type DateFilter struct {
	// Column is the name of the date column to filter on.
	Column string

	// Timestamp is the minimum value (rows where column > timestamp are included).
	Timestamp time.Time
}

// StrictParallelCapability declares the named mechanism a source dialect can
// use to keep parallel strict-consistency readers on one stable view. Callers
// use a type assertion so engines without the optional capability retain the
// conservative single-reader path.
type StrictParallelCapability interface {
	StrictParallelStrategy() string
}

// Dialect abstracts database-specific SQL syntax differences.
// Each database driver provides its own Dialect implementation.
type Dialect interface {
	// DBType returns the database type (e.g., "mssql", "postgres").
	DBType() string

	// QuoteIdentifier quotes an identifier (table, column name).
	// PostgreSQL: "identifier"
	// MSSQL: [identifier]
	// MySQL: `identifier`
	QuoteIdentifier(name string) string

	// QualifyTable returns a fully qualified table reference.
	// PostgreSQL: "schema"."table"
	// MSSQL: [schema].[table]
	QualifyTable(schema, table string) string

	// ParameterPlaceholder returns the parameter placeholder for the given index.
	// PostgreSQL: $1, $2, $3
	// MSSQL: @p1, @p2, @p3
	// MySQL: ?, ?, ?
	ParameterPlaceholder(index int) string

	// BuildDSN builds a connection string for this database.
	BuildDSN(host string, port int, database, user, password string, opts map[string]any) string

	// TableHint returns a table hint for read queries.
	// MSSQL: WITH (NOLOCK) when strictConsistency is false
	// PostgreSQL: empty (no hints)
	TableHint(strictConsistency bool) string

	// ColumnList formats a list of columns for SELECT.
	ColumnList(cols []string) string

	// ValueConverters resolves each source column type to a scan-value
	// normalization func for the transfer pipeline (#477); nil entries
	// mean pass-through. Resolved once per transfer, applied per value
	// only on columns that convert. Built-in dialects delegate to
	// DefaultValueConverters; catalog engines (#191) supply their own
	// from catalog-declared rules.
	ValueConverters(colTypes []string, targetDBType string) []func(any) any

	// ColumnListForSelect formats columns for SELECT with spatial conversions.
	// When reading from one database to write to another, spatial columns
	// need to be converted to WKT text format.
	ColumnListForSelect(cols, colTypes []string, targetDBType string) string

	// BuildKeysetQuery builds a keyset pagination query. An inclusive lower
	// bound is used only for the first page of fresh work; acknowledged
	// checkpoint watermarks and subsequent pages remain exclusive.
	BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK, inclusiveLowerBound bool, dateFilter *DateFilter) string

	// BuildKeysetArgs builds arguments for a keyset pagination query.
	// Parameters:
	//   - lastPK: the last primary key value from previous chunk
	//   - maxPK: upper bound for PK range (nil if no upper bound)
	//   - limit: number of rows to fetch
	//   - hasMaxPK: whether maxPK is being used for bounded query
	//   - dateFilter: optional date filter for incremental sync
	BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *DateFilter) []any

	// SupportsCompositeKeyset reports whether this engine's primary keys are
	// unique, so tuple keyset paging (WHERE (a,b) > last) is safe (#616). It
	// is false for engines whose "primary key" is a non-unique sorting key
	// (e.g. ClickHouse), where a duplicate key tuple split across a chunk
	// boundary would be skipped — those keep the ROW_NUMBER fallback.
	SupportsCompositeKeyset() bool

	// BuildCompositeKeysetQuery builds a single-reader tuple keyset query for
	// an all-integer composite primary key (#616): WHERE (a,b) > (?,?)
	// ORDER BY a,b. hasLowerBound is false only for the first unbounded
	// chunk. pkCols is the ordered PK column list.
	BuildCompositeKeysetQuery(cols string, pkCols []string, schema, table, tableHint string, hasLowerBound bool, dateFilter *DateFilter) string

	// BuildCompositeKeysetArgs builds args for BuildCompositeKeysetQuery.
	// lastPK is the previous chunk's PK tuple (ignored when unbounded).
	BuildCompositeKeysetArgs(lastPK []any, limit int, hasLowerBound bool, dateFilter *DateFilter) []any

	// SupportsCompositeRangeKeyset reports whether the dialect has an explicit
	// range-scoped tuple-keyset template. It is separate from tuple eligibility
	// so catalogs can conservatively keep the single-reader path.
	SupportsCompositeRangeKeyset() bool

	// BuildCompositeKeysetRangeQuery adds inclusive bounds on the numeric
	// leading PK component around the normal tuple watermark comparison.
	BuildCompositeKeysetRangeQuery(cols string, pkCols []string, schema, table, tableHint string, hasLowerBound, rangeMinInclusive bool, dateFilter *DateFilter) string

	// BuildCompositeKeysetRangeArgs matches BuildCompositeKeysetRangeQuery.
	// rangeMin and rangeMax are always present; tuple values are present only
	// after the first page of a range.
	BuildCompositeKeysetRangeArgs(lastPK []any, rangeMin, rangeMax int64, limit int, hasLowerBound bool, dateFilter *DateFilter) []any

	// BuildRowNumberQuery builds a ROW_NUMBER pagination query.
	// Parameters:
	//   - cols: column list for SELECT
	//   - orderBy: ORDER BY clause
	//   - schema: table schema
	//   - table: table name
	//   - tableHint: optional table hint (e.g., NOLOCK)
	//   - dateFilter: optional date filter for incremental sync
	BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string, dateFilter *DateFilter) string

	// BuildRowNumberArgs builds arguments for a ROW_NUMBER pagination query.
	// Parameters:
	//   - rowNum: starting row number (0-indexed)
	//   - limit: number of rows to fetch
	//   - dateFilter: optional date filter for incremental sync
	BuildRowNumberArgs(rowNum int64, limit int, dateFilter *DateFilter) []any

	// PartitionBoundariesQuery returns a query to get partition boundaries.
	PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string

	// RowCountQuery returns a query to get the row count.
	// If useStats is true, may use statistics tables for faster results.
	RowCountQuery(useStats bool) string

	// DateColumnQuery returns a query to find date columns.
	DateColumnQuery() string

	// ValidDateTypes returns a map of valid date/timestamp types.
	ValidDateTypes() map[string]bool

	// AIPromptAugmentation returns database-specific instructions to append to AI prompts.
	// This allows each driver to specify its own constraints for DDL generation.
	// Returns empty string if no augmentation is needed.
	AIPromptAugmentation() string
}

// GetDialect returns the appropriate dialect for the given database type.
// This uses the driver registry to get the dialect, eliminating switch statements.
// Returns nil if no driver is registered for the given type.
func GetDialect(dbType string) Dialect {
	d, err := Get(strings.ToLower(dbType))
	if err != nil {
		return nil
	}
	return d.Dialect()
}
