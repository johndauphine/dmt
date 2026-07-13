package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// TableStatRow is the fixed schema-statistics shape consumed by smartconfig.
// Zero row count or width means the catalog could not determine that value.
type TableStatRow struct {
	Name            string
	RowCount        int64
	AvgRowSizeBytes int64
}

// TableFilter reports whether a table belongs to the workload being analyzed.
// A nil filter means all tables are in scope.
type TableFilter func(name string) bool

// SchemaStatsReader provides the schema metadata required by smartconfig.
// Implementations must apply include before expensive per-table work and must
// restrict date-column work to allowedTables.
type SchemaStatsReader interface {
	TableStats(ctx context.Context, db *sql.DB, schema string, include TableFilter) ([]TableStatRow, error)
	DateColumns(ctx context.Context, db *sql.DB, schema string, allowedTables []string) (map[string][]string, error)
}

// SchemaStatsProvider is an optional Driver capability. Support is explicit:
// callers require the assertion, ok=true, and a non-nil reader.
type SchemaStatsProvider interface {
	SchemaStatsReader() (SchemaStatsReader, bool)
}

type querySchemaStatsReader struct {
	tableStatsSQL  string
	dateColumnsSQL string
}

// NewQuerySchemaStatsReader builds the shared fixed-shape reader used by
// catalogs whose metadata can be fetched in two schema-wide queries.
func NewQuerySchemaStatsReader(tableStatsSQL, dateColumnsSQL string) SchemaStatsReader {
	return &querySchemaStatsReader{
		tableStatsSQL:  tableStatsSQL,
		dateColumnsSQL: dateColumnsSQL,
	}
}

func (r *querySchemaStatsReader) TableStats(
	ctx context.Context,
	db *sql.DB,
	schema string,
	include TableFilter,
) ([]TableStatRow, error) {
	if db == nil {
		return nil, fmt.Errorf("schema statistics database is unavailable")
	}
	if strings.TrimSpace(r.tableStatsSQL) == "" {
		return nil, fmt.Errorf("table-statistics query is empty")
	}

	rows, err := db.QueryContext(ctx, r.tableStatsSQL, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TableStatRow
	for rows.Next() {
		var row TableStatRow
		if err := rows.Scan(&row.Name, &row.RowCount, &row.AvgRowSizeBytes); err != nil {
			return nil, err
		}
		if include == nil || include(row.Name) {
			result = append(result, row)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *querySchemaStatsReader) DateColumns(
	ctx context.Context,
	db *sql.DB,
	schema string,
	allowedTables []string,
) (map[string][]string, error) {
	result := make(map[string][]string)
	if len(allowedTables) == 0 {
		return result, nil
	}
	if db == nil {
		return nil, fmt.Errorf("schema statistics database is unavailable")
	}
	if strings.TrimSpace(r.dateColumnsSQL) == "" {
		return nil, fmt.Errorf("date-column query is empty")
	}

	rows, err := db.QueryContext(ctx, r.dateColumnsSQL, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	exact, folded := allowedTableNames(allowedTables)
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, err
		}
		canonical, ok := exact[table]
		if !ok {
			canonical, ok = folded[strings.ToLower(table)]
			ok = ok && canonical != ""
		}
		if ok {
			result[canonical] = append(result[canonical], column)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// allowedTableNames preserves the table spelling returned by TableStats.
// Exact matches win; ambiguous case-folded names are deliberately rejected.
func allowedTableNames(tables []string) (map[string]string, map[string]string) {
	exact := make(map[string]string, len(tables))
	folded := make(map[string]string, len(tables))
	for _, table := range tables {
		exact[table] = table
		key := strings.ToLower(table)
		if existing, ok := folded[key]; ok && existing != table {
			folded[key] = ""
		} else if !ok {
			folded[key] = table
		}
	}
	return exact, folded
}
