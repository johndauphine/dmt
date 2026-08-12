package generic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

const sqliteSchemaStatsTableCap = 100

const (
	sqliteSchemaStatsTablesQuery = `
		SELECT name
		FROM pragma_table_list
		WHERE schema = 'main'
		  AND type IN ('table', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY name`
	sqliteSchemaStatsDBStatQuery = `
		SELECT CAST(COALESCE(SUM(payload) / NULLIF(SUM(ncell), 0), 0) AS INTEGER)
		FROM dbstat
		WHERE name = ? AND pagetype = 'leaf'`
	sqliteSchemaStatsDateColumnsQuery = `
		SELECT name, type
		FROM pragma_table_info(?)
		ORDER BY cid`
)

// sqliteSchemaStatsReader handles the per-table work that cannot be expressed
// as the two fixed-shape, schema-wide catalog queries used by server engines.
// A fresh instance is returned for each analyzer so the catalog and its quoting
// rules remain explicit even though SQLite currently has only one catalog.
type sqliteSchemaStatsReader struct {
	dialect *Dialect
}

func newSQLiteSchemaStatsReader(cat *Catalog) driver.SchemaStatsReader {
	return &sqliteSchemaStatsReader{dialect: NewDialect(cat)}
}

func (r *sqliteSchemaStatsReader) TableStats(
	ctx context.Context,
	db *sql.DB,
	_ string,
	include driver.TableFilter,
) ([]driver.TableStatRow, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite schema stats: database is nil")
	}

	rows, err := db.QueryContext(ctx, sqliteSchemaStatsTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("enumerating sqlite tables: %w", err)
	}

	// Collect and close the enumeration result before issuing COUNT/dbstat
	// queries. SQLite source pools may have one connection; retaining rows here
	// would otherwise deadlock waiting for a second connection from that pool.
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning sqlite table name: %w", err)
		}
		if include != nil && !include(name) {
			continue
		}
		names = append(names, name)
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if rowsErr != nil {
		return nil, fmt.Errorf("enumerating sqlite tables: %w", rowsErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("closing sqlite table enumeration: %w", closeErr)
	}

	// The cap protects an unconstrained interactive `dmt analyze` from an
	// accidental N-query catalog walk. Explicit include/exclude scope has
	// already reduced names above and is intentionally not capped.
	if include == nil && len(names) > sqliteSchemaStatsTableCap {
		logging.Warn("SQLite schema analysis is limited to the first %d of %d tables; use include_tables/exclude_tables to analyze a specific scope",
			sqliteSchemaStatsTableCap, len(names))
		names = names[:sqliteSchemaStatsTableCap]
	}

	stats := make([]driver.TableStatRow, 0, len(names))
	for _, name := range names {
		// Names originate in sqlite_schema but are still untrusted identifiers.
		// Quote through the catalog dialect so embedded double quotes are doubled
		// before the only dynamic SQL statement in this reader.
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QuoteIdentifier(name))
		var count int64
		if err := db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			return nil, fmt.Errorf("counting sqlite table %q: %w", name, err)
		}

		var avgRowSize int64
		if count > 0 {
			avgRowSize, err = sqliteDBStatAveragePayload(ctx, db, name)
			if err != nil {
				return nil, fmt.Errorf("reading sqlite dbstat for %q: %w", name, err)
			}
		}
		stats = append(stats, driver.TableStatRow{
			Name:            name,
			RowCount:        count,
			AvgRowSizeBytes: avgRowSize,
		})
	}
	sort.SliceStable(stats, func(i, j int) bool {
		if stats[i].RowCount == stats[j].RowCount {
			return stats[i].Name < stats[j].Name
		}
		return stats[i].RowCount > stats[j].RowCount
	})
	return stats, nil
}

// sqliteDBStatAveragePayload is best-effort because SQLite builds may omit the
// dbstat virtual table. Cancellation remains fatal: silently converting a
// canceled analysis into an unknown width would keep doing work after the
// caller explicitly stopped it.
func sqliteDBStatAveragePayload(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var avg int64
	err := db.QueryRowContext(ctx, sqliteSchemaStatsDBStatQuery, table).Scan(&avg)
	if err == nil {
		if avg < 0 {
			return 0, nil
		}
		return avg, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return 0, err
	}
	logging.Debug("SQLite dbstat unavailable for table %q; row width is unknown: %v", table, err)
	return 0, nil
}

func (r *sqliteSchemaStatsReader) DateColumns(
	ctx context.Context,
	db *sql.DB,
	_ string,
	allowedTables []string,
) (map[string][]string, error) {
	result := make(map[string][]string)
	if len(allowedTables) == 0 {
		return result, nil
	}
	if db == nil {
		return nil, fmt.Errorf("sqlite date columns: database is nil")
	}

	seen := make(map[string]struct{}, len(allowedTables))
	for _, table := range allowedTables {
		if _, duplicate := seen[table]; duplicate {
			continue
		}
		seen[table] = struct{}{}

		rows, err := db.QueryContext(ctx, sqliteSchemaStatsDateColumnsQuery, table)
		if err != nil {
			return nil, fmt.Errorf("reading sqlite columns for %q: %w", table, err)
		}
		var columns []string
		for rows.Next() {
			var name, declaredType string
			if err := rows.Scan(&name, &declaredType); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scanning sqlite column for %q: %w", table, err)
			}
			if sqliteDeclaredDateType(declaredType) {
				columns = append(columns, name)
			}
		}
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("reading sqlite columns for %q: %w", table, rowsErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("closing sqlite columns for %q: %w", table, closeErr)
		}
		if len(columns) > 0 {
			result[table] = columns
		}
	}
	return result, nil
}

// SQLite has type affinity rather than a closed type system. Recognize the
// conventional declared date/time families while avoiding the former broad
// TEXT match, which suggested arbitrary text columns as incremental watermarks.
func sqliteDeclaredDateType(declared string) bool {
	base := strings.ToUpper(strings.TrimSpace(declared))
	if end := strings.IndexAny(base, "( \t\r\n"); end >= 0 {
		base = base[:end]
	}
	switch base {
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		return true
	default:
		return false
	}
}

var _ driver.SchemaStatsReader = (*sqliteSchemaStatsReader)(nil)
