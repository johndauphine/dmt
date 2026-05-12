package validation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/source"
)

// NullParity (Pass C) compares per-column NULL counts between
// source and target. Catches the very common "NOT NULL drift" bug
// — e.g. a column declared NOT NULL on source ended up nullable on
// target and now has rows where it shouldn't. Cheapest of the
// three new passes: one aggregation query per table.
type NullParity struct{}

func (NullParity) Name() string { return "null_parity" }

func (NullParity) Run(ctx context.Context, src, tgt Endpoint, table source.Table) PassResult {
	cols := allColumnNames(table)
	if len(cols) == 0 {
		return PassResult{Pass: "null_parity", Status: "skipped", Detail: "no columns"}
	}

	srcCounts, err := countNullsPerColumn(ctx, src, table, cols)
	if err != nil {
		return PassResult{
			Pass:   "null_parity",
			Status: "fail",
			Detail: fmt.Sprintf("source null-count query failed: %v", err),
		}
	}
	tgtCounts, err := countNullsPerColumn(ctx, tgt, table, cols)
	if err != nil {
		return PassResult{
			Pass:   "null_parity",
			Status: "fail",
			Detail: fmt.Sprintf("target null-count query failed: %v", err),
		}
	}

	var findings []Finding
	for _, c := range cols {
		if srcCounts[c] != tgtCounts[c] {
			findings = append(findings, Finding{
				Pass:     "null_parity",
				Table:    table.Name,
				Column:   c,
				Severity: "error",
				Detail: fmt.Sprintf("source has %d NULL(s), target has %d NULL(s) (diff=%d)",
					srcCounts[c], tgtCounts[c], tgtCounts[c]-srcCounts[c]),
			})
		}
	}

	if len(findings) > 0 {
		return PassResult{
			Pass:     "null_parity",
			Status:   "fail",
			Detail:   fmt.Sprintf("%d column(s) have NULL-count mismatches", len(findings)),
			Findings: findings,
		}
	}
	return PassResult{
		Pass:   "null_parity",
		Status: "ok",
		Detail: fmt.Sprintf("%d columns checked, all NULL counts match", len(cols)),
	}
}

// countNullsPerColumn runs a single aggregation query that returns
// one NULL-count column per data column. This keeps the per-table
// cost to one round-trip rather than N round-trips, which matters
// when the same validation runs against hundreds of tables.
//
// The SQL is portable: SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)
// works identically on PostgreSQL, SQL Server, MySQL. (PG and SQL
// Server have shorter alternatives — COUNT(*) FILTER and COUNT vs
// COUNT(col) — but the CASE form is the lowest common denominator
// and reads the same on every driver.)
func countNullsPerColumn(ctx context.Context, ep Endpoint, table source.Table, cols []string) (map[string]int64, error) {
	if ep.DB == nil {
		return nil, fmt.Errorf("endpoint %s: no DB connection", ep.Driver)
	}

	selectExprs := make([]string, len(cols))
	for i, c := range cols {
		selectExprs[i] = fmt.Sprintf("SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END)",
			ep.quoteCol(c))
	}

	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(selectExprs, ", "),
		ep.qualifyTable(table.Name))

	row := ep.DB.QueryRowContext(ctx, query)
	values := make([]sql.NullInt64, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := row.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("scan null-counts: %w", err)
	}

	out := make(map[string]int64, len(cols))
	for i, c := range cols {
		// An empty table returns NULL from SUM — treat as 0.
		if values[i].Valid {
			out[c] = values[i].Int64
		}
	}
	return out, nil
}
