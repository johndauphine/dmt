// Package validation implements cross-DB deep validation passes
// layered on top of the orchestrator's existing row-count check
// (#226). The package is deliberately independent of the
// rest of the orchestrator: it takes plain *sql.DB handles +
// table metadata and returns a structured Result the orchestrator
// can render.
//
// The validation pipeline runs as a sequence of independent passes:
//
//   - count_only (existing behavior): COUNT(*) per table. drop_recreate
//     requires parity; upsert permits the target to be a superset.
//     Provided by the orchestrator, not this package.
//   - null_parity: per-column NULL count parity. One aggregation
//     query per side. Catches systematic NULL drift with strict
//     equality.
//   - sample:      N deterministically-chosen rows fetched from
//     both sides and compared field-by-field via a Go-side
//     canonicalizer.
//
// "full" is reserved as a mode name for a possible future
// whole-table row-hash pass; the orchestrator rejects it at
// runtime with a clear pointer.
//
// Default remains count_only so the new code is opt-in and never
// silently slows down a pre-existing migration.
package validation

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/johndauphine/dmt/v5/internal/source"
)

// Mode selects which validation passes run. Modes are inclusive:
// each one runs everything below it plus its named pass.
type Mode string

const (
	// ModeCountOnly is the default COUNT(*) row-count check.
	// drop_recreate requires parity; upsert permits the target
	// to be a superset.
	ModeCountOnly Mode = "count_only"
	// ModeNullParity adds per-column "how many NULLs?" check.
	// Cheap: one aggregation per table.
	ModeNullParity Mode = "null_parity"
	// ModeSample adds value-level sampling: N random PKs fetched
	// from both sides and compared field-by-field. Cost is
	// bounded by sample size, not table size.
	ModeSample Mode = "sample"
	// ModeFull is reserved for the in-DB row-hashing pass tracked
	// as a #226 follow-up. Setting validation.mode: full in YAML
	// today fails with an explicit error pointing at the
	// follow-up issue — better than silently degrading to a
	// weaker check.
	ModeFull Mode = "full"
)

// Config wraps the user-facing knobs for validation behavior.
// Zero value means default (count_only, fail on mismatch, no
// per-table timeout).
type Config struct {
	// Mode picks which passes run.
	Mode Mode
	// SampleRows is the row count for the sample pass. Zero
	// uses the default of 1000.
	SampleRows int
	// SampleRowsPercent is an alternative: take min(SampleRows, P% of table).
	// Zero disables the cap.
	SampleRowsPercent float64
	// HashColumns is reserved for the in-DB row-hashing follow-up
	// (current PR ships Pass B + Pass C only). Kept in the config
	// surface so YAML written against the follow-up doesn't break
	// against this PR's binary.
	HashColumns []string
	// FailOnMismatch returns an error from Validate when any
	// pass fails. Defaults true; set false to log-only.
	FailOnMismatch *bool
	// Timeout caps per-table validation runtime. Zero means
	// no cap (use the surrounding context's deadline only).
	Timeout time.Duration

	// MaxParallel bounds how many tables validate concurrently.
	// Each parallel table consumes ~one connection per side per
	// pass, so unbounded fan-out overwhelms connection pools on
	// schemas with 100+ tables. Zero uses the documented default
	// (4) — sized to fit within most pool budgets without
	// starving the rest of the orchestrator.
	MaxParallel int
}

// EffectiveSampleRows returns the row count to sample, applying
// the percent cap when configured. Returns 0 to mean "skip the
// sample pass" only when the caller explicitly wants none.
func (c Config) EffectiveSampleRows(tableRows int64) int {
	n := c.SampleRows
	if n <= 0 {
		n = 1000
	}
	if c.SampleRowsPercent > 0 && tableRows > 0 {
		capRows := int(float64(tableRows) * c.SampleRowsPercent / 100.0)
		if capRows < n {
			n = capRows
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

// FailOnMismatchOrDefault returns the configured fail-on-mismatch
// flag or the documented default (true) when unset.
func (c Config) FailOnMismatchOrDefault() bool {
	if c.FailOnMismatch == nil {
		return true
	}
	return *c.FailOnMismatch
}

// Finding is one observation from any validation pass. Pass and
// Table are always populated; Column is populated for per-column
// findings (NULL parity, column hash); Detail carries the
// human-readable specifics. Severity is "info", "warn", or "error".
type Finding struct {
	Pass     string
	Table    string
	Column   string
	Severity string
	Detail   string
}

// TableResult aggregates findings for one table.
type TableResult struct {
	TableName string
	Passes    []PassResult
}

// PassResult records what one pass observed for one table.
type PassResult struct {
	Pass     string
	Status   string // "ok", "warn", "fail", "skipped"
	Detail   string
	Findings []Finding
}

// HasFailure reports whether any pass in this table failed
// outright. Warnings (e.g. count-estimate fallbacks) don't count.
func (r TableResult) HasFailure() bool {
	for _, p := range r.Passes {
		if p.Status == "fail" {
			return true
		}
	}
	return false
}

// Result is the report for a full validation run.
type Result struct {
	Tables []TableResult
}

// HasFailure reports whether ANY table had a failing pass.
func (r Result) HasFailure() bool {
	for _, t := range r.Tables {
		if t.HasFailure() {
			return true
		}
	}
	return false
}

// Endpoint groups a DB connection with the schema we're validating.
// Both source and target are passed in by the orchestrator.
//
// IdentifierFor is the per-endpoint identifier transform. The
// orchestrator sanitizes source identifiers when writing to the
// target (PostgreSQL targets get lowercase via
// target.SanitizePGIdentifier); validation queries on the target
// must apply the same transform or they hit "relation does not
// exist" errors against tables that DO exist under a sanitized
// name. nil means identity transform.
type Endpoint struct {
	DB            *sql.DB
	Driver        string // "postgres", "mssql", "mysql" — needed for SQL dialect choices
	Schema        string
	IdentifierFor func(string) string
}

// resolve applies the endpoint's identifier transform if set.
// Used everywhere a table or column name from the source schema is
// referenced in a query against this endpoint.
func (ep Endpoint) resolve(name string) string {
	if ep.IdentifierFor == nil {
		return name
	}
	return ep.IdentifierFor(name)
}

// qualifyTable returns a fully-qualified, properly-quoted reference
// to the given table after applying the endpoint's identifier
// transform to the TABLE name only. The schema name is used
// verbatim: dmt's transfer phase doesn't rewrite the configured
// target schema, so a target schema like `Reporting` is created
// as `"Reporting"` and queries that lowercase it would fail with
// "schema does not exist" (Codex review on #226).
func (ep Endpoint) qualifyTable(table string) string {
	return qualifiedName(ep.Driver, ep.Schema, ep.resolve(table))
}

// quoteCol returns a properly-quoted column identifier after
// applying the endpoint's identifier transform.
func (ep Endpoint) quoteCol(col string) string {
	return quoteIdent(ep.Driver, ep.resolve(col))
}

// Pass is the interface every validation pass implements. The
// orchestrator builds the configured passes once and runs them per
// table. Passes don't share state across tables; each Run call is
// independent.
type Pass interface {
	// Name returns a stable identifier ("null_parity", etc) used in
	// findings and logs.
	Name() string
	// Run executes the pass for one table against both endpoints
	// and returns a PassResult. The function must not panic on
	// schema mismatches — callers expect a "fail" PassResult, not
	// a Go error.
	Run(ctx context.Context, src, tgt Endpoint, table source.Table) PassResult
}

// qualifiedName returns a driver-quoted reference to schema.table
// (or just table when no schema is configured). For MySQL, the
// schema name is treated as the database — the writer qualifies
// cross-database tables as `db`.`table` and this helper does too
// when schema is non-empty (Codex caught the earlier version
// silently dropping the schema for MySQL on #226).
func qualifiedName(driver, schema, table string) string {
	switch driver {
	case "postgres", "postgresql", "pg":
		if schema == "" {
			return pgQuote(table)
		}
		return pgQuote(schema) + "." + pgQuote(table)
	case "mssql", "sqlserver", "sql-server":
		if schema == "" {
			return mssqlQuote(table)
		}
		return mssqlQuote(schema) + "." + mssqlQuote(table)
	case "mysql", "mariadb", "maria":
		if schema == "" {
			return mysqlQuote(table)
		}
		return mysqlQuote(schema) + "." + mysqlQuote(table)
	default:
		// Conservative fallback. Most drivers accept double-quoted
		// identifiers per ANSI SQL.
		if schema == "" {
			return pgQuote(table)
		}
		return pgQuote(schema) + "." + pgQuote(table)
	}
}

// quoteIdent returns a driver-quoted column or other identifier
// with embedded delimiters properly escaped (Codex caught the
// earlier version on #226 — names containing the dialect's
// delimiter character would produce invalid SQL).
func quoteIdent(driver, name string) string {
	switch driver {
	case "postgres", "postgresql", "pg":
		return pgQuote(name)
	case "mssql", "sqlserver", "sql-server":
		return mssqlQuote(name)
	case "mysql", "mariadb", "maria":
		return mysqlQuote(name)
	default:
		return pgQuote(name)
	}
}

// pgQuote returns a PostgreSQL-style "double-quoted" identifier.
// Embedded `"` characters are doubled per the SQL standard.
func pgQuote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// mssqlQuote returns a SQL Server-style [bracketed] identifier.
// Embedded `]` characters are doubled.
func mssqlQuote(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// mysqlQuote returns a MySQL-style `backticked` identifier.
// Embedded backticks are doubled per MySQL's manual.
func mysqlQuote(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// nonPKColumns returns table columns excluding the PK columns (the
// PK is implicit in PK-based fetches; passes that hash non-PK data
// don't need to re-hash the PK on top of it).
func nonPKColumns(table source.Table) []string {
	pkSet := make(map[string]struct{}, len(table.PKColumns))
	for _, c := range table.PKColumns {
		pkSet[strings.ToLower(c.Name)] = struct{}{}
	}
	out := make([]string, 0, len(table.Columns))
	for _, c := range table.Columns {
		if _, isPK := pkSet[strings.ToLower(c.Name)]; isPK {
			continue
		}
		out = append(out, c.Name)
	}
	return out
}

// allColumnNames returns every column name in declaration order.
func allColumnNames(table source.Table) []string {
	out := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		out[i] = c.Name
	}
	return out
}

// pkColumnNames returns the PK columns in their declared order.
// Returns nil if the table has no PK (callers must handle).
func pkColumnNames(table source.Table) []string {
	if len(table.PKColumns) == 0 {
		return nil
	}
	out := make([]string, len(table.PKColumns))
	for i, c := range table.PKColumns {
		out[i] = c.Name
	}
	return out
}
