package validation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// SampleRow (Pass B) picks a deterministic random sample of PK
// values from the source, fetches the full row from BOTH endpoints,
// canonicalizes each column on both sides, and compares row hashes.
// This is dmt's primary "the data really migrated" check: catches
// systematic bugs (column truncation, type coercion, NULL drift,
// value corruption) at a cost bounded by sample size, not table
// size.
//
// Sampling is deterministic across runs: we ORDER BY a hash of the
// PK columns, take the first N. Both sides see the same N PKs (the
// hash is computed by the SQL engine, not driver-side, so we use
// MD5 which is portable across PG/MSSQL/MySQL).
type SampleRow struct {
	// N is the maximum number of rows to sample. Zero means the
	// default (1000).
	N int
	// PercentCap optionally caps the sample at a percentage of the
	// table's row count: effective_n = min(N, rows * PercentCap/100).
	// Zero disables the percent cap. Codex caught the original
	// runner ignoring this knob (#226); the pass now consumes
	// table.RowCount (populated by ExtractSchema) without an extra
	// COUNT round-trip.
	PercentCap float64
}

func (SampleRow) Name() string { return "sample_row" }

func (s SampleRow) Run(ctx context.Context, src, tgt Endpoint, table source.Table) PassResult {
	if !table.HasPK() {
		return PassResult{
			Pass:   "sample_row",
			Status: "skipped",
			Detail: "table has no primary key — sampling without a deterministic key isn't reproducible",
		}
	}

	n := s.N
	if n <= 0 {
		n = 1000
	}
	// Apply percent cap when configured. ExtractSchema populates
	// table.RowCount so we don't need a separate COUNT.
	if s.PercentCap > 0 && table.RowCount > 0 {
		capN := int(float64(table.RowCount) * s.PercentCap / 100.0)
		if capN < n {
			n = capN
		}
		if n < 1 {
			n = 1
		}
	}

	pks, err := samplePKValues(ctx, src, table, n)
	if err != nil {
		return PassResult{
			Pass:   "sample_row",
			Status: "fail",
			Detail: fmt.Sprintf("source PK sampling failed: %v", err),
		}
	}
	if len(pks) == 0 {
		return PassResult{
			Pass:   "sample_row",
			Status: "ok",
			Detail: "table is empty, nothing to sample",
		}
	}

	cols := allColumnNames(table)
	pkCols := pkColumnNames(table)

	var missing, mismatched int
	var findings []Finding
	for _, pk := range pks {
		srcDigest, srcFound, err := fetchRowDigest(ctx, src, table, cols, pkCols, pk)
		if err != nil {
			findings = append(findings, Finding{
				Pass:     "sample_row",
				Table:    table.Name,
				Severity: "error",
				Detail:   fmt.Sprintf("source fetch for PK %v failed: %v", pk, err),
			})
			continue
		}
		tgtDigest, tgtFound, err := fetchRowDigest(ctx, tgt, table, cols, pkCols, pk)
		if err != nil {
			findings = append(findings, Finding{
				Pass:     "sample_row",
				Table:    table.Name,
				Severity: "error",
				Detail:   fmt.Sprintf("target fetch for PK %v failed: %v", pk, err),
			})
			continue
		}

		// We started from source PKs so srcFound should always be
		// true (unless concurrent activity is deleting under us).
		// tgtFound=false → missing row.
		if !srcFound {
			// Treat as warning — racy, not a migration bug per se.
			continue
		}
		if !tgtFound {
			missing++
			findings = append(findings, Finding{
				Pass:     "sample_row",
				Table:    table.Name,
				Severity: "error",
				Detail:   fmt.Sprintf("PK %v exists in source but not target", pk),
			})
			continue
		}
		if !equalDigests(srcDigest, tgtDigest) {
			mismatched++
			findings = append(findings, Finding{
				Pass:     "sample_row",
				Table:    table.Name,
				Severity: "error",
				Detail: fmt.Sprintf("PK %v row digests differ: src=%s… tgt=%s…",
					pk, hex.EncodeToString(srcDigest)[:16], hex.EncodeToString(tgtDigest)[:16]),
			})
		}
	}

	if len(findings) == 0 {
		return PassResult{
			Pass:   "sample_row",
			Status: "ok",
			Detail: fmt.Sprintf("%d sampled rows; all values match", len(pks)),
		}
	}
	return PassResult{
		Pass:   "sample_row",
		Status: "fail",
		Detail: fmt.Sprintf("%d sampled rows; %d missing, %d value-mismatch",
			len(pks), missing, mismatched),
		Findings: findings,
	}
}

// samplePKValues pulls N PK tuples in deterministic order from the
// source. ORDER BY MD5(PK) → first N. Same sort key on the target
// side would give the same N PKs, but we don't need that — we use
// the source's sample to drive PK-lookups against the target.
//
// Cross-driver compatible hash: MD5 exists everywhere. The output
// formatting differs per DB but only matters for ORDER BY purposes
// — the actual values driving the sort are the bytes of the digest.
func samplePKValues(ctx context.Context, ep Endpoint, table source.Table, n int) ([][]any, error) {
	if ep.DB == nil {
		return nil, fmt.Errorf("endpoint %s: no DB connection", ep.Driver)
	}
	pkCols := pkColumnNames(table)
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("table has no PK")
	}

	pkSelect := make([]string, len(pkCols))
	for i, c := range pkCols {
		pkSelect[i] = ep.quoteCol(c)
	}

	// MD5 of CONCAT(pk cols cast to text) is portable enough.
	// PG: md5(col1::text || ...).
	// MSSQL: HASHBYTES('MD5', CONVERT(...) + CONVERT(...)).
	// MySQL: MD5(CONCAT_WS('|', col1, col2, ...)).
	// We build the dialect-specific hash expression here and feed
	// it into the ORDER BY. The exact algorithm doesn't matter —
	// only that it produces a deterministic order so the same N
	// rows surface every run.
	hashExpr := samplingOrderExpr(ep, pkCols)
	limitClause := samplingLimitClause(ep.Driver, n)

	var query string
	switch normalizeDriver(ep.Driver) {
	case "mssql":
		// TOP goes BEFORE the column list in MSSQL.
		query = fmt.Sprintf("SELECT TOP %d %s FROM %s ORDER BY %s",
			n, strings.Join(pkSelect, ", "),
			ep.qualifyTable(table.Name), hashExpr)
	default:
		query = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s %s",
			strings.Join(pkSelect, ", "),
			ep.qualifyTable(table.Name),
			hashExpr, limitClause)
	}

	logging.Debug("validation.sample_row: PK sample query: %s", query)

	rows, err := ep.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out [][]any
	for rows.Next() {
		vals := make([]any, len(pkCols))
		ptrs := make([]any, len(pkCols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

// fetchRowDigest pulls one row by PK from the endpoint and returns
// the SHA-256 of its canonicalized contents (or found=false if no
// row matches). Used by both source and target sides of the
// sample-row comparison.
func fetchRowDigest(ctx context.Context, ep Endpoint, table source.Table,
	cols, pkCols []string, pkValues []any) (digest []byte, found bool, err error) {
	if ep.DB == nil {
		return nil, false, fmt.Errorf("no DB connection")
	}

	selectList := make([]string, len(cols))
	for i, c := range cols {
		selectList[i] = ep.quoteCol(c)
	}

	// Build WHERE pk1 = ? AND pk2 = ? with the right placeholder
	// style per driver. Use sql.Named for MSSQL to dodge the
	// `?`-vs-`@p1` placeholder mismatch (Codex caught the same
	// trap on #166).
	driver := normalizeDriver(ep.Driver)
	whereClauses := make([]string, len(pkCols))
	args := make([]any, len(pkCols))
	for i, c := range pkCols {
		quoted := ep.quoteCol(c)
		switch driver {
		case "postgres":
			whereClauses[i] = fmt.Sprintf("%s = $%d", quoted, i+1)
			args[i] = pkValues[i]
		case "mssql":
			whereClauses[i] = fmt.Sprintf("%s = @p%d", quoted, i+1)
			args[i] = sql.Named(fmt.Sprintf("p%d", i+1), pkValues[i])
		default: // mysql + fallback use ?
			whereClauses[i] = fmt.Sprintf("%s = ?", quoted)
			args[i] = pkValues[i]
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(selectList, ", "),
		ep.qualifyTable(table.Name),
		strings.Join(whereClauses, " AND "))

	row := ep.DB.QueryRowContext(ctx, query, args...)
	scanVals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanVals {
		scanPtrs[i] = &scanVals[i]
	}
	if err := row.Scan(scanPtrs...); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("scan: %w", err)
	}

	h := sha256.New()
	enc := CanonicalEncoder{}
	for i := range cols {
		if err := enc.WriteValue(h, scanVals[i]); err != nil {
			return nil, true, fmt.Errorf("canonicalize column %s: %w", cols[i], err)
		}
	}
	return h.Sum(nil), true, nil
}

func equalDigests(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// samplingOrderExpr returns a driver-specific hash-of-PK expression
// to use in ORDER BY for deterministic sampling. The hash output
// doesn't need to be cryptographically strong; it just needs to
// give a stable, pseudo-random order. Uses the endpoint's
// identifier transform so column names match the target's actual
// casing (PG sanitizes to lowercase).
func samplingOrderExpr(ep Endpoint, pkCols []string) string {
	switch normalizeDriver(ep.Driver) {
	case "postgres":
		parts := make([]string, len(pkCols))
		for i, c := range pkCols {
			parts[i] = fmt.Sprintf("COALESCE(%s::text, '')", ep.quoteCol(c))
		}
		return fmt.Sprintf("md5(%s)", strings.Join(parts, " || '|' || "))
	case "mssql":
		parts := make([]string, len(pkCols))
		for i, c := range pkCols {
			parts[i] = fmt.Sprintf("ISNULL(CONVERT(NVARCHAR(MAX), %s), N'')", ep.quoteCol(c))
		}
		return fmt.Sprintf("HASHBYTES('MD5', %s)", strings.Join(parts, " + N'|' + "))
	case "mysql":
		parts := make([]string, len(pkCols))
		for i, c := range pkCols {
			parts[i] = ep.quoteCol(c)
		}
		return fmt.Sprintf("MD5(CONCAT_WS('|', %s))", strings.Join(parts, ", "))
	default:
		// Fall back to lexicographic PK order — not random, but
		// stable. The sampling still produces a comparable subset.
		quoted := make([]string, len(pkCols))
		for i, c := range pkCols {
			quoted[i] = ep.quoteCol(c)
		}
		return strings.Join(quoted, ", ")
	}
}

// samplingLimitClause returns the right LIMIT-style clause for the
// driver. MSSQL uses TOP (handled inline in the caller).
func samplingLimitClause(driver string, n int) string {
	switch normalizeDriver(driver) {
	case "mssql":
		return ""
	default:
		return fmt.Sprintf("LIMIT %d", n)
	}
}

func normalizeDriver(d string) string {
	switch d {
	case "postgres", "postgresql", "pg":
		return "postgres"
	case "mssql", "sqlserver", "sql-server":
		return "mssql"
	case "mysql", "mariadb", "maria":
		return "mysql"
	default:
		return d
	}
}
