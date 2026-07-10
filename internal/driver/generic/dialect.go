package generic

import (
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

// Dialect implements driver.Dialect from a Catalog. All SQL shape comes
// from catalog templates; this type only renders.
type Dialect struct {
	cat *Catalog
}

// NewDialect wraps a validated catalog.
func NewDialect(cat *Catalog) *Dialect { return &Dialect{cat: cat} }

// The generic dialect must satisfy the full driver contract.
var (
	_ driver.Dialect                  = (*Dialect)(nil)
	_ driver.StrictParallelCapability = (*Dialect)(nil)
)

func (d *Dialect) DBType() string { return d.cat.Name }

func (d *Dialect) StrictParallelStrategy() string {
	return d.cat.StrictParallelStrategy
}

func (d *Dialect) QuoteIdentifier(name string) string {
	esc := d.cat.Quoting.EscapedChar
	escaped := strings.ReplaceAll(name, esc, esc+esc)
	return fmt.Sprintf(d.cat.Quoting.IdentifierFormat, escaped)
}

func (d *Dialect) QualifyTable(schema, table string) string {
	if d.cat.Quoting.SchemaIgnored || schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

// ParameterPlaceholder renders "?"-style or indexed placeholders from
// the catalog format ("?", "$%d", "@p%d", ":%d").
func (d *Dialect) ParameterPlaceholder(index int) string {
	if !strings.Contains(d.cat.Placeholder, "%d") {
		return d.cat.Placeholder
	}
	return fmt.Sprintf(d.cat.Placeholder, index)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	if s := d.cat.Connection.DSNStrategy; s != "" {
		return dsnStrategies[s](host, port, database, user, password, opts)
	}
	r := strings.NewReplacer(
		"{host}", host,
		"{port}", fmt.Sprintf("%d", port),
		"{database}", database,
		"{user}", user,
		"{password}", password,
	)
	return r.Replace(d.cat.Connection.URLTemplate)
}

func (d *Dialect) TableHint(strictConsistency bool) string {
	if strictConsistency {
		return d.cat.TableHints.Strict
	}
	return d.cat.TableHints.Relaxed
}

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

// ColumnListForSelect quotes the column list, wrapping spatial columns
// per the catalog's spatial_select spec on cross-engine reads (the
// hand-written postgres/mysql dialects' ST_AsText conversion — targets'
// staging paths expect WKT text).
func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	sp := d.cat.SpatialSelect
	if sp.Wrapper == "" || targetDBType == d.cat.Name {
		return d.ColumnList(cols)
	}
	spatial := make(map[string]bool, len(sp.Types))
	for _, t := range sp.Types {
		spatial[t] = true
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}
		if spatial[colType] {
			quoted[i] = strings.ReplaceAll(sp.Wrapper, "{col}", d.QuoteIdentifier(c))
			continue
		}
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK, inclusiveLowerBound bool, dateFilter *driver.DateFilter) string {
	k := d.cat.Pagination.Keyset
	maxClause, dateClause := "", ""
	lowerOp := ">"
	if inclusiveLowerBound {
		lowerOp = ">="
	}
	if hasMaxPK {
		maxClause = k.MaxPKClause
	}
	if dateFilter != nil {
		dateClause = strings.ReplaceAll(k.DateClause, "{date_column}", d.QuoteIdentifier(dateFilter.Column))
	}
	q := k.Query
	q = strings.ReplaceAll(q, "{lower_op}", lowerOp)
	q = strings.ReplaceAll(q, "{max_pk_clause}", maxClause)
	q = strings.ReplaceAll(q, "{date_clause}", dateClause)
	return d.render(q, cols, "", pkCol, schema, table, tableHint)
}

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *driver.DateFilter) []any {
	a := d.cat.Pagination.Keyset.Args
	var order []string
	switch {
	case hasMaxPK && dateFilter != nil:
		order = a.WithMaxDate
	case hasMaxPK:
		order = a.WithMax
	case dateFilter != nil:
		order = a.NoMaxDate
	default:
		order = a.NoMax
	}
	args := make([]any, 0, len(order))
	for _, sym := range order {
		switch sym {
		case "last_pk":
			args = append(args, lastPK)
		case "max_pk":
			args = append(args, maxPK)
		case "date_from":
			args = append(args, d.dateArg(dateFilter.Timestamp))
		case "limit":
			args = append(args, limit)
		}
	}
	return args
}

// compositeLowerClause builds the tuple "greater than last" comparison for a
// composite keyset and the ordered PK-component indices that fill its
// placeholders (#616). Both the query and the args derive from the same
// (quotedPKs, style) so they can never drift. rowvalue → "(a, b) > ({?},
// {?})"; orchain → "(a > {?} OR (a = {?} AND b > {?}))".
func compositeLowerClause(quotedPKs []string, style string) (clause string, argIdx []int) {
	n := len(quotedPKs)
	if style == "orchain" {
		terms := make([]string, n)
		for k := 0; k < n; k++ {
			parts := make([]string, 0, k+1)
			for j := 0; j < k; j++ {
				parts = append(parts, quotedPKs[j]+" = {?}")
				argIdx = append(argIdx, j)
			}
			parts = append(parts, quotedPKs[k]+" > {?}")
			argIdx = append(argIdx, k)
			if len(parts) == 1 {
				terms[k] = parts[0]
			} else {
				terms[k] = "(" + strings.Join(parts, " AND ") + ")"
			}
		}
		return "(" + strings.Join(terms, " OR ") + ")", argIdx
	}
	// rowvalue (default)
	ph := make([]string, n)
	for i := range quotedPKs {
		ph[i] = "{?}"
		argIdx = append(argIdx, i)
	}
	return "(" + strings.Join(quotedPKs, ", ") + ") > (" + strings.Join(ph, ", ") + ")", argIdx
}

// SupportsCompositeKeyset reports whether this engine opted into tuple keyset
// paging by declaring a composite_keyset catalog section (#616). Engines with
// non-unique "primary keys" (ClickHouse) omit it and keep ROW_NUMBER.
func (d *Dialect) SupportsCompositeKeyset() bool {
	return d.cat.Pagination.CompositeKeyset.Query != ""
}

// SupportsCompositeRangeKeyset reports whether this catalog explicitly
// supports range-scoped tuple pagination (#667). Keeping this opt-in avoids
// manufacturing SQL for engines whose tuple semantics have not been vetted.
func (d *Dialect) SupportsCompositeRangeKeyset() bool {
	return d.cat.Pagination.CompositeKeyset.RangeQuery != ""
}

// BuildCompositeKeysetQuery builds a single-reader tuple keyset query for an
// all-integer composite PK (#616). hasLowerBound is false only for the first
// (unbounded) chunk, where the comparison degrades to "1=1".
func (d *Dialect) BuildCompositeKeysetQuery(cols string, pkCols []string, schema, table, tableHint string, hasLowerBound bool, dateFilter *driver.DateFilter) string {
	ck := d.cat.Pagination.CompositeKeyset
	quoted := make([]string, len(pkCols))
	for i, c := range pkCols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	clause := "1=1"
	if hasLowerBound {
		clause, _ = compositeLowerClause(quoted, ck.Style)
	}
	dateClause := ""
	if dateFilter != nil {
		dateClause = strings.ReplaceAll(ck.DateClause, "{date_column}", d.QuoteIdentifier(dateFilter.Column))
	}
	q := ck.Query
	q = strings.ReplaceAll(q, "{composite_clause}", clause)
	q = strings.ReplaceAll(q, "{order_by}", strings.Join(quoted, ", "))
	q = strings.ReplaceAll(q, "{date_clause}", dateClause)
	return d.render(q, cols, "", "", schema, table, tableHint)
}

// BuildCompositeKeysetArgs builds args matching the placeholder order of
// BuildCompositeKeysetQuery: the tuple-comparison values (from lastPK,
// omitted when unbounded), the optional date bound, and the limit — with the
// limit placed first for engines whose template puts it up front (mssql TOP).
func (d *Dialect) BuildCompositeKeysetArgs(lastPK []any, limit int, hasLowerBound bool, dateFilter *driver.DateFilter) []any {
	ck := d.cat.Pagination.CompositeKeyset
	var lowerArgs []any
	if hasLowerBound {
		_, argIdx := compositeLowerClause(make([]string, len(lastPK)), ck.Style)
		for _, i := range argIdx {
			lowerArgs = append(lowerArgs, lastPK[i])
		}
	}
	args := make([]any, 0, len(lowerArgs)+2)
	if ck.LimitFirst {
		args = append(args, limit)
	}
	args = append(args, lowerArgs...)
	if dateFilter != nil {
		args = append(args, d.dateArg(dateFilter.Timestamp))
	}
	if !ck.LimitFirst {
		args = append(args, limit)
	}
	return args
}

// BuildCompositeKeysetRangeQuery builds the bounded form used by parallel
// tuple readers. The tuple comparison and its arguments remain generated by
// compositeLowerClause; only the leading component receives a range bound.
func (d *Dialect) BuildCompositeKeysetRangeQuery(cols string, pkCols []string, schema, table, tableHint string, hasLowerBound, rangeMinInclusive bool, dateFilter *driver.DateFilter) string {
	ck := d.cat.Pagination.CompositeKeyset
	if ck.RangeQuery == "" || len(pkCols) == 0 {
		return ""
	}
	quoted := make([]string, len(pkCols))
	for i, c := range pkCols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	clause := "1=1"
	if hasLowerBound {
		clause, _ = compositeLowerClause(quoted, ck.Style)
	}
	dateClause := ""
	if dateFilter != nil {
		dateClause = strings.ReplaceAll(ck.DateClause, "{date_column}", d.QuoteIdentifier(dateFilter.Column))
	}
	q := ck.RangeQuery
	q = strings.ReplaceAll(q, "{composite_clause}", clause)
	lowerOp := ">"
	if rangeMinInclusive {
		lowerOp = ">="
	}
	q = strings.ReplaceAll(q, "{range_clause}", quoted[0]+" "+lowerOp+" {?} AND "+quoted[0]+" <= {?}")
	q = strings.ReplaceAll(q, "{order_by}", strings.Join(quoted, ", "))
	q = strings.ReplaceAll(q, "{date_clause}", dateClause)
	return d.render(q, cols, "", "", schema, table, tableHint)
}

// BuildCompositeKeysetRangeArgs is deliberately coupled to
// compositeLowerClause, just like the unbounded tuple path: changing the
// OR-chain shape cannot silently desynchronize placeholder order and values.
func (d *Dialect) BuildCompositeKeysetRangeArgs(lastPK []any, rangeMin, rangeMax int64, limit int, hasLowerBound bool, dateFilter *driver.DateFilter) []any {
	ck := d.cat.Pagination.CompositeKeyset
	var lowerArgs []any
	if hasLowerBound {
		_, argIdx := compositeLowerClause(make([]string, len(lastPK)), ck.Style)
		for _, i := range argIdx {
			lowerArgs = append(lowerArgs, lastPK[i])
		}
	}
	args := make([]any, 0, len(lowerArgs)+4)
	if ck.LimitFirst {
		args = append(args, limit)
	}
	args = append(args, lowerArgs...)
	args = append(args, rangeMin, rangeMax)
	if dateFilter != nil {
		args = append(args, d.dateArg(dateFilter.Timestamp))
	}
	if !ck.LimitFirst {
		args = append(args, limit)
	}
	return args
}

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string, dateFilter *driver.DateFilter) string {
	rn := d.cat.Pagination.RowNumber
	whereDate := ""
	if dateFilter != nil {
		whereDate = strings.ReplaceAll(rn.DateClause, "{date_column}", d.QuoteIdentifier(dateFilter.Column))
	}
	q := strings.ReplaceAll(rn.Query, "{where_date}", whereDate)
	q = strings.ReplaceAll(q, "{outer_columns}", extractColumnAliases(cols))
	q = strings.ReplaceAll(q, "{order_by}", orderBy)
	return d.render(q, cols, orderBy, "", schema, table, tableHint)
}

func (d *Dialect) BuildRowNumberArgs(rowNum int64, limit int, dateFilter *driver.DateFilter) []any {
	a := d.cat.Pagination.RowNumber.Args
	order := a.NoDate
	if dateFilter != nil {
		order = a.Date
	}
	args := make([]any, 0, len(order))
	for _, sym := range order {
		switch sym {
		case "date_from":
			args = append(args, d.dateArg(dateFilter.Timestamp))
		case "row_start":
			args = append(args, rowNum)
		case "row_end":
			args = append(args, rowNum+int64(limit))
		}
	}
	return args
}

func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	q := strings.ReplaceAll(d.cat.Queries.PartitionBoundaries, "{n}", fmt.Sprintf("%d", numPartitions))
	return d.render(q, "", "", pkCol, schema, table, "")
}

func (d *Dialect) RowCountQuery(useStats bool) string {
	if useStats && d.cat.Queries.RowCountStats != "" {
		return d.cat.Queries.RowCountStats
	}
	return d.cat.Queries.RowCount
}

func (d *Dialect) DateColumnQuery() string { return d.cat.Queries.DateColumn }

func (d *Dialect) ValidDateTypes() map[string]bool {
	m := make(map[string]bool, len(d.cat.DateTypes))
	for _, t := range d.cat.DateTypes {
		m[t] = true
	}
	return m
}

// ValueConverters is strategy-selected (#477/#478); the only phase-1
// strategy is the shared default normalization table.
func (d *Dialect) ValueConverters(colTypes []string, targetDBType string) []func(any) any {
	return driver.DefaultValueConverters(colTypes)
}

func (d *Dialect) AIPromptAugmentation() string { return d.cat.AI.PromptAugmentation }

func (d *Dialect) AIDropTablePromptAugmentation() string {
	return d.cat.AI.DropTablePromptAugmentation
}

// dateArg binds a date-filter timestamp: engines with a declared
// date_arg_format (mssql) get the UTC-formatted string form; everyone
// else binds time.Time directly.
func (d *Dialect) dateArg(ts time.Time) any {
	if layout := d.cat.Pagination.DateArgFormat; layout != "" {
		return ts.UTC().Format(layout)
	}
	return ts
}

// render substitutes the shared tokens, then numbers the {?} parameter
// markers in order of appearance — indexed dialects ($1, @p1) get
// correct numbering regardless of which optional clauses rendered.
func (d *Dialect) render(q, cols, orderBy, pkCol, schema, table, tableHint string) string {
	r := strings.NewReplacer(
		"{columns}", cols,
		"{table}", d.QualifyTable(schema, table),
		"{hint}", tableHint,
		"{pk}", d.QuoteIdentifier(pkCol),
	)
	q = r.Replace(q)
	if strings.Contains(q, "{?}") {
		var b strings.Builder
		idx := 1
		for {
			i := strings.Index(q, "{?}")
			if i < 0 {
				b.WriteString(q)
				break
			}
			b.WriteString(q[:i])
			b.WriteString(d.ParameterPlaceholder(idx))
			idx++
			q = q[i+len("{?}"):]
		}
		return b.String()
	}
	return q
}

// extractColumnAliases keeps the outer-SELECT column list of a
// ROW_NUMBER subquery in sync with aliased inner columns ("x AS y" →
// "y"). Same semantics as the hand-written dialects.
func extractColumnAliases(cols string) string {
	parts := strings.Split(cols, ",")
	aliases := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		upperPart := strings.ToUpper(part)
		if idx := strings.LastIndex(upperPart, " AS "); idx != -1 {
			aliases[i] = strings.TrimSpace(part[idx+4:])
		} else {
			aliases[i] = part
		}
	}
	return strings.Join(aliases, ", ")
}
