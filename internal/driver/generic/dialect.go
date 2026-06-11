package generic

import (
	"fmt"
	"strings"

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
var _ driver.Dialect = (*Dialect)(nil)

func (d *Dialect) DBType() string { return d.cat.Name }

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

// ColumnListForSelect is strategy-selected per the audit; no phase-1
// catalog declares select wrappers (sqlite has no spatial types), so
// the plain quoted list is the only strategy yet.
func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	return d.ColumnList(cols)
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
	k := d.cat.Pagination.Keyset
	maxClause, dateClause := "", ""
	if hasMaxPK {
		maxClause = k.MaxPKClause
	}
	if dateFilter != nil {
		dateClause = strings.ReplaceAll(k.DateClause, "{date_column}", d.QuoteIdentifier(dateFilter.Column))
	}
	q := k.Query
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
			args = append(args, dateFilter.Timestamp)
		case "limit":
			args = append(args, limit)
		}
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
			args = append(args, dateFilter.Timestamp)
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
