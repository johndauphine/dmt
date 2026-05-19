package shared

import (
	"fmt"
	"strings"
)

// IdentifierQuoter is implemented by driver dialects that can quote database
// identifiers such as table and column names.
type IdentifierQuoter interface {
	QuoteIdentifier(name string) string
}

// PlaceholderFormatter is implemented by driver dialects that can format a
// parameter placeholder for a 1-based argument index.
type PlaceholderFormatter interface {
	ParameterPlaceholder(index int) string
}

// TableQualifier is implemented by driver dialects that can render a fully
// qualified table reference.
type TableQualifier interface {
	QualifyTable(schema, table string) string
}

// QuotedColumnList returns a comma-separated list of quoted column names.
func QuotedColumnList(quoter IdentifierQuoter, columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoter.QuoteIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

// OrderedKeySelect returns a SELECT statement that scans primary-key columns in
// deterministic key order.
func OrderedKeySelect(
	qualifier TableQualifier,
	quoter IdentifierQuoter,
	schema, table string,
	keyColumns []string,
) (string, error) {
	if qualifier == nil {
		return "", fmt.Errorf("ordered key select: qualifier is nil")
	}
	if quoter == nil {
		return "", fmt.Errorf("ordered key select: quoter is nil")
	}
	if len(keyColumns) == 0 {
		return "", fmt.Errorf("ordered key select: key columns are required")
	}
	if table == "" {
		return "", fmt.Errorf("ordered key select: table is required")
	}

	keyList := QuotedColumnList(quoter, keyColumns)
	return fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", keyList, qualifier.QualifyTable(schema, table), keyList), nil
}

// MultiRowPlaceholders returns comma-separated placeholder groups for a
// multi-row INSERT using 1-based parameter numbering.
func MultiRowPlaceholders(formatter PlaceholderFormatter, rowCount, columnCount int) (string, error) {
	return MultiRowPlaceholdersFrom(formatter, 1, rowCount, columnCount)
}

// MultiRowPlaceholdersFrom returns comma-separated placeholder groups for a
// multi-row INSERT using the supplied 1-based first parameter index.
func MultiRowPlaceholdersFrom(formatter PlaceholderFormatter, firstIndex, rowCount, columnCount int) (string, error) {
	if firstIndex < 1 {
		return "", fmt.Errorf("multi-row placeholders: first index must be positive")
	}
	if rowCount < 0 {
		return "", fmt.Errorf("multi-row placeholders: row count must not be negative")
	}
	if columnCount < 0 {
		return "", fmt.Errorf("multi-row placeholders: column count must not be negative")
	}
	if rowCount == 0 || columnCount == 0 {
		return "", nil
	}
	if formatter == nil {
		return "", fmt.Errorf("multi-row placeholders: formatter is nil")
	}

	nextIndex := firstIndex
	rows := make([]string, rowCount)
	for row := range rows {
		placeholders := make([]string, columnCount)
		for i := range placeholders {
			placeholders[i] = formatter.ParameterPlaceholder(nextIndex)
			nextIndex++
		}
		rows[row] = "(" + strings.Join(placeholders, ", ") + ")"
	}

	return strings.Join(rows, ", "), nil
}

// KeyEqualityPredicate returns an equality predicate for one key tuple and the
// next 1-based parameter index.
func KeyEqualityPredicate(
	quoter IdentifierQuoter,
	formatter PlaceholderFormatter,
	keyColumns []string,
	firstIndex int,
) (string, int, error) {
	if quoter == nil {
		return "", firstIndex, fmt.Errorf("key equality predicate: quoter is nil")
	}
	if formatter == nil {
		return "", firstIndex, fmt.Errorf("key equality predicate: formatter is nil")
	}
	if firstIndex < 1 {
		return "", firstIndex, fmt.Errorf("key equality predicate: first index must be positive")
	}
	if len(keyColumns) == 0 {
		return "", firstIndex, fmt.Errorf("key equality predicate: key columns are required")
	}

	nextIndex := firstIndex
	parts := make([]string, len(keyColumns))
	for i, column := range keyColumns {
		parts[i] = fmt.Sprintf("%s = %s", quoter.QuoteIdentifier(column), formatter.ParameterPlaceholder(nextIndex))
		nextIndex++
	}
	return strings.Join(parts, " AND "), nextIndex, nil
}

// KeyBatchPredicate returns an OR-of-ANDs predicate for a batch of key tuples
// and the next 1-based parameter index.
func KeyBatchPredicate(
	quoter IdentifierQuoter,
	formatter PlaceholderFormatter,
	keyColumns []string,
	rowCount, firstIndex int,
) (string, int, error) {
	if rowCount <= 0 {
		return "", firstIndex, fmt.Errorf("key batch predicate: row count must be positive")
	}

	nextIndex := firstIndex
	rows := make([]string, rowCount)
	for row := range rows {
		predicate, next, err := KeyEqualityPredicate(quoter, formatter, keyColumns, nextIndex)
		if err != nil {
			return "", firstIndex, err
		}
		if len(keyColumns) > 1 {
			rows[row] = "(" + predicate + ")"
		} else {
			rows[row] = predicate
		}
		nextIndex = next
	}
	return "(" + strings.Join(rows, " OR ") + ")", nextIndex, nil
}

// DeleteByKeyBatch returns a DELETE statement for a bounded batch of key tuples
// and the next 1-based parameter index.
func DeleteByKeyBatch(
	qualifier TableQualifier,
	quoter IdentifierQuoter,
	formatter PlaceholderFormatter,
	schema, table string,
	keyColumns []string,
	rowCount, firstIndex int,
) (string, int, error) {
	if qualifier == nil {
		return "", firstIndex, fmt.Errorf("delete by key batch: qualifier is nil")
	}
	if table == "" {
		return "", firstIndex, fmt.Errorf("delete by key batch: table is required")
	}

	predicate, nextIndex, err := KeyBatchPredicate(quoter, formatter, keyColumns, rowCount, firstIndex)
	if err != nil {
		return "", firstIndex, fmt.Errorf("delete by key batch: %w", err)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", qualifier.QualifyTable(schema, table), predicate), nextIndex, nil
}

// FlattenRows validates and flattens row-major values into a single argument
// slice suitable for database/sql Exec calls.
func FlattenRows(rows [][]any, columnCount int) ([]any, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if columnCount <= 0 {
		return nil, fmt.Errorf("flatten rows: column count must be positive")
	}

	args := make([]any, 0, len(rows)*columnCount)
	for i, row := range rows {
		if len(row) != columnCount {
			return nil, fmt.Errorf("flatten rows: row %d has %d values, want %d", i, len(row), columnCount)
		}
		args = append(args, row...)
	}
	return args, nil
}
