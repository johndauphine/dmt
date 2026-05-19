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
