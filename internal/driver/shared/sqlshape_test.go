package shared

import (
	"fmt"
	"reflect"
	"testing"
)

type bracketDialect struct{}

func (bracketDialect) QuoteIdentifier(name string) string {
	return "[" + name + "]"
}

func (bracketDialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

type questionDialect struct{}

func (questionDialect) ParameterPlaceholder(int) string {
	return "?"
}

type dollarDialect struct{}

func (dollarDialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

func TestQuotedColumnList(t *testing.T) {
	got := QuotedColumnList(bracketDialect{}, []string{"id", "user name", "created_at"})

	want := "[id], [user name], [created_at]"
	if got != want {
		t.Fatalf("QuotedColumnList() = %q, want %q", got, want)
	}
}

func TestQuotedColumnListAllowsEmptyColumns(t *testing.T) {
	if got := QuotedColumnList(bracketDialect{}, nil); got != "" {
		t.Fatalf("QuotedColumnList() = %q, want empty string", got)
	}
}

func TestMultiRowPlaceholders(t *testing.T) {
	got, err := MultiRowPlaceholdersFrom(bracketDialect{}, 3, 2, 3)
	if err != nil {
		t.Fatalf("MultiRowPlaceholdersFrom returned error: %v", err)
	}

	want := "(@p3, @p4, @p5), (@p6, @p7, @p8)"
	if got != want {
		t.Fatalf("MultiRowPlaceholdersFrom() = %q, want %q", got, want)
	}
}

func TestMultiRowPlaceholdersSupportsDollarNumbering(t *testing.T) {
	got, err := MultiRowPlaceholders(dollarDialect{}, 2, 2)
	if err != nil {
		t.Fatalf("MultiRowPlaceholders returned error: %v", err)
	}

	want := "($1, $2), ($3, $4)"
	if got != want {
		t.Fatalf("MultiRowPlaceholders() = %q, want %q", got, want)
	}
}

func TestMultiRowPlaceholdersSupportsRepeatedPlaceholders(t *testing.T) {
	got, err := MultiRowPlaceholders(questionDialect{}, 2, 2)
	if err != nil {
		t.Fatalf("MultiRowPlaceholders returned error: %v", err)
	}

	want := "(?, ?), (?, ?)"
	if got != want {
		t.Fatalf("MultiRowPlaceholders() = %q, want %q", got, want)
	}
}

func TestMultiRowPlaceholdersAllowsEmptyInput(t *testing.T) {
	tests := []struct {
		name        string
		rowCount    int
		columnCount int
	}{
		{name: "zero rows", rowCount: 0, columnCount: 1},
		{name: "zero columns", rowCount: 1, columnCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MultiRowPlaceholders(questionDialect{}, tt.rowCount, tt.columnCount)
			if err != nil {
				t.Fatalf("MultiRowPlaceholders returned error: %v", err)
			}
			if got != "" {
				t.Fatalf("MultiRowPlaceholders() = %q, want empty string", got)
			}
		})
	}
}

func TestMultiRowPlaceholdersRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name        string
		formatter   PlaceholderFormatter
		firstIndex  int
		rowCount    int
		columnCount int
	}{
		{name: "zero first index", formatter: questionDialect{}, firstIndex: 0, rowCount: 1, columnCount: 1},
		{name: "negative rows", formatter: questionDialect{}, firstIndex: 1, rowCount: -1, columnCount: 1},
		{name: "negative columns", formatter: questionDialect{}, firstIndex: 1, rowCount: 1, columnCount: -1},
		{name: "nil formatter with work", formatter: nil, firstIndex: 1, rowCount: 1, columnCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := MultiRowPlaceholdersFrom(tt.formatter, tt.firstIndex, tt.rowCount, tt.columnCount); err == nil {
				t.Fatal("MultiRowPlaceholdersFrom returned nil error")
			}
		})
	}
}

func TestFlattenRows(t *testing.T) {
	rows := [][]any{
		{1, "Ada"},
		{2, "Grace"},
	}

	got, err := FlattenRows(rows, 2)
	if err != nil {
		t.Fatalf("FlattenRows returned error: %v", err)
	}

	want := []any{1, "Ada", 2, "Grace"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FlattenRows() = %#v, want %#v", got, want)
	}
}

func TestFlattenRowsAllowsNoRows(t *testing.T) {
	got, err := FlattenRows(nil, 0)
	if err != nil {
		t.Fatalf("FlattenRows returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("FlattenRows() = %#v, want nil", got)
	}
}

func TestFlattenRowsRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name        string
		rows        [][]any
		columnCount int
	}{
		{name: "zero columns", rows: [][]any{{1}}, columnCount: 0},
		{name: "short row", rows: [][]any{{1}, {2, "Grace"}}, columnCount: 2},
		{name: "long row", rows: [][]any{{1, "Ada", true}}, columnCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := FlattenRows(tt.rows, tt.columnCount); err == nil {
				t.Fatal("FlattenRows returned nil error")
			}
		})
	}
}
