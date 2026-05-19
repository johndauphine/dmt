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

func (bracketDialect) QualifyTable(schema, table string) string {
	if schema == "" {
		return "[" + table + "]"
	}
	return "[" + schema + "].[" + table + "]"
}

func (bracketDialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

type questionDialect struct{}

func (questionDialect) ParameterPlaceholder(int) string {
	return "?"
}

type dollarDialect struct{}

func (dollarDialect) QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

func (dollarDialect) QualifyTable(schema, table string) string {
	if schema == "" {
		return `"` + table + `"`
	}
	return `"` + schema + `"."` + table + `"`
}

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

func TestOrderedKeySelect(t *testing.T) {
	got, err := OrderedKeySelect(dollarDialect{}, dollarDialect{}, "public", "users", []string{"tenant_id", "id"})
	if err != nil {
		t.Fatalf("OrderedKeySelect returned error: %v", err)
	}

	want := `SELECT "tenant_id", "id" FROM "public"."users" ORDER BY "tenant_id", "id"`
	if got != want {
		t.Fatalf("OrderedKeySelect() = %q, want %q", got, want)
	}
}

func TestOrderedKeySelectRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name       string
		qualifier  TableQualifier
		quoter     IdentifierQuoter
		table      string
		keyColumns []string
	}{
		{name: "nil qualifier", qualifier: nil, quoter: bracketDialect{}, table: "users", keyColumns: []string{"id"}},
		{name: "nil quoter", qualifier: bracketDialect{}, quoter: nil, table: "users", keyColumns: []string{"id"}},
		{name: "empty table", qualifier: bracketDialect{}, quoter: bracketDialect{}, table: "", keyColumns: []string{"id"}},
		{name: "empty keys", qualifier: bracketDialect{}, quoter: bracketDialect{}, table: "users", keyColumns: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := OrderedKeySelect(tt.qualifier, tt.quoter, "dbo", tt.table, tt.keyColumns); err == nil {
				t.Fatal("OrderedKeySelect returned nil error")
			}
		})
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

func TestKeyEqualityPredicate(t *testing.T) {
	got, nextIndex, err := KeyEqualityPredicate(bracketDialect{}, bracketDialect{}, []string{"tenant_id", "id"}, 5)
	if err != nil {
		t.Fatalf("KeyEqualityPredicate returned error: %v", err)
	}

	want := "[tenant_id] = @p5 AND [id] = @p6"
	if got != want {
		t.Fatalf("KeyEqualityPredicate() = %q, want %q", got, want)
	}
	if nextIndex != 7 {
		t.Fatalf("next index = %d, want 7", nextIndex)
	}
}

func TestKeyBatchPredicate(t *testing.T) {
	got, nextIndex, err := KeyBatchPredicate(dollarDialect{}, dollarDialect{}, []string{"tenant_id", "id"}, 2, 1)
	if err != nil {
		t.Fatalf("KeyBatchPredicate returned error: %v", err)
	}

	want := `(("tenant_id" = $1 AND "id" = $2) OR ("tenant_id" = $3 AND "id" = $4))`
	if got != want {
		t.Fatalf("KeyBatchPredicate() = %q, want %q", got, want)
	}
	if nextIndex != 5 {
		t.Fatalf("next index = %d, want 5", nextIndex)
	}
}

func TestKeyBatchPredicateSupportsRepeatedPlaceholders(t *testing.T) {
	got, nextIndex, err := KeyBatchPredicate(bracketDialect{}, questionDialect{}, []string{"id"}, 3, 1)
	if err != nil {
		t.Fatalf("KeyBatchPredicate returned error: %v", err)
	}

	want := "([id] = ? OR [id] = ? OR [id] = ?)"
	if got != want {
		t.Fatalf("KeyBatchPredicate() = %q, want %q", got, want)
	}
	if nextIndex != 4 {
		t.Fatalf("next index = %d, want 4", nextIndex)
	}
}

func TestKeyBatchPredicateRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name       string
		quoter     IdentifierQuoter
		formatter  PlaceholderFormatter
		keyColumns []string
		rowCount   int
		firstIndex int
	}{
		{
			name: "nil quoter", quoter: nil, formatter: bracketDialect{},
			keyColumns: []string{"id"}, rowCount: 1, firstIndex: 1,
		},
		{
			name: "nil formatter", quoter: bracketDialect{}, formatter: nil,
			keyColumns: []string{"id"}, rowCount: 1, firstIndex: 1,
		},
		{
			name: "empty keys", quoter: bracketDialect{}, formatter: bracketDialect{},
			keyColumns: nil, rowCount: 1, firstIndex: 1,
		},
		{
			name: "zero rows", quoter: bracketDialect{}, formatter: bracketDialect{},
			keyColumns: []string{"id"}, rowCount: 0, firstIndex: 1,
		},
		{
			name: "zero first index", quoter: bracketDialect{}, formatter: bracketDialect{},
			keyColumns: []string{"id"}, rowCount: 1, firstIndex: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := KeyBatchPredicate(tt.quoter, tt.formatter, tt.keyColumns, tt.rowCount, tt.firstIndex); err == nil {
				t.Fatal("KeyBatchPredicate returned nil error")
			}
		})
	}
}

func TestDeleteByKeyBatch(t *testing.T) {
	got, nextIndex, err := DeleteByKeyBatch(
		bracketDialect{}, bracketDialect{}, bracketDialect{},
		"dbo", "Users", []string{"tenant_id", "id"}, 2, 3,
	)
	if err != nil {
		t.Fatalf("DeleteByKeyBatch returned error: %v", err)
	}

	want := "DELETE FROM [dbo].[Users] WHERE (([tenant_id] = @p3 AND [id] = @p4) OR ([tenant_id] = @p5 AND [id] = @p6))"
	if got != want {
		t.Fatalf("DeleteByKeyBatch() = %q, want %q", got, want)
	}
	if nextIndex != 7 {
		t.Fatalf("next index = %d, want 7", nextIndex)
	}
}

func TestDeleteByKeyBatchRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		qualifier TableQualifier
		table     string
		rowCount  int
	}{
		{name: "nil qualifier", qualifier: nil, table: "Users", rowCount: 1},
		{name: "empty table", qualifier: bracketDialect{}, table: "", rowCount: 1},
		{name: "zero rows", qualifier: bracketDialect{}, table: "Users", rowCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DeleteByKeyBatch(
				tt.qualifier, bracketDialect{}, bracketDialect{},
				"dbo", tt.table, []string{"id"}, tt.rowCount, 1,
			)
			if err == nil {
				t.Fatal("DeleteByKeyBatch returned nil error")
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
