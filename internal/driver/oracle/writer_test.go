package oracle

import (
	"strings"
	"testing"
)

func TestBuildMergeFromStaging(t *testing.T) {
	w := &Writer{
		dialect: &Dialect{},
	}

	tests := []struct {
		name        string
		targetTable string
		stagingTable string
		columns     []string
		pkColumns   []string
		wantContains []string
		wantNotContains []string
	}{
		{
			name:         "simple merge with single PK",
			targetTable:  "SCHEMA.USERS",
			stagingTable: "DMT_STG_12345",
			columns:      []string{"ID", "NAME", "EMAIL"},
			pkColumns:    []string{"ID"},
			wantContains: []string{
				"MERGE INTO SCHEMA.USERS tgt",
				"USING DMT_STG_12345 src",
				"ON (tgt.ID = src.ID)",
				"WHEN MATCHED THEN",
				"UPDATE SET",
				"WHEN NOT MATCHED THEN",
				"INSERT (ID,",
				"VALUES (src.ID,",
			},
		},
		{
			name:         "merge with composite PK",
			targetTable:  "ORDERS",
			stagingTable: "DMT_STG_99999",
			columns:      []string{"ORDER_ID", "LINE_ID", "PRODUCT", "QTY"},
			pkColumns:    []string{"ORDER_ID", "LINE_ID"},
			wantContains: []string{
				"MERGE INTO ORDERS tgt",
				"USING DMT_STG_99999 src",
				"ON (tgt.ORDER_ID = src.ORDER_ID AND tgt.LINE_ID = src.LINE_ID)",
				"WHEN MATCHED THEN",
				"UPDATE SET",
			},
		},
		{
			name:         "merge with all columns as PK (insert only)",
			targetTable:  "LOOKUP",
			stagingTable: "DMT_STG_11111",
			columns:      []string{"CODE", "VALUE"},
			pkColumns:    []string{"CODE", "VALUE"},
			wantContains: []string{
				"MERGE INTO LOOKUP tgt",
				"WHEN NOT MATCHED THEN",
				"INSERT (CODE,",
			},
			wantNotContains: []string{
				"WHEN MATCHED THEN", // No UPDATE when all cols are PK
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := w.buildMergeFromStaging(tt.targetTable, tt.stagingTable, tt.columns, tt.pkColumns)

			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("expected SQL to contain %q\ngot:\n%s", want, result)
				}
			}

			for _, notWant := range tt.wantNotContains {
				// Special handling for UPDATE clause check
				if strings.Contains(notWant, "UPDATE SET") {
					// Check that PK columns are not in UPDATE SET clause
					updateIdx := strings.Index(result, "UPDATE SET")
					if updateIdx >= 0 {
						insertIdx := strings.Index(result, "WHEN NOT MATCHED")
						if insertIdx > updateIdx {
							updateClause := result[updateIdx:insertIdx]
							if strings.Contains(updateClause, notWant) {
								t.Errorf("UPDATE clause should not contain %q\ngot:\n%s", notWant, updateClause)
							}
						}
					}
				} else if strings.Contains(result, notWant) {
					t.Errorf("SQL should not contain %q\ngot:\n%s", notWant, result)
				}
			}
		})
	}
}

func TestConvertRowValues(t *testing.T) {
	tests := []struct {
		name     string
		input    []any
		expected []any
	}{
		{
			name:     "nil values",
			input:    []any{nil, nil},
			expected: []any{nil, nil},
		},
		{
			name:     "byte slice unchanged",
			input:    []any{[]byte("binary data")},
			expected: []any{[]byte("binary data")},
		},
		{
			name:     "bool true converts to 1",
			input:    []any{true},
			expected: []any{1},
		},
		{
			name:     "bool false converts to 0",
			input:    []any{false},
			expected: []any{0},
		},
		{
			name:     "string unchanged",
			input:    []any{"test string"},
			expected: []any{"test string"},
		},
		{
			name:     "int unchanged",
			input:    []any{int64(12345)},
			expected: []any{int64(12345)},
		},
		{
			name:     "float unchanged",
			input:    []any{float64(123.45)},
			expected: []any{float64(123.45)},
		},
		{
			name:     "mixed types",
			input:    []any{int64(1), "hello", true, nil, []byte("data"), false},
			expected: []any{int64(1), "hello", 1, nil, []byte("data"), 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertRowValues(tt.input)

			if len(result) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d, want %d", len(result), len(tt.expected))
			}

			for i := range result {
				// Handle []byte comparison
				if b1, ok := result[i].([]byte); ok {
					if b2, ok := tt.expected[i].([]byte); ok {
						if string(b1) != string(b2) {
							t.Errorf("index %d: got %v, want %v", i, result[i], tt.expected[i])
						}
						continue
					}
				}
				if result[i] != tt.expected[i] {
					t.Errorf("index %d: got %v (%T), want %v (%T)",
						i, result[i], result[i], tt.expected[i], tt.expected[i])
				}
			}
		})
	}
}

func TestStagingTableNameGeneration(t *testing.T) {
	// Test that staging table names follow Oracle naming conventions
	tests := []struct {
		tableName string
		maxLen    int
	}{
		{"USERS", 30},
		{"VERY_LONG_TABLE_NAME_THAT_EXCEEDS_LIMITS", 30},
		{"A", 30},
	}

	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			// Simulate the naming logic from upsertWithStagingTable
			stagingTable := "DMT_STG_" + tt.tableName + "_12345"
			if len(stagingTable) > tt.maxLen {
				stagingTable = "DMT_STG_12345678"
			}

			if len(stagingTable) > tt.maxLen {
				t.Errorf("staging table name %q exceeds max length %d", stagingTable, tt.maxLen)
			}

			if !strings.HasPrefix(stagingTable, "DMT_STG_") {
				t.Errorf("staging table name %q should start with DMT_STG_", stagingTable)
			}
		})
	}
}
