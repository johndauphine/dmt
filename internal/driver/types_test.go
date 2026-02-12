package driver

import (
	"testing"
)

func TestSupportsKeysetPagination(t *testing.T) {
	tests := []struct {
		name     string
		table    Table
		expected bool
	}{
		// SQL Server types
		{
			name: "MSSQL int PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "int", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "MSSQL bigint PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "bigint", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "MSSQL smallint PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "smallint", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "MSSQL tinyint PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "tinyint", Scale: 0}},
			},
			expected: true,
		},

		// PostgreSQL types
		{
			name: "PostgreSQL integer PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "integer", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "PostgreSQL serial PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "serial", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "PostgreSQL bigserial PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "bigserial", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "PostgreSQL int4 PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "int4", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "PostgreSQL int8 PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "int8", Scale: 0}},
			},
			expected: true,
		},

		// Non-integer types (should return false)
		{
			name: "VARCHAR PK",
			table: Table{
				PKColumns: []Column{{Name: "code", DataType: "varchar", Scale: 0}},
			},
			expected: false,
		},
		{
			name: "UUID PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "uuid", Scale: 0}},
			},
			expected: false,
		},
		{
			name: "DECIMAL PK",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "decimal", Scale: 0}},
			},
			expected: false,
		},

		// Composite PKs (should return false)
		{
			name: "composite PK with two int columns",
			table: Table{
				PKColumns: []Column{
					{Name: "id1", DataType: "int", Scale: 0},
					{Name: "id2", DataType: "int", Scale: 0},
				},
			},
			expected: false, // Only single-column PKs supported
		},

		// No PK
		{
			name: "no PK columns",
			table: Table{
				PKColumns: []Column{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table.SupportsKeysetPagination()
			if result != tt.expected {
				t.Errorf("SupportsKeysetPagination() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetPKColumn(t *testing.T) {
	tests := []struct {
		name     string
		table    Table
		wantNil  bool
		wantName string
	}{
		{
			name: "single PK column",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "int"}},
			},
			wantNil:  false,
			wantName: "id",
		},
		{
			name: "no PK columns",
			table: Table{
				PKColumns: []Column{},
			},
			wantNil: true,
		},
		{
			name: "multiple PK columns",
			table: Table{
				PKColumns: []Column{
					{Name: "id1", DataType: "int"},
					{Name: "id2", DataType: "int"},
				},
			},
			wantNil: true, // Only returns for single-column PK
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table.GetPKColumn()

			if tt.wantNil {
				if result != nil {
					t.Errorf("GetPKColumn() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Error("GetPKColumn() = nil, want non-nil")
				} else if result.Name != tt.wantName {
					t.Errorf("GetPKColumn().Name = %s, want %s", result.Name, tt.wantName)
				}
			}
		})
	}
}

func TestIsIntegerType(t *testing.T) {
	tests := []struct {
		dataType string
		expected bool
	}{
		// SQL Server (lowercase as stored in metadata)
		{"int", true},
		{"bigint", true},
		{"smallint", true},
		{"tinyint", true},

		// PostgreSQL
		{"integer", true},
		{"serial", true},
		{"bigserial", true},
		{"smallserial", true},
		{"int4", true},
		{"int8", true},
		{"int2", true},

		// Non-integers
		{"varchar", false},
		{"text", false},
		{"decimal", false},
		{"numeric", false},
		{"float", false},
		{"double", false},
		{"uuid", false},
		{"timestamp", false},
		{"INT", false}, // Uppercase - function is case-sensitive
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			col := Column{DataType: tt.dataType}
			result := col.IsIntegerType()
			if result != tt.expected {
				t.Errorf("IsIntegerType() for %s = %v, want %v", tt.dataType, result, tt.expected)
			}
		})
	}
}
