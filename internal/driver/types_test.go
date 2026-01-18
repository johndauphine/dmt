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

		// Oracle NUMBER types
		{
			name: "Oracle NUMBER with scale 0 (integer)",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "NUMBER", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "Oracle number lowercase with scale 0",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "number", Scale: 0}},
			},
			expected: true,
		},
		{
			name: "Oracle NUMBER with scale > 0 (decimal)",
			table: Table{
				PKColumns: []Column{{Name: "id", DataType: "NUMBER", Scale: 2}},
			},
			expected: false, // Decimal types should not use keyset
		},
		{
			name: "Oracle NUMBER with precision and scale",
			table: Table{
				PKColumns: []Column{{Name: "price", DataType: "NUMBER", Precision: 10, Scale: 4}},
			},
			expected: false, // Has decimal places
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

func TestSupportsKeysetPagination_OracleSpecific(t *testing.T) {
	// Detailed Oracle NUMBER tests
	tests := []struct {
		name      string
		dataType  string
		precision int
		scale     int
		expected  bool
	}{
		{"NUMBER(10,0) - integer", "NUMBER", 10, 0, true},
		{"NUMBER(19,0) - big integer", "NUMBER", 19, 0, true},
		{"NUMBER(5,0) - small integer", "NUMBER", 5, 0, true},
		{"NUMBER(10,2) - decimal", "NUMBER", 10, 2, false},
		{"NUMBER(15,4) - high precision decimal", "NUMBER", 15, 4, false},
		{"NUMBER - no precision/scale", "NUMBER", 0, 0, true}, // Default is integer
		{"number lowercase", "number", 10, 0, true},
		{"Number mixed case", "Number", 10, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := Table{
				PKColumns: []Column{{
					Name:      "id",
					DataType:  tt.dataType,
					Precision: tt.precision,
					Scale:     tt.scale,
				}},
			}

			result := table.SupportsKeysetPagination()
			if result != tt.expected {
				t.Errorf("SupportsKeysetPagination() for %s = %v, want %v",
					tt.name, result, tt.expected)
			}
		})
	}
}

func TestGetPKColumn(t *testing.T) {
	tests := []struct {
		name      string
		table     Table
		wantNil   bool
		wantName  string
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
		{"INT", false},   // Uppercase - function is case-sensitive
		{"NUMBER", false}, // Oracle NUMBER not in IsIntegerType (handled separately)
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
