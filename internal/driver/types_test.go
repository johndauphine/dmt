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

func TestGoHeapBytesPerRow(t *testing.T) {
	tests := []struct {
		name    string
		columns []Column
		wantMin int64
		wantMax int64
	}{
		{
			name: "empty table",
			columns: nil,
			wantMin: 0,
			wantMax: 0,
		},
		{
			name: "single int column",
			columns: []Column{{Name: "id", DataType: "int"}},
			// slice header (24) + 1 iface slot (16) + int64 value (8) = 48
			wantMin: 40,
			wantMax: 56,
		},
		{
			name: "SO2013 Votes-like table (narrow rows)",
			columns: []Column{
				{Name: "Id", DataType: "int"},
				{Name: "PostId", DataType: "int"},
				{Name: "UserId", DataType: "int"},
				{Name: "BountyAmount", DataType: "int"},
				{Name: "VoteTypeId", DataType: "int"},
				{Name: "CreationDate", DataType: "datetime"},
			},
			// 6 columns: slice(24) + 6×iface(96) + 5×int(40) + 1×time(24) = 184
			wantMin: 150,
			wantMax: 250,
		},
		{
			name: "SO2013 Posts-like table (wide rows with text)",
			columns: []Column{
				{Name: "Id", DataType: "int"},
				{Name: "Body", DataType: "nvarchar", MaxLength: -1},       // MAX → 4096 (same as TEXT/NTEXT)
				{Name: "Title", DataType: "nvarchar", MaxLength: 250},
				{Name: "Tags", DataType: "nvarchar", MaxLength: 250},
				{Name: "OwnerUserId", DataType: "int"},
				{Name: "Score", DataType: "int"},
				{Name: "ViewCount", DataType: "int"},
				{Name: "CreationDate", DataType: "datetime"},
			},
			// Much larger than Votes due to nvarchar(MAX) Body column
			wantMin: 4400,
			wantMax: 5200,
		},
		{
			name: "all scalar types",
			columns: []Column{
				{Name: "a", DataType: "bigint"},
				{Name: "b", DataType: "float8"},
				{Name: "c", DataType: "bool"},
				{Name: "d", DataType: "uuid"},
				{Name: "e", DataType: "timestamptz"},
			},
			wantMin: 100,
			wantMax: 250,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := Table{Columns: tt.columns}
			got := table.GoHeapBytesPerRow()
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("GoHeapBytesPerRow() = %d, want [%d, %d]", got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestGoValueBytes(t *testing.T) {
	// Verify that text columns with known MaxLength produce predictable sizes
	col := Column{Name: "name", DataType: "varchar", MaxLength: 100}
	got := col.GoValueBytes()
	// string header (16) + 100 bytes of data = 116
	if got != 116 {
		t.Errorf("GoValueBytes() for varchar(100) = %d, want 116", got)
	}

	// Unbounded text defaults to 4096
	col2 := Column{Name: "body", DataType: "text"}
	got2 := col2.GoValueBytes()
	// string header (16) + 4096 default = 4112
	if got2 != 4112 {
		t.Errorf("GoValueBytes() for text = %d, want 4112", got2)
	}

	// Int column
	col3 := Column{Name: "id", DataType: "int"}
	got3 := col3.GoValueBytes()
	if got3 != 8 {
		t.Errorf("GoValueBytes() for int = %d, want 8", got3)
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
