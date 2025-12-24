package typemap

import "testing"

func TestGetDirection(t *testing.T) {
	tests := []struct {
		source   string
		target   string
		expected Direction
	}{
		{"mssql", "postgres", MSSQLToPG},
		{"postgres", "mssql", PGToMSSQL},
		{"postgres", "postgres", PGToPG},
		{"mssql", "mssql", MSSQLToMSSQL},
	}

	for _, tt := range tests {
		t.Run(tt.source+"->"+tt.target, func(t *testing.T) {
			got := GetDirection(tt.source, tt.target)
			if got != tt.expected {
				t.Errorf("GetDirection(%q, %q) = %v, want %v", tt.source, tt.target, got, tt.expected)
			}
		})
	}
}

func TestIsSameEngine(t *testing.T) {
	tests := []struct {
		source   string
		target   string
		expected bool
	}{
		{"mssql", "postgres", false},
		{"postgres", "mssql", false},
		{"postgres", "postgres", true},
		{"mssql", "mssql", true},
	}

	for _, tt := range tests {
		t.Run(tt.source+"->"+tt.target, func(t *testing.T) {
			got := IsSameEngine(tt.source, tt.target)
			if got != tt.expected {
				t.Errorf("IsSameEngine(%q, %q) = %v, want %v", tt.source, tt.target, got, tt.expected)
			}
		})
	}
}

func TestMapType(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		target    string
		dataType  string
		maxLen    int
		precision int
		scale     int
		expected  string
	}{
		// Cross-engine MSSQL -> PG
		{"mssql int to pg", "mssql", "postgres", "int", 0, 0, 0, "integer"},
		{"mssql nvarchar to pg", "mssql", "postgres", "nvarchar", 100, 0, 0, "varchar(100)"},
		{"mssql datetime to pg", "mssql", "postgres", "datetime", 0, 0, 0, "timestamp"},

		// Cross-engine PG -> MSSQL
		{"pg integer to mssql", "postgres", "mssql", "integer", 0, 0, 0, "int"},
		{"pg text to mssql", "postgres", "mssql", "text", 0, 0, 0, "nvarchar(max)"},
		{"pg timestamptz to mssql", "postgres", "mssql", "timestamptz", 0, 0, 0, "datetimeoffset"},

		// Same-engine PG -> PG (normalization)
		{"pg int4 normalized", "postgres", "postgres", "int4", 0, 0, 0, "integer"},
		{"pg int8 normalized", "postgres", "postgres", "int8", 0, 0, 0, "bigint"},
		{"pg bool normalized", "postgres", "postgres", "bool", 0, 0, 0, "boolean"},
		{"pg varchar passthrough", "postgres", "postgres", "varchar", 255, 0, 0, "varchar(255)"},
		{"pg text passthrough", "postgres", "postgres", "text", 0, 0, 0, "text"},
		{"pg numeric passthrough", "postgres", "postgres", "numeric", 0, 18, 2, "numeric(18,2)"},

		// Same-engine MSSQL -> MSSQL (normalization)
		{"mssql varchar with len", "mssql", "mssql", "varchar", 100, 0, 0, "varchar(100)"},
		{"mssql varchar max", "mssql", "mssql", "varchar", -1, 0, 0, "varchar(max)"},
		{"mssql nvarchar with len", "mssql", "mssql", "nvarchar", 50, 0, 0, "nvarchar(50)"},
		{"mssql decimal passthrough", "mssql", "mssql", "decimal", 0, 10, 2, "decimal(10,2)"},
		{"mssql int passthrough", "mssql", "mssql", "int", 0, 0, 0, "int"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapType(tt.source, tt.target, tt.dataType, tt.maxLen, tt.precision, tt.scale)
			if got != tt.expected {
				t.Errorf("MapType(%q, %q, %q, %d, %d, %d) = %q, want %q",
					tt.source, tt.target, tt.dataType, tt.maxLen, tt.precision, tt.scale, got, tt.expected)
			}
		})
	}
}

func TestNormalizePostgresType(t *testing.T) {
	tests := []struct {
		dataType  string
		maxLen    int
		precision int
		scale     int
		expected  string
	}{
		{"int2", 0, 0, 0, "smallint"},
		{"int4", 0, 0, 0, "integer"},
		{"int8", 0, 0, 0, "bigint"},
		{"float4", 0, 0, 0, "real"},
		{"float8", 0, 0, 0, "double precision"},
		{"bool", 0, 0, 0, "boolean"},
		{"bpchar", 10, 0, 0, "char(10)"},
		{"character varying", 100, 0, 0, "varchar(100)"},
		{"numeric", 0, 18, 4, "numeric(18,4)"},
		{"timestamp without time zone", 0, 0, 0, "timestamp"},
		{"timestamp with time zone", 0, 0, 0, "timestamptz"},
		{"uuid", 0, 0, 0, "uuid"},   // passthrough
		{"jsonb", 0, 0, 0, "jsonb"}, // passthrough
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := NormalizePostgresType(tt.dataType, tt.maxLen, tt.precision, tt.scale)
			if got != tt.expected {
				t.Errorf("NormalizePostgresType(%q, %d, %d, %d) = %q, want %q",
					tt.dataType, tt.maxLen, tt.precision, tt.scale, got, tt.expected)
			}
		})
	}
}

func TestNormalizeMSSQLType(t *testing.T) {
	tests := []struct {
		dataType  string
		maxLen    int
		precision int
		scale     int
		expected  string
	}{
		{"varchar", 100, 0, 0, "varchar(100)"},
		{"varchar", -1, 0, 0, "varchar(max)"},
		{"nvarchar", 50, 0, 0, "nvarchar(50)"},
		{"nvarchar", -1, 0, 0, "nvarchar(max)"},
		{"char", 10, 0, 0, "char(10)"},
		{"nchar", 5, 0, 0, "nchar(5)"},
		{"varbinary", 100, 0, 0, "varbinary(100)"},
		{"varbinary", -1, 0, 0, "varbinary(max)"},
		{"decimal", 0, 18, 2, "decimal(18,2)"},
		{"int", 0, 0, 0, "int"},             // passthrough
		{"bigint", 0, 0, 0, "bigint"},       // passthrough
		{"datetime2", 0, 0, 0, "datetime2"}, // passthrough
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := NormalizeMSSQLType(tt.dataType, tt.maxLen, tt.precision, tt.scale)
			if got != tt.expected {
				t.Errorf("NormalizeMSSQLType(%q, %d, %d, %d) = %q, want %q",
					tt.dataType, tt.maxLen, tt.precision, tt.scale, got, tt.expected)
			}
		})
	}
}
