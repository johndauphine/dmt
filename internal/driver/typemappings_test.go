package driver

import (
	"testing"
)

func TestLookupMSSQLToPostgres(t *testing.T) {
	tests := []struct {
		name      string
		mssqlType string
		maxLength int
		precision int
		scale     int
		expected  string
	}{
		// Simple types
		{"bit to boolean", "bit", 0, 0, 0, "boolean"},
		{"tinyint to smallint", "tinyint", 0, 0, 0, "smallint"},
		{"smallint to smallint", "smallint", 0, 0, 0, "smallint"},
		{"int to integer", "int", 0, 0, 0, "integer"},
		{"bigint to bigint", "bigint", 0, 0, 0, "bigint"},
		{"float to double precision", "float", 0, 0, 0, "double precision"},
		{"real to real", "real", 0, 0, 0, "real"},
		{"money to numeric", "money", 0, 0, 0, "numeric(19,4)"},
		{"smallmoney to numeric", "smallmoney", 0, 0, 0, "numeric(10,4)"},
		{"date to date", "date", 0, 0, 0, "date"},
		{"time to time", "time", 0, 0, 0, "time"},
		{"datetime to timestamp", "datetime", 0, 0, 0, "timestamp"},
		{"datetime2 to timestamp", "datetime2", 0, 0, 0, "timestamp"},
		{"datetimeoffset to timestamptz", "datetimeoffset", 0, 0, 0, "timestamptz"},
		{"uniqueidentifier to uuid", "uniqueidentifier", 0, 0, 0, "uuid"},
		{"xml to xml", "xml", 0, 0, 0, "xml"},
		{"text to text", "text", 0, 0, 0, "text"},
		{"ntext to text", "ntext", 0, 0, 0, "text"},
		{"image to bytea", "image", 0, 0, 0, "bytea"},

		// Sized types - varchar
		{"varchar(50) to varchar(50)", "varchar", 50, 0, 0, "varchar(50)"},
		{"varchar(max) to text", "varchar", -1, 0, 0, "text"},
		{"nvarchar(100) to varchar(100)", "nvarchar", 100, 0, 0, "varchar(100)"},
		{"nvarchar(max) to text", "nvarchar", -1, 0, 0, "text"},

		// Sized types - char
		{"char(10) to char(10)", "char", 10, 0, 0, "char(10)"},
		{"nchar(20) to char(20)", "nchar", 20, 0, 0, "char(20)"},

		// Sized types - decimal
		{"decimal(10,2) to numeric(10,2)", "decimal", 0, 10, 2, "numeric(10,2)"},
		{"numeric(18,4) to numeric(18,4)", "numeric", 0, 18, 4, "numeric(18,4)"},
		{"decimal (no precision) to numeric", "decimal", 0, 0, 0, "numeric"},

		// Binary types
		{"binary to bytea", "binary", 10, 0, 0, "bytea"},
		{"varbinary to bytea", "varbinary", 100, 0, 0, "bytea"},
		{"varbinary(max) to bytea", "varbinary", -1, 0, 0, "bytea"},

		// Spatial types
		{"geography to text", "geography", 0, 0, 0, "text"},
		{"geometry to text", "geometry", 0, 0, 0, "text"},

		// Unknown type fallback
		{"unknown to text", "unknowntype", 0, 0, 0, "text"},

		// Case insensitivity
		{"BIT to boolean", "BIT", 0, 0, 0, "boolean"},
		{"VARCHAR to varchar", "VARCHAR", 50, 0, 0, "varchar(50)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LookupMSSQLToPostgres(tt.mssqlType, tt.maxLength, tt.precision, tt.scale)
			if result != tt.expected {
				t.Errorf("LookupMSSQLToPostgres(%q, %d, %d, %d) = %q, want %q",
					tt.mssqlType, tt.maxLength, tt.precision, tt.scale, result, tt.expected)
			}
		})
	}
}

func TestLookupPostgresToMSSQL(t *testing.T) {
	tests := []struct {
		name      string
		pgType    string
		maxLength int
		precision int
		scale     int
		expected  string
	}{
		// Simple types
		{"boolean to bit", "boolean", 0, 0, 0, "bit"},
		{"bool to bit", "bool", 0, 0, 0, "bit"},
		{"smallint to smallint", "smallint", 0, 0, 0, "smallint"},
		{"int2 to smallint", "int2", 0, 0, 0, "smallint"},
		{"integer to int", "integer", 0, 0, 0, "int"},
		{"int4 to int", "int4", 0, 0, 0, "int"},
		{"bigint to bigint", "bigint", 0, 0, 0, "bigint"},
		{"int8 to bigint", "int8", 0, 0, 0, "bigint"},
		{"serial to int", "serial", 0, 0, 0, "int"},
		{"bigserial to bigint", "bigserial", 0, 0, 0, "bigint"},
		{"real to real", "real", 0, 0, 0, "real"},
		{"float4 to real", "float4", 0, 0, 0, "real"},
		{"double precision to float", "double precision", 0, 0, 0, "float"},
		{"float8 to float", "float8", 0, 0, 0, "float"},
		{"date to date", "date", 0, 0, 0, "date"},
		{"time to time", "time", 0, 0, 0, "time"},
		{"timestamp to datetime2", "timestamp", 0, 0, 0, "datetime2"},
		{"timestamptz to datetimeoffset", "timestamptz", 0, 0, 0, "datetimeoffset"},
		{"uuid to uniqueidentifier", "uuid", 0, 0, 0, "uniqueidentifier"},
		{"xml to xml", "xml", 0, 0, 0, "xml"},
		{"text to nvarchar(max)", "text", 0, 0, 0, "nvarchar(max)"},
		{"bytea to varbinary(max)", "bytea", 0, 0, 0, "varbinary(max)"},
		{"json to nvarchar(max)", "json", 0, 0, 0, "nvarchar(max)"},
		{"jsonb to nvarchar(max)", "jsonb", 0, 0, 0, "nvarchar(max)"},

		// Sized types - varchar
		{"varchar(50) to nvarchar(50)", "varchar", 50, 0, 0, "nvarchar(50)"},
		{"varchar(max) to nvarchar(max)", "varchar", -1, 0, 0, "nvarchar(max)"},
		{"varchar(9000) to nvarchar(max)", "varchar", 9000, 0, 0, "nvarchar(max)"},

		// Sized types - char
		{"char(10) to char(10)", "char", 10, 0, 0, "char(10)"},
		{"char(1) default", "char", 0, 0, 0, "char(1)"},

		// Sized types - decimal
		{"numeric(10,2) to decimal(10,2)", "numeric", 0, 10, 2, "decimal(10,2)"},
		{"decimal(18,4) to decimal(18,4)", "decimal", 0, 18, 4, "decimal(18,4)"},

		// Network types
		{"inet to nvarchar(50)", "inet", 0, 0, 0, "nvarchar(50)"},
		{"cidr to nvarchar(50)", "cidr", 0, 0, 0, "nvarchar(50)"},
		{"macaddr to nvarchar(50)", "macaddr", 0, 0, 0, "nvarchar(50)"},

		// Range types
		{"int4range to nvarchar(100)", "int4range", 0, 0, 0, "nvarchar(100)"},
		{"daterange to nvarchar(100)", "daterange", 0, 0, 0, "nvarchar(100)"},

		// Array types
		{"integer[] to nvarchar(max)", "integer[]", 0, 0, 0, "nvarchar(max)"},
		{"text[] to nvarchar(max)", "text[]", 0, 0, 0, "nvarchar(max)"},

		// Bit types
		{"bit(8) to binary(1)", "bit", 8, 0, 0, "binary(1)"},
		{"bit(64) to binary(8)", "bit", 64, 0, 0, "binary(8)"},
		{"bit varying to varbinary(max)", "bit varying", 0, 0, 0, "varbinary(max)"},

		// Unknown type fallback
		{"unknown to nvarchar(max)", "unknowntype", 0, 0, 0, "nvarchar(max)"},

		// Case insensitivity
		{"BOOLEAN to bit", "BOOLEAN", 0, 0, 0, "bit"},
		{"VARCHAR to nvarchar", "VARCHAR", 50, 0, 0, "nvarchar(50)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LookupPostgresToMSSQL(tt.pgType, tt.maxLength, tt.precision, tt.scale)
			if result != tt.expected {
				t.Errorf("LookupPostgresToMSSQL(%q, %d, %d, %d) = %q, want %q",
					tt.pgType, tt.maxLength, tt.precision, tt.scale, result, tt.expected)
			}
		})
	}
}

func TestNormalizePostgresType(t *testing.T) {
	tests := []struct {
		name      string
		pgType    string
		maxLength int
		precision int
		scale     int
		expected  string
	}{
		// Alias resolution
		{"int2 to smallint", "int2", 0, 0, 0, "smallint"},
		{"int4 to integer", "int4", 0, 0, 0, "integer"},
		{"int8 to bigint", "int8", 0, 0, 0, "bigint"},
		{"float4 to real", "float4", 0, 0, 0, "real"},
		{"float8 to double precision", "float8", 0, 0, 0, "double precision"},
		{"bool to boolean", "bool", 0, 0, 0, "boolean"},
		{"bpchar to char", "bpchar", 10, 0, 0, "char(10)"},

		// Varchar handling
		{"varchar(100)", "varchar", 100, 0, 0, "varchar(100)"},
		{"varchar too long to text", "varchar", 20000000, 0, 0, "text"},

		// Char handling
		{"char(10)", "char", 10, 0, 0, "char(10)"},
		{"char default", "char", 0, 0, 0, "char(1)"},

		// Numeric handling
		{"numeric(10,2)", "numeric", 0, 10, 2, "numeric(10,2)"},
		{"numeric no precision", "numeric", 0, 0, 0, "numeric"},

		// Passthrough
		{"text passthrough", "text", 0, 0, 0, "text"},
		{"uuid passthrough", "uuid", 0, 0, 0, "uuid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizePostgresType(tt.pgType, tt.maxLength, tt.precision, tt.scale)
			if result != tt.expected {
				t.Errorf("NormalizePostgresType(%q, %d, %d, %d) = %q, want %q",
					tt.pgType, tt.maxLength, tt.precision, tt.scale, result, tt.expected)
			}
		})
	}
}

func TestNormalizeMSSQLType(t *testing.T) {
	tests := []struct {
		name      string
		mssqlType string
		maxLength int
		precision int
		scale     int
		expected  string
	}{
		// Varchar handling
		{"varchar(50)", "varchar", 50, 0, 0, "varchar(50)"},
		{"varchar(max)", "varchar", -1, 0, 0, "varchar(max)"},
		{"varchar default", "varchar", 0, 0, 0, "varchar(max)"},

		// Nvarchar handling
		{"nvarchar(100)", "nvarchar", 100, 0, 0, "nvarchar(100)"},
		{"nvarchar(max)", "nvarchar", -1, 0, 0, "nvarchar(max)"},

		// Char handling
		{"char(10)", "char", 10, 0, 0, "char(10)"},
		{"char default", "char", 0, 0, 0, "char(1)"},

		// Nchar handling
		{"nchar(20)", "nchar", 20, 0, 0, "nchar(20)"},
		{"nchar default", "nchar", 0, 0, 0, "nchar(1)"},

		// Varbinary handling
		{"varbinary(100)", "varbinary", 100, 0, 0, "varbinary(100)"},
		{"varbinary(max)", "varbinary", -1, 0, 0, "varbinary(max)"},

		// Binary handling
		{"binary(10)", "binary", 10, 0, 0, "binary(10)"},
		{"binary default", "binary", 0, 0, 0, "binary(1)"},

		// Decimal handling
		{"decimal(10,2)", "decimal", 0, 10, 2, "decimal(10,2)"},
		{"decimal no precision", "decimal", 0, 0, 0, "decimal"},

		// Passthrough
		{"int passthrough", "int", 0, 0, 0, "int"},
		{"bigint passthrough", "bigint", 0, 0, 0, "bigint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeMSSQLType(tt.mssqlType, tt.maxLength, tt.precision, tt.scale)
			if result != tt.expected {
				t.Errorf("NormalizeMSSQLType(%q, %d, %d, %d) = %q, want %q",
					tt.mssqlType, tt.maxLength, tt.precision, tt.scale, result, tt.expected)
			}
		})
	}
}

// TestBidirectionalMapping verifies that mappings work in both directions consistently.
func TestBidirectionalMapping(t *testing.T) {
	// For simple types, verify round-trip consistency
	bidirectionalTests := []struct {
		mssqlType string
		pgType    string
	}{
		{"bit", "boolean"},
		{"smallint", "smallint"},
		{"int", "integer"},
		{"bigint", "bigint"},
		{"real", "real"},
		{"date", "date"},
		{"time", "time"},
		{"uniqueidentifier", "uuid"},
		{"xml", "xml"},
	}

	for _, tt := range bidirectionalTests {
		t.Run(tt.mssqlType+"<->"+tt.pgType, func(t *testing.T) {
			// MSSQL -> Postgres
			pgResult := LookupMSSQLToPostgres(tt.mssqlType, 0, 0, 0)
			if pgResult != tt.pgType {
				t.Errorf("MSSQL->PG: %q -> %q, want %q", tt.mssqlType, pgResult, tt.pgType)
			}

			// Postgres -> MSSQL
			mssqlResult := LookupPostgresToMSSQL(tt.pgType, 0, 0, 0)
			if mssqlResult != tt.mssqlType {
				t.Errorf("PG->MSSQL: %q -> %q, want %q", tt.pgType, mssqlResult, tt.mssqlType)
			}
		})
	}
}
