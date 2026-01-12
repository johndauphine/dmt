package driver

import (
	"fmt"
	"strings"
)

// TypeMapping defines a simple 1:1 type mapping between databases.
type TypeMapping struct {
	MSSQL    string // SQL Server type name
	Postgres string // PostgreSQL type name
}

// SizedTypeMapping defines type mappings that require length/precision handling.
type SizedTypeMapping struct {
	MSSQL             string // SQL Server type name
	Postgres          string // PostgreSQL type name
	MSSQLMax          string // SQL Server type for MAX length (e.g., "varchar(max)")
	PostgresMax       string // PostgreSQL type for unlimited length (e.g., "text")
	PreservePrecision bool   // Whether to preserve precision/scale
	PreserveLength    bool   // Whether to preserve length
}

// SimpleTypeMappings defines 1:1 type mappings between MSSQL and PostgreSQL.
// These types don't require length or precision handling.
var SimpleTypeMappings = []TypeMapping{
	// Boolean
	{MSSQL: "bit", Postgres: "boolean"},

	// Integer types
	{MSSQL: "tinyint", Postgres: "smallint"},
	{MSSQL: "smallint", Postgres: "smallint"},
	{MSSQL: "int", Postgres: "integer"},
	{MSSQL: "bigint", Postgres: "bigint"},

	// Floating point
	{MSSQL: "float", Postgres: "double precision"},
	{MSSQL: "real", Postgres: "real"},

	// Fixed precision money
	{MSSQL: "money", Postgres: "numeric(19,4)"},
	{MSSQL: "smallmoney", Postgres: "numeric(10,4)"},

	// Date/time types
	{MSSQL: "date", Postgres: "date"},
	{MSSQL: "time", Postgres: "time"},
	{MSSQL: "datetime", Postgres: "timestamp"},
	{MSSQL: "datetime2", Postgres: "timestamp"},
	{MSSQL: "smalldatetime", Postgres: "timestamp"},
	{MSSQL: "datetimeoffset", Postgres: "timestamptz"},

	// GUID
	{MSSQL: "uniqueidentifier", Postgres: "uuid"},

	// XML
	{MSSQL: "xml", Postgres: "xml"},

	// Legacy text types
	{MSSQL: "text", Postgres: "text"},
	{MSSQL: "ntext", Postgres: "text"},
	{MSSQL: "image", Postgres: "bytea"},

	// Other
	{MSSQL: "hierarchyid", Postgres: "text"},
	{MSSQL: "sql_variant", Postgres: "text"},
	{MSSQL: "geometry", Postgres: "text"},
	{MSSQL: "geography", Postgres: "text"},
}

// SizedTypeMappings defines type mappings that require length/precision handling.
var SizedTypeMappings = []SizedTypeMapping{
	// String types
	{MSSQL: "char", Postgres: "char", MSSQLMax: "", PostgresMax: "text", PreserveLength: true},
	{MSSQL: "nchar", Postgres: "char", MSSQLMax: "", PostgresMax: "text", PreserveLength: true},
	{MSSQL: "varchar", Postgres: "varchar", MSSQLMax: "varchar(max)", PostgresMax: "text", PreserveLength: true},
	{MSSQL: "nvarchar", Postgres: "varchar", MSSQLMax: "nvarchar(max)", PostgresMax: "text", PreserveLength: true},

	// Binary types
	{MSSQL: "binary", Postgres: "bytea", MSSQLMax: "", PostgresMax: "bytea", PreserveLength: false},
	{MSSQL: "varbinary", Postgres: "bytea", MSSQLMax: "varbinary(max)", PostgresMax: "bytea", PreserveLength: false},

	// Decimal types
	{MSSQL: "decimal", Postgres: "numeric", MSSQLMax: "", PostgresMax: "numeric", PreservePrecision: true},
	{MSSQL: "numeric", Postgres: "numeric", MSSQLMax: "", PostgresMax: "numeric", PreservePrecision: true},
}

// PostgresAliases maps PostgreSQL type aliases to their canonical forms.
var PostgresAliases = map[string]string{
	"bool":                       "boolean",
	"int2":                       "smallint",
	"int4":                       "integer",
	"int":                        "integer",
	"int8":                       "bigint",
	"float4":                     "real",
	"float8":                     "double precision",
	"bpchar":                     "char",
	"character":                  "char",
	"character varying":          "varchar",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":   "timestamptz",
	"time without time zone":     "time",
	"time with time zone":        "timetz",
}

// PostgresToMSSQLMappings defines PostgreSQL-specific types that map to MSSQL.
// These are types that don't have a direct MSSQL equivalent in SimpleTypeMappings.
var PostgresToMSSQLMappings = map[string]string{
	// Integer types with aliases
	"boolean":          "bit",
	"smallint":         "smallint",
	"integer":          "int",
	"bigint":           "bigint",
	"serial":           "int",
	"bigserial":        "bigint",
	"smallserial":      "smallint",

	// Floating point
	"real":             "real",
	"double precision": "float",

	// Money
	"money":            "money",

	// Date/time
	"date":             "date",
	"time":             "time",
	"timetz":           "time",
	"timestamp":        "datetime2",
	"timestamptz":      "datetimeoffset",
	"interval":         "nvarchar(100)",

	// Text
	"text":             "nvarchar(max)",
	"citext":           "nvarchar(max)",

	// Binary
	"bytea":            "varbinary(max)",

	// UUID
	"uuid":             "uniqueidentifier",

	// JSON
	"json":             "nvarchar(max)",
	"jsonb":            "nvarchar(max)",

	// XML
	"xml":              "xml",

	// Network types
	"inet":             "nvarchar(50)",
	"cidr":             "nvarchar(50)",
	"macaddr":          "nvarchar(50)",
	"macaddr8":         "nvarchar(50)",

	// Geometric types (PostgreSQL native, not PostGIS)
	"point":            "nvarchar(max)",
	"line":             "nvarchar(max)",
	"lseg":             "nvarchar(max)",
	"box":              "nvarchar(max)",
	"path":             "nvarchar(max)",
	"polygon":          "nvarchar(max)",
	"circle":           "nvarchar(max)",

	// Range types
	"int4range":        "nvarchar(100)",
	"int8range":        "nvarchar(100)",
	"numrange":         "nvarchar(100)",
	"tsrange":          "nvarchar(100)",
	"tstzrange":        "nvarchar(100)",
	"daterange":        "nvarchar(100)",

	// Full-text search
	"tsvector":         "nvarchar(max)",
	"tsquery":          "nvarchar(max)",

	// Bit string
	"bit varying":      "varbinary(max)",
	"varbit":           "varbinary(max)",

	// OID types
	"oid":              "int",
	"regproc":          "int",
	"regprocedure":     "int",
	"regoper":          "int",
	"regoperator":      "int",
	"regclass":         "int",
	"regtype":          "int",
	"regconfig":        "int",
	"regdictionary":    "int",
}

// PostgreSQL maximum varchar length before converting to text
const pgMaxVarcharLength = 10485760

// MSSQL maximum varchar/nvarchar length before using MAX
const mssqlMaxVarcharLength = 8000

// LookupMSSQLToPostgres looks up the PostgreSQL equivalent of an MSSQL type.
func LookupMSSQLToPostgres(mssqlType string, maxLength, precision, scale int) string {
	mssqlType = strings.ToLower(mssqlType)

	// Check simple mappings first
	for _, m := range SimpleTypeMappings {
		if strings.EqualFold(m.MSSQL, mssqlType) {
			return m.Postgres
		}
	}

	// Check sized mappings
	for _, m := range SizedTypeMappings {
		if strings.EqualFold(m.MSSQL, mssqlType) {
			return applySizedMappingToPostgres(m, maxLength, precision, scale)
		}
	}

	// Default fallback
	return "text"
}

// LookupPostgresToMSSQL looks up the MSSQL equivalent of a PostgreSQL type.
func LookupPostgresToMSSQL(pgType string, maxLength, precision, scale int) string {
	pgType = strings.ToLower(pgType)

	// Resolve aliases first
	if canonical, ok := PostgresAliases[pgType]; ok {
		pgType = canonical
	}

	// Check direct mappings
	if mssqlType, ok := PostgresToMSSQLMappings[pgType]; ok {
		return mssqlType
	}

	// Handle varchar type with length - use nvarchar for Unicode support
	// This must come before SizedTypeMappings check
	if pgType == "varchar" {
		if maxLength == -1 || maxLength > mssqlMaxVarcharLength {
			return "nvarchar(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("nvarchar(%d)", maxLength)
		}
		return "nvarchar(max)"
	}

	// Handle char type with length
	if pgType == "char" {
		if maxLength > 0 && maxLength <= mssqlMaxVarcharLength {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	}

	// Handle decimal/numeric with precision
	if pgType == "numeric" || pgType == "decimal" {
		if precision > 0 {
			return fmt.Sprintf("decimal(%d,%d)", precision, scale)
		}
		return "decimal"
	}

	// Handle bit type
	if pgType == "bit" {
		if maxLength > 0 && maxLength <= 64 {
			return fmt.Sprintf("binary(%d)", (maxLength+7)/8)
		}
		return "varbinary(max)"
	}

	// Handle array types
	if strings.HasSuffix(pgType, "[]") || strings.HasPrefix(pgType, "_") {
		return "nvarchar(max)"
	}

	// Default fallback
	return "nvarchar(max)"
}

// NormalizePostgresType returns the canonical PostgreSQL type representation.
func NormalizePostgresType(pgType string, maxLength, precision, scale int) string {
	pgType = strings.ToLower(pgType)

	// Resolve aliases
	if canonical, ok := PostgresAliases[pgType]; ok {
		pgType = canonical
	}

	// Handle char with length
	if pgType == "char" {
		if maxLength > 0 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	}

	// Handle varchar with length
	if pgType == "varchar" {
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	}

	// Handle numeric with precision
	if pgType == "numeric" || pgType == "decimal" {
		if precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", precision, scale)
		}
		return "numeric"
	}

	return pgType
}

// NormalizeMSSQLType returns the canonical SQL Server type representation.
func NormalizeMSSQLType(mssqlType string, maxLength, precision, scale int) string {
	mssqlType = strings.ToLower(mssqlType)

	switch mssqlType {
	case "varchar":
		if maxLength == -1 {
			return "varchar(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "varchar(max)"
	case "nvarchar":
		if maxLength == -1 {
			return "nvarchar(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("nvarchar(%d)", maxLength)
		}
		return "nvarchar(max)"
	case "char":
		if maxLength > 0 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	case "nchar":
		if maxLength > 0 {
			return fmt.Sprintf("nchar(%d)", maxLength)
		}
		return "nchar(1)"
	case "varbinary":
		if maxLength == -1 {
			return "varbinary(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("varbinary(%d)", maxLength)
		}
		return "varbinary(max)"
	case "binary":
		if maxLength > 0 {
			return fmt.Sprintf("binary(%d)", maxLength)
		}
		return "binary(1)"
	case "decimal", "numeric":
		if precision > 0 {
			return fmt.Sprintf("decimal(%d,%d)", precision, scale)
		}
		return "decimal"
	default:
		return mssqlType
	}
}

// applySizedMappingToPostgres applies a sized type mapping to get a PostgreSQL type.
func applySizedMappingToPostgres(m SizedTypeMapping, maxLength, precision, scale int) string {
	// Handle MAX length (-1 in MSSQL)
	if maxLength == -1 {
		return m.PostgresMax
	}

	// Handle precision for decimal types
	if m.PreservePrecision && precision > 0 {
		return fmt.Sprintf("%s(%d,%d)", m.Postgres, precision, scale)
	}

	// Handle length for string types
	if m.PreserveLength && maxLength > 0 {
		if maxLength > pgMaxVarcharLength {
			return m.PostgresMax
		}
		return fmt.Sprintf("%s(%d)", m.Postgres, maxLength)
	}

	// Default to max type
	return m.PostgresMax
}

// applySizedMappingToMSSQL applies a sized type mapping to get an MSSQL type.
func applySizedMappingToMSSQL(m SizedTypeMapping, maxLength, precision, scale int) string {
	// Handle precision for decimal types
	if m.PreservePrecision && precision > 0 {
		return fmt.Sprintf("%s(%d,%d)", m.MSSQL, precision, scale)
	}

	// Handle length for string types
	if m.PreserveLength && maxLength > 0 {
		if maxLength > mssqlMaxVarcharLength {
			return m.MSSQLMax
		}
		// Use nvarchar instead of varchar for Unicode support
		if m.MSSQL == "varchar" {
			return fmt.Sprintf("nvarchar(%d)", maxLength)
		}
		if m.MSSQL == "char" {
			return fmt.Sprintf("nchar(%d)", maxLength)
		}
		return fmt.Sprintf("%s(%d)", m.MSSQL, maxLength)
	}

	// Default to max type
	if m.MSSQLMax != "" {
		return m.MSSQLMax
	}
	return m.MSSQL
}
