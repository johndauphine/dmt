package typemap

import (
	"fmt"
	"strings"
)

// Direction represents the migration direction
type Direction int

const (
	MSSQLToPG Direction = iota
	PGToMSSQL
	PGToPG
	MSSQLToMSSQL
)

// GetDirection returns the migration direction based on source and target types
func GetDirection(sourceType, targetType string) Direction {
	if sourceType == "postgres" && targetType == "mssql" {
		return PGToMSSQL
	}
	if sourceType == "postgres" && targetType == "postgres" {
		return PGToPG
	}
	if sourceType == "mssql" && targetType == "mssql" {
		return MSSQLToMSSQL
	}
	return MSSQLToPG
}

// IsSameEngine returns true if source and target are the same database type
func IsSameEngine(sourceType, targetType string) bool {
	return sourceType == targetType
}

// MapType converts a source type to the appropriate target type based on direction
// For same-engine migrations, it normalizes but doesn't convert the type
func MapType(sourceType, targetType, dataType string, maxLength, precision, scale int) string {
	direction := GetDirection(sourceType, targetType)

	switch direction {
	case MSSQLToPG:
		return MSSQLToPostgres(dataType, maxLength, precision, scale)
	case PGToMSSQL:
		return PostgresToMSSQL(dataType, maxLength, precision, scale)
	case PGToPG:
		return NormalizePostgresType(dataType, maxLength, precision, scale)
	case MSSQLToMSSQL:
		return NormalizeMSSQLType(dataType, maxLength, precision, scale)
	default:
		return dataType
	}
}

// NormalizePostgresType returns the canonical PostgreSQL type representation
func NormalizePostgresType(pgType string, maxLength, precision, scale int) string {
	pgType = strings.ToLower(pgType)

	switch pgType {
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double precision"
	case "bool":
		return "boolean"
	case "bpchar":
		if maxLength > 0 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	case "character":
		if maxLength > 0 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	case "character varying":
		if maxLength > 0 && maxLength < 10485760 {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "varchar":
		if maxLength > 0 && maxLength < 10485760 {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "decimal", "numeric":
		if precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", precision, scale)
		}
		return "numeric"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	case "time without time zone":
		return "time"
	case "time with time zone":
		return "timetz"
	default:
		// For most types, return as-is (already canonical)
		return pgType
	}
}

// NormalizeMSSQLType returns the canonical SQL Server type representation
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
		// For most types, return as-is
		return mssqlType
	}
}

// MSSQLToPostgres converts MSSQL data types to PostgreSQL equivalents
func MSSQLToPostgres(mssqlType string, maxLength, precision, scale int) string {
	mssqlType = strings.ToLower(mssqlType)

	switch mssqlType {
	// Integer types
	case "bit":
		return "boolean"
	case "tinyint":
		return "smallint"
	case "smallint":
		return "smallint"
	case "int":
		return "integer"
	case "bigint":
		return "bigint"

	// Decimal/numeric types
	case "decimal", "numeric":
		if precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", precision, scale)
		}
		return "numeric"
	case "money":
		return "numeric(19,4)"
	case "smallmoney":
		return "numeric(10,4)"

	// Floating point types
	case "float":
		return "double precision"
	case "real":
		return "real"

	// String types
	case "char":
		if maxLength > 0 && maxLength <= 10485760 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "text"
	case "nchar":
		if maxLength > 0 && maxLength <= 10485760 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "text"
	case "varchar":
		if maxLength == -1 { // varchar(max)
			return "text"
		}
		if maxLength > 0 && maxLength <= 10485760 {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "nvarchar":
		if maxLength == -1 { // nvarchar(max)
			return "text"
		}
		if maxLength > 0 && maxLength <= 10485760 {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "text", "ntext":
		return "text"

	// Binary types
	case "binary":
		return "bytea"
	case "varbinary":
		return "bytea"
	case "image":
		return "bytea"

	// Date/time types
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetime":
		return "timestamp"
	case "datetime2":
		return "timestamp"
	case "smalldatetime":
		return "timestamp"
	case "datetimeoffset":
		return "timestamptz"

	// GUID
	case "uniqueidentifier":
		return "uuid"

	// XML
	case "xml":
		return "xml"

	// Spatial types (simplified)
	case "geometry":
		return "text" // Or use PostGIS: "geometry"
	case "geography":
		return "text" // Or use PostGIS: "geography"

	// Hierarchyid
	case "hierarchyid":
		return "text"

	// SQL Variant
	case "sql_variant":
		return "text"

	default:
		return "text"
	}
}

// PostgresToMSSQL converts PostgreSQL data types to SQL Server equivalents
func PostgresToMSSQL(pgType string, maxLength, precision, scale int) string {
	pgType = strings.ToLower(pgType)

	switch pgType {
	// Integer types
	case "boolean", "bool":
		return "bit"
	case "smallint", "int2":
		return "smallint"
	case "integer", "int", "int4":
		return "int"
	case "bigint", "int8":
		return "bigint"
	case "serial":
		return "int" // IDENTITY will be handled separately
	case "bigserial":
		return "bigint" // IDENTITY will be handled separately
	case "smallserial":
		return "smallint" // IDENTITY will be handled separately

	// Decimal/numeric types
	case "decimal", "numeric":
		if precision > 0 {
			return fmt.Sprintf("decimal(%d,%d)", precision, scale)
		}
		return "decimal"
	case "money":
		return "money"

	// Floating point types
	case "real", "float4":
		return "real"
	case "double precision", "float8":
		return "float"

	// String types
	case "char", "character", "bpchar":
		if maxLength > 0 && maxLength <= 8000 {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "char(1)"
	case "varchar", "character varying":
		if maxLength == -1 || maxLength > 8000 { // unlimited or > 8000
			return "nvarchar(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("nvarchar(%d)", maxLength)
		}
		return "nvarchar(max)"
	case "text":
		return "nvarchar(max)"
	case "citext":
		return "nvarchar(max)" // Case-insensitive text

	// Binary types
	case "bytea":
		return "varbinary(max)"

	// Date/time types
	case "date":
		return "date"
	case "time", "time without time zone":
		return "time"
	case "time with time zone", "timetz":
		return "time" // SQL Server time doesn't have timezone
	case "timestamp", "timestamp without time zone":
		return "datetime2"
	case "timestamp with time zone", "timestamptz":
		return "datetimeoffset"
	case "interval":
		return "nvarchar(100)" // No direct equivalent

	// UUID
	case "uuid":
		return "uniqueidentifier"

	// JSON
	case "json", "jsonb":
		return "nvarchar(max)" // Could also use JSON type in SQL Server 2016+

	// XML
	case "xml":
		return "xml"

	// Network types
	case "inet", "cidr", "macaddr", "macaddr8":
		return "nvarchar(50)"

	// Geometric types (store as text for simplicity)
	case "point", "line", "lseg", "box", "path", "polygon", "circle":
		return "nvarchar(max)"

	// Arrays (store as JSON)
	case "array":
		return "nvarchar(max)"

	// Range types (store as text)
	case "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange":
		return "nvarchar(100)"

	// Full-text search (store as text)
	case "tsvector", "tsquery":
		return "nvarchar(max)"

	// Bit string
	case "bit":
		if maxLength > 0 && maxLength <= 64 {
			return fmt.Sprintf("binary(%d)", (maxLength+7)/8)
		}
		return "varbinary(max)"
	case "bit varying", "varbit":
		return "varbinary(max)"

	// OID types
	case "oid", "regproc", "regprocedure", "regoper", "regoperator", "regclass", "regtype", "regconfig", "regdictionary":
		return "int"

	default:
		// Check for array types (e.g., "integer[]", "_int4")
		if strings.HasSuffix(pgType, "[]") || strings.HasPrefix(pgType, "_") {
			return "nvarchar(max)" // Store arrays as JSON
		}
		return "nvarchar(max)"
	}
}
