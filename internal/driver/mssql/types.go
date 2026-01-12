package mssql

import (
	"fmt"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// TypeMapper implements driver.TypeMapper for SQL Server.
type TypeMapper struct{}

// MapType maps a source type to SQL Server.
func (m *TypeMapper) MapType(info driver.TypeInfo) string {
	switch info.SourceDBType {
	case "postgres":
		return m.fromPostgres(info.DataType, info.MaxLength, info.Precision, info.Scale)
	case "mssql":
		return m.normalize(info.DataType, info.MaxLength, info.Precision, info.Scale)
	default:
		return m.fromPostgres(info.DataType, info.MaxLength, info.Precision, info.Scale)
	}
}

// CanMap returns true if this mapper can handle the conversion.
func (m *TypeMapper) CanMap(sourceDBType, targetDBType string) bool {
	return targetDBType == "mssql"
}

// SupportedTargets returns the target types this mapper supports.
func (m *TypeMapper) SupportedTargets() []string {
	return []string{"mssql"}
}

// normalize returns the canonical SQL Server type representation.
func (m *TypeMapper) normalize(mssqlType string, maxLength, precision, scale int) string {
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

// fromPostgres converts PostgreSQL data types to SQL Server equivalents.
func (m *TypeMapper) fromPostgres(pgType string, maxLength, precision, scale int) string {
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
		return "int" // IDENTITY handled separately
	case "bigserial":
		return "bigint" // IDENTITY handled separately
	case "smallserial":
		return "smallint" // IDENTITY handled separately

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
		if maxLength == -1 || maxLength > 8000 {
			return "nvarchar(max)"
		}
		if maxLength > 0 {
			return fmt.Sprintf("nvarchar(%d)", maxLength)
		}
		return "nvarchar(max)"
	case "text":
		return "nvarchar(max)"
	case "citext":
		return "nvarchar(max)"

	// Binary types
	case "bytea":
		return "varbinary(max)"

	// Date/time types
	case "date":
		return "date"
	case "time", "time without time zone":
		return "time"
	case "time with time zone", "timetz":
		return "time"
	case "timestamp", "timestamp without time zone":
		return "datetime2"
	case "timestamp with time zone", "timestamptz":
		return "datetimeoffset"
	case "interval":
		return "nvarchar(100)"

	// UUID
	case "uuid":
		return "uniqueidentifier"

	// JSON
	case "json", "jsonb":
		return "nvarchar(max)"

	// XML
	case "xml":
		return "xml"

	// Network types
	case "inet", "cidr", "macaddr", "macaddr8":
		return "nvarchar(50)"

	// Geometric types
	case "point", "line", "lseg", "box", "path", "polygon", "circle":
		return "nvarchar(max)"

	// Arrays
	case "array":
		return "nvarchar(max)"

	// Range types
	case "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange":
		return "nvarchar(100)"

	// Full-text search
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
		// Check for array types
		if strings.HasSuffix(pgType, "[]") || strings.HasPrefix(pgType, "_") {
			return "nvarchar(max)"
		}
		return "nvarchar(max)"
	}
}
