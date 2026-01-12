package postgres

import (
	"fmt"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// PostgreSQL maximum varchar length before converting to text (1GB - 1 header byte)
const pgMaxVarcharLength = 10485760

// TypeMapper implements driver.TypeMapper for PostgreSQL.
type TypeMapper struct{}

// MapType maps a source type to PostgreSQL.
func (m *TypeMapper) MapType(info driver.TypeInfo) string {
	switch info.SourceDBType {
	case "mssql":
		return m.fromMSSQL(info.DataType, info.MaxLength, info.Precision, info.Scale)
	case "postgres":
		return m.normalize(info.DataType, info.MaxLength, info.Precision, info.Scale)
	default:
		return m.fromMSSQL(info.DataType, info.MaxLength, info.Precision, info.Scale)
	}
}

// CanMap returns true if this mapper can handle the conversion.
func (m *TypeMapper) CanMap(sourceDBType, targetDBType string) bool {
	return targetDBType == "postgres"
}

// SupportedTargets returns the target types this mapper supports.
func (m *TypeMapper) SupportedTargets() []string {
	return []string{"postgres"}
}

// normalize returns the canonical PostgreSQL type representation.
func (m *TypeMapper) normalize(pgType string, maxLength, precision, scale int) string {
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
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "varchar":
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
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
		return pgType
	}
}

// fromMSSQL converts MSSQL data types to PostgreSQL equivalents.
func (m *TypeMapper) fromMSSQL(mssqlType string, maxLength, precision, scale int) string {
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
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "text"
	case "nchar":
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("char(%d)", maxLength)
		}
		return "text"
	case "varchar":
		if maxLength == -1 { // varchar(max)
			return "text"
		}
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "nvarchar":
		if maxLength == -1 { // nvarchar(max)
			return "text"
		}
		if maxLength > 0 && maxLength <= pgMaxVarcharLength {
			return fmt.Sprintf("varchar(%d)", maxLength)
		}
		return "text"
	case "text", "ntext":
		return "text"

	// Binary types
	case "binary", "varbinary", "image":
		return "bytea"

	// Date/time types
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetime", "datetime2", "smalldatetime":
		return "timestamp"
	case "datetimeoffset":
		return "timestamptz"

	// GUID
	case "uniqueidentifier":
		return "uuid"

	// XML
	case "xml":
		return "xml"

	// Spatial types
	case "geometry", "geography":
		return "text" // Or use PostGIS: "geometry"/"geography"

	// Other
	case "hierarchyid", "sql_variant":
		return "text"

	default:
		return "text"
	}
}
