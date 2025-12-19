package typemap

import (
	"fmt"
	"strings"
)

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
