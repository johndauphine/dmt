package driver

import (
	"encoding/hex"
	"fmt"
	"time"
)

// DefaultValueConverters resolves each source column type to a scan-value
// conversion func, nil meaning pass-through (#477). This is the
// normalization table that lived hardcoded in transfer/scan.go — the
// MSSQL→PostgreSQL-era conversions, keyed on MSSQL-flavored type names.
// The built-in dialects all delegate here: other engines' lowercase
// information_schema names mostly don't collide with these cases, and
// where they do (PG/MySQL "bit", MySQL "datetime") the conversion is
// the established cross-target behavior. Per-engine divergence starts
// when a dialect (or a #191 catalog) returns its own table instead.
func DefaultValueConverters(colTypes []string) []func(any) any {
	convs := make([]func(any) any, len(colTypes))
	for i, ct := range colTypes {
		convs[i] = defaultConverterForType(ct)
	}
	return convs
}

func defaultConverterForType(colType string) func(any) any {
	switch colType {
	case "binary", "varbinary", "image":
		return convertBinaryValue
	case "uniqueidentifier":
		return convertUniqueIdentifierValue
	case "bit":
		return convertBitValue
	case "datetime", "datetime2", "smalldatetime", "datetimeoffset":
		return convertDateTimeValue
	default:
		return nil
	}
}

// convertBinaryValue converts binary data for bytea targets.
func convertBinaryValue(val any) any {
	switch v := val.(type) {
	case []byte:
		if len(v) == 0 {
			return nil
		}
		return v // pgx handles []byte directly
	}
	return val
}

// convertUniqueIdentifierValue handles SQL Server GUID conversion.
func convertUniqueIdentifierValue(val any) any {
	switch v := val.(type) {
	case []byte:
		if len(v) == 16 {
			// SQL Server GUID to PostgreSQL UUID
			return formatMixedEndianUUID(v)
		}
		return string(v)
	case string:
		return v
	}
	return val
}

// convertBitValue converts bit to boolean.
func convertBitValue(val any) any {
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case int:
		return v != 0
	}
	return val
}

// convertDateTimeValue ensures proper timestamp format, mapping SQL
// Server's pre-year-1 sentinel datetimes to NULL.
func convertDateTimeValue(val any) any {
	switch v := val.(type) {
	case time.Time:
		// Handle SQL Server minimum datetime (1753-01-01)
		if v.Year() < 1 {
			return nil
		}
		return v
	}
	return val
}

// formatMixedEndianUUID converts SQL Server GUID bytes (mixed-endian)
// to a standard UUID string.
func formatMixedEndianUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], // time_low (reversed)
		b[5], b[4], // time_mid (reversed)
		b[7], b[6], // time_hi_and_version (reversed)
		b[8], b[9], // clock_seq
		b[10], b[11], b[12], b[13], b[14], b[15]) // node
}
