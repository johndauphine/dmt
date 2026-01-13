package mssql

import (
	"github.com/johndauphine/dmt/internal/driver"
)

// TypeMapper implements driver.TypeMapper for SQL Server.
type TypeMapper struct{}

// MapType maps a source type to SQL Server.
func (m *TypeMapper) MapType(info driver.TypeInfo) string {
	switch info.SourceDBType {
	case "postgres":
		return driver.LookupPostgresToMSSQL(info.DataType, info.MaxLength, info.Precision, info.Scale)
	case "mssql":
		return driver.NormalizeMSSQLType(info.DataType, info.MaxLength, info.Precision, info.Scale)
	default:
		return driver.LookupPostgresToMSSQL(info.DataType, info.MaxLength, info.Precision, info.Scale)
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
