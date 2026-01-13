package postgres

import (
	"github.com/johndauphine/dmt/internal/driver"
)

// TypeMapper implements driver.TypeMapper for PostgreSQL.
type TypeMapper struct{}

// MapType maps a source type to PostgreSQL.
func (m *TypeMapper) MapType(info driver.TypeInfo) string {
	switch info.SourceDBType {
	case "mssql":
		return driver.LookupMSSQLToPostgres(info.DataType, info.MaxLength, info.Precision, info.Scale)
	case "postgres":
		return driver.NormalizePostgresType(info.DataType, info.MaxLength, info.Precision, info.Scale)
	default:
		return driver.LookupMSSQLToPostgres(info.DataType, info.MaxLength, info.Precision, info.Scale)
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
