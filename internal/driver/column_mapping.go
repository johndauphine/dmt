package driver

import "fmt"

// MapColumnType maps a source column to a target SQL type using the configured
// type mapper. It is shared by DDL paths that need a single column definition
// without regenerating a full CREATE TABLE statement.
func MapColumnType(mapper TypeMapper, sourceDBType, targetDBType string, column Column) (string, error) {
	if mapper == nil {
		return "", fmt.Errorf("type mapper is required")
	}

	mapped := mapper.MapType(TypeInfo{
		SourceDBType: sourceDBType,
		TargetDBType: targetDBType,
		DataType:     column.DataType,
		FullDataType: column.FullDataType,
		MaxLength:    column.MaxLength,
		Precision:    column.Precision,
		Scale:        column.Scale,
		SampleValues: column.SampleValues,
	})
	if mapped == "" {
		return "", fmt.Errorf("no type mapping for %s column %q (%s) to %s",
			sourceDBType, column.Name, column.DataType, targetDBType)
	}
	return mapped, nil
}
