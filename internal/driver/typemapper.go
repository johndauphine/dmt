package driver

// TypeMapper handles data type conversions between databases.
type TypeMapper interface {
	// MapType converts a source type to the target type.
	// Returns the mapped type string (e.g., "varchar(255)", "numeric(10,2)").
	MapType(info TypeInfo) string

	// CanMap returns true if this mapper can handle the given conversion.
	CanMap(sourceDBType, targetDBType string) bool

	// SupportedTargets returns the list of target database types this mapper supports.
	// Returns ["*"] if it can map to any target (e.g., AI mapper).
	SupportedTargets() []string
}

// TypeInfo contains metadata about a column type.
type TypeInfo struct {
	// SourceDBType is the source database type (e.g., "mssql", "postgres").
	SourceDBType string

	// TargetDBType is the target database type.
	TargetDBType string

	// DataType is the source column's data type.
	DataType string

	// MaxLength is the maximum length for string types (-1 for MAX).
	MaxLength int

	// Precision is the numeric precision.
	Precision int

	// Scale is the numeric scale.
	Scale int

	// SampleValues contains sample data values from the source column.
	// Used by AI mapper to provide context for better type mapping decisions.
	SampleValues []string
}

// ChainedTypeMapper tries multiple mappers in order until one succeeds.
// This allows combining static mappers with AI fallback.
type ChainedTypeMapper struct {
	mappers []TypeMapper
}

// NewChainedTypeMapper creates a mapper that tries each mapper in order.
func NewChainedTypeMapper(mappers ...TypeMapper) *ChainedTypeMapper {
	return &ChainedTypeMapper{mappers: mappers}
}

// MapType tries each mapper in order and returns the first successful mapping.
func (c *ChainedTypeMapper) MapType(info TypeInfo) string {
	for _, m := range c.mappers {
		if m.CanMap(info.SourceDBType, info.TargetDBType) {
			return m.MapType(info)
		}
	}
	// Ultimate fallback: text for PostgreSQL, nvarchar(max) for others
	if info.TargetDBType == "postgres" {
		return "text"
	}
	return "nvarchar(max)"
}

// CanMap returns true if any mapper in the chain can handle the conversion.
func (c *ChainedTypeMapper) CanMap(sourceDBType, targetDBType string) bool {
	for _, m := range c.mappers {
		if m.CanMap(sourceDBType, targetDBType) {
			return true
		}
	}
	return false
}

// SupportedTargets returns all targets supported by any mapper in the chain.
func (c *ChainedTypeMapper) SupportedTargets() []string {
	seen := make(map[string]bool)
	for _, m := range c.mappers {
		for _, t := range m.SupportedTargets() {
			seen[t] = true
		}
	}
	targets := make([]string, 0, len(seen))
	for t := range seen {
		targets = append(targets, t)
	}
	return targets
}

// Direction represents a migration direction for type mapping.
type Direction int

const (
	// MSSQLToPostgres is MSSQL source to PostgreSQL target.
	MSSQLToPostgres Direction = iota
	// PostgresToMSSQL is PostgreSQL source to MSSQL target.
	PostgresToMSSQL
	// PostgresToPostgres is PostgreSQL to PostgreSQL (normalization).
	PostgresToPostgres
	// MSSQLToMSSQL is MSSQL to MSSQL (normalization).
	MSSQLToMSSQL
)

// GetDirection returns the migration direction for the given source and target types.
func GetDirection(sourceType, targetType string) Direction {
	switch {
	case sourceType == "mssql" && targetType == "postgres":
		return MSSQLToPostgres
	case sourceType == "postgres" && targetType == "mssql":
		return PostgresToMSSQL
	case sourceType == "postgres" && targetType == "postgres":
		return PostgresToPostgres
	case sourceType == "mssql" && targetType == "mssql":
		return MSSQLToMSSQL
	default:
		// Default to MSSQL→PG for backward compatibility
		return MSSQLToPostgres
	}
}

// NewTypeMapperFromConfig creates a TypeMapper based on the configuration.
// If AITypeMappingConfig is provided and enabled, returns an AI-powered mapper
// that falls back to the static mapper when AI is unavailable.
// Otherwise returns the provided static fallback mapper.
// This helper avoids duplicating AI mapper initialization in each writer.
func NewTypeMapperFromConfig(aiConfig *AITypeMappingConfig, staticMapper TypeMapper) (TypeMapper, error) {
	if aiConfig == nil || !aiConfig.Enabled {
		return staticMapper, nil
	}

	// Pass the static mapper as fallback for when AI API fails
	aiMapper, err := NewAITypeMapper(*aiConfig, staticMapper)
	if err != nil {
		return nil, err
	}

	return aiMapper, nil
}
