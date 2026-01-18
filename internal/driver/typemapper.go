package driver

import "context"

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

// TableTypeMapper handles table-level DDL generation using AI.
// Unlike TypeMapper which maps individual columns, TableTypeMapper receives
// complete source table metadata and generates full target DDL.
// This provides better context for AI to make smart decisions about:
// - Character vs byte semantics (e.g., VARCHAR2 CHAR for Oracle)
// - Appropriate type sizing based on column semantics
// - Constraint handling across database platforms
type TableTypeMapper interface {
	// GenerateTableDDL generates complete CREATE TABLE DDL for the target database.
	// It takes the full source table metadata and produces target-specific DDL.
	// Returns an error if the AI fails - no fallback, caller must handle failure.
	GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error)

	// CanMap returns true if this mapper can handle the given conversion.
	CanMap(sourceDBType, targetDBType string) bool
}

// DatabaseContext contains metadata about a database for AI context.
type DatabaseContext struct {
	// Version is the full database version string (e.g., "PostgreSQL 15.4", "Oracle 23ai").
	Version string

	// MajorVersion is the parsed major version number (e.g., 15 for PostgreSQL 15.4).
	MajorVersion int

	// Charset is the database character set (e.g., "UTF8", "AL32UTF8", "utf8mb4").
	Charset string

	// NationalCharset is the national character set (Oracle: AL16UTF16, etc.).
	NationalCharset string

	// Collation is the default collation (e.g., "en_US.UTF-8", "utf8mb4_unicode_ci").
	Collation string

	// CodePage is the Windows code page number (e.g., 1252 for Latin1, 65001 for UTF-8).
	CodePage int

	// Encoding is the encoding name (e.g., "UTF-8", "LATIN1", "CP1252").
	Encoding string

	// CaseSensitiveIdentifiers indicates if unquoted identifiers are case-sensitive.
	// false = case-insensitive (Oracle uppercase, SQL Server, MySQL default)
	// true = case-sensitive (PostgreSQL lowercase preserves case)
	CaseSensitiveIdentifiers bool

	// IdentifierCase is how unquoted identifiers are stored:
	// "upper" (Oracle), "lower" (PostgreSQL), "preserve" (MySQL), "insensitive" (SQL Server)
	IdentifierCase string

	// CaseSensitiveData indicates if string comparisons are case-sensitive by default.
	CaseSensitiveData bool

	// MaxIdentifierLength is the maximum length for table/column names.
	MaxIdentifierLength int

	// MaxVarcharLength is the maximum varchar length in characters or bytes.
	MaxVarcharLength int

	// VarcharSemantics indicates if varchar uses "byte" or "char" semantics.
	VarcharSemantics string

	// BytesPerChar is the maximum bytes per character (1 for Latin1, 4 for UTF-8).
	BytesPerChar int

	// Features lists available features (e.g., "InnoDB", "CLOB", "JSON", "GEOGRAPHY").
	Features []string

	// StorageEngine is the storage engine (MySQL: InnoDB, Oracle: tablespace, etc.).
	StorageEngine string

	// DatabaseName is the specific database/schema name being used.
	DatabaseName string

	// ServerName is the database server hostname.
	ServerName string

	// Notes contains any additional context about the database.
	Notes string
}

// TableDDLRequest contains all information needed to generate target table DDL.
type TableDDLRequest struct {
	// SourceDBType is the source database type (e.g., "postgres", "mssql").
	SourceDBType string

	// TargetDBType is the target database type (e.g., "oracle", "mysql").
	TargetDBType string

	// SourceTable contains complete table metadata from the source database.
	SourceTable *Table

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// SourceContext contains metadata about the source database.
	SourceContext *DatabaseContext

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext

	// Note: Indexes and CHECK constraints are always created separately in Finalize,
	// not included in the initial CREATE TABLE DDL.
}

// TableDDLResponse contains the generated DDL and metadata.
type TableDDLResponse struct {
	// CreateTableDDL is the complete CREATE TABLE statement for the target database.
	CreateTableDDL string

	// ColumnTypes maps column names to their target types (for reference/logging).
	ColumnTypes map[string]string

	// Notes contains any AI-generated notes about the mapping decisions.
	Notes string
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

// GetAITypeMapper returns the global AI type mapper loaded from secrets.
// This is the only type mapper available - all type mapping is done via AI.
func GetAITypeMapper() (TypeMapper, error) {
	return NewAITypeMapperFromSecrets()
}
