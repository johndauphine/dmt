package driver

import (
	"context"
	"errors"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
)

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

	// MaxNVarcharLength is the max NVARCHAR length in characters (MSSQL: 4000).
	// Set only for databases that have a separate national character type.
	MaxNVarcharLength int

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

// FinalizationDDLMapper handles AI-driven DDL generation for finalization phase.
type FinalizationDDLMapper interface {
	// GenerateFinalizationDDL generates DDL for indexes, foreign keys, or check constraints.
	GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error)
}

// DDLType specifies the type of DDL to generate.
type DDLType string

const (
	DDLTypeIndex           DDLType = "index"
	DDLTypeForeignKey      DDLType = "foreign_key"
	DDLTypeCheckConstraint DDLType = "check_constraint"
	DDLTypeDropTable       DDLType = "drop_table"
)

// FinalizationDDLRequest contains information needed to generate finalization DDL.
type FinalizationDDLRequest struct {
	// Type specifies what kind of DDL to generate.
	Type DDLType

	// SourceDBType is the source database type (e.g., "postgres", "mssql").
	SourceDBType string

	// TargetDBType is the target database type (e.g., "oracle", "mysql").
	TargetDBType string

	// Table contains the target table metadata.
	Table *Table

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext

	// Index contains index metadata (when Type is DDLTypeIndex).
	Index *Index

	// ForeignKey contains FK metadata (when Type is DDLTypeForeignKey).
	ForeignKey *ForeignKey

	// CheckConstraint contains check constraint metadata (when Type is DDLTypeCheckConstraint).
	CheckConstraint *CheckConstraint

	// TargetTableDDL is the CREATE TABLE DDL for the target table.
	// This helps AI understand the actual table structure when generating indexes/FKs.
	TargetTableDDL string
}

// TableDropDDLMapper handles AI-driven DDL generation for dropping tables.
type TableDropDDLMapper interface {
	// GenerateDropTableDDL generates DDL statement(s) for dropping a table.
	// The AI will generate database-specific syntax that properly handles
	// foreign key constraints and other database-specific requirements.
	GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error)
}

// DropTableDDLRequest contains information needed to generate DROP TABLE DDL.
type DropTableDDLRequest struct {
	// TargetDBType is the target database type (e.g., "mysql", "postgres").
	TargetDBType string

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// TableName is the name of the table to drop.
	TableName string

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext
}

// GetTypeMapper returns the type mapper for use by drivers + writers.
// Default behavior since #170:
//
//   - When no AI is configured (no ~/.secrets/dmt-config.yaml AI
//     section, no provider, no API key), returns the deterministic
//     mapper alone. AI is no longer required for migrations.
//   - When AI IS configured, returns a FallbackChain that routes
//     deterministic-first and falls back to the AI mapper for the
//     narrow cases the deterministic path can't handle (Raw types,
//     ErrUnsupportedDDL).
//
// The unmapped_type_action knob (default "fail") controls what the
// chain does at column level for Raw types when no AI fallback exists.
// Threaded through via the action argument; pass UnmappedActionFail
// to use the safe default.
//
// Both this function and GetAIMapper share a single underlying AI
// mapper instance (cached via sync.Once) so the on-disk type cache
// at ~/.dmt/type-cache.json doesn't get loaded twice or written
// concurrently from two AITypeMapper instances (Copilot review on
// PR #192).
func GetTypeMapper(action UnmappedAction) (TypeMapper, error) {
	primary := NewDeterministicMapper()
	// Avoid Go's typed-nil-into-interface trap: assigning a
	// (*AITypeMapper)(nil) directly to the TypeMapper interface
	// parameter would produce a non-nil interface with a nil
	// underlying value, and every `c.fallback != nil` check in the
	// chain would silently pass — eventually crashing on a Raw type
	// with a nil-pointer-deref. Pass nil interface explicitly when
	// the AI mapper isn't available.
	if ai := loadCachedAIMapper(); ai != nil {
		return NewFallbackChain(primary, ai, action), nil
	}
	return NewFallbackChain(primary, nil, action), nil
}

// GetAIMapper returns the AI type mapper if AI is configured, or nil
// when no AI is available. Used by callers that need AI-specific
// functionality which is conceptually separate from the type-mapping
// path — runtime monitor (internal/monitor.AIMonitor), error
// diagnoser (driver.AIErrorDiagnoser), config status display.
//
// Returns nil silently when AI isn't configured; callers branch on
// nil rather than checking errors. Returns the SAME instance as
// GetTypeMapper's fallback (singleton — see GetTypeMapper doc).
func GetAIMapper() *AITypeMapper {
	return loadCachedAIMapper()
}

// loadCachedAIMapper memoizes the AI mapper construction so the chain
// and standalone callers share a single instance. Without this, both
// would hold their own AITypeMapper, both would load the on-disk
// type cache, and concurrent cache writes from the two instances
// could lose entries (Copilot review on PR #192).
var (
	cachedAIMapper     *AITypeMapper
	cachedAIMapperOnce sync.Once
)

func loadCachedAIMapper() *AITypeMapper {
	cachedAIMapperOnce.Do(func() {
		cachedAIMapper = tryLoadAIMapper()
	})
	return cachedAIMapper
}

// resetCachedAIMapper clears the singleton — used by tests to force
// re-evaluation after manipulating DMT_SECRETS_FILE / secrets.Reset().
// Not exported; tests in this package use it directly.
func resetCachedAIMapper() {
	cachedAIMapper = nil
	cachedAIMapperOnce = sync.Once{}
}

// tryLoadAIMapper attempts to construct an AITypeMapper from secrets.
// Returns nil for both "AI not configured" and "AI configured but
// failed to load," with different log behavior:
//
//   - SecretsNotFoundError (no ~/.secrets/dmt-config.yaml file at
//     all, or DMT_SECRETS_FILE points at a missing path) — silent;
//     this is the expected state when running without AI configured.
//   - Any other error (insecure file permissions, YAML parse error,
//     misconfigured provider, missing API key for a configured
//     provider) — logged at WARN with the full error so users can
//     act on actionable failures rather than silently losing AI
//     fallback (Copilot review on PR #192).
func tryLoadAIMapper() *AITypeMapper {
	mapper, err := NewAITypeMapperFromSecrets()
	if err == nil {
		return mapper
	}
	var notFound *secrets.SecretsNotFoundError
	if errors.As(err, &notFound) {
		// Expected when running without AI; debug-log only.
		logging.Debug("typemap: no secrets file at %s, AI fallback disabled", notFound.Path)
		return nil
	}
	logging.Warn("typemap: failed to load AI mapper (AI fallback disabled): %v", err)
	return nil
}
