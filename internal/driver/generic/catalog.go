// Package generic implements the catalog-driven driver engine (#191):
// one Driver/Reader/Writer/Dialect implementation parameterized by a
// per-engine declarative catalog. The catalog carries what the
// template-expressibility audit (docs/dialect-catalog-audit.md, #478)
// classified as data and parameterized templates; the genuinely
// imperative surfaces (DSN construction, bulk write, value conversion)
// are named Go strategies the catalog selects.
//
// A catalog's definition of done is the same conformance.DriverCase the
// hand-written engines run — write the case from the engine's docs
// first, then make the catalog satisfy it.
package generic

// Catalog is one engine's declarative description. Field groups map
// 1:1 onto the audit's dispositions: plain data fields, the two
// parameterized pagination templates with explicitly declared argument
// order, and strategy selections.
type Catalog struct {
	Name    string   `yaml:"name"`
	Aliases []string `yaml:"aliases"`

	Connection ConnectionSpec `yaml:"connection"`
	Defaults   DefaultsSpec   `yaml:"defaults"`

	// Capabilities declares the optional driver interfaces this engine
	// honors (#460). The generic Writer/Reader expose exactly these and
	// the conformance harness pins them.
	Capabilities CapabilitiesSpec `yaml:"capabilities"`

	Quoting     QuotingSpec    `yaml:"quoting"`
	Placeholder string         `yaml:"placeholder"` // "?", "$%d", "@p%d", ":%d"
	TableHints  TableHintsSpec `yaml:"table_hints"`

	Pagination    PaginationSpec    `yaml:"pagination"`
	Queries       QueriesSpec       `yaml:"queries"`
	Introspection IntrospectionSpec `yaml:"introspection"`
	DDL           DDLSpec           `yaml:"ddl"`
	Bulk          BulkSpec          `yaml:"bulk"`
	Upsert        UpsertSpec        `yaml:"upsert"`
	Sequence      SequenceSpec      `yaml:"sequence"`
	Context       ContextSpec       `yaml:"context"`

	// DateTypes lists the engine's date/timestamp type names accepted
	// for incremental sync columns.
	DateTypes []string `yaml:"date_types"`

	// ValueConverters names the scan-value normalization strategy
	// (audit: strategy-selected). Empty or "default" selects
	// driver.DefaultValueConverters.
	ValueConverters string `yaml:"value_converters"`

	// SpatialSelect wraps spatial columns in SELECT lists for
	// cross-engine reads (PostGIS/MySQL geometry → WKT text that the
	// target's staging path expects). Same-engine reads always use the
	// plain column list.
	SpatialSelect SpatialSelectSpec `yaml:"spatial_select"`

	// Preflight names the engine's preflight battery (imperative;
	// strategy-selected). Empty selects the shared minimal battery.
	Preflight string `yaml:"preflight_strategy"`

	AI AISpec `yaml:"ai"`
}

// ConnectionSpec describes DSN construction. Exactly one of URLTemplate
// or DSNStrategy must be set: URLTemplate covers engines whose DSN is a
// plain substitution ({host}, {port}, {database}, {user}, {password});
// DSNStrategy names a Go function for engines with conditional DSN
// logic (sqlite's file/:memory:/pragma handling).
type ConnectionSpec struct {
	URLTemplate string `yaml:"url_template"`
	DSNStrategy string `yaml:"dsn_strategy"`
	DefaultPort int    `yaml:"default_port"`
	// Backend is the database/sql driver name to open ("sqlite",
	// "pgx", "sqlserver", "mysql"). The generic package blank-imports
	// each supported backend in backends.go.
	Backend string `yaml:"backend"`
	// SingleWriter caps the writer pool at one open connection
	// (sqlite's file-lock constraint).
	SingleWriter bool `yaml:"single_writer"`
	// ValidateStrategy names a post-connect gate run by NewReader
	// ("mssql_compat" requires compatibility level 140+ for
	// STRING_AGG). Connection fails when the gate errors.
	ValidateStrategy string `yaml:"validate_strategy"`
}

// DefaultsSpec mirrors driver.DriverDefaults.
type DefaultsSpec struct {
	Schema                string `yaml:"schema"`
	SSLMode               string `yaml:"ssl_mode"`
	Encrypt               bool   `yaml:"encrypt"`
	PacketSize            int    `yaml:"packet_size"`
	WriteAheadWriters     int    `yaml:"write_ahead_writers"`
	ScaleWritersWithCores bool   `yaml:"scale_writers_with_cores"`
	OptimumBulkChunkBytes int    `yaml:"optimum_bulk_chunk_bytes"`
}

// CapabilitiesSpec mirrors the conformance capability matrix.
type CapabilitiesSpec struct {
	Upserter              bool `yaml:"upserter"`
	SequenceResetter      bool `yaml:"sequence_resetter"`
	ConstraintWriter      bool `yaml:"constraint_writer"`
	IncrementalDateReader bool `yaml:"incremental_date_reader"`
}

// SpatialSelectSpec declares the cross-engine SELECT wrapper for
// spatial column types. Wrapper uses {col} for the quoted column name,
// e.g. "ST_AsText({col}) AS {col}".
type SpatialSelectSpec struct {
	Types   []string `yaml:"types"`
	Wrapper string   `yaml:"wrapper"`
}

// QuotingSpec is the identifier-quoting style.
type QuotingSpec struct {
	// IdentifierFormat is a fmt template applied to the escaped name,
	// e.g. `"%s"` (postgres/sqlite), "[%s]" (mssql), "`%s`" (mysql).
	IdentifierFormat string `yaml:"identifier_format"`
	// EscapedChar is the character that must be doubled inside a quoted
	// identifier (`"` for `"%s"`, `]` for `[%s]`, backtick for mysql).
	EscapedChar string `yaml:"escaped_char"`
	// SchemaIgnored marks engines with no schema concept (sqlite):
	// QualifyTable returns only the quoted table name.
	SchemaIgnored bool `yaml:"schema_ignored"`
	// IdentifierSanitizer names a writer-side identifier rewrite
	// strategy ("postgres" → ident.SanitizePG). The writer applies it
	// to source-derived table/column names before quoting so DDL and
	// introspection match what CreateTable emitted (the deterministic
	// mapper sanitizes its own DDL with the same function). Readers
	// never sanitize — source names are used verbatim.
	IdentifierSanitizer string `yaml:"identifier_sanitizer"`
}

// TableHintsSpec carries the two TableHint literals keyed by the
// strict-consistency flag (audit: plain data).
type TableHintsSpec struct {
	Strict  string `yaml:"strict"`
	Relaxed string `yaml:"relaxed"`
}

// PaginationSpec holds the two parameterized templates. Argument order
// is declared exhaustively per variant — the audit calls arg order the
// highest-risk surface, so nothing is inferred.
type PaginationSpec struct {
	Keyset          KeysetSpec          `yaml:"keyset"`
	CompositeKeyset CompositeKeysetSpec `yaml:"composite_keyset"`
	RowNumber       RowNumberSpec       `yaml:"row_number"`
	// DateArgFormat, when set, binds date-filter arguments as
	// UTC-formatted strings in this Go layout instead of time.Time
	// (mssql compares via CONVERT(datetime2(7), ..., 126), which
	// needs the 7-fraction ISO text form).
	DateArgFormat string `yaml:"date_arg_format"`
}

// KeysetSpec renders the keyset queries. Query is the base template;
// MaxPKClause and DateClause are substituted into the {max_pk_clause}
// and {date_clause} markers when the variant calls for them, otherwise
// the markers render empty. Recognized tokens: {columns}, {table},
// {hint}, {pk}, {date_column}, {max_pk_clause}, {date_clause}, and {?}
// for placeholders (occurrence-numbered for indexed dialects).
type KeysetSpec struct {
	Query       string         `yaml:"query"`
	MaxPKClause string         `yaml:"max_pk_clause"`
	DateClause  string         `yaml:"date_clause"`
	Args        KeysetArgsSpec `yaml:"args"`
}

// CompositeKeysetSpec renders single-reader tuple keyset queries for
// all-integer composite primary keys (#616). The lower-bound comparison is
// generated in Go from the PK column list because its shape depends on the
// column count and the engine style; the template supplies the surrounding
// SELECT. Tokens: {columns}, {table}, {hint}, {composite_clause},
// {order_by}, {date_clause}, {date_column}, {?}. Style is "rowvalue"
// (WHERE (a,b) > (?,?) — postgres/mysql/sqlite) or "orchain"
// (WHERE a > ? OR (a = ? AND b > ?) — mssql, which lacks row-value
// comparison). LimitFirst places the limit placeholder before the WHERE
// args (mssql TOP).
type CompositeKeysetSpec struct {
	Query      string `yaml:"query"`
	Style      string `yaml:"style"`
	DateClause string `yaml:"date_clause"`
	LimitFirst bool   `yaml:"limit_first"`
}

// KeysetArgsSpec declares argument order per variant. Symbols:
// last_pk, max_pk, date_from, limit.
type KeysetArgsSpec struct {
	NoMax       []string `yaml:"no_max"`
	NoMaxDate   []string `yaml:"no_max_date"`
	WithMax     []string `yaml:"with_max"`
	WithMaxDate []string `yaml:"with_max_date"`
}

// RowNumberSpec renders the ROW_NUMBER queries. Additional tokens:
// {outer_columns} (alias-extracted column list for the outer SELECT),
// {order_by}, {where_date} (replaced by DateClause or empty).
type RowNumberSpec struct {
	Query      string            `yaml:"query"`
	DateClause string            `yaml:"date_clause"`
	Args       RowNumberArgsSpec `yaml:"args"`
}

// RowNumberArgsSpec declares argument order per variant. Symbols:
// date_from, row_start, row_end (row_start + limit).
type RowNumberArgsSpec struct {
	NoDate []string `yaml:"no_date"`
	Date   []string `yaml:"date"`
}

// QueriesSpec carries the plain-data query templates.
type QueriesSpec struct {
	// PartitionBoundaries tokens: {pk}, {table}, {n} (partition count).
	PartitionBoundaries string `yaml:"partition_boundaries"`
	// RowCount/RowCountStats keep the existing %s table-name contract
	// of Dialect.RowCountQuery. RowCountStats is optional; when empty,
	// RowCount serves both.
	RowCount      string `yaml:"row_count"`
	RowCountStats string `yaml:"row_count_stats"`
	DateColumn    string `yaml:"date_column"`
	// MaxDate overrides the plain MAX({column}) watermark query;
	// tokens {column} (quoted) and {table} (qualified). mssql casts
	// through datetime2(7) to keep full tick precision.
	MaxDate string `yaml:"max_date"`
	// PartitionStrategy selects an imperative partitioner instead of
	// PartitionBoundaries ("min_max_even": MIN/MAX index seeks + even
	// ranges — mssql's fast path; NTILE over 10M+ rows is a full scan).
	PartitionStrategy string `yaml:"partition_strategy"`
	// ProbeMaxAllowedPacket, when set, is run by ProbeTarget against a
	// live target connection and feeds the packet-aware chunk cap
	// (smartconfig: (value * 0.8) / avg_row_bytes). MySQL's
	// @@max_allowed_packet is the only engine cap today.
	ProbeMaxAllowedPacket string `yaml:"probe_max_allowed_packet"`
}

// IntrospectionSpec carries the schema-extraction queries. Each query
// must SELECT the canonical column shape documented on its field —
// the generic Reader's row mapping is fixed; the catalog adapts the
// engine's catalog tables to it. {?} placeholders are positional.
type IntrospectionSpec struct {
	// UsesSchema: the engine's catalog tables are keyed by schema (or
	// database) as well as table — ClickHouse's system.columns. When
	// true, ListTables binds (schema) and the per-table queries bind
	// (schema, table); the date_column query binds (schema, table,
	// column). When false (sqlite), the schema is omitted entirely.
	UsesSchema bool `yaml:"uses_schema"`
	// ListTables: no params → rows of (table_name).
	ListTables string `yaml:"list_tables"`
	// DescribeTable: param (table) → rows of (ordinal, name, decl_type,
	// max_length, precision, scale, is_nullable 0|1, default_value,
	// pk_ordinal 0=not in PK). Engines whose catalog can't compute
	// length/precision/scale in SQL return NULLs and set
	// ParseTypeParams.
	DescribeTable string `yaml:"describe_table"`
	// ParseTypeParams applies the declared-type parser ("VARCHAR(255)"
	// → varchar/255, "NUMERIC(10,2)" → numeric/10/2) to fill
	// max_length/precision/scale from decl_type.
	ParseTypeParams bool `yaml:"parse_type_params"`
	// IdentityStrategy names the Go routine that flags identity /
	// auto-increment columns (imperative per engine). Empty = none.
	IdentityStrategy string `yaml:"identity_strategy"`
	// IdentityInDescribe: the engine's catalog exposes identity as a
	// column expression (mysql EXTRA LIKE '%auto_increment%'), so
	// describe_table returns a 10th column (is_identity 0|1) instead
	// of needing a Go strategy.
	IdentityInDescribe bool `yaml:"identity_in_describe"`
	// FullTypeInDescribe: describe_table returns a full dialect type
	// declaration immediately after decl_type. MySQL uses this for
	// COLUMN_TYPE ("int unsigned", "enum('a','b')") while retaining
	// DATA_TYPE as the base dispatch type.
	FullTypeInDescribe bool `yaml:"full_type_in_describe"`
	// AvgRowLength is an optional stats query — params like the other
	// per-table queries — returning the engine's average row size in
	// bytes (mysql information_schema.TABLES.AVG_ROW_LENGTH). Empty
	// falls back to the Go heap estimate.
	AvgRowLength string `yaml:"avg_row_length"`
	// IndexList: param (table) → rows of (index_name, is_unique 0|1),
	// excluding PK-backing indexes.
	IndexList string `yaml:"index_list"`
	// IndexListAgg is the single-query alternative to IndexList +
	// IndexColumns: one row per index of (name, is_unique,
	// is_clustered, key columns, include columns), the column lists
	// CHAR(1)-joined (identifiers cannot contain SOH). Used by engines
	// whose indexes carry include columns (mssql).
	IndexListAgg string `yaml:"index_list_agg"`
	// IndexColumns: param (index_name) → ordered rows of (column_name).
	// With IndexColumnsByTable, params are (table, index_name) —
	// engines where index names are only unique per table (mysql).
	IndexColumns        string `yaml:"index_columns"`
	IndexColumnsByTable bool   `yaml:"index_columns_by_table"`
	// ForeignKeys: param (table) → ordered rows of (fk_id, seq, name,
	// ref_schema, ref_table, from_column, to_column, on_update,
	// on_delete). An empty name falls back to the synthesized
	// fk_<table>_<id> form (sqlite has no constraint names in pragma
	// output); ref_schema is empty for schema-less engines.
	ForeignKeys string `yaml:"foreign_keys"`
	// CheckConstraints: param (table) → rows of (name, expression).
	// Empty means the engine can't surface CHECKs (sqlite: inline-only)
	// and LoadCheckConstraints is a documented no-op.
	CheckConstraints string `yaml:"check_constraints"`
	// ColumnExists: params (table, column) → a row iff present.
	ColumnExists string `yaml:"column_exists"`
	// TableExists: param (table) → a row iff present.
	TableExists string `yaml:"table_exists"`
	// HasPrimaryKey: param (table) → a row iff the table has a PK.
	HasPrimaryKey string `yaml:"has_primary_key"`
}

// DDLSpec carries the writer's DDL templates. Statement lists use the
// {table} token (dialect-qualified); empty templates mean the engine
// doesn't support (or need) the operation and the writer degrades the
// way the capability matrix declares.
type DDLSpec struct {
	// CreateSchema is the CREATE SCHEMA template ({schema}); empty =
	// no-op (sqlite: a file IS the database).
	CreateSchema string `yaml:"create_schema"`
	// CreateSchemaDefaultsToDatabase substitutes the connection
	// database when the configured schema is empty (clickhouse, where
	// schema IS the database). Engines with a default-database notion
	// (mysql) keep empty-schema as a no-op — least-privilege upsert
	// users must not be forced through CREATE DATABASE.
	CreateSchemaDefaultsToDatabase bool `yaml:"create_schema_defaults_to_database"`
	// DropTableStmts run in order ({table}); engines that must toggle
	// FK enforcement around the drop declare it here.
	DropTableStmts []string `yaml:"drop_table_stmts"`
	// TruncateStmts run in order ({table}).
	TruncateStmts []string `yaml:"truncate_stmts"`
	// TruncateCleanup is an optional parameterized statement run after
	// truncate with the bare table name as its argument (sqlite:
	// sqlite_sequence reset).
	TruncateCleanup string `yaml:"truncate_cleanup"`
	// AddColumn is the ALTER TABLE template: {table}, {column}, {type}.
	AddColumn string `yaml:"add_column"`
	// CreatePrimaryKey ({table}, {columns}); empty = no-op (inline-PK
	// engines).
	CreatePrimaryKey string `yaml:"create_primary_key"`
	// CanDropNotNull / CanAlterColumnType: false returns the uniform
	// "requires a table rebuild" error instead of attempting DDL.
	CanDropNotNull     bool `yaml:"can_drop_not_null"`
	CanAlterColumnType bool `yaml:"can_alter_column_type"`
	// DropNotNull / AlterColumnType templates ({table}, {column},
	// {type}, {nullability}, {default_clause}); required when the
	// corresponding Can* flag is true.
	DropNotNull     string `yaml:"drop_not_null"`
	AlterColumnType string `yaml:"alter_column_type"`
}

// BulkSpec selects the bulk-write strategy.
type BulkSpec struct {
	Strategy string `yaml:"strategy"`
	// MaxBindVariables is the engine's bind-variable ceiling per
	// statement (sqlite: SQLITE_MAX_VARIABLE_NUMBER). 0 = unlimited.
	MaxBindVariables int `yaml:"max_bind_variables"`
	// RowConverter names the write-side value normalization (empty =
	// pass through).
	RowConverter string `yaml:"row_converter"`
	// IdempotentVerb is the INSERT verb that silently skips duplicate
	// PKs, used by the #227 ROW_NUMBER resume path ("INSERT OR IGNORE
	// INTO" on sqlite). IdempotentSuffix is the trailing-clause form
	// ("ON DUPLICATE KEY UPDATE {pk} = {pk}" on mysql; {pk} renders as
	// the quoted first PK column). At most one may be set; neither
	// means idempotent replay is refused with a clear error instead of
	// emitting another engine's syntax.
	IdempotentVerb   string `yaml:"idempotent_verb"`
	IdempotentSuffix string `yaml:"idempotent_suffix"`
	// NonTransactional disables the one-transaction wrapper around a
	// batch sequence (mysql autocommits per statement, matching its
	// hand-written writer's resume semantics).
	NonTransactional bool `yaml:"non_transactional"`
}

// UpsertSpec selects the upsert strategy (capability Upserter).
type UpsertSpec struct {
	Strategy string `yaml:"strategy"`
}

// SequenceSpec selects the identity-reset strategy (capability
// SequenceResetter).
type SequenceSpec struct {
	Strategy string `yaml:"strategy"`
}

// ContextSpec is the static driver.DatabaseContext metadata plus the
// version query.
type ContextSpec struct {
	VersionQuery             string   `yaml:"version_query"`
	VersionPrefix            string   `yaml:"version_prefix"`
	IdentifierCase           string   `yaml:"identifier_case"`
	CaseSensitiveIdentifiers bool     `yaml:"case_sensitive_identifiers"`
	Charset                  string   `yaml:"charset"`
	Encoding                 string   `yaml:"encoding"`
	MaxIdentifierLength      int      `yaml:"max_identifier_length"`
	VarcharSemantics         string   `yaml:"varchar_semantics"`
	BytesPerChar             int      `yaml:"bytes_per_char"`
	Features                 []string `yaml:"features"`
}

// AISpec carries the prompt-augmentation literals.
type AISpec struct {
	PromptAugmentation          string `yaml:"prompt_augmentation"`
	DropTablePromptAugmentation string `yaml:"drop_table_prompt_augmentation"`
}
