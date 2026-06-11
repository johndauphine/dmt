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

	// DateTypes lists the engine's date/timestamp type names accepted
	// for incremental sync columns.
	DateTypes []string `yaml:"date_types"`

	// ValueConverters names the scan-value normalization strategy
	// (audit: strategy-selected). Empty or "default" selects
	// driver.DefaultValueConverters.
	ValueConverters string `yaml:"value_converters"`

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
}

// DefaultsSpec mirrors driver.DriverDefaults.
type DefaultsSpec struct {
	Schema                string `yaml:"schema"`
	SSLMode               string `yaml:"ssl_mode"`
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
	Keyset    KeysetSpec    `yaml:"keyset"`
	RowNumber RowNumberSpec `yaml:"row_number"`
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
}

// IntrospectionSpec carries the schema-extraction queries. Each query
// must SELECT the canonical column shape documented on its field —
// the generic Reader's row mapping is fixed; the catalog adapts the
// engine's catalog tables to it. {?} placeholders are positional.
type IntrospectionSpec struct {
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
	// IndexList: param (table) → rows of (index_name, is_unique 0|1),
	// excluding PK-backing indexes.
	IndexList string `yaml:"index_list"`
	// IndexColumns: param (index_name) → ordered rows of (column_name).
	IndexColumns string `yaml:"index_columns"`
	// ForeignKeys: param (table) → ordered rows of (fk_id, seq,
	// ref_table, from_column, to_column, on_update, on_delete).
	ForeignKeys string `yaml:"foreign_keys"`
	// CheckConstraints: param (table) → rows of (name, expression).
	// Empty means the engine can't surface CHECKs (sqlite: inline-only)
	// and LoadCheckConstraints is a documented no-op.
	CheckConstraints string `yaml:"check_constraints"`
}

// AISpec carries the prompt-augmentation literals.
type AISpec struct {
	PromptAugmentation          string `yaml:"prompt_augmentation"`
	DropTablePromptAugmentation string `yaml:"drop_table_prompt_augmentation"`
}
