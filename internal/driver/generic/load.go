package generic

import (
	"embed"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Catalogs ship embedded — dmt stays a single self-contained binary
// and every catalog is repo-tested knowledge, not user-managed config.
//
//go:embed catalogs/*.yaml
var catalogFS embed.FS

// LoadCatalog parses and validates one embedded catalog by engine name.
func LoadCatalog(name string) (*Catalog, error) {
	data, err := catalogFS.ReadFile("catalogs/" + name + ".yaml")
	if err != nil {
		return nil, fmt.Errorf("no embedded catalog %q: %w", name, err)
	}
	return ParseCatalog(data)
}

// ParseCatalog decodes and validates catalog YAML. Strict decoding:
// an unknown field is a typo, and a typo in a catalog is a wrong query
// at migration time.
func ParseCatalog(data []byte) (*Catalog, error) {
	var c Catalog
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parsing catalog: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("catalog %q: %w", c.Name, err)
	}
	return &c, nil
}

// Validate enforces the structural rules a catalog must satisfy before
// it can drive the engine. This is the load-time half of the contract;
// the behavioral half is the conformance DriverCase.
func (c *Catalog) Validate() error {
	var errs []string
	require := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, msg)
		}
	}

	require(c.Name != "", "name is required")
	require(c.Quoting.IdentifierFormat != "" && strings.Contains(c.Quoting.IdentifierFormat, "%s"),
		"quoting.identifier_format must contain %s")
	require(c.Quoting.EscapedChar != "", "quoting.escaped_char is required")
	require(c.Placeholder != "", "placeholder is required")
	require(c.Defaults.WriteAheadWriters > 0, "defaults.write_ahead_writers must be > 0")

	hasURL := c.Connection.URLTemplate != ""
	hasStrategy := c.Connection.DSNStrategy != ""
	require(hasURL != hasStrategy,
		"connection: exactly one of url_template or dsn_strategy must be set")
	if hasStrategy {
		if _, ok := dsnStrategies[c.Connection.DSNStrategy]; !ok {
			errs = append(errs, fmt.Sprintf("connection.dsn_strategy %q is not a known strategy", c.Connection.DSNStrategy))
		}
	}

	// Pagination: templates and every variant's argument order are
	// mandatory — arg order is the surface where a wrong catalog fails
	// mid-migration instead of at review time (#478).
	k := c.Pagination.Keyset
	require(k.Query != "", "pagination.keyset.query is required")
	require(strings.Contains(k.Query, "{max_pk_clause}"), "pagination.keyset.query must contain {max_pk_clause}")
	require(strings.Contains(k.Query, "{date_clause}"), "pagination.keyset.query must contain {date_clause}")
	require(k.MaxPKClause != "", "pagination.keyset.max_pk_clause is required")
	require(k.DateClause != "", "pagination.keyset.date_clause is required")
	for variant, args := range map[string][]string{
		"no_max":        k.Args.NoMax,
		"no_max_date":   k.Args.NoMaxDate,
		"with_max":      k.Args.WithMax,
		"with_max_date": k.Args.WithMaxDate,
	} {
		require(len(args) > 0, "pagination.keyset.args."+variant+" is required")
		for _, sym := range args {
			switch sym {
			case "last_pk", "max_pk", "date_from", "limit":
			default:
				errs = append(errs, fmt.Sprintf("pagination.keyset.args.%s: unknown symbol %q", variant, sym))
			}
		}
	}

	rn := c.Pagination.RowNumber
	require(rn.Query != "", "pagination.row_number.query is required")
	require(strings.Contains(rn.Query, "{where_date}"), "pagination.row_number.query must contain {where_date}")
	require(rn.DateClause != "", "pagination.row_number.date_clause is required")
	for variant, args := range map[string][]string{
		"no_date": rn.Args.NoDate,
		"date":    rn.Args.Date,
	} {
		require(len(args) > 0, "pagination.row_number.args."+variant+" is required")
		for _, sym := range args {
			switch sym {
			case "date_from", "row_start", "row_end":
			default:
				errs = append(errs, fmt.Sprintf("pagination.row_number.args.%s: unknown symbol %q", variant, sym))
			}
		}
	}

	require(c.Queries.PartitionBoundaries != "", "queries.partition_boundaries is required")
	require(c.Queries.RowCount != "", "queries.row_count is required")
	require(strings.Contains(c.Queries.RowCount, "%s"), "queries.row_count must contain the %s table placeholder")
	require(c.Queries.DateColumn != "", "queries.date_column is required")
	require(len(c.DateTypes) > 0, "date_types is required")

	require(c.Connection.Backend != "", "connection.backend is required")
	if c.Connection.Backend != "" {
		if _, ok := knownBackends[c.Connection.Backend]; !ok {
			errs = append(errs, fmt.Sprintf("connection.backend %q is not a known backend", c.Connection.Backend))
		}
	}
	in := c.Introspection
	require(in.ListTables != "", "introspection.list_tables is required")
	require(in.DescribeTable != "", "introspection.describe_table is required")
	require(in.IndexList != "", "introspection.index_list is required")
	require(in.IndexColumns != "", "introspection.index_columns is required")
	require(in.ForeignKeys != "", "introspection.foreign_keys is required")
	if in.IdentityStrategy != "" {
		if _, ok := identityStrategies[in.IdentityStrategy]; !ok {
			errs = append(errs, fmt.Sprintf("introspection.identity_strategy %q is not a known strategy", in.IdentityStrategy))
		}
	}
	require(in.ColumnExists != "", "introspection.column_exists is required")
	require(in.TableExists != "", "introspection.table_exists is required")
	require(in.HasPrimaryKey != "", "introspection.has_primary_key is required")

	// Writer strategies: required, and capability declarations must
	// match — a catalog claiming Upserter without an upsert strategy
	// would pass conformance and fail mid-migration.
	require(c.Bulk.Strategy != "", "bulk.strategy is required")
	if c.Bulk.Strategy != "" {
		if _, ok := bulkStrategies[c.Bulk.Strategy]; !ok {
			errs = append(errs, fmt.Sprintf("bulk.strategy %q is not a known strategy", c.Bulk.Strategy))
		}
	}
	require(!(c.Bulk.IdempotentVerb != "" && c.Bulk.IdempotentSuffix != ""),
		"bulk: idempotent_verb and idempotent_suffix are mutually exclusive")
	if c.Bulk.RowConverter != "" {
		if _, ok := rowConverters[c.Bulk.RowConverter]; !ok {
			errs = append(errs, fmt.Sprintf("bulk.row_converter %q is not a known converter", c.Bulk.RowConverter))
		}
	}
	require(c.Capabilities.Upserter == (c.Upsert.Strategy != ""),
		"capabilities.upserter must match presence of upsert.strategy")
	if c.Upsert.Strategy != "" {
		if _, ok := upsertStrategies[c.Upsert.Strategy]; !ok {
			errs = append(errs, fmt.Sprintf("upsert.strategy %q is not a known strategy", c.Upsert.Strategy))
		}
	}
	require(c.Capabilities.SequenceResetter == (c.Sequence.Strategy != ""),
		"capabilities.sequence_resetter must match presence of sequence.strategy")
	if c.Sequence.Strategy != "" {
		if _, ok := sequenceStrategies[c.Sequence.Strategy]; !ok {
			errs = append(errs, fmt.Sprintf("sequence.strategy %q is not a known strategy", c.Sequence.Strategy))
		}
	}
	require(len(c.DDL.DropTableStmts) > 0, "ddl.drop_table_stmts is required")
	require(len(c.DDL.TruncateStmts) > 0, "ddl.truncate_stmts is required")
	require(c.DDL.AddColumn != "", "ddl.add_column is required")
	require(c.Context.VersionQuery != "", "context.version_query is required")
	if c.Preflight != "" {
		if _, ok := preflightStrategies[c.Preflight]; !ok {
			errs = append(errs, fmt.Sprintf("preflight_strategy %q is not a known strategy", c.Preflight))
		}
	}
	require(c.ValueConverters == "" || c.ValueConverters == "default",
		"value_converters: only the \"default\" strategy exists yet")

	if len(errs) > 0 {
		return fmt.Errorf("invalid catalog:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}
