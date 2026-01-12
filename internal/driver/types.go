package driver

import (
	"fmt"
	"strings"
)

// Table represents a database table with its metadata.
type Table struct {
	Schema           string            `json:"schema"`
	Name             string            `json:"name"`
	Columns          []Column          `json:"columns"`
	PrimaryKey       []string          `json:"primary_key"`
	PKColumns        []Column          `json:"pk_columns"` // Full column metadata for PKs
	RowCount         int64             `json:"row_count"`
	EstimatedRowSize int64             `json:"estimated_row_size"` // Average bytes per row from system stats
	DateColumn       string            `json:"date_column,omitempty"`
	DateColumnType   string            `json:"date_column_type,omitempty"`
	Indexes          []Index           `json:"indexes"`
	ForeignKeys      []ForeignKey      `json:"foreign_keys"`
	CheckConstraints []CheckConstraint `json:"check_constraints"`
}

// FullName returns the fully qualified table name (schema.table).
func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

// HasPK returns true if the table has a primary key.
func (t *Table) HasPK() bool {
	return len(t.PrimaryKey) > 0
}

// HasSinglePK returns true if table has a single-column primary key.
func (t *Table) HasSinglePK() bool {
	return len(t.PrimaryKey) == 1
}

// IsLarge returns true if the table exceeds the large table threshold.
func (t *Table) IsLarge(threshold int64) bool {
	return t.RowCount > threshold
}

// PopulatePKColumns fills PKColumns with full column metadata from Columns.
// Call this after both PrimaryKey and Columns are populated.
func (t *Table) PopulatePKColumns() {
	t.PKColumns = nil // Reset
	for _, pkCol := range t.PrimaryKey {
		for _, col := range t.Columns {
			if col.Name == pkCol {
				t.PKColumns = append(t.PKColumns, col)
				break
			}
		}
	}
}

// SupportsKeysetPagination returns true if the table can use keyset pagination.
// This requires a single-column integer primary key.
func (t *Table) SupportsKeysetPagination() bool {
	if len(t.PKColumns) != 1 {
		return false
	}
	pkType := strings.ToLower(t.PKColumns[0].DataType)
	// SQL Server types
	if pkType == "int" || pkType == "bigint" || pkType == "smallint" || pkType == "tinyint" {
		return true
	}
	// PostgreSQL types (data_type names)
	if pkType == "integer" || pkType == "serial" || pkType == "bigserial" || pkType == "smallserial" {
		return true
	}
	// PostgreSQL internal types (udt_name values)
	if pkType == "int4" || pkType == "int8" || pkType == "int2" {
		return true
	}
	return false
}

// GetPKColumn returns the PK column metadata if single-column PK.
func (t *Table) GetPKColumn() *Column {
	if len(t.PKColumns) == 1 {
		return &t.PKColumns[0]
	}
	return nil
}

// GetName returns the table name (implements target.TableInfo interface).
func (t *Table) GetName() string {
	return t.Name
}

// GetColumnNames returns a slice of column names (implements target.TableInfo interface).
func (t *Table) GetColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}
	return names
}

// Column represents a table column.
type Column struct {
	Name         string   `json:"name"`
	DataType     string   `json:"data_type"`
	MaxLength    int      `json:"max_length"`
	Precision    int      `json:"precision"`
	Scale        int      `json:"scale"`
	IsNullable   bool     `json:"is_nullable"`
	IsIdentity   bool     `json:"is_identity"`
	OrdinalPos   int      `json:"ordinal_position"`
	SRID         int      `json:"srid,omitempty"`         // Spatial Reference ID for geography/geometry columns (0 = default/unset)
	SampleValues []string `json:"sample_values,omitempty"` // Sample data values for AI type mapping context
}

// IsIntegerType returns true if the column is an integer type.
func (c *Column) IsIntegerType() bool {
	switch c.DataType {
	case "int", "integer", "bigint", "smallint", "tinyint",
		"int2", "int4", "int8", "serial", "bigserial", "smallserial":
		return true
	}
	return false
}

// IsSpatialType returns true if the column is a spatial type.
func (c *Column) IsSpatialType() bool {
	switch c.DataType {
	case "geography", "geometry":
		return true
	}
	return false
}

// Partition represents a data partition for parallel processing.
type Partition struct {
	TableName   string `json:"table_name"`
	PartitionID int    `json:"partition_id"`
	MinPK       any    `json:"min_pk"`    // For keyset pagination
	MaxPK       any    `json:"max_pk"`    // For keyset pagination
	StartRow    int64  `json:"start_row"` // For ROW_NUMBER pagination (0-indexed)
	EndRow      int64  `json:"end_row"`   // For ROW_NUMBER pagination (exclusive)
	RowCount    int64  `json:"row_count"`
}

// Index represents a table index.
type Index struct {
	Name        string   `json:"name"`
	Columns     []string `json:"columns"`
	IsUnique    bool     `json:"is_unique"`
	IsClustered bool     `json:"is_clustered"`
	IncludeCols []string `json:"include_cols"` // Non-key included columns (covering index)
	Filter      string   `json:"filter"`       // Filter expression (filtered index)
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefTable   string   `json:"ref_table"`
	RefSchema  string   `json:"ref_schema"`
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete"` // CASCADE, SET NULL, NO ACTION, etc.
	OnUpdate   string   `json:"on_update"`
}

// CheckConstraint represents a check constraint.
type CheckConstraint struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

// ValidateIdentifier checks if a database identifier (schema, table, column name)
// is safe to use in SQL queries. Returns an error if the identifier contains
// potentially dangerous characters that could enable SQL injection.
//
// Valid identifiers:
// - Start with letter or underscore
// - Contain only letters, digits, underscores, and spaces (spaces allowed for SQL Server)
// - Maximum length of 128 characters (SQL Server limit)
// - Not empty
func ValidateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}

	if len(name) > 128 {
		return fmt.Errorf("identifier too long: %d characters (max 128)", len(name))
	}

	// Check first character: must be letter or underscore
	first := rune(name[0])
	if !isValidIdentifierStart(first) {
		return fmt.Errorf("identifier must start with letter or underscore: %q", name)
	}

	// Check remaining characters
	for i, r := range name {
		if i == 0 {
			continue // Already checked
		}
		if !isValidIdentifierChar(r) {
			return fmt.Errorf("identifier contains invalid character %q at position %d: %q", r, i, name)
		}
	}

	return nil
}

// isValidIdentifierStart returns true if r is a valid first character for an identifier.
func isValidIdentifierStart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

// isValidIdentifierChar returns true if r is valid anywhere in an identifier.
func isValidIdentifierChar(r rune) bool {
	return isValidIdentifierStart(r) ||
		(r >= '0' && r <= '9') ||
		r == ' ' || // SQL Server allows spaces in identifiers
		r == '$' || // PostgreSQL allows $ in identifiers
		r == '#' // SQL Server allows # for temp tables
}

// MustValidateIdentifier validates an identifier and panics if invalid.
// Use only when identifier comes from trusted source (e.g., INFORMATION_SCHEMA).
func MustValidateIdentifier(name string) {
	if err := ValidateIdentifier(name); err != nil {
		panic(fmt.Sprintf("invalid identifier: %v", err))
	}
}
