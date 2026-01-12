package driver

// Table represents a database table with its metadata.
type Table struct {
	Schema           string
	Name             string
	Columns          []Column
	PrimaryKey       []string
	RowCount         int64
	DateColumn       string
	DateColumnType   string
	Indexes          []Index
	ForeignKeys      []ForeignKey
	CheckConstraints []CheckConstraint
}

// FullName returns the fully qualified table name (schema.table).
func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

// HasPK returns true if the table has a primary key.
func (t *Table) HasPK() bool {
	return len(t.PrimaryKey) > 0
}

// IsLarge returns true if the table exceeds the large table threshold.
func (t *Table) IsLarge(threshold int64) bool {
	return t.RowCount > threshold
}

// SupportsKeysetPagination returns true if the table can use keyset pagination.
// This requires a single-column integer primary key.
func (t *Table) SupportsKeysetPagination() bool {
	if len(t.PrimaryKey) != 1 {
		return false
	}
	pkCol := t.PrimaryKey[0]
	for _, c := range t.Columns {
		if c.Name == pkCol {
			return c.IsIntegerType()
		}
	}
	return false
}

// Column represents a table column.
type Column struct {
	Name       string
	DataType   string
	MaxLength  int
	Precision  int
	Scale      int
	IsNullable bool
	IsIdentity bool
	OrdinalPos int
	SRID       int // Spatial reference ID for geography/geometry columns
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
	TableName   string
	PartitionID int
	MinPK       any
	MaxPK       any
	StartRow    int64
	EndRow      int64
	RowCount    int64
}

// Index represents a table index.
type Index struct {
	Name       string
	Columns    []string
	IsUnique   bool
	IsClustered bool
	Include    []string // Included columns (covering index)
	Filter     string   // Filter expression (filtered index)
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	Name             string
	Columns          []string
	RefTable         string
	RefSchema        string
	RefColumns       []string
	OnDelete         string
	OnUpdate         string
}

// CheckConstraint represents a check constraint.
type CheckConstraint struct {
	Name       string
	Definition string
}
