package source

// Table represents a source table's metadata
type Table struct {
	Schema     string   `json:"schema"`
	Name       string   `json:"name"`
	Columns    []Column `json:"columns"`
	PrimaryKey []string `json:"primary_key"`
	RowCount   int64    `json:"row_count"`
}

// FullName returns schema.table format
func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

// IsLarge returns true if the table exceeds the threshold
func (t *Table) IsLarge(threshold int64) bool {
	return t.RowCount > threshold
}

// HasSinglePK returns true if table has a single-column primary key
func (t *Table) HasSinglePK() bool {
	return len(t.PrimaryKey) == 1
}

// Column represents a table column's metadata
type Column struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	MaxLength  int    `json:"max_length"`
	Precision  int    `json:"precision"`
	Scale      int    `json:"scale"`
	IsNullable bool   `json:"is_nullable"`
	IsIdentity bool   `json:"is_identity"`
	OrdinalPos int    `json:"ordinal_position"`
}

// Partition represents a portion of a large table for parallel transfer
type Partition struct {
	TableName   string `json:"table_name"`
	PartitionID int    `json:"partition_id"`
	MinPK       any    `json:"min_pk"`
	MaxPK       any    `json:"max_pk"`
	RowCount    int64  `json:"row_count"`
}
