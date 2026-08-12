package drift

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

// TableSnapshot is the persisted source schema shape for one table. It
// deliberately excludes row counts and samples; only structural metadata belongs
// in the run-to-run drift baseline.
type TableSnapshot struct {
	Schema           string                    `json:"schema"`
	Name             string                    `json:"name"`
	Columns          []ColumnSnapshot          `json:"columns"`
	PrimaryKey       []string                  `json:"primary_key,omitempty"`
	Indexes          []IndexSnapshot           `json:"indexes,omitempty"`
	ForeignKeys      []ForeignKeySnapshot      `json:"foreign_keys,omitempty"`
	CheckConstraints []CheckConstraintSnapshot `json:"check_constraints,omitempty"`
}

type ColumnSnapshot struct {
	Name            string `json:"name"`
	DataType        string `json:"data_type"`
	MaxLength       int    `json:"max_length,omitempty"`
	Precision       int    `json:"precision,omitempty"`
	Scale           int    `json:"scale,omitempty"`
	IsNullable      bool   `json:"is_nullable"`
	IsIdentity      bool   `json:"is_identity,omitempty"`
	DefaultValue    string `json:"default_value,omitempty"`
	OrdinalPosition int    `json:"ordinal_position,omitempty"`
	SRID            int    `json:"srid,omitempty"`
}

type IndexSnapshot struct {
	Name           string   `json:"name"`
	Columns        []string `json:"columns,omitempty"`
	IsUnique       bool     `json:"is_unique,omitempty"`
	IsClustered    bool     `json:"is_clustered,omitempty"`
	IncludeColumns []string `json:"include_columns,omitempty"`
	Filter         string   `json:"filter,omitempty"`
}

type ForeignKeySnapshot struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns,omitempty"`
	RefSchema  string   `json:"ref_schema,omitempty"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns,omitempty"`
	OnDelete   string   `json:"on_delete,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
}

type CheckConstraintSnapshot struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

func BuildTableSnapshots(tables []driver.Table) []TableSnapshot {
	snapshots := make([]TableSnapshot, 0, len(tables))
	for _, table := range tables {
		snapshots = append(snapshots, BuildTableSnapshot(table))
	}
	sort.SliceStable(snapshots, func(i, j int) bool {
		return snapshots[i].key() < snapshots[j].key()
	})
	return snapshots
}

func BuildTableSnapshot(table driver.Table) TableSnapshot {
	snapshot := TableSnapshot{
		Schema:     table.Schema,
		Name:       table.Name,
		PrimaryKey: append([]string(nil), table.PrimaryKey...),
	}

	snapshot.Columns = make([]ColumnSnapshot, 0, len(table.Columns))
	for _, column := range table.Columns {
		snapshot.Columns = append(snapshot.Columns, ColumnSnapshot{
			Name:            column.Name,
			DataType:        column.DataType,
			MaxLength:       column.MaxLength,
			Precision:       column.Precision,
			Scale:           column.Scale,
			IsNullable:      column.IsNullable,
			IsIdentity:      column.IsIdentity,
			DefaultValue:    normalizeDefaultValue(column),
			OrdinalPosition: column.OrdinalPos,
			SRID:            column.SRID,
		})
	}
	sort.SliceStable(snapshot.Columns, func(i, j int) bool {
		left, right := snapshot.Columns[i], snapshot.Columns[j]
		if left.OrdinalPosition != right.OrdinalPosition {
			return left.OrdinalPosition < right.OrdinalPosition
		}
		return left.Name < right.Name
	})

	snapshot.Indexes = make([]IndexSnapshot, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		snapshot.Indexes = append(snapshot.Indexes, IndexSnapshot{
			Name:           index.Name,
			Columns:        append([]string(nil), index.Columns...),
			IsUnique:       index.IsUnique,
			IsClustered:    index.IsClustered,
			IncludeColumns: append([]string(nil), index.IncludeCols...),
			Filter:         index.Filter,
		})
	}
	sort.SliceStable(snapshot.Indexes, func(i, j int) bool {
		return snapshot.Indexes[i].Name < snapshot.Indexes[j].Name
	})

	snapshot.ForeignKeys = make([]ForeignKeySnapshot, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		snapshot.ForeignKeys = append(snapshot.ForeignKeys, ForeignKeySnapshot{
			Name:       fk.Name,
			Columns:    append([]string(nil), fk.Columns...),
			RefSchema:  fk.RefSchema,
			RefTable:   fk.RefTable,
			RefColumns: append([]string(nil), fk.RefColumns...),
			OnDelete:   fk.OnDelete,
			OnUpdate:   fk.OnUpdate,
		})
	}
	sort.SliceStable(snapshot.ForeignKeys, func(i, j int) bool {
		return snapshot.ForeignKeys[i].Name < snapshot.ForeignKeys[j].Name
	})

	snapshot.CheckConstraints = make([]CheckConstraintSnapshot, 0, len(table.CheckConstraints))
	for _, check := range table.CheckConstraints {
		snapshot.CheckConstraints = append(snapshot.CheckConstraints, CheckConstraintSnapshot{
			Name:       check.Name,
			Definition: check.Definition,
		})
	}
	sort.SliceStable(snapshot.CheckConstraints, func(i, j int) bool {
		return snapshot.CheckConstraints[i].Name < snapshot.CheckConstraints[j].Name
	})

	return snapshot
}

func MarshalTableSnapshot(snapshot TableSnapshot) (string, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func UnmarshalTableSnapshot(schemaJSON string) (TableSnapshot, error) {
	var snapshot TableSnapshot
	err := json.Unmarshal([]byte(schemaJSON), &snapshot)
	return snapshot, err
}

func (s TableSnapshot) key() string {
	return tableKey(s.Schema, s.Name)
}

func tableKey(schema, name string) string {
	return schema + "\x00" + name
}

func normalizeDefaultValue(column driver.Column) string {
	value := strings.TrimSpace(column.DefaultValue)
	if column.IsIdentity && strings.HasPrefix(strings.ToLower(value), "nextval(") {
		return "nextval"
	}
	return value
}
