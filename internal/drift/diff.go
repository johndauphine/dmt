package drift

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type ChangeKind string

const (
	TableAdded        ChangeKind = "table_added"
	TableDropped      ChangeKind = "table_dropped"
	AddedColumn       ChangeKind = "added_column"
	DroppedColumn     ChangeKind = "dropped_column"
	TypeWidened       ChangeKind = "type_widened"
	TypeNarrowed      ChangeKind = "type_narrowed"
	TypeChangedLossy  ChangeKind = "type_changed_lossy"
	NullabilityChange ChangeKind = "nullability_change"
	DefaultChange     ChangeKind = "default_change"
	PKChange          ChangeKind = "pk_change"
	IndexAdded        ChangeKind = "index_added"
	IndexDropped      ChangeKind = "index_dropped"
	FKAdded           ChangeKind = "fk_added"
	FKDropped         ChangeKind = "fk_dropped"
	CheckAdded        ChangeKind = "check_added"
	CheckDropped      ChangeKind = "check_dropped"
)

type Change struct {
	Kind       ChangeKind `json:"kind"`
	Schema     string     `json:"schema,omitempty"`
	TableName  string     `json:"table_name"`
	ObjectName string     `json:"object_name,omitempty"`
	Previous   string     `json:"previous,omitempty"`
	Current    string     `json:"current,omitempty"`
}

type Report struct {
	Changes []Change `json:"changes"`
}

func Compare(previous, current []TableSnapshot) Report {
	prevByTable := snapshotsByTable(previous)
	currByTable := snapshotsByTable(current)

	var report Report
	for _, key := range sortedKeys(prevByTable) {
		if _, ok := currByTable[key]; !ok {
			table := prevByTable[key]
			report.add(TableDropped, table, table.Name, "", "")
		}
	}
	for _, key := range sortedKeys(currByTable) {
		currTable := currByTable[key]
		prevTable, ok := prevByTable[key]
		if !ok {
			report.add(TableAdded, currTable, currTable.Name, "", "")
			continue
		}
		compareTable(&report, prevTable, currTable)
	}
	return report
}

func compareTable(report *Report, previous, current TableSnapshot) {
	compareColumns(report, previous, current)
	if !reflect.DeepEqual(previous.PrimaryKey, current.PrimaryKey) {
		report.add(PKChange, current, "primary_key", strings.Join(previous.PrimaryKey, ", "), strings.Join(current.PrimaryKey, ", "))
	}
	compareNamedObjects(report, previous, current, previous.indexMap(), current.indexMap(), IndexDropped, IndexAdded)
	compareNamedObjects(report, previous, current, previous.fkMap(), current.fkMap(), FKDropped, FKAdded)
	compareNamedObjects(report, previous, current, previous.checkMap(), current.checkMap(), CheckDropped, CheckAdded)
}

func compareColumns(report *Report, previous, current TableSnapshot) {
	prevCols := previous.columnMap()
	currCols := current.columnMap()

	for _, name := range sortedKeys(prevCols) {
		if _, ok := currCols[name]; !ok {
			col := prevCols[name]
			report.add(DroppedColumn, previous, name, columnSignature(col), "")
		}
	}
	for _, name := range sortedKeys(currCols) {
		currCol := currCols[name]
		prevCol, ok := prevCols[name]
		if !ok {
			report.add(AddedColumn, current, name, "", columnSignature(currCol))
			continue
		}

		if !sameType(prevCol, currCol) {
			report.add(classifyTypeChange(prevCol, currCol), current, name, typeSignature(prevCol), typeSignature(currCol))
		}
		if prevCol.IsNullable != currCol.IsNullable {
			report.add(NullabilityChange, current, name, nullability(prevCol), nullability(currCol))
		}
		if prevCol.DefaultValue != currCol.DefaultValue {
			report.add(DefaultChange, current, name, prevCol.DefaultValue, currCol.DefaultValue)
		}
	}
}

func compareNamedObjects[T comparable](
	report *Report,
	previous TableSnapshot,
	current TableSnapshot,
	prevObjects map[string]T,
	currObjects map[string]T,
	dropped ChangeKind,
	added ChangeKind,
) {
	for _, name := range sortedKeys(prevObjects) {
		curr, ok := currObjects[name]
		prev := prevObjects[name]
		if !ok {
			report.add(dropped, previous, name, fmt.Sprint(prev), "")
			continue
		}
		if curr != prev {
			report.add(dropped, previous, name, fmt.Sprint(prev), "")
			report.add(added, current, name, "", fmt.Sprint(curr))
		}
	}
	for _, name := range sortedKeys(currObjects) {
		if _, ok := prevObjects[name]; !ok {
			report.add(added, current, name, "", fmt.Sprint(currObjects[name]))
		}
	}
}

func (r *Report) add(kind ChangeKind, table TableSnapshot, objectName, previous, current string) {
	r.Changes = append(r.Changes, Change{
		Kind:       kind,
		Schema:     table.Schema,
		TableName:  table.Name,
		ObjectName: objectName,
		Previous:   previous,
		Current:    current,
	})
}

func snapshotsByTable(snapshots []TableSnapshot) map[string]TableSnapshot {
	out := make(map[string]TableSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		out[snapshot.key()] = snapshot
	}
	return out
}

func (s TableSnapshot) columnMap() map[string]ColumnSnapshot {
	out := make(map[string]ColumnSnapshot, len(s.Columns))
	for _, column := range s.Columns {
		out[column.Name] = column
	}
	return out
}

func (s TableSnapshot) indexMap() map[string]string {
	out := make(map[string]string, len(s.Indexes))
	for _, index := range s.Indexes {
		out[index.Name] = indexSignature(index)
	}
	return out
}

func (s TableSnapshot) fkMap() map[string]string {
	out := make(map[string]string, len(s.ForeignKeys))
	for _, fk := range s.ForeignKeys {
		out[fk.Name] = fkSignature(fk)
	}
	return out
}

func (s TableSnapshot) checkMap() map[string]string {
	out := make(map[string]string, len(s.CheckConstraints))
	for _, check := range s.CheckConstraints {
		out[check.Name] = check.Definition
	}
	return out
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
