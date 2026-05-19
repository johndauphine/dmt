package drift

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestCompareCategorizesSchemaDrift(t *testing.T) {
	base := []TableSnapshot{baseUsersSnapshot()}

	tests := []struct {
		name     string
		previous []TableSnapshot
		current  []TableSnapshot
		want     []ChangeKind
	}{
		{
			name:     "table added",
			previous: nil,
			current:  base,
			want:     []ChangeKind{TableAdded},
		},
		{
			name:     "table dropped",
			previous: base,
			current:  nil,
			want:     []ChangeKind{TableDropped},
		},
		{
			name:     "column added",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns = append(s.Columns, ColumnSnapshot{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true})
			}),
			want: []ChangeKind{AddedColumn},
		},
		{
			name:     "column dropped",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns = s.Columns[:1]
			}),
			want: []ChangeKind{DroppedColumn},
		},
		{
			name:     "type widened",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].MaxLength = 500
			}),
			want: []ChangeKind{TypeWidened},
		},
		{
			name: "integer aliases are equivalent",
			previous: mutate(base, func(s *TableSnapshot) {
				s.Columns[0].DataType = "int"
			}),
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[0].DataType = "integer"
			}),
			want: []ChangeKind{},
		},
		{
			name: "decimal mixed range change is lossy",
			previous: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DataType = "numeric"
				s.Columns[1].MaxLength = 0
				s.Columns[1].Precision = 10
				s.Columns[1].Scale = 2
			}),
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DataType = "numeric"
				s.Columns[1].MaxLength = 0
				s.Columns[1].Precision = 11
				s.Columns[1].Scale = 4
			}),
			want: []ChangeKind{TypeChangedLossy},
		},
		{
			name: "float widened",
			previous: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DataType = "real"
				s.Columns[1].MaxLength = 0
			}),
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DataType = "double precision"
				s.Columns[1].MaxLength = 0
			}),
			want: []ChangeKind{TypeWidened},
		},
		{
			name:     "type narrowed",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].MaxLength = 50
			}),
			want: []ChangeKind{TypeNarrowed},
		},
		{
			name: "unbounded string narrowed",
			previous: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].MaxLength = 0
			}),
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].MaxLength = 50
			}),
			want: []ChangeKind{TypeNarrowed},
		},
		{
			name:     "type changed lossy",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DataType = "int"
				s.Columns[1].MaxLength = 0
			}),
			want: []ChangeKind{TypeChangedLossy},
		},
		{
			name:     "nullability changed",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].IsNullable = false
			}),
			want: []ChangeKind{NullabilityChange},
		},
		{
			name:     "default changed",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Columns[1].DefaultValue = "'unknown'"
			}),
			want: []ChangeKind{DefaultChange},
		},
		{
			name:     "primary key changed",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.PrimaryKey = []string{"id", "name"}
			}),
			want: []ChangeKind{PKChange},
		},
		{
			name:     "index added",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.Indexes = append(s.Indexes, IndexSnapshot{Name: "ix_users_name", Columns: []string{"name"}})
			}),
			want: []ChangeKind{IndexAdded},
		},
		{
			name:     "foreign key dropped",
			previous: base,
			current: mutate(base, func(s *TableSnapshot) {
				s.ForeignKeys = nil
			}),
			want: []ChangeKind{FKDropped},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Compare(tt.previous, tt.current)
			if got := changeKinds(report); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("kinds = %v, want %v\nreport:\n%s", got, tt.want, report.Format())
			}
		})
	}
}

func TestReportFormatGroupsTables(t *testing.T) {
	report := Compare(nil, []TableSnapshot{baseUsersSnapshot()})
	formatted := report.Format()
	for _, want := range []string{
		"Schema drift detected (1 table affected):",
		"dbo.Users:",
		"+ added table",
		"No automatic schema alignment will be applied",
	} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("Format() missing %q:\n%s", want, formatted)
		}
	}
}

func baseUsersSnapshot() TableSnapshot {
	return TableSnapshot{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []ColumnSnapshot{
			{Name: "id", DataType: "int", IsNullable: false, OrdinalPosition: 1},
			{Name: "name", DataType: "varchar", MaxLength: 100, IsNullable: true, OrdinalPosition: 2},
		},
		Indexes: []IndexSnapshot{
			{Name: "ix_users_id", Columns: []string{"id"}, IsUnique: true},
		},
		ForeignKeys: []ForeignKeySnapshot{
			{Name: "fk_users_org", Columns: []string{"org_id"}, RefSchema: "dbo", RefTable: "Orgs", RefColumns: []string{"id"}},
		},
	}
}

func mutate(snapshots []TableSnapshot, fn func(*TableSnapshot)) []TableSnapshot {
	data, _ := json.Marshal(snapshots)
	var cloned []TableSnapshot
	_ = json.Unmarshal(data, &cloned)
	fn(&cloned[0])
	return cloned
}

func changeKinds(report Report) []ChangeKind {
	kinds := make([]ChangeKind, len(report.Changes))
	for i, change := range report.Changes {
		kinds[i] = change.Kind
	}
	return kinds
}
