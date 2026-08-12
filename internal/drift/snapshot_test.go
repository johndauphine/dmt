package drift

import (
	"testing"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

func TestBuildTableSnapshotDeterministic(t *testing.T) {
	table := driver.Table{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []driver.Column{
			{Name: "name", DataType: "varchar", MaxLength: 100, IsNullable: true, OrdinalPos: 2},
			{Name: "id", DataType: "int", OrdinalPos: 1, IsIdentity: true},
		},
		Indexes: []driver.Index{
			{Name: "ix_users_name", Columns: []string{"name"}},
			{Name: "ix_users_id", Columns: []string{"id"}, IsUnique: true},
		},
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk_users_org", Columns: []string{"org_id"}, RefSchema: "dbo", RefTable: "Orgs", RefColumns: []string{"id"}},
		},
		CheckConstraints: []driver.CheckConstraint{
			{Name: "ck_users_name", Definition: "name <> ''"},
		},
	}

	snapshot := BuildTableSnapshot(table)
	if got := snapshot.Columns[0].Name; got != "id" {
		t.Fatalf("first column = %q, want id", got)
	}
	if got := snapshot.Indexes[0].Name; got != "ix_users_id" {
		t.Fatalf("first index = %q, want ix_users_id", got)
	}

	first, err := MarshalTableSnapshot(snapshot)
	if err != nil {
		t.Fatalf("MarshalTableSnapshot: %v", err)
	}
	second, err := MarshalTableSnapshot(BuildTableSnapshot(table))
	if err != nil {
		t.Fatalf("MarshalTableSnapshot second: %v", err)
	}
	if first != second {
		t.Fatalf("snapshot JSON is not deterministic:\nfirst:  %s\nsecond: %s", first, second)
	}

	roundTrip, err := UnmarshalTableSnapshot(first)
	if err != nil {
		t.Fatalf("UnmarshalTableSnapshot: %v", err)
	}
	if roundTrip.Name != "Users" || len(roundTrip.Columns) != 2 {
		t.Fatalf("roundTrip = %+v, want Users with 2 columns", roundTrip)
	}
}

func TestBuildTableSnapshotNormalizesSerialDefault(t *testing.T) {
	snapshot := BuildTableSnapshot(driver.Table{
		Schema: "public",
		Name:   "users",
		Columns: []driver.Column{{
			Name:         "id",
			DataType:     "int4",
			IsIdentity:   true,
			DefaultValue: "nextval('users_id_seq'::regclass)",
			OrdinalPos:   1,
		}},
	})

	if got := snapshot.Columns[0].DefaultValue; got != "nextval" {
		t.Fatalf("DefaultValue = %q, want normalized nextval", got)
	}
}
