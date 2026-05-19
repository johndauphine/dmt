package shared

import (
	"context"
	"reflect"
	"testing"
)

func TestQuerySampleColumnValues(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	if _, err := ExecRaw(ctx, db, `CREATE TABLE samples (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ExecRaw(ctx, db, `INSERT INTO samples (value) VALUES (?), (NULL), (?)`, "Ada", ""); err != nil {
		t.Fatalf("insert samples: %v", err)
	}

	got, err := QuerySampleColumnValues(ctx, db, `SELECT value FROM samples ORDER BY id`)
	if err != nil {
		t.Fatalf("QuerySampleColumnValues returned error: %v", err)
	}

	want := []string{"Ada", ""}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QuerySampleColumnValues() = %#v, want %#v", got, want)
	}
}

func TestQuerySampleRows(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	if _, err := ExecRaw(ctx, db, `CREATE TABLE people (name TEXT, city TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ExecRaw(ctx, db,
		`INSERT INTO people (name, city) VALUES (?, ?), (?, ?), (NULL, ?)`,
		"Ada", "", "Grace", "London", "Paris"); err != nil {
		t.Fatalf("insert people: %v", err)
	}

	got, err := QuerySampleRows(ctx, db, `SELECT name, city FROM people`, []string{"name", "city"}, 3)
	if err != nil {
		t.Fatalf("QuerySampleRows returned error: %v", err)
	}

	want := map[string][]string{
		"name": {"Ada", "Grace"},
		"city": {"London", "Paris"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QuerySampleRows() = %#v, want %#v", got, want)
	}
}
