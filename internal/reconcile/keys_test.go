package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	drvsqlite "github.com/johndauphine/dmt/internal/driver/sqlite"
	_ "modernc.org/sqlite"
)

func TestScanKeys(t *testing.T) {
	db := openKeyTestDB(t)
	execKeyTestSQL(t, db, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT);
		INSERT INTO items (id, code) VALUES (2, 'b'), (1, 'a'), (3, 'c');
	`)

	var got [][]any
	err := ScanKeys(context.Background(), db, &drvsqlite.Dialect{}, "", "items", []string{"id"}, 2,
		func(keys [][]any) error {
			got = append(got, copyKeys(keys)...)
			return nil
		})
	if err != nil {
		t.Fatalf("ScanKeys() error: %v", err)
	}

	want := [][]any{{int64(1)}, {int64(2)}, {int64(3)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanKeys() = %#v, want %#v", got, want)
	}
}

func TestFindTargetOnlyKeys(t *testing.T) {
	source := openKeyTestDB(t)
	target := openKeyTestDB(t)
	execKeyTestSQL(t, source, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (3);
	`)
	execKeyTestSQL(t, target, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (2), (3), (4);
	`)

	var batches [][][]any
	missing, err := FindTargetOnlyKeys(
		context.Background(),
		source, target,
		&drvsqlite.Dialect{}, &drvsqlite.Dialect{},
		KeyDiffOptions{
			Table:      "items",
			KeyColumns: []string{"id"},
			BatchSize:  1,
		},
		func(keys [][]any) error {
			batches = append(batches, copyKeys(keys))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("FindTargetOnlyKeys() error: %v", err)
	}
	if missing != 2 {
		t.Fatalf("missing = %d, want 2", missing)
	}

	want := [][][]any{{{int64(2)}}, {{int64(4)}}}
	if !reflect.DeepEqual(batches, want) {
		t.Fatalf("missing batches = %#v, want %#v", batches, want)
	}
}

func TestDeleteKeys(t *testing.T) {
	db := openKeyTestDB(t)
	execKeyTestSQL(t, db, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT);
		INSERT INTO items (id, code) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
	`)

	deleted, err := DeleteKeys(
		context.Background(),
		db,
		&drvsqlite.Dialect{},
		"", "items",
		[]string{"id"},
		[][]any{{int64(2)}, {int64(4)}},
		1,
	)
	if err != nil {
		t.Fatalf("DeleteKeys() error: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}

	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&remaining); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("remaining rows = %d, want 2", remaining)
	}
}

func TestDeleteKeysHonorsCanceledContext(t *testing.T) {
	db := openKeyTestDB(t)
	execKeyTestSQL(t, db, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1);
	`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deleted, err := DeleteKeys(
		ctx,
		db,
		&drvsqlite.Dialect{},
		"", "items",
		[]string{"id"},
		[][]any{{int64(1)}},
		1,
	)
	if err == nil {
		t.Fatal("DeleteKeys() error = nil, want canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteKeys() error = %v, want context.Canceled", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0 after canceled context", deleted)
	}
}

func TestKeyFingerprintNormalizesCommonDriverValues(t *testing.T) {
	stringKey, err := KeyFingerprint([]any{"abc", int64(42)})
	if err != nil {
		t.Fatalf("KeyFingerprint(string) error: %v", err)
	}
	byteKey, err := KeyFingerprint([]any{[]byte("abc"), int(42)})
	if err != nil {
		t.Fatalf("KeyFingerprint(bytes) error: %v", err)
	}
	if stringKey != byteKey {
		t.Fatalf("fingerprints differ for equivalent values: %q != %q", stringKey, byteKey)
	}

	if _, err := KeyFingerprint([]any{nil}); err == nil {
		t.Fatal("KeyFingerprint(nil) error = nil, want error")
	}
}

func TestEffectiveDeleteBatchSize(t *testing.T) {
	tests := []struct {
		name     string
		dbType   string
		keys     int
		request  int
		expected int
	}{
		{name: "default", dbType: "postgres", keys: 1, expected: defaultKeyBatchSize},
		{name: "mssql single key", dbType: "mssql", keys: 1, request: 10000, expected: 2000},
		{name: "mssql composite key", dbType: "mssql", keys: 3, request: 10000, expected: 666},
		{name: "small requested", dbType: "postgres", keys: 1, request: 25, expected: 25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EffectiveDeleteBatchSize(tt.dbType, tt.keys, tt.request)
			if got != tt.expected {
				t.Fatalf("EffectiveDeleteBatchSize() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func openKeyTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func execKeyTestSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec SQL: %v", err)
	}
}

func copyKeys(keys [][]any) [][]any {
	out := make([][]any, len(keys))
	for i, key := range keys {
		out[i] = append([]any(nil), key...)
	}
	return out
}
