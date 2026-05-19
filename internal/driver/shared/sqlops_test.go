package shared

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"
)

func TestExecRawAndQueryRowRaw(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	if _, err := ExecRaw(ctx, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("ExecRaw create table: %v", err)
	}

	affected, err := ExecRaw(ctx, db, `INSERT INTO users (name) VALUES (?)`, "Ada")
	if err != nil {
		t.Fatalf("ExecRaw insert: %v", err)
	}
	if affected != 1 {
		t.Fatalf("ExecRaw affected rows = %d, want 1", affected)
	}

	var name string
	if err := QueryRowRaw(ctx, db, `SELECT name FROM users WHERE id = ?`, &name, 1); err != nil {
		t.Fatalf("QueryRowRaw: %v", err)
	}
	if name != "Ada" {
		t.Fatalf("QueryRowRaw name = %q, want Ada", name)
	}
}

func TestExactRowCountQueryWithSuffix(t *testing.T) {
	got, err := ExactRowCountQueryWithSuffix(bracketDialect{}, "dbo", "Users", " WITH (NOLOCK)")
	if err != nil {
		t.Fatalf("ExactRowCountQueryWithSuffix returned error: %v", err)
	}

	want := "SELECT COUNT(*) FROM [dbo].[Users] WITH (NOLOCK)"
	if got != want {
		t.Fatalf("ExactRowCountQueryWithSuffix() = %q, want %q", got, want)
	}
}

func TestExactRowCount(t *testing.T) {
	ctx := context.Background()
	db := openSharedSQLite(t)

	if _, err := ExecRaw(ctx, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ExecRaw(ctx, db, `INSERT INTO users (name) VALUES (?), (?)`, "Ada", "Grace"); err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	got, err := ExactRowCount(ctx, db, dollarDialect{}, "", "users")
	if err != nil {
		t.Fatalf("ExactRowCount returned error: %v", err)
	}
	if got != 2 {
		t.Fatalf("ExactRowCount() = %d, want 2", got)
	}
}

func TestRowCountWithFallback(t *testing.T) {
	sentinel := errors.New("fast count unavailable")

	tests := []struct {
		name           string
		fastCount      RowCountFunc
		want           int64
		wantExactCalls int
	}{
		{
			name: "uses fast nonzero count",
			fastCount: func() (int64, error) {
				return 42, nil
			},
			want: 42,
		},
		{
			name: "falls back on fast error",
			fastCount: func() (int64, error) {
				return 0, sentinel
			},
			want:           7,
			wantExactCalls: 1,
		},
		{
			name: "falls back on zero count",
			fastCount: func() (int64, error) {
				return 0, nil
			},
			want:           7,
			wantExactCalls: 1,
		},
		{
			name:           "uses exact when fast is nil",
			want:           7,
			wantExactCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exactCalls := 0
			got, err := RowCountWithFallback(tt.fastCount, func() (int64, error) {
				exactCalls++
				return 7, nil
			})
			if err != nil {
				t.Fatalf("RowCountWithFallback returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("RowCountWithFallback() = %d, want %d", got, tt.want)
			}
			if exactCalls != tt.wantExactCalls {
				t.Fatalf("exact count calls = %d, want %d", exactCalls, tt.wantExactCalls)
			}
		})
	}
}

func openSharedSQLite(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
