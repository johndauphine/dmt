package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	_ "github.com/johndauphine/dmt/internal/driver/generic" // register generic dialects
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"

	_ "modernc.org/sqlite"
)

type snapshotPage func(sourceQueryer) ([]string, error)

// TestStrictSnapshotKeepsEveryPaginationShapeStable drives the real dialect
// queries through a pinned SQLite read transaction. The writer commits between
// pages through a separate WAL connection; the second reader page must still
// see the original source version (#640).
func TestStrictSnapshotKeepsEveryPaginationShapeStable(t *testing.T) {
	dialect := driver.GetDialect("sqlite")
	if dialect == nil {
		t.Fatal("no sqlite dialect registered")
	}

	tests := []struct {
		name     string
		setup    []string
		first    snapshotPage
		mutate   func(*sql.DB) error
		second   snapshotPage
		wantPage []string
	}{
		{
			name: "keyset",
			setup: []string{
				`CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`,
				`INSERT INTO events VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')`,
			},
			first: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildKeysetQuery(`"id", "payload"`, "id", "", "events", "", false, false, nil)
				args := dialect.BuildKeysetArgs(int64(0), nil, 2, false, nil)
				return scanSnapshotPage(context.Background(), q, query, args...)
			},
			mutate: func(db *sql.DB) error {
				_, err := db.Exec(`UPDATE events SET payload = 'changed' WHERE id = 3`)
				return err
			},
			second: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildKeysetQuery(`"id", "payload"`, "id", "", "events", "", false, false, nil)
				args := dialect.BuildKeysetArgs(int64(2), nil, 2, false, nil)
				return scanSnapshotPage(context.Background(), q, query, args...)
			},
			wantPage: []string{"3:c", "4:d"},
		},
		{
			name: "tuple keyset",
			setup: []string{
				`CREATE TABLE events (a INTEGER NOT NULL, b INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (a, b))`,
				`INSERT INTO events VALUES (1,1,'a'),(1,2,'b'),(2,1,'c'),(2,2,'d')`,
			},
			first: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildCompositeKeysetQuery(`"a", "b", "payload"`, []string{"a", "b"}, "", "events", "", false, nil)
				args := dialect.BuildCompositeKeysetArgs(nil, 2, false, nil)
				return scanTupleSnapshotPage(context.Background(), q, query, args...)
			},
			mutate: func(db *sql.DB) error {
				_, err := db.Exec(`UPDATE events SET payload = 'changed' WHERE a = 2 AND b = 1`)
				return err
			},
			second: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildCompositeKeysetQuery(`"a", "b", "payload"`, []string{"a", "b"}, "", "events", "", true, nil)
				args := dialect.BuildCompositeKeysetArgs([]any{int64(1), int64(2)}, 2, true, nil)
				return scanTupleSnapshotPage(context.Background(), q, query, args...)
			},
			wantPage: []string{"2,1:c", "2,2:d"},
		},
		{
			name: "row number",
			setup: []string{
				`CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`,
				`INSERT INTO events VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')`,
			},
			first: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildRowNumberQuery(`"id", "payload"`, `"id"`, "", "events", "", nil)
				args := dialect.BuildRowNumberArgs(0, 2, nil)
				return scanSnapshotPage(context.Background(), q, query, args...)
			},
			mutate: func(db *sql.DB) error {
				if _, err := db.Exec(`DELETE FROM events WHERE id = 1`); err != nil {
					return err
				}
				_, err := db.Exec(`INSERT INTO events VALUES (5, 'e')`)
				return err
			},
			second: func(q sourceQueryer) ([]string, error) {
				query := dialect.BuildRowNumberQuery(`"id", "payload"`, `"id"`, "", "events", "", nil)
				args := dialect.BuildRowNumberArgs(2, 2, nil)
				return scanSnapshotPage(context.Background(), q, query, args...)
			},
			wantPage: []string{"3:c", "4:d"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader, writer := openSnapshotSQLite(t, tc.setup)
			defer reader.Close()
			defer writer.Close()

			ctx, release, err := beginStrictSourceSnapshot(context.Background(), &keysetRuntimeSourcePool{db: reader}, source.Table{Name: "events"})
			if err != nil {
				t.Fatalf("beginStrictSourceSnapshot: %v", err)
			}
			defer release()
			queryer := sourceQueryerFor(ctx, reader)

			if _, err := tc.first(queryer); err != nil {
				t.Fatalf("first snapshot page: %v", err)
			}
			if err := tc.mutate(writer); err != nil {
				t.Fatalf("concurrent mutation: %v", err)
			}
			got, err := tc.second(queryer)
			if err != nil {
				t.Fatalf("second snapshot page: %v", err)
			}
			if !reflect.DeepEqual(got, tc.wantPage) {
				t.Fatalf("second page = %v, want original snapshot %v", got, tc.wantPage)
			}
		})
	}
}

func TestStrictSnapshotRejectsUnsupportedSource(t *testing.T) {
	if _, err := strictSnapshotTxOptions("clickhouse"); err == nil {
		t.Fatal("strictSnapshotTxOptions(clickhouse) = nil, want fail-closed error")
	}
}

func TestStrictSnapshotMSSQLOptionsUseSerializableWithoutReadOnly(t *testing.T) {
	opts, err := strictSnapshotTxOptions("mssql")
	if err != nil {
		t.Fatal(err)
	}
	if opts.Isolation != sql.LevelSerializable || opts.ReadOnly {
		t.Fatalf("mssql tx options = %+v, want serializable and ReadOnly=false", opts)
	}
}

func TestStrictSnapshotMariaDBUsesMySQLRepeatableRead(t *testing.T) {
	for _, dbType := range []string{"mysql", "mariadb", "maria"} {
		t.Run(dbType, func(t *testing.T) {
			opts, err := strictSnapshotTxOptions(dbType)
			if err != nil {
				t.Fatal(err)
			}
			if opts.Isolation != sql.LevelRepeatableRead || !opts.ReadOnly {
				t.Fatalf("%s tx options = %+v, want repeatable-read and ReadOnly=true", dbType, opts)
			}
		})
	}
}

func TestStrictSnapshotFailsBeforeTargetPreparation(t *testing.T) {
	reader, writer := openSnapshotSQLite(t, []string{
		`CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`,
		`INSERT INTO events VALUES (1, 'a')`,
	})
	defer reader.Close()
	defer writer.Close()

	target := &strictSnapshotGuardTarget{}
	_, err := Execute(
		context.Background(),
		&unsupportedStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: reader}},
		target,
		&config.Config{Migration: config.MigrationConfig{
			StrictConsistency: true,
			TargetMode:        "drop_recreate",
			ChunkSize:         1,
		}, Target: config.TargetConfig{Schema: "public"}},
		Job{Table: source.Table{
			Name:       "events",
			PrimaryKey: []string{"id"},
			Columns: []source.Column{
				{Name: "id", DataType: "integer"},
				{Name: "payload", DataType: "text"},
			},
		}},
		progress.New(),
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "strict_consistency") {
		t.Fatalf("Execute unsupported strict source error = %v, want fail-closed strict_consistency error", err)
	}
	if target.truncated {
		t.Fatal("unsupported strict source reached target truncation")
	}
}

func TestStrictSnapshotRejectsPartitionedJobs(t *testing.T) {
	reader, writer := openSnapshotSQLite(t, []string{
		`CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`,
		`INSERT INTO events VALUES (1, 'a')`,
	})
	defer reader.Close()
	defer writer.Close()

	_, err := Execute(
		context.Background(),
		&keysetRuntimeSourcePool{db: reader},
		&strictSnapshotGuardTarget{},
		&config.Config{Migration: config.MigrationConfig{
			StrictConsistency: true,
			TargetMode:        "drop_recreate",
			ChunkSize:         1,
		}, Target: config.TargetConfig{Schema: "public"}},
		Job{
			Table: source.Table{Name: "events"},
			Partition: &source.Partition{
				PartitionID: 1,
			},
		},
		progress.New(),
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "does not support partitioned jobs") {
		t.Fatalf("Execute partitioned strict job error = %v, want unsupported-partitions error", err)
	}
}

type unsupportedStrictSnapshotSource struct {
	*keysetRuntimeSourcePool
}

func (p *unsupportedStrictSnapshotSource) DBType() string { return "clickhouse" }

type strictSnapshotGuardTarget struct {
	keysetRuntimeTargetPool
	truncated bool
}

func (p *strictSnapshotGuardTarget) TruncateTable(context.Context, string, string) error {
	p.truncated = true
	return nil
}

func openSnapshotSQLite(t *testing.T, setup []string) (*sql.DB, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	reader, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetMaxOpenConns(4)
	if _, err := reader.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	for _, query := range setup {
		if _, err := reader.Exec(query); err != nil {
			_ = reader.Close()
			t.Fatalf("setup %q: %v", query, err)
		}
	}
	writer, err := sql.Open("sqlite", path)
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if _, err := writer.Exec(`PRAGMA busy_timeout = 1000`); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	return reader, writer
}

func scanSnapshotPage(ctx context.Context, q sourceQueryer, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%d:%s", id, payload))
	}
	return out, rows.Err()
}

func scanTupleSnapshotPage(ctx context.Context, q sourceQueryer, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a, b int
		var payload string
		if err := rows.Scan(&a, &b, &payload); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%d,%d:%s", a, b, payload))
	}
	return out, rows.Err()
}
