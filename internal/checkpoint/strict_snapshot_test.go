package checkpoint

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestSQLiteStrictSnapshotEvidenceUpgradeAddsColumns(t *testing.T) {
	dataDir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE runs (
			id TEXT PRIMARY KEY,
			started_at TEXT NOT NULL,
			completed_at TEXT,
			status TEXT NOT NULL,
			source_schema TEXT NOT NULL,
			target_schema TEXT NOT NULL,
			config TEXT
		);
		CREATE TABLE tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT,
			task_type TEXT NOT NULL,
			task_key TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending'
		);
	`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := New(dataDir)
	if err != nil {
		t.Fatalf("New after legacy schema: %v", err)
	}
	defer state.Close()

	for _, column := range []string{"strict_consistency", "snapshot_row_count"} {
		var found bool
		var table string
		if column == "strict_consistency" {
			table = "runs"
		} else {
			table = "tasks"
		}
		rows, err := state.db.Query("PRAGMA table_info(" + table + ")")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var cid int
			var name, typeName string
			var notNull, primaryKey int
			var defaultValue any
			if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryKey); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			if name == column {
				found = true
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("legacy upgrade did not add %s.%s", table, column)
		}
	}
}
