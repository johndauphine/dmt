package transfer

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"

	_ "github.com/johndauphine/dmt/internal/driver/generic"
)

func TestKeysetBoundaryQueryErrorReturnsError(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	stats, err := executeKeysetPagination(
		context.Background(),
		&keysetRuntimeSourcePool{db: db},
		&keysetRuntimeTargetPool{},
		keysetBoundaryTestConfig(),
		keysetBoundaryTestJob(),
		[]string{"id", "payload"},
		[]string{"id", "payload"},
		[]string{"integer", "text"},
		[]int{0, 0},
		nil,
		nil,
		0,
		nil,
		"items",
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("expected boundary query error, got nil")
	}
	if stats != nil {
		t.Fatalf("stats = %+v, want nil on boundary query error", stats)
	}
	if !strings.Contains(err.Error(), "keyset boundary query") {
		t.Fatalf("error = %q, want boundary query context", err)
	}
}

func TestKeysetBoundaryEmptyTableStillSucceeds(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	stats, err := executeKeysetPagination(
		context.Background(),
		&keysetRuntimeSourcePool{db: db},
		&keysetRuntimeTargetPool{},
		keysetBoundaryTestConfig(),
		keysetBoundaryTestJob(),
		[]string{"id", "payload"},
		[]string{"id", "payload"},
		[]string{"integer", "text"},
		[]int{0, 0},
		nil,
		nil,
		0,
		nil,
		"items",
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}
	if stats == nil {
		t.Fatal("stats is nil, want empty success stats")
	}
	if stats.Rows != 0 {
		t.Fatalf("stats.Rows = %d, want 0", stats.Rows)
	}
}

func keysetBoundaryTestConfig() *config.Config {
	return &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:         10,
			ParallelReaders:   1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}
}

func keysetBoundaryTestJob() Job {
	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         0,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	return Job{Table: table, TaskID: 539}
}
