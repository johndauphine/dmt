package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"

	_ "github.com/johndauphine/dmt/v5/internal/driver/generic"
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

func TestKeysetBoundaryIncludesMinimumInt64(t *testing.T) {
	for _, readers := range []int{1, 4} {
		t.Run(fmt.Sprintf("readers_%d", readers), func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "min-int.db"))
			if err != nil {
				t.Fatalf("open sqlite: %v", err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close sqlite: %v", err)
				}
			})
			db.SetMaxOpenConns(readers)
			if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
				t.Fatalf("create table: %v", err)
			}
			if _, err := db.Exec(`INSERT INTO items (id, payload) VALUES (?, 'minimum'), (?, 'next')`, int64(math.MinInt64), int64(math.MinInt64+1)); err != nil {
				t.Fatalf("insert rows: %v", err)
			}

			cfg := keysetBoundaryTestConfig()
			cfg.Migration.ParallelReaders = readers
			cfg.Migration.ChunkSize = 1
			job := keysetBoundaryTestJob()
			job.Table.RowCount = 2
			target := &keysetRuntimeTargetPool{updated: true}

			stats, err := executeKeysetPagination(
				context.Background(),
				&keysetRuntimeSourcePool{db: db},
				target,
				cfg,
				job,
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
			ids, _ := target.snapshot()
			sort.Ints(ids)
			want := []int{math.MinInt64, math.MinInt64 + 1}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("transferred IDs = %v, want %v", ids, want)
			}
			if stats.Rows != 2 {
				t.Fatalf("stats.Rows = %d, want 2", stats.Rows)
			}
		})
	}
}

func TestKeysetBoundaryResumeRemainsExclusive(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "resume-exclusive.db"))
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
	if _, err := db.Exec(`INSERT INTO items (id, payload) VALUES (?, 'checkpoint'), (?, 'next')`, int64(math.MinInt64), int64(math.MinInt64+1)); err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	cfg := keysetBoundaryTestConfig()
	cfg.Migration.ChunkSize = 1
	job := keysetBoundaryTestJob()
	job.Table.RowCount = 2
	target := &keysetRuntimeTargetPool{updated: true}
	stats, err := executeKeysetPagination(
		context.Background(),
		&keysetRuntimeSourcePool{db: db},
		target,
		cfg,
		job,
		[]string{"id", "payload"},
		[]string{"id", "payload"},
		[]string{"integer", "text"},
		[]int{0, 0},
		nil,
		int64(math.MinInt64),
		1,
		nil,
		"items",
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}
	ids, _ := target.snapshot()
	want := []int{math.MinInt64 + 1}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("transferred IDs = %v, want %v", ids, want)
	}
	if stats.Rows != 2 {
		t.Fatalf("stats.Rows = %d, want 2", stats.Rows)
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
