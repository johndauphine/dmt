package transfer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/progress"
)

type cleanupTestTarget struct {
	*keysetRuntimeTargetPool
	mu        sync.Mutex
	dbType    string
	execErr   error
	queries   []string
	writeCall atomic.Int64
}

func (p *cleanupTestTarget) DBType() string { return p.dbType }

func (p *cleanupTestTarget) ExecRaw(_ context.Context, query string, _ ...any) (int64, error) {
	p.mu.Lock()
	p.queries = append(p.queries, query)
	p.mu.Unlock()
	return 0, p.execErr
}

func (p *cleanupTestTarget) WriteBatch(context.Context, driver.WriteBatchOptions) error {
	p.writeCall.Add(1)
	return nil
}

type clickHouseResumeSaver struct{}

func (clickHouseResumeSaver) SaveProgress(int64, string, *int, any, int64, int64, string) error {
	return nil
}

func (clickHouseResumeSaver) GetProgress(int64) (any, int64, string, error) {
	return int64(1), 1, "", nil
}

func TestClickHouseResumeCleanupFailsBeforeReplay(t *testing.T) {
	db := seedKeysetRuntimeTunerDB(t, 3)
	target := &cleanupTestTarget{
		keysetRuntimeTargetPool: &keysetRuntimeTargetPool{updated: true},
		dbType:                  "clickhouse",
		execErr:                 errors.New("ClickHouse rejected cleanup SQL"),
	}
	table := memBudgetTestTable()
	table.RowCount = 3
	job := Job{Table: table, TaskID: 644, Saver: clickHouseResumeSaver{}, IsResume: true}
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: "default"},
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			ParallelReaders:   1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	_, err := Execute(
		context.Background(),
		&keysetRuntimeSourcePool{db: db},
		target,
		cfg,
		job,
		progress.New(),
		nil,
	)
	if err == nil {
		t.Fatal("Execute succeeded despite unsupported ClickHouse resume cleanup")
	}
	if !strings.Contains(err.Error(), "fresh run") || !strings.Contains(err.Error(), "truncating") {
		t.Fatalf("error %q lacks actionable fresh-run recovery guidance", err)
	}
	if target.writeCall.Load() != 0 {
		t.Fatalf("writer called %d time(s) after cleanup failure, want 0", target.writeCall.Load())
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if len(target.queries) != 0 {
		t.Fatalf("ClickHouse received cleanup SQL %q; unsupported cleanup must fail before ExecRaw", target.queries)
	}
}

func TestClickHousePartitionCleanupRequiredOnlyForReplay(t *testing.T) {
	db := seedKeysetRuntimeTunerDB(t, 3)
	table := memBudgetTestTable()
	table.RowCount = 3
	partition := &driver.Partition{PartitionID: 1, MinPK: int64(1), MaxPK: int64(3), RowCount: 3}
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: "default"},
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			ParallelReaders:   1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	t.Run("fresh", func(t *testing.T) {
		target := &cleanupTestTarget{
			keysetRuntimeTargetPool: &keysetRuntimeTargetPool{updated: true},
			dbType:                  "clickhouse",
		}
		job := Job{Table: table, Partition: partition}
		if _, err := Execute(context.Background(), &keysetRuntimeSourcePool{db: db}, target, cfg, job, progress.New(), nil); err != nil {
			t.Fatalf("fresh partition transfer: %v", err)
		}
		if target.writeCall.Load() == 0 {
			t.Fatal("fresh partition transfer made no writes")
		}
		target.mu.Lock()
		defer target.mu.Unlock()
		if len(target.queries) != 0 {
			t.Fatalf("fresh partition issued cleanup SQL: %q", target.queries)
		}
	})

	t.Run("replay", func(t *testing.T) {
		target := &cleanupTestTarget{
			keysetRuntimeTargetPool: &keysetRuntimeTargetPool{updated: true},
			dbType:                  "clickhouse",
		}
		job := Job{Table: table, Partition: partition, ReplayPossible: true}
		if _, err := Execute(context.Background(), &keysetRuntimeSourcePool{db: db}, target, cfg, job, progress.New(), nil); err == nil {
			t.Fatal("partition replay succeeded without synchronous cleanup")
		}
		if target.writeCall.Load() != 0 {
			t.Fatalf("partition replay wrote %d batch(es) after unsupported cleanup", target.writeCall.Load())
		}
		target.mu.Lock()
		defer target.mu.Unlock()
		if len(target.queries) != 0 {
			t.Fatalf("ClickHouse replay received cleanup SQL: %q", target.queries)
		}
	})
}

func TestResumeCleanupExecutionFailureStopsReplay(t *testing.T) {
	db := seedKeysetRuntimeTunerDB(t, 3)
	sentinel := errors.New("cleanup permission denied")
	target := &cleanupTestTarget{
		keysetRuntimeTargetPool: &keysetRuntimeTargetPool{updated: true},
		dbType:                  "sqlite",
		execErr:                 sentinel,
	}
	table := memBudgetTestTable()
	table.RowCount = 3
	job := Job{Table: table, TaskID: 644, Saver: clickHouseResumeSaver{}, IsResume: true}
	cfg := &config.Config{
		Migration: config.MigrationConfig{
			ChunkSize:         1,
			ParallelReaders:   1,
			WriteAheadWriters: 1,
			TargetMode:        "drop_recreate",
		},
	}

	_, err := Execute(context.Background(), &keysetRuntimeSourcePool{db: db}, target, cfg, job, progress.New(), nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Execute error = %v, want cleanup failure", err)
	}
	if target.writeCall.Load() != 0 {
		t.Fatalf("writer called %d time(s) after cleanup failure, want 0", target.writeCall.Load())
	}
}

func TestKeysetCleanupCapabilities(t *testing.T) {
	tests := []struct {
		dbType      string
		wantSQL     string
		unsupported bool
	}{
		{dbType: "postgres", wantSQL: `DELETE FROM public."items"`},
		{dbType: "mysql", wantSQL: "DELETE FROM `public`.`items`"},
		{dbType: "sqlite", wantSQL: `DELETE FROM "items"`},
		{dbType: "mssql", wantSQL: `DELETE FROM [public].[items]`},
		{dbType: "clickhouse", unsupported: true},
		{dbType: "unknown", unsupported: true},
	}
	partitionID := 1
	job := &Job{
		Table: driver.Table{Name: "items", PrimaryKey: []string{"id"}},
		Partition: &driver.Partition{
			PartitionID: partitionID,
			MinPK:       int64(1),
			MaxPK:       int64(10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			target := &cleanupTestTarget{
				keysetRuntimeTargetPool: &keysetRuntimeTargetPool{updated: true},
				dbType:                  tt.dbType,
			}
			partitionErr := cleanupPartitionDataGeneric(context.Background(), target, "public", job)
			partialErr := cleanupPartialData(context.Background(), target, "public", "items", "id", int64(5), int64(10))

			target.mu.Lock()
			queries := append([]string(nil), target.queries...)
			target.mu.Unlock()
			if tt.unsupported {
				if partitionErr == nil || partialErr == nil {
					t.Fatalf("cleanup errors = (%v, %v), want explicit unsupported errors", partitionErr, partialErr)
				}
				if len(queries) != 0 {
					t.Fatalf("unsupported target received cleanup SQL: %q", queries)
				}
				return
			}
			if partitionErr != nil || partialErr != nil {
				t.Fatalf("cleanup errors = (%v, %v)", partitionErr, partialErr)
			}
			if len(queries) != 2 {
				t.Fatalf("cleanup queries = %q, want partition and partial queries", queries)
			}
			for _, query := range queries {
				if !strings.Contains(query, tt.wantSQL) {
					t.Fatalf("query %q does not use %s target syntax", query, tt.dbType)
				}
			}
		})
	}
}
