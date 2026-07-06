package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/stats"

	_ "github.com/johndauphine/dmt/internal/driver/generic"
)

func TestKeysetRuntimeChunkSizeMutationCoversRowsAndCheckpoints(t *testing.T) {
	const (
		totalRows        = 96
		initialChunkSize = 8
		updatedChunkSize = 5
	)

	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}

	tuner := NewRuntimeTuner(RuntimeSnapshot{
		ChunkSize:           initialChunkSize,
		WriteAheadWriters:   1,
		CheckpointFrequency: 1,
	})
	tgtPool := &keysetRuntimeTargetPool{
		tuner:    tuner,
		updateTo: updatedChunkSize,
	}
	saver := &keysetRuntimeProgressSaver{}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         totalRows,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:           initialChunkSize,
			ReadAheadBuffers:    0,
			ParallelReaders:     2,
			WriteAheadWriters:   1,
			TargetMode:          "drop_recreate",
			CheckpointFrequency: 1,
		},
	}
	job := Job{
		Table:  table,
		TaskID: 389,
		Saver:  saver,
	}

	transferStats, err := executeKeysetPagination(
		context.Background(),
		srcPool,
		tgtPool,
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
		tuner,
		nil,
	)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}
	if transferStats.Rows != totalRows {
		t.Fatalf("stats rows = %d, want %d", transferStats.Rows, totalRows)
	}

	ids, batchSizes := tgtPool.snapshot()
	if len(ids) != totalRows {
		t.Fatalf("wrote %d rows, want %d", len(ids), totalRows)
	}
	sort.Ints(ids)
	for i, got := range ids {
		if want := i + 1; got != want {
			t.Fatalf("written id[%d] = %d, want %d", i, got, want)
		}
	}

	if !containsInt(batchSizes, initialChunkSize) {
		t.Fatalf("batch sizes %v never observed initial chunk size %d", batchSizes, initialChunkSize)
	}
	if !containsInt(batchSizes, updatedChunkSize) {
		t.Fatalf("batch sizes %v never observed runtime-updated chunk size %d", batchSizes, updatedChunkSize)
	}

	lastSave, ok := saver.last()
	if !ok {
		t.Fatal("expected at least one checkpoint save")
	}
	lastPK, ok := keysetRuntimeInt(lastSave.lastPK)
	if !ok || lastPK != totalRows {
		t.Fatalf("final checkpoint lastPK = %v, want %d", lastSave.lastPK, totalRows)
	}
	if lastSave.rowsDone != totalRows {
		t.Fatalf("final checkpoint rowsDone = %d, want %d", lastSave.rowsDone, totalRows)
	}
}

func seedKeysetRuntimeTunerDB(t *testing.T, totalRows int) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatalf("open sqlite source: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close sqlite source: %v", err)
		}
	})
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	if _, err := db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO items (id, payload) VALUES (?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("prepare seed insert: %v", err)
	}
	for i := 1; i <= totalRows; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("row-%03d", i)); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			t.Fatalf("insert seed row %d: %v", i, err)
		}
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("close seed statement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}

	return db
}

type keysetRuntimeSourcePool struct {
	db *sql.DB
}

func (p *keysetRuntimeSourcePool) Close() error { return nil }
func (p *keysetRuntimeSourcePool) DB() *sql.DB  { return p.db }
func (p *keysetRuntimeSourcePool) ExtractSchema(context.Context, string) ([]driver.Table, error) {
	return nil, nil
}
func (p *keysetRuntimeSourcePool) LoadIndexes(context.Context, *driver.Table) error {
	return nil
}
func (p *keysetRuntimeSourcePool) LoadForeignKeys(context.Context, *driver.Table) error {
	return nil
}
func (p *keysetRuntimeSourcePool) LoadCheckConstraints(context.Context, *driver.Table) error {
	return nil
}

func (p *keysetRuntimeSourcePool) GetRowCount(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeSourcePool) GetRowCountFast(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeSourcePool) GetRowCountExact(context.Context, string, string, bool) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeSourcePool) GetPartitionBoundaries(context.Context, *driver.Table, int) ([]driver.Partition, error) {
	return nil, nil
}
func (p *keysetRuntimeSourcePool) GetDateColumnInfo(context.Context, string, string, []string) (string, string, bool) {
	return "", "", false
}
func (p *keysetRuntimeSourcePool) GetMaxDateColumnValue(context.Context, string, string, string) (*time.Time, error) {
	return nil, nil
}
func (p *keysetRuntimeSourcePool) SampleColumnValues(context.Context, string, string, string, int) ([]string, error) {
	return nil, nil
}
func (p *keysetRuntimeSourcePool) SampleRows(context.Context, string, string, []string, int) (map[string][]string, error) {
	return nil, nil
}
func (p *keysetRuntimeSourcePool) MaxConns() int              { return 4 }
func (p *keysetRuntimeSourcePool) DBType() string             { return "sqlite" }
func (p *keysetRuntimeSourcePool) PoolStats() stats.PoolStats { return stats.PoolStats{} }

type keysetRuntimeTargetPool struct {
	mu          sync.Mutex
	ids         []int
	batchSizes  []int
	writes      int
	updated     bool
	tuner       RuntimeTuner
	updateTo    int
	truncateErr error // when set, TruncateTable returns it (#619)
	writeErr    error // when set, WriteBatch fails after failAfter writes (#617)
	failAfter   int   // number of successful writes before writeErr kicks in
}

func (p *keysetRuntimeTargetPool) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.writeErr != nil {
		p.mu.Lock()
		over := p.writes >= p.failAfter
		p.mu.Unlock()
		if over {
			return p.writeErr
		}
	}

	ids := make([]int, 0, len(opts.Rows))
	for _, row := range opts.Rows {
		id, ok := keysetRuntimeInt(row[0])
		if !ok {
			return fmt.Errorf("row id has unexpected type %T", row[0])
		}
		ids = append(ids, id)
	}

	var updateNow bool
	p.mu.Lock()
	p.ids = append(p.ids, ids...)
	p.batchSizes = append(p.batchSizes, len(opts.Rows))
	p.writes++
	if p.writes == 1 && !p.updated {
		p.updated = true
		updateNow = true
	}
	p.mu.Unlock()

	if updateNow {
		updated := p.updateTo
		if err := p.tuner.Update(RuntimeUpdate{ChunkSize: &updated}); err != nil {
			return err
		}
	}

	return nil
}

func (p *keysetRuntimeTargetPool) snapshot() ([]int, []int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids := append([]int(nil), p.ids...)
	batchSizes := append([]int(nil), p.batchSizes...)
	return ids, batchSizes
}

func (p *keysetRuntimeTargetPool) Close()                     {}
func (p *keysetRuntimeTargetPool) Ping(context.Context) error { return nil }
func (p *keysetRuntimeTargetPool) DB() *sql.DB                { return nil }
func (p *keysetRuntimeTargetPool) CreateSchema(context.Context, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreateTable(context.Context, *driver.Table, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreateTableWithOptions(context.Context, *driver.Table, string, driver.TableOptions) error {
	return nil
}
func (p *keysetRuntimeTargetPool) AddColumn(context.Context, *driver.Table, *driver.Column, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) DropColumnNotNull(context.Context, *driver.Table, *driver.Column, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) AlterColumnType(context.Context, *driver.Table, *driver.Column, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) DropTable(context.Context, string, string) error { return nil }
func (p *keysetRuntimeTargetPool) TruncateTable(context.Context, string, string) error {
	return p.truncateErr
}
func (p *keysetRuntimeTargetPool) TableExists(context.Context, string, string) (bool, error) {
	return true, nil
}
func (p *keysetRuntimeTargetPool) SetTableLogged(context.Context, string, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreatePrimaryKey(context.Context, *driver.Table, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreateIndex(context.Context, *driver.Table, *driver.Index, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreateForeignKey(context.Context, *driver.Table, *driver.ForeignKey, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) CreateCheckConstraint(context.Context, *driver.Table, *driver.CheckConstraint, string) error {
	return nil
}
func (p *keysetRuntimeTargetPool) HasPrimaryKey(context.Context, string, string) (bool, error) {
	return true, nil
}
func (p *keysetRuntimeTargetPool) GetTableDDL(context.Context, string, string) string {
	return ""
}
func (p *keysetRuntimeTargetPool) GetRowCount(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeTargetPool) GetRowCountFast(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeTargetPool) GetRowCountExact(context.Context, string, string, bool) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeTargetPool) ResetSequence(context.Context, string, *driver.Table) error {
	return nil
}
func (p *keysetRuntimeTargetPool) UpsertBatch(context.Context, driver.UpsertBatchOptions) error {
	return nil
}
func (p *keysetRuntimeTargetPool) ExecRaw(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (p *keysetRuntimeTargetPool) QueryRowRaw(context.Context, string, any, ...any) error {
	return nil
}
func (p *keysetRuntimeTargetPool) MaxConns() int              { return 1 }
func (p *keysetRuntimeTargetPool) DBType() string             { return "sqlite" }
func (p *keysetRuntimeTargetPool) PoolStats() stats.PoolStats { return stats.PoolStats{} }

type keysetRuntimeProgressSaver struct {
	mu    sync.Mutex
	saves []keysetRuntimeProgressSave
}

type keysetRuntimeProgressSave struct {
	lastPK   any
	rowsDone int64
}

func (s *keysetRuntimeProgressSaver) SaveProgress(_ int64, _ string, _ *int, lastPK any, rowsDone, _ int64, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves = append(s.saves, keysetRuntimeProgressSave{
		lastPK:   lastPK,
		rowsDone: rowsDone,
	})
	return nil
}

func (s *keysetRuntimeProgressSaver) GetProgress(int64) (any, int64, string, error) {
	return nil, 0, "", nil
}

func (s *keysetRuntimeProgressSaver) last() (keysetRuntimeProgressSave, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saves) == 0 {
		return keysetRuntimeProgressSave{}, false
	}
	return s.saves[len(s.saves)-1], true
}

func keysetRuntimeInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	default:
		return 0, false
	}
}

func containsInt(values []int, want int) bool {
	for _, got := range values {
		if got == want {
			return true
		}
	}
	return false
}
