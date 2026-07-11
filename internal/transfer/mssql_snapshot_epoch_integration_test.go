package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
)

func TestMSSQLMigrationSnapshotEpochTwoTableIntegration(t *testing.T) {
	admin, db, writer, _, dbName := openMSSQLStrictIntegration(t)
	if _, err := db.Exec(`CREATE TABLE dbo.other_events (id INT NOT NULL PRIMARY KEY, val INT NOT NULL); INSERT INTO dbo.other_events VALUES (1, 10), (2, 20)`); err != nil {
		t.Fatal(err)
	}
	runID := fmt.Sprintf("%08x-snapshot", uint32(time.Now().UnixNano()))
	epoch, err := BeginStrictSnapshotEpochForRun(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, StrictSnapshotEpochOptions{
		RunID: runID,
		SourceConfig: config.SourceConfig{
			Type: "mssql", Host: "localhost", Port: 1433, Database: dbName,
			User: "sa", Password: "TestPass2024", Schema: "dbo", SSLMode: "disable",
		},
		MaxConnections: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshotName := epoch.SnapshotName()
	defer func() {
		if epoch != nil {
			epoch.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := writer.ExecContext(ctx, `UPDATE dbo.events SET val = 1001 WHERE id = 1; DELETE FROM dbo.other_events WHERE id = 1; INSERT INTO dbo.other_events VALUES (3, 30)`); err != nil {
		t.Fatalf("live-source writes blocked by database snapshot: %v", err)
	}
	var eventVal, otherCount, otherVal int
	if err := epoch.queryer().QueryRowContext(context.Background(), `SELECT val FROM dbo.events WHERE id = 1`).Scan(&eventVal); err != nil {
		t.Fatal(err)
	}
	if err := epoch.queryer().QueryRowContext(context.Background(), `SELECT COUNT(*), SUM(val) FROM dbo.other_events`).Scan(&otherCount, &otherVal); err != nil {
		t.Fatal(err)
	}
	if eventVal != 1 || otherCount != 2 || otherVal != 30 {
		t.Fatalf("snapshot values = event:%d other:(%d,%d), want original 1 and (2,30)", eventVal, otherCount, otherVal)
	}
	events := source.Table{Schema: "dbo", Name: "events", Columns: []source.Column{{Name: "id", DataType: "int"}, {Name: "val", DataType: "int"}}, PrimaryKey: []string{"id"}, RowCount: 100}
	other := source.Table{Schema: "dbo", Name: "other_events", Columns: []source.Column{{Name: "id", DataType: "int"}, {Name: "val", DataType: "int"}}, PrimaryKey: []string{"id"}, RowCount: 2}
	events.PopulatePKColumns()
	other.PopulatePKColumns()
	cfg := &config.Config{Migration: config.MigrationConfig{
		StrictConsistency: true, StrictConsistencyScope: "migration", TargetMode: "drop_recreate",
		ChunkSize: 5, ParallelReaders: 4, MaxSourceConnections: 4, ReadAheadBuffers: 1, WriteAheadWriters: 1,
	}}
	target := &mssqlEpochCaptureTarget{rows: make(map[string]map[int]int)}
	for _, sourceTable := range []source.Table{events, other} {
		stats, err := Execute(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, target, cfg, Job{Table: sourceTable, StrictSnapshotEpoch: epoch}, progress.New(), nil)
		if err != nil || stats.Rows != sourceTable.RowCount {
			t.Fatalf("snapshot Execute %s = (rows=%v, err=%v), want %d", sourceTable.Name, stats, err, sourceTable.RowCount)
		}
	}
	copied := target.snapshot()
	if copied["events"][1] != 1 || len(copied["events"]) != 100 || copied["other_events"][1] != 10 || copied["other_events"][2] != 20 || len(copied["other_events"]) != 2 {
		t.Fatalf("two-table copied snapshot = %+v", copied)
	}
	queryer, release, err := epoch.queryerForWorker(context.Background(), 3)
	if err != nil || queryer == nil || release == nil {
		t.Fatalf("parallel snapshot reader = (%v, release=%t, err=%v)", queryer, release != nil, err)
	}
	release()
	epoch.Close()
	epoch = nil
	var exists int
	if err := admin.QueryRow(`SELECT CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END`, snapshotName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatalf("SQL Server snapshot [%s] remained after epoch close", snapshotName)
	}
}

type mssqlEpochCaptureTarget struct {
	keysetRuntimeTargetPool
	mu   sync.Mutex
	rows map[string]map[int]int
}

func (p *mssqlEpochCaptureTarget) WriteBatch(_ context.Context, opts driver.WriteBatchOptions) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rows[opts.Table] == nil {
		p.rows[opts.Table] = make(map[int]int)
	}
	for _, row := range opts.Rows {
		id, idOK := keysetRuntimeInt(row[0])
		val, valOK := keysetRuntimeInt(row[1])
		if !idOK || !valOK {
			return fmt.Errorf("snapshot row has unexpected values (%T, %T)", row[0], row[1])
		}
		p.rows[opts.Table][id] = val
	}
	return nil
}

func (p *mssqlEpochCaptureTarget) snapshot() map[string]map[int]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	copyRows := make(map[string]map[int]int, len(p.rows))
	for table, rows := range p.rows {
		copyRows[table] = make(map[int]int, len(rows))
		for id, val := range rows {
			copyRows[table][id] = val
		}
	}
	return copyRows
}

func TestMSSQLMigrationSnapshotResumeAndDropRetryIntegration(t *testing.T) {
	admin, db, writer, _, dbName := openMSSQLStrictIntegration(t)
	runID := fmt.Sprintf("%08x-resume", uint32(time.Now().UnixNano()))
	name, err := mssqlSnapshotName(runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := createMSSQLDatabaseSnapshot(context.Background(), db, dbName, name); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`UPDATE dbo.events SET val = 7001 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	epoch, err := BeginStrictSnapshotEpochForRun(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, StrictSnapshotEpochOptions{
		RunID: runID,
		SourceConfig: config.SourceConfig{
			Type: "mssql", Host: "localhost", Port: 1433, Database: dbName,
			User: "sa", Password: "TestPass2024", Schema: "dbo", SSLMode: "disable",
		},
		MaxConnections: 5,
		Resume:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var snapshotValue int
	if err := epoch.queryer().QueryRowContext(context.Background(), `SELECT val FROM dbo.events WHERE id = 1`).Scan(&snapshotValue); err != nil {
		t.Fatal(err)
	}
	if snapshotValue != 1 {
		t.Fatalf("resumed SQL Server snapshot value = %d, want original instant value 1", snapshotValue)
	}
	holdDB, err := sql.Open("sqlserver", "sqlserver://sa:TestPass2024@localhost:1433?database="+name+"&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	hold, err := holdDB.Conn(context.Background())
	if err != nil {
		_ = holdDB.Close()
		t.Fatal(err)
	}
	if err := hold.PingContext(context.Background()); err != nil {
		_ = hold.Close()
		_ = holdDB.Close()
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		epoch.Close()
		close(closed)
	}()
	time.Sleep(250 * time.Millisecond)
	_ = hold.Close()
	_ = holdDB.Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot drop retry did not finish after held connection closed")
	}
	var exists int
	if err := admin.QueryRow(`SELECT CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatalf("resumed SQL Server snapshot [%s] remained after retrying drop", name)
	}
}

func TestMSSQLMigrationSnapshotPartitionedIntegration(t *testing.T) {
	_, db, writer, _, dbName := openMSSQLStrictIntegration(t)
	runID := fmt.Sprintf("%08x-partition", uint32(time.Now().UnixNano()))
	epoch, err := BeginStrictSnapshotEpochForRun(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, StrictSnapshotEpochOptions{
		RunID: runID,
		SourceConfig: config.SourceConfig{
			Type: "mssql", Host: "localhost", Port: 1433, Database: dbName,
			User: "sa", Password: "TestPass2024", Schema: "dbo", SSLMode: "disable",
		},
		MaxConnections: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer epoch.Close()
	if _, err := writer.Exec(`UPDATE dbo.events SET val = 7500 WHERE id = 75; INSERT INTO dbo.events VALUES (101, 101)`); err != nil {
		t.Fatalf("live mutation after database snapshot: %v", err)
	}
	table := source.Table{Schema: "dbo", Name: "events", Columns: []source.Column{{Name: "id", DataType: "int"}, {Name: "val", DataType: "int"}}, PrimaryKey: []string{"id"}, RowCount: 100}
	table.PopulatePKColumns()
	cfg := &config.Config{Migration: config.MigrationConfig{
		StrictConsistency: true, StrictConsistencyScope: "migration", TargetMode: "drop_recreate",
		ChunkSize: 5, ParallelReaders: 4, MaxSourceConnections: 4, ReadAheadBuffers: 1, WriteAheadWriters: 1,
	}}
	target := &mssqlEpochCaptureTarget{rows: make(map[string]map[int]int)}
	partitions := []source.Partition{
		{TableName: table.Name, PartitionID: 1, MinPK: int64(1), MaxPK: int64(50), RowCount: 50, IsFirstPartition: true},
		{TableName: table.Name, PartitionID: 2, MinPK: int64(51), MaxPK: int64(100), RowCount: 50},
	}
	for i := range partitions {
		if _, err := Execute(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, target, cfg, Job{Table: table, Partition: &partitions[i], StrictSnapshotEpoch: epoch}, progress.New(), nil); err != nil {
			t.Fatalf("snapshot partition %d: %v", partitions[i].PartitionID, err)
		}
	}
	copied := target.snapshot()[table.Name]
	if len(copied) != 100 || copied[75] != 75 {
		t.Fatalf("partitioned snapshot copied %d rows with id75=%d, want 100 rows at original instant", len(copied), copied[75])
	}
}

func TestMSSQLMigrationSnapshotResumeMissingFailsIntegration(t *testing.T) {
	admin, db, _, _, dbName := openMSSQLStrictIntegration(t)
	runID := fmt.Sprintf("%08x-missing", uint32(time.Now().UnixNano()))
	name, err := mssqlSnapshotName(runID)
	if err != nil {
		t.Fatal(err)
	}
	exists, err := mssqlDatabaseExists(context.Background(), admin, name)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		if err := dropMSSQLDatabaseSnapshot(context.Background(), admin, name); err != nil {
			t.Fatal(err)
		}
	}
	epoch, err := BeginStrictSnapshotEpochForRun(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, StrictSnapshotEpochOptions{
		RunID: runID,
		SourceConfig: config.SourceConfig{
			Type: "mssql", Host: "localhost", Port: 1433, Database: dbName,
			User: "sa", Password: "TestPass2024", Schema: "dbo", SSLMode: "disable",
		},
		MaxConnections: 5,
		Resume:         true,
	})
	if epoch != nil {
		epoch.Close()
		t.Fatal("resume unexpectedly created an epoch without the surviving SQL Server snapshot")
	}
	if err == nil || !strings.Contains(err.Error(), "requires surviving SQL Server snapshot ["+name+"]") {
		t.Fatalf("missing-snapshot resume error = %v, want fail-closed error naming %s", err, name)
	}
	var remaining int
	if err := admin.QueryRow(`SELECT CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END`, name).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("resume created replacement SQL Server snapshot [%s], want original instant to remain unavailable", name)
	}
}
