package generic

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	mssqldrv "github.com/johndauphine/dmt/internal/driver/mssql"
)

// Differential proof for the mssql conversion (#509): the catalog
// engine's view and effects against a live SQL Server must match the
// hand-written driver's, with the hand-written implementation as the
// oracle — the same bar the sqlite, mysql, and postgres conversions
// cleared.
//
// Requires the mssql-bench container (localhost:1433, sa/TestPass2024).
// Skips when unreachable unless MSSQL_REQUIRED=1.

const mssqlDSNBase = "sqlserver://sa:TestPass2024@localhost:1433?database="

func mssqlBootstrap(t *testing.T, dbName string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	raw, err := sql.Open("sqlserver", mssqlDSNBase+"master&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.Ping(); err != nil {
		if os.Getenv("MSSQL_REQUIRED") == "1" {
			t.Fatalf("mssql required but unreachable: %v", err)
		}
		t.Skipf("mssql not reachable: %v", err)
	}
	stmts := []string{
		fmt.Sprintf("IF DB_ID('%s') IS NOT NULL BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s] END", dbName, dbName, dbName),
		fmt.Sprintf("CREATE DATABASE [%s]", dbName),
	}
	for _, s := range stmts {
		if _, err := raw.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
}

func mssqlExecAll(t *testing.T, dbName string, stmts []string) {
	t.Helper()
	db, err := sql.Open("sqlserver", mssqlDSNBase+dbName+"&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

var mssqlFixtureDDL = []string{
	`CREATE TABLE Users (
		Id INT IDENTITY(1,1) PRIMARY KEY,
		Name NVARCHAR(120) NOT NULL,
		Bio NVARCHAR(MAX),
		Balance DECIMAL(10,2) DEFAULT 0.00,
		Active BIT NOT NULL DEFAULT 1,
		CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
	)`,
	`CREATE INDEX IX_Users_Name ON Users(Name)`,
	`CREATE UNIQUE INDEX IX_Users_Name_Created ON Users(Name, CreatedAt)`,
	// Include columns are mssql-specific index shape the catalog must
	// round-trip (index_list_agg).
	`CREATE INDEX IX_Users_Balance ON Users(Balance) INCLUDE (Active, Bio)`,
	`CREATE TABLE Orders (
		Region NVARCHAR(10) NOT NULL,
		OrderNo INT NOT NULL,
		UserId INT,
		Note NVARCHAR(MAX),
		PRIMARY KEY (OrderNo, Region),
		CONSTRAINT FK_Orders_User FOREIGN KEY (UserId) REFERENCES Users(Id) ON DELETE CASCADE,
		CONSTRAINT CHK_Region CHECK (Region <> '')
	)`,
	`INSERT INTO Users (Name, Balance) VALUES ('a', 1.50), ('b', 2.50), ('c', 0)`,
	`INSERT INTO Orders (Region, OrderNo, UserId) VALUES ('us', 1, 1), ('eu', 2, 2)`,
}

func TestMssqlCatalogMatchesHandWrittenReader(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_msdiff_src"
	mssqlBootstrap(t, dbName)
	mssqlExecAll(t, dbName, mssqlFixtureDDL)

	cfg := &dbconfig.SourceConfig{
		Type: "mssql", Host: "localhost", Port: 1433,
		Database: dbName, User: "sa", Password: "TestPass2024",
		Schema: "dbo",
	}
	f := false
	cfg.Encrypt = &f

	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("generic NewReader: %v", err)
	}
	defer gen.Close()
	ref, err := mssqldrv.NewReader(cfg, 4)
	if err != nil {
		t.Fatalf("mssql NewReader: %v", err)
	}
	defer ref.Close()

	genTables, err := gen.ExtractSchema(ctx, "dbo")
	if err != nil {
		t.Fatalf("generic ExtractSchema: %v", err)
	}
	refTables, err := ref.ExtractSchema(ctx, "dbo")
	if err != nil {
		t.Fatalf("mssql ExtractSchema: %v", err)
	}
	if len(genTables) != len(refTables) {
		t.Fatalf("table count: %d != %d", len(genTables), len(refTables))
	}

	for i := range refTables {
		genT, refT := &genTables[i], &refTables[i]
		for name, fn := range map[string]func(context.Context, *driver.Table) error{
			"gen indexes": gen.LoadIndexes, "gen fks": gen.LoadForeignKeys, "gen checks": gen.LoadCheckConstraints,
		} {
			if err := fn(ctx, genT); err != nil {
				t.Fatalf("%s(%s): %v", name, genT.Name, err)
			}
		}
		for name, fn := range map[string]func(context.Context, *driver.Table) error{
			"ref indexes": ref.LoadIndexes, "ref fks": ref.LoadForeignKeys, "ref checks": ref.LoadCheckConstraints,
		} {
			if err := fn(ctx, refT); err != nil {
				t.Fatalf("%s(%s): %v", name, refT.Name, err)
			}
		}
		// Oracle defect normalization: the hand-written index loader
		// aggregates included columns into the KEY column list too
		// (key_ordinal 0 — they sort first), which corrupts
		// covering-index recreation. The catalog excludes them; strip
		// them from the oracle's view before comparing.
		for ii := range refT.Indexes {
			inc := make(map[string]bool, len(refT.Indexes[ii].IncludeCols))
			for _, c := range refT.Indexes[ii].IncludeCols {
				inc[c] = true
			}
			var keys []string
			for _, c := range refT.Indexes[ii].Columns {
				if !inc[c] {
					keys = append(keys, c)
				}
			}
			refT.Indexes[ii].Columns = keys
		}
		if !reflect.DeepEqual(genT, refT) {
			t.Errorf("table %s diverges:\n  generic: %+v\n  mssql:   %+v", refT.Name, genT, refT)
		}
	}

	t.Run("counts and partitions", func(t *testing.T) {
		for _, tbl := range refTables {
			g, err := gen.GetRowCountExact(ctx, "dbo", tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ref.GetRowCountExact(ctx, "dbo", tbl.Name, false)
			if err != nil {
				t.Fatal(err)
			}
			if g != r {
				t.Errorf("count(%s): %d != %d", tbl.Name, g, r)
			}
		}
		// Users has the single-column int PK both partitioners need.
		var users *driver.Table
		for i := range refTables {
			if refTables[i].Name == "Users" {
				users = &refTables[i]
			}
		}
		gp, err := gen.GetPartitionBoundaries(ctx, users, 2)
		if err != nil {
			t.Fatal(err)
		}
		rp, err := ref.GetPartitionBoundaries(ctx, users, 2)
		if err != nil {
			t.Fatal(err)
		}
		// The hand-written mssql partitioner never sets
		// IsFirstPartition — the same oracle defect the mysql and pg
		// differentials normalized. The generic engine's
		// true-on-partition-1 is the correct shared semantic.
		for i := range rp {
			rp[i].IsFirstPartition = rp[i].PartitionID == 1
		}
		if !reflect.DeepEqual(gp, rp) {
			t.Errorf("partitions: %+v != %+v", gp, rp)
		}
	})

	t.Run("incremental dates", func(t *testing.T) {
		gd := gen.(driver.IncrementalDateReader)
		var rd driver.IncrementalDateReader = ref
		gc, gt, gf := gd.GetDateColumnInfo(ctx, "dbo", "Users", []string{"missing", "CreatedAt"})
		rc, rt, rf := rd.GetDateColumnInfo(ctx, "dbo", "Users", []string{"missing", "CreatedAt"})
		if gc != rc || gt != rt || gf != rf {
			t.Errorf("date info: (%q,%q,%v) != (%q,%q,%v)", gc, gt, gf, rc, rt, rf)
		}
		gm, err := gd.GetMaxDateColumnValue(ctx, "dbo", "Users", "CreatedAt")
		if err != nil {
			t.Fatal(err)
		}
		rm, err := rd.GetMaxDateColumnValue(ctx, "dbo", "Users", "CreatedAt")
		if err != nil {
			t.Fatal(err)
		}
		if (gm == nil) != (rm == nil) || (gm != nil && !gm.Equal(*rm)) {
			t.Errorf("max date: %v != %v", gm, rm)
		}
	})
}

// Writer equivalence: identical operation sequences into two databases
// leave identical observable state (INFORMATION_SCHEMA shape, identity
// seed, and data).
func TestMssqlCatalogMatchesHandWrittenWriter(t *testing.T) {
	ctx := context.Background()
	mssqlBootstrap(t, "dmt_msdiff_gen")
	mssqlBootstrap(t, "dmt_msdiff_ref")

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	wopts := driver.WriterOptions{BatchSize: 1000, SourceType: "mssql", TypeMapper: tm}

	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	f := false
	genCfg := &dbconfig.TargetConfig{
		Type: "mssql", Host: "localhost", Port: 1433,
		Database: "dmt_msdiff_gen", User: "sa", Password: "TestPass2024",
		Schema: "dbo", ChunkSize: 1000,
	}
	genCfg.Encrypt = &f
	refCfg := &dbconfig.TargetConfig{
		Type: "mssql", Host: "localhost", Port: 1433,
		Database: "dmt_msdiff_ref", User: "sa", Password: "TestPass2024",
		Schema: "dbo", ChunkSize: 1000,
	}
	refCfg.Encrypt = &f

	genW, err := NewWriter(cat, genCfg, 4, wopts)
	if err != nil {
		t.Fatalf("generic NewWriter: %v", err)
	}
	defer genW.Close()
	refW, err := mssqldrv.NewWriter(refCfg, 4, wopts)
	if err != nil {
		t.Fatalf("mssql NewWriter: %v", err)
	}
	defer refW.Close()

	tbl := &driver.Table{
		Name: "Items",
		Columns: []driver.Column{
			{Name: "Id", DataType: "int", IsIdentity: true, OrdinalPos: 1},
			{Name: "Name", DataType: "nvarchar", MaxLength: 80, IsNullable: false, OrdinalPos: 2},
			{Name: "Active", DataType: "bit", IsNullable: true, OrdinalPos: 3},
			{Name: "Price", DataType: "decimal", Precision: 10, Scale: 2, IsNullable: true, OrdinalPos: 4},
		},
		PrimaryKey: []string{"Id"},
	}

	cols := []string{"Id", "Name", "Active", "Price"}
	colTypes := []string{"int", "nvarchar", "bit", "decimal"}
	rows := [][]any{
		{int64(1), "a", true, 1.50},
		{int64(2), "b", false, 2.25},
		{int64(3), "c", nil, nil},
	}

	run := func(w driver.Writer, label string) {
		t.Helper()
		if err := w.CreateSchema(ctx, "dbo"); err != nil {
			t.Fatalf("CreateSchema(%s): %v", label, err)
		}
		if err := w.CreateTable(ctx, tbl, "dbo"); err != nil {
			t.Fatalf("CreateTable(%s): %v", label, err)
		}
		// drop_recreate rerun shape.
		if err := w.DropTable(ctx, "dbo", tbl.Name); err != nil {
			t.Fatalf("DropTable(%s): %v", label, err)
		}
		if err := w.CreateTable(ctx, tbl, "dbo"); err != nil {
			t.Fatalf("CreateTable after drop (%s): %v", label, err)
		}
		// Orchestrator calls CreatePrimaryKey after transfer — must no-op.
		if err := w.CreatePrimaryKey(ctx, tbl, "dbo"); err != nil {
			t.Fatalf("CreatePrimaryKey(%s): %v", label, err)
		}
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, Rows: rows,
		}); err != nil {
			t.Fatalf("WriteBatch(%s): %v", label, err)
		}
		// #227 idempotent replay is a no-op.
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, Rows: rows,
			IdempotentOnDup: true, PKColumns: []string{"Id"},
		}); err != nil {
			t.Fatalf("idempotent WriteBatch(%s): %v", label, err)
		}
		up := w.(driver.Upserter)
		if err := up.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, PKColumns: []string{"Id"},
			Rows: [][]any{
				{int64(2), "b-updated", true, 9.99},
				{int64(4), "d", false, 4.00},
			},
		}); err != nil {
			t.Fatalf("UpsertBatch(%s): %v", label, err)
		}
		seq := w.(driver.SequenceResetter)
		if err := seq.ResetSequence(ctx, "dbo", tbl); err != nil {
			t.Fatalf("ResetSequence(%s): %v", label, err)
		}
		newCol := &driver.Column{Name: "Note", DataType: "nvarchar", MaxLength: -1, IsNullable: true}
		if err := w.AddColumn(ctx, tbl, newCol, "dbo"); err != nil {
			t.Fatalf("AddColumn(%s): %v", label, err)
		}
		if err := w.AddColumn(ctx, tbl, newCol, "dbo"); err != nil {
			t.Fatalf("AddColumn idempotent(%s): %v", label, err)
		}
		relax := &driver.Column{Name: "Name", DataType: "nvarchar", MaxLength: 80, IsNullable: true}
		if err := w.DropColumnNotNull(ctx, tbl, relax, "dbo"); err != nil {
			t.Fatalf("DropColumnNotNull(%s): %v", label, err)
		}
	}
	run(genW, "generic")
	run(refW, "mssql")

	got := dumpMssql(t, "dmt_msdiff_gen")
	want := dumpMssql(t, "dmt_msdiff_ref")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state diverges:\n  generic: %v\n  mssql:   %v", got, want)
	}

	// Validation's timeout fallback depends on a genuinely cheap
	// stats-based count (sys.partitions) on the writer side.
	t.Run("writer fast counts match", func(t *testing.T) {
		g, err := genW.GetRowCountFast(ctx, "dbo", "Items")
		if err != nil {
			t.Fatal(err)
		}
		r, err := refW.GetRowCountFast(ctx, "dbo", "Items")
		if err != nil {
			t.Fatal(err)
		}
		if g != r {
			t.Errorf("fast count: %d != %d", g, r)
		}
	})

	// Generic-only (oracle defect, fixed in the catalog engine): bulk
	// load must KEEP_NULLS — without it SQL Server writes the column
	// DEFAULT over source NULLs.
	t.Run("bulk load preserves NULL over DEFAULT", func(t *testing.T) {
		db, err := sql.Open("sqlserver", mssqlDSNBase+"dmt_msdiff_gen&encrypt=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE Defaulted (Id INT PRIMARY KEY, Qty INT DEFAULT 42)`); err != nil {
			t.Fatal(err)
		}
		if err := genW.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "dbo", Table: "Defaulted",
			Columns: []string{"Id", "Qty"}, ColumnTypes: []string{"int", "int"},
			Rows: [][]any{{int64(1), nil}},
		}); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
		var qty sql.NullInt64
		if err := db.QueryRow("SELECT Qty FROM Defaulted WHERE Id = 1").Scan(&qty); err != nil {
			t.Fatal(err)
		}
		if qty.Valid {
			t.Errorf("NULL was replaced by DEFAULT: got %d", qty.Int64)
		}
	})
}

// dumpMssql reads observable state: column shapes, identity seed, and
// full data.
func dumpMssql(t *testing.T, dbName string) map[string]any {
	t.Helper()
	db, err := sql.Open("sqlserver", mssqlDSNBase+dbName+"&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state := map[string]any{}

	rows, err := db.Query(`SELECT COLUMN_NAME, DATA_TYPE,
			ISNULL(CHARACTER_MAXIMUM_LENGTH, 0), ISNULL(NUMERIC_PRECISION, 0),
			ISNULL(NUMERIC_SCALE, 0), IS_NULLABLE, ISNULL(COLUMN_DEFAULT, '')
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Items'
		ORDER BY ORDINAL_POSITION`)
	if err != nil {
		t.Fatal(err)
	}
	var colShapes []string
	for rows.Next() {
		var name, dt, nullable, def string
		var maxLen, prec, scale int
		if err := rows.Scan(&name, &dt, &maxLen, &prec, &scale, &nullable, &def); err != nil {
			t.Fatal(err)
		}
		colShapes = append(colShapes, fmt.Sprintf("%s %s %d %d %d %s %s", name, dt, maxLen, prec, scale, nullable, def))
	}
	rows.Close()
	state["columns"] = colShapes

	var seed sql.NullInt64
	_ = db.QueryRow(`SELECT CONVERT(BIGINT, IDENT_CURRENT('dbo.Items'))`).Scan(&seed)
	state["identity_seed"] = seed.Int64

	dataRows, err := db.Query("SELECT Id, Name, Active, Price FROM Items ORDER BY Id")
	if err != nil {
		t.Fatal(err)
	}
	var data []string
	for dataRows.Next() {
		var id int64
		var name string
		var active, price sql.NullString
		if err := dataRows.Scan(&id, &name, &active, &price); err != nil {
			t.Fatal(err)
		}
		data = append(data, fmt.Sprintf("%d|%s|%s|%s", id, name, active.String, price.String))
	}
	dataRows.Close()
	state["data"] = data
	return state
}
