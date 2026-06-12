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
)

// Live mssql behavior tests (#509 cleanup). These replace the
// differential proof that compared the catalog engine against the
// hand-written driver: with the oracle removed, the expectations the
// differential established are pinned here as literals.
//
// Requires the mssql-bench container (localhost:1433, sa/TestPass2024).
// Skips when unreachable unless MSSQL_REQUIRED=1.

const mssqlDSNBase = "sqlserver://sa:TestPass2024@localhost:1433?database="

func mssqlTestBootstrap(t *testing.T, dbName string) {
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

func TestMssqlCatalogReaderBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_msbehav_src"
	mssqlTestBootstrap(t, dbName)
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
	r, err := NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, "dbo")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "Orders" || tables[1].Name != "Users" {
		t.Fatalf("tables = %v", tables)
	}
	users, orders := &tables[1], &tables[0]

	if !reflect.DeepEqual(users.PrimaryKey, []string{"Id"}) || !users.Columns[0].IsIdentity {
		t.Errorf("Users PK/identity wrong: %+v", users)
	}
	if !reflect.DeepEqual(orders.PrimaryKey, []string{"OrderNo", "Region"}) {
		t.Errorf("Orders composite PK wrong: %v", orders.PrimaryKey)
	}

	if err := r.LoadIndexes(ctx, users); err != nil {
		t.Fatal(err)
	}
	// Covering index: INCLUDE columns must be reported separately from
	// key columns (the hand-written reader merged them — fixed in the
	// catalog conversion).
	if len(users.Indexes) != 3 {
		t.Fatalf("Users indexes: %+v", users.Indexes)
	}
	balance := users.Indexes[0]
	if balance.Name != "IX_Users_Balance" ||
		!reflect.DeepEqual(balance.Columns, []string{"Balance"}) ||
		!reflect.DeepEqual(balance.IncludeCols, []string{"Active", "Bio"}) {
		t.Errorf("covering index wrong: %+v", balance)
	}
	if users.Indexes[2].Name != "IX_Users_Name_Created" || !users.Indexes[2].IsUnique {
		t.Errorf("unique index wrong: %+v", users.Indexes[2])
	}

	if err := r.LoadForeignKeys(ctx, orders); err != nil {
		t.Fatal(err)
	}
	if err := r.LoadCheckConstraints(ctx, orders); err != nil {
		t.Fatal(err)
	}
	if len(orders.ForeignKeys) != 1 {
		t.Fatalf("Orders FKs: %+v", orders.ForeignKeys)
	}
	fk := orders.ForeignKeys[0]
	if fk.Name != "FK_Orders_User" || fk.RefTable != "Users" ||
		!reflect.DeepEqual(fk.Columns, []string{"UserId"}) ||
		!reflect.DeepEqual(fk.RefColumns, []string{"Id"}) ||
		fk.OnDelete != "CASCADE" || fk.OnUpdate != "NO ACTION" {
		t.Errorf("FK_Orders_User wrong: %+v", fk)
	}
	if len(orders.CheckConstraints) != 1 || orders.CheckConstraints[0].Name != "CHK_Region" {
		t.Errorf("CHK_Region wrong: %+v", orders.CheckConstraints)
	}

	t.Run("counts and partitions", func(t *testing.T) {
		n, err := r.GetRowCountExact(ctx, "dbo", "Users", false)
		if err != nil || n != 3 {
			t.Errorf("count = %d (%v), want 3", n, err)
		}
		parts, err := r.GetPartitionBoundaries(ctx, users, 2)
		if err != nil {
			t.Fatal(err)
		}
		// min_max_even: even PK ranges, last takes the remainder.
		if len(parts) != 2 || !parts[0].IsFirstPartition || parts[1].IsFirstPartition {
			t.Errorf("partitions: %+v", parts)
		}
		if parts[0].MinPK != int64(1) || parts[1].MaxPK != int64(3) {
			t.Errorf("partition bounds: %+v", parts)
		}
	})

	t.Run("incremental dates", func(t *testing.T) {
		rd := r.(driver.IncrementalDateReader)
		col, typ, found := rd.GetDateColumnInfo(ctx, "dbo", "Users", []string{"missing", "CreatedAt"})
		if col != "CreatedAt" || typ != "datetime2" || !found {
			t.Errorf("date info: (%q,%q,%v)", col, typ, found)
		}
		m, err := rd.GetMaxDateColumnValue(ctx, "dbo", "Users", "CreatedAt")
		if err != nil || m == nil {
			t.Errorf("max date: %v (%v)", m, err)
		}
	})
}

// Writer behavior: the op sequence the differential ran, with the
// observable state asserted directly.
func TestMssqlCatalogWriterBehavior(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_msbehav_tgt"
	mssqlTestBootstrap(t, dbName)

	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	f := false
	cfg := &dbconfig.TargetConfig{
		Type: "mssql", Host: "localhost", Port: 1433,
		Database: dbName, User: "sa", Password: "TestPass2024",
		Schema: "dbo", ChunkSize: 1000,
	}
	cfg.Encrypt = &f
	w, err := NewWriter(cat, cfg, 4, driver.WriterOptions{BatchSize: 1000, SourceType: "mssql", TypeMapper: tm})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

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

	if err := w.CreateSchema(ctx, "dbo"); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	if err := w.CreateTable(ctx, tbl, "dbo"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := w.DropTable(ctx, "dbo", tbl.Name); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if err := w.CreateTable(ctx, tbl, "dbo"); err != nil {
		t.Fatalf("CreateTable after drop: %v", err)
	}
	if err := w.CreatePrimaryKey(ctx, tbl, "dbo"); err != nil {
		t.Fatalf("CreatePrimaryKey: %v", err)
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, Rows: rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	// #227 idempotent replay is a no-op.
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, Rows: rows,
		IdempotentOnDup: true, PKColumns: []string{"Id"},
	}); err != nil {
		t.Fatalf("idempotent WriteBatch: %v", err)
	}
	if err := w.(driver.Upserter).UpsertBatch(ctx, driver.UpsertBatchOptions{
		Schema: "dbo", Table: "Items", Columns: cols, ColumnTypes: colTypes, PKColumns: []string{"Id"},
		Rows: [][]any{
			{int64(2), "b-updated", true, 9.99},
			{int64(4), "d", false, 4.00},
		},
	}); err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}
	if err := w.(driver.SequenceResetter).ResetSequence(ctx, "dbo", tbl); err != nil {
		t.Fatalf("ResetSequence: %v", err)
	}
	newCol := &driver.Column{Name: "Note", DataType: "nvarchar", MaxLength: -1, IsNullable: true}
	if err := w.AddColumn(ctx, tbl, newCol, "dbo"); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if err := w.AddColumn(ctx, tbl, newCol, "dbo"); err != nil {
		t.Fatalf("AddColumn idempotent: %v", err)
	}
	relax := &driver.Column{Name: "Name", DataType: "nvarchar", MaxLength: 80, IsNullable: true}
	if err := w.DropColumnNotNull(ctx, tbl, relax, "dbo"); err != nil {
		t.Fatalf("DropColumnNotNull: %v", err)
	}

	db, err := sql.Open("sqlserver", mssqlDSNBase+dbName+"&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var data []string
	dataRows, err := db.Query("SELECT Id, Name, Active, Price FROM Items ORDER BY Id")
	if err != nil {
		t.Fatal(err)
	}
	for dataRows.Next() {
		var id int64
		var name string
		var active sql.NullBool
		var price sql.NullString
		if err := dataRows.Scan(&id, &name, &active, &price); err != nil {
			t.Fatal(err)
		}
		a := ""
		if active.Valid {
			a = fmt.Sprintf("%v", active.Bool)
		}
		data = append(data, fmt.Sprintf("%d|%s|%s|%s", id, name, a, price.String))
	}
	dataRows.Close()
	want := []string{"1|a|true|1.50", "2|b-updated|true|9.99", "3|c||", "4|d|false|4.00"}
	if !reflect.DeepEqual(data, want) {
		t.Errorf("data = %v, want %v", data, want)
	}

	// CHECKIDENT reseeded the identity to MAX(Id).
	var seed sql.NullInt64
	_ = db.QueryRow(`SELECT CONVERT(BIGINT, IDENT_CURRENT('dbo.Items'))`).Scan(&seed)
	if seed.Int64 != 4 {
		t.Errorf("identity seed = %d, want 4", seed.Int64)
	}

	// Validation's timeout fallback depends on a genuinely cheap
	// stats-based count (sys.partitions).
	n, err := w.GetRowCountFast(ctx, "dbo", "Items")
	if err != nil || n != 4 {
		t.Errorf("fast count = %d (%v), want 4", n, err)
	}

	var nameNullable string
	if err := db.QueryRow(`SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Items' AND COLUMN_NAME = 'Name'`).Scan(&nameNullable); err != nil {
		t.Fatal(err)
	}
	if nameNullable != "YES" {
		t.Errorf("Name nullable = %q after DropColumnNotNull, want YES", nameNullable)
	}

	// Generic-only (oracle defect, fixed in the catalog engine): bulk
	// load must KEEP_NULLS — without it SQL Server writes the column
	// DEFAULT over source NULLs.
	t.Run("bulk load preserves NULL over DEFAULT", func(t *testing.T) {
		if _, err := db.Exec(`CREATE TABLE Defaulted (Id INT PRIMARY KEY, Qty INT DEFAULT 42)`); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
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
