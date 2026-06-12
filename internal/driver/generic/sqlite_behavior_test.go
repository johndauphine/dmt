// Ported from the hand-written sqlite package when #506 removed it —
// the same behavioral expectations now run against the catalog engine.
package generic

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// memorySourceCfg builds a SourceConfig pointed at an in-memory SQLite DB
// that's shared across opens via the cache=shared scheme. Each call uses
// a unique URI so tests don't collide.
func memoryDBPath(t *testing.T, name string) string {
	t.Helper()
	// On-disk temp file gives the cleanest isolation; :memory: with
	// shared cache requires the file: prefix and a unique URI per test.
	return filepath.Join(t.TempDir(), name+".db")
}

// ---------- Dialect ----------

func TestDialect_QuoteAndQualify(t *testing.T) {
	d := NewDialect(testCatalog(t))

	if got := d.QuoteIdentifier(`my"col`); got != `"my""col"` {
		t.Errorf("QuoteIdentifier: got %q, want %q", got, `"my""col"`)
	}

	// Schema is always ignored — SQLite has no schemas.
	if got := d.QualifyTable("ignored", "users"); got != `"users"` {
		t.Errorf("QualifyTable: got %q, want %q", got, `"users"`)
	}

	if got := d.ParameterPlaceholder(0); got != "?" {
		t.Errorf("ParameterPlaceholder: got %q, want ?", got)
	}
}

func TestDialect_BuildDSN(t *testing.T) {
	d := NewDialect(testCatalog(t))

	// File path passes through; PRAGMAs are baked in.
	dsn := d.BuildDSN("", 0, "/tmp/test.db", "", "", nil)
	if dsn == "" {
		t.Fatal("BuildDSN returned empty")
	}
	for _, want := range []string{
		"/tmp/test.db",
		"journal_mode%28WAL%29",
		"foreign_keys%28ON%29",
		"busy_timeout%2830000%29",
	} {
		if !contains(dsn, want) {
			t.Errorf("DSN missing %q; got %s", want, dsn)
		}
	}

	// Empty database resolves to in-memory.
	dsn2 := d.BuildDSN("", 0, "", "", "", nil)
	if !contains(dsn2, ":memory:") {
		t.Errorf("empty database should resolve to :memory:; got %s", dsn2)
	}
}

// ---------- Writer + Reader end-to-end (in-process) ----------

// newTestWriter builds a Writer pointed at a fresh temp .db file. The
// caller must Close.
func testCatalog(t *testing.T) *Catalog {
	t.Helper()
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func newTestWriter(t *testing.T, path string) driver.Writer {
	t.Helper()
	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatalf("GetTypeMapper: %v", err)
	}
	w, err := NewWriter(testCatalog(t),
		&dbconfig.TargetConfig{Type: "sqlite", Database: path, ChunkSize: 1000},
		1,
		driver.WriterOptions{
			BatchSize:  1000,
			SourceType: "sqlite",
			TypeMapper: tm,
		},
	)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w
}

func newTestReader(t *testing.T, path string) driver.Reader {
	t.Helper()
	r, err := NewReader(testCatalog(t),
		&dbconfig.SourceConfig{Type: "sqlite", Database: path, ChunkSize: 1000},
		2,
	)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r
}

func TestWriter_CreateAndInsert_SqliteToSqlite(t *testing.T) {
	path := memoryDBPath(t, "sqlite_to_sqlite")
	w := newTestWriter(t, path)
	defer w.Close()
	ctx := context.Background()

	src := &driver.Table{
		Schema: "",
		Name:   "users",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsNullable: false, IsIdentity: true, OrdinalPos: 1},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false, OrdinalPos: 2},
			{Name: "age", DataType: "integer", IsNullable: true, OrdinalPos: 3},
		},
		PrimaryKey: []string{"id"},
	}
	src.PopulatePKColumns()

	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	rows := [][]any{
		{int64(1), "a@x.com", int64(30)},
		{int64(2), "b@x.com", nil},
		{int64(3), "c@x.com", int64(45)},
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema:  "",
		Table:   "users",
		Columns: []string{"id", "email", "age"},
		Rows:    rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	count, err := w.GetRowCount(ctx, "", "users")
	if err != nil {
		t.Fatalf("GetRowCount: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestWriter_ConcurrentWriteBatchSerializesSingleConnection(t *testing.T) {
	path := memoryDBPath(t, "concurrent_write_batch")
	w := newTestWriter(t, path)
	defer w.Close()
	ctx := context.Background()

	if got := w.MaxConns(); got != 1 {
		t.Fatalf("MaxConns() = %d, want 1 for SQLite single-writer serialization", got)
	}
	if stats := w.PoolStats(); stats.MaxConns != 1 {
		t.Fatalf("PoolStats().MaxConns = %d, want 1 for SQLite single-writer serialization", stats.MaxConns)
	}

	src := &driver.Table{
		Name: "concurrent_rows",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsNullable: false, OrdinalPos: 1},
			{Name: "label", DataType: "text", IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey: []string{"id"},
	}
	src.PopulatePKColumns()
	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	const workers = 8
	const rowsPerWorker = 25

	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, workers)
	for workerID := 0; workerID < workers; workerID++ {
		workerID := workerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			rows := make([][]any, rowsPerWorker)
			baseID := workerID * rowsPerWorker
			for i := range rows {
				rows[i] = []any{int64(baseID + i + 1), "row"}
			}
			if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
				Table:     "concurrent_rows",
				Columns:   []string{"id", "label"},
				Rows:      rows,
				BatchSize: 5,
			}); err != nil {
				errCh <- err
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent WriteBatch returned error: %v", err)
	}

	count, err := w.GetRowCount(ctx, "", "concurrent_rows")
	if err != nil {
		t.Fatalf("GetRowCount: %v", err)
	}
	if want := int64(workers * rowsPerWorker); count != want {
		t.Fatalf("row count = %d, want %d", count, want)
	}
}

func TestWriter_AddColumn(t *testing.T) {
	path := memoryDBPath(t, "add_column")
	w := newTestWriter(t, path)
	defer w.Close()
	ctx := context.Background()

	table := &driver.Table{
		Name: "users",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsNullable: false, IsIdentity: true, OrdinalPos: 1},
		},
		PrimaryKey: []string{"id"},
	}
	if err := w.CreateTable(ctx, table, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	email := &driver.Column{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false}
	if err := w.AddColumn(ctx, table, email, ""); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if err := w.AddColumn(ctx, table, email, ""); err != nil {
		t.Fatalf("AddColumn second call should be idempotent: %v", err)
	}

	var colType string
	var notNull int
	err := w.DB().QueryRowContext(ctx,
		`SELECT type, "notnull" FROM pragma_table_info(?) WHERE name = ?`,
		"users", "email").Scan(&colType, &notNull)
	if err != nil {
		t.Fatalf("querying added column: %v", err)
	}
	if colType == "" {
		t.Fatal("added column type is empty")
	}
	if notNull != 0 {
		t.Fatalf("added column notnull = %d, want 0", notNull)
	}
}

func TestWriter_DropColumnNotNullUnsupported(t *testing.T) {
	w := &Writer{cat: testCatalog(t)}
	err := w.DropColumnNotNull(
		context.Background(),
		&driver.Table{Name: "users"},
		&driver.Column{Name: "email"},
		"",
	)
	if err == nil {
		t.Fatal("DropColumnNotNull returned nil error")
	}
	if !strings.Contains(err.Error(), "cannot relax NOT NULL") {
		t.Fatalf("error = %q, want unsupported message", err.Error())
	}
}

func TestWriter_AlterColumnTypeUnsupported(t *testing.T) {
	w := &Writer{cat: testCatalog(t)}
	err := w.AlterColumnType(
		context.Background(),
		&driver.Table{Name: "users"},
		&driver.Column{Name: "email"},
		"",
	)
	if err == nil {
		t.Fatal("AlterColumnType returned nil error")
	}
	if !strings.Contains(err.Error(), "cannot alter type") {
		t.Fatalf("error = %q, want unsupported message", err.Error())
	}
}

func TestWriter_IdempotentOnDup(t *testing.T) {
	path := memoryDBPath(t, "idempotent")
	w := newTestWriter(t, path)
	defer w.Close()
	ctx := context.Background()

	src := &driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsNullable: false, OrdinalPos: 1},
			{Name: "label", DataType: "text", IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey: []string{"id"},
	}
	src.PopulatePKColumns()

	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	cols := []string{"id", "label"}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Table:   "items",
		Columns: cols,
		Rows:    [][]any{{int64(1), "one"}, {int64(2), "two"}},
	}); err != nil {
		t.Fatalf("first WriteBatch: %v", err)
	}

	// Replay with IdempotentOnDup — should not error, should not duplicate.
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Table:           "items",
		Columns:         cols,
		PKColumns:       []string{"id"},
		IdempotentOnDup: true,
		Rows: [][]any{
			{int64(1), "one-modified"},
			{int64(2), "two-modified"},
			{int64(3), "three"},
		},
	}); err != nil {
		t.Fatalf("idempotent WriteBatch: %v", err)
	}

	count, _ := w.GetRowCount(ctx, "", "items")
	if count != 3 {
		t.Errorf("expected 3 rows after idempotent replay, got %d", count)
	}

	// INSERT OR IGNORE preserves the original value (does NOT update).
	var label string
	if err := w.DB().QueryRow(`SELECT label FROM items WHERE id = 1`).Scan(&label); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if label != "one" {
		t.Errorf("INSERT OR IGNORE should preserve original; got label=%q", label)
	}
}

func TestWriter_UpsertBatch(t *testing.T) {
	path := memoryDBPath(t, "upsert")
	w := newTestWriter(t, path)
	defer w.Close()
	ctx := context.Background()

	src := &driver.Table{
		Name: "kv",
		Columns: []driver.Column{
			{Name: "k", DataType: "text", IsNullable: false, OrdinalPos: 1},
			{Name: "v", DataType: "integer", IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey: []string{"k"},
	}
	src.PopulatePKColumns()
	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// First write seeds the table.
	if err := w.(driver.Upserter).UpsertBatch(ctx, driver.UpsertBatchOptions{
		Table:     "kv",
		Columns:   []string{"k", "v"},
		PKColumns: []string{"k"},
		Rows:      [][]any{{"a", int64(1)}, {"b", int64(2)}},
	}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	// Second write changes "a" and adds "c". UPSERT should update "a"
	// and insert "c".
	if err := w.(driver.Upserter).UpsertBatch(ctx, driver.UpsertBatchOptions{
		Table:     "kv",
		Columns:   []string{"k", "v"},
		PKColumns: []string{"k"},
		Rows:      [][]any{{"a", int64(99)}, {"c", int64(3)}},
	}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	var v int64
	if err := w.DB().QueryRow(`SELECT v FROM kv WHERE k = 'a'`).Scan(&v); err != nil {
		t.Fatalf("read a: %v", err)
	}
	if v != 99 {
		t.Errorf("upsert should have updated a; got v=%d", v)
	}
	count, _ := w.GetRowCount(ctx, "", "kv")
	if count != 3 {
		t.Errorf("expected 3 rows; got %d", count)
	}
}

func TestReader_SchemaExtraction(t *testing.T) {
	path := memoryDBPath(t, "schema_extract")

	// Seed the database directly via raw SQL so we exercise the
	// reader's introspection path without depending on the writer.
	dsn := (NewDialect(testCatalog(t))).BuildDSN("", 0, path, "", "", nil)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email VARCHAR(255) NOT NULL,
			age INTEGER DEFAULT 0
		)`,
		`CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`,
		`CREATE INDEX idx_posts_user ON posts(user_id)`,
		`INSERT INTO users (email, age) VALUES ('a@x', 30), ('b@x', NULL)`,
		`INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'hi')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v\nstmt: %s", err, s)
		}
	}

	r := newTestReader(t, path)
	defer r.Close()
	ctx := context.Background()

	tables, err := r.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	// tables returned sorted by name — check posts first, users second.
	names := []string{tables[0].Name, tables[1].Name}
	sort.Strings(names)
	if names[0] != "posts" || names[1] != "users" {
		t.Errorf("unexpected table names: %v", names)
	}

	// Find users table by name.
	var users *driver.Table
	for i := range tables {
		if tables[i].Name == "users" {
			users = &tables[i]
		}
	}
	if users == nil {
		t.Fatal("users table not found")
	}
	if len(users.Columns) != 3 {
		t.Errorf("users should have 3 columns; got %d", len(users.Columns))
	}
	if len(users.PrimaryKey) != 1 || users.PrimaryKey[0] != "id" {
		t.Errorf("users PK: got %v, want [id]", users.PrimaryKey)
	}
	// AUTOINCREMENT detected.
	if !users.Columns[0].IsIdentity {
		t.Error("users.id should be marked IsIdentity (AUTOINCREMENT)")
	}
	if users.RowCount != 2 {
		t.Errorf("users RowCount: got %d, want 2", users.RowCount)
	}
	if users.Columns[2].DefaultValue != "0" {
		t.Errorf("users.age DefaultValue: got %q, want 0", users.Columns[2].DefaultValue)
	}

	// Indexes
	var posts *driver.Table
	for i := range tables {
		if tables[i].Name == "posts" {
			posts = &tables[i]
		}
	}
	if err := r.LoadIndexes(ctx, posts); err != nil {
		t.Fatalf("LoadIndexes: %v", err)
	}
	if len(posts.Indexes) != 1 || posts.Indexes[0].Name != "idx_posts_user" {
		t.Errorf("posts indexes: %v", posts.Indexes)
	}

	// FKs
	if err := r.LoadForeignKeys(ctx, posts); err != nil {
		t.Fatalf("LoadForeignKeys: %v", err)
	}
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts should have 1 FK; got %d", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.RefTable != "users" || len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("posts FK shape: %+v", fk)
	}
}

func TestReader_GetDateColumnInfo_AcceptsConfiguredTextColumn(t *testing.T) {
	path := memoryDBPath(t, "date_column_text")
	dsn := (NewDialect(testCatalog(t))).BuildDSN("", 0, path, "", "", nil)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			updated_at TEXT NOT NULL,
			note TEXT
		)
	`); err != nil {
		t.Fatalf("create: %v", err)
	}

	r := newTestReader(t, path)
	defer r.Close()

	col, dataType, found := r.(driver.IncrementalDateReader).GetDateColumnInfo(
		context.Background(), "", "events", []string{"missing", "updated_at"})
	if !found {
		t.Fatal("expected configured TEXT date column to be found")
	}
	if col != "updated_at" || dataType != "text" {
		t.Fatalf("GetDateColumnInfo = (%q, %q, true), want (%q, %q, true)",
			col, dataType, "updated_at", "text")
	}
}

func TestReader_ReadTable_Keyset(t *testing.T) {
	path := memoryDBPath(t, "read_keyset")

	w := newTestWriter(t, path)
	ctx := context.Background()

	src := &driver.Table{
		Name: "rows",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer", IsNullable: false, OrdinalPos: 1},
			{Name: "v", DataType: "text", IsNullable: false, OrdinalPos: 2},
		},
		PrimaryKey: []string{"id"},
	}
	src.PopulatePKColumns()
	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	const N = 1000
	rows := make([][]any, N)
	for i := 0; i < N; i++ {
		rows[i] = []any{int64(i + 1), "row"}
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Table: "rows", Columns: []string{"id", "v"}, Rows: rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	w.Close()

	// Read back.
	r := newTestReader(t, path)
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	var tbl *driver.Table
	for i := range tables {
		if tables[i].Name == "rows" {
			tbl = &tables[i]
		}
	}
	if tbl == nil {
		t.Fatal("rows table not found")
	}

	parts, err := r.GetPartitionBoundaries(ctx, tbl, 4)
	if err != nil {
		t.Fatalf("GetPartitionBoundaries: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("expected single partition for sqlite; got %d", len(parts))
	}

	// Round-trip read-back coverage lives in the transfer-pipeline
	// integration tests; the dead driver-level ReadTable path was
	// removed in #463.
	_ = N
}

// ---------- Cross-engine type mapping ----------

func TestTypemap_CrossEngine_SqliteSourceToMSSQLTarget(t *testing.T) {
	m := driver.NewDeterministicMapper()
	cases := []struct {
		sqliteType string
		wantMSSQL  string
	}{
		{"INTEGER", "BIGINT"},
		{"REAL", "FLOAT"},
		{"TEXT", "NVARCHAR(MAX)"},
		{"BLOB", "VARBINARY(MAX)"},
		{"BOOLEAN", "BIT"},
		{"DATETIME", "DATETIME2"},
	}
	for _, tc := range cases {
		t.Run(tc.sqliteType+"_to_mssql", func(t *testing.T) {
			got := m.MapType(driver.TypeInfo{
				SourceDBType: "sqlite",
				TargetDBType: "mssql",
				DataType:     tc.sqliteType,
			})
			if got != tc.wantMSSQL {
				t.Errorf("sqlite %s → mssql: got %q, want %q", tc.sqliteType, got, tc.wantMSSQL)
			}
		})
	}
}

func TestTypemap_CrossEngine_AnySourceToSqliteTarget(t *testing.T) {
	m := driver.NewDeterministicMapper()
	cases := []struct {
		source     string
		sourceType string
		wantSqlite string
	}{
		{"postgres", "int4", "INTEGER"},
		{"postgres", "varchar", "TEXT"}, // unbounded varchar
		{"postgres", "bytea", "BLOB"},
		{"postgres", "uuid", "TEXT"},
		{"mssql", "uniqueidentifier", "TEXT"},
		{"mssql", "datetime2", "DATETIME"},
		{"mysql", "tinyint", "SMALLINT"},
		{"mysql", "longtext", "TEXT"},
	}
	for _, tc := range cases {
		t.Run(tc.source+"_"+tc.sourceType+"_to_sqlite", func(t *testing.T) {
			got := m.MapType(driver.TypeInfo{
				SourceDBType: tc.source,
				TargetDBType: "sqlite",
				DataType:     tc.sourceType,
			})
			if got != tc.wantSqlite {
				t.Errorf("%s %s → sqlite: got %q, want %q", tc.source, tc.sourceType, got, tc.wantSqlite)
			}
		})
	}
}

func TestGenerateTableDDL_SqliteTarget_AutoIncrementPK(t *testing.T) {
	m := driver.NewDeterministicMapper()
	resp, err := m.GenerateTableDDL(context.Background(), driver.TableDDLRequest{
		SourceDBType: "postgres",
		TargetDBType: "sqlite",
		SourceTable: &driver.Table{
			Name: "users",
			Columns: []driver.Column{
				{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
				{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false},
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	ddl := resp.CreateTableDDL
	// Must be INTEGER PRIMARY KEY AUTOINCREMENT inline — not a separate
	// CONSTRAINT clause.
	if !contains(ddl, "INTEGER PRIMARY KEY AUTOINCREMENT") {
		t.Errorf("DDL missing INTEGER PRIMARY KEY AUTOINCREMENT; got:\n%s", ddl)
	}
	if contains(ddl, `CONSTRAINT "pk_users"`) {
		t.Errorf("DDL should not have a separate PK constraint for autoincrement; got:\n%s", ddl)
	}
	if !contains(ddl, `"email" VARCHAR(255) NOT NULL`) {
		t.Errorf("DDL missing email column; got:\n%s", ddl)
	}
}

// ---------- Edge cases for SQLite DDL emission ----------

// SQLite's `INTEGER PRIMARY KEY AUTOINCREMENT` form is valid ONLY for a
// single-column integer PK. Composite PKs and identity-columns-outside-PK
// must NOT use the inline form, otherwise the generated DDL is invalid
// or silently loses the composite PK shape.
func TestGenerateTableDDL_SqliteTarget_CompositePKWithIdentity(t *testing.T) {
	m := driver.NewDeterministicMapper()
	resp, err := m.GenerateTableDDL(context.Background(), driver.TableDDLRequest{
		SourceDBType: "postgres",
		TargetDBType: "sqlite",
		SourceTable: &driver.Table{
			Name: "audit",
			Columns: []driver.Column{
				{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
				{Name: "tenant", DataType: "int4", IsNullable: false},
			},
			PrimaryKey: []string{"tenant", "id"},
		},
	})
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	ddl := resp.CreateTableDDL

	// Inline AUTOINCREMENT must NOT appear — composite PK is incompatible.
	if contains(ddl, "AUTOINCREMENT") {
		t.Errorf("composite PK must not use AUTOINCREMENT inline; got:\n%s", ddl)
	}
	// Table-level composite PK must be present.
	if !contains(ddl, `PRIMARY KEY ("tenant", "id")`) {
		t.Errorf("composite PK missing from DDL; got:\n%s", ddl)
	}
}

// SQLite's table-level PRIMARY KEY constraint does NOT implicitly mark
// its columns NOT NULL (unlike PG/MSSQL/MySQL). So when we suppress the
// inline `PRIMARY KEY AUTOINCREMENT` form for a composite PK, we must
// still emit `NOT NULL` on the identity column — otherwise the source's
// non-null constraint is silently dropped.
func TestGenerateTableDDL_SqliteTarget_CompositePKKeepsNotNull(t *testing.T) {
	m := driver.NewDeterministicMapper()
	resp, err := m.GenerateTableDDL(context.Background(), driver.TableDDLRequest{
		SourceDBType: "postgres",
		TargetDBType: "sqlite",
		SourceTable: &driver.Table{
			Name: "audit",
			Columns: []driver.Column{
				{Name: "tenant", DataType: "int4", IsNullable: false},
				{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
			},
			PrimaryKey: []string{"tenant", "id"},
		},
	})
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	ddl := resp.CreateTableDDL

	if !contains(ddl, `"id" INTEGER NOT NULL`) {
		t.Errorf("identity column in composite PK must remain NOT NULL on SQLite; got:\n%s", ddl)
	}
	if !contains(ddl, `"tenant" INTEGER NOT NULL`) {
		t.Errorf("tenant column should be NOT NULL; got:\n%s", ddl)
	}
}

func TestGenerateTableDDL_SqliteTarget_IdentityOutsidePK(t *testing.T) {
	m := driver.NewDeterministicMapper()
	resp, err := m.GenerateTableDDL(context.Background(), driver.TableDDLRequest{
		SourceDBType: "postgres",
		TargetDBType: "sqlite",
		SourceTable: &driver.Table{
			Name: "events",
			Columns: []driver.Column{
				// PK is "uuid"; "seq" is identity but NOT in the PK.
				{Name: "uuid", DataType: "uuid", IsNullable: false},
				{Name: "seq", DataType: "int4", IsNullable: false, IsIdentity: true},
			},
			PrimaryKey: []string{"uuid"},
		},
	})
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	ddl := resp.CreateTableDDL

	// Identity column outside PK must NOT use AUTOINCREMENT.
	if contains(ddl, "AUTOINCREMENT") {
		t.Errorf("identity column outside PK must not use AUTOINCREMENT; got:\n%s", ddl)
	}
	// The PK on "uuid" must be present (table-level since it's not the
	// identity column path).
	if !contains(ddl, `PRIMARY KEY ("uuid")`) {
		t.Errorf("PK on uuid missing; got:\n%s", ddl)
	}
}

// ---------- End-to-end: postgres source schema → sqlite target ----------

func TestEndToEnd_PostgresSchemaToSqlite(t *testing.T) {
	path := memoryDBPath(t, "pg_to_sqlite")
	// Built directly (not via newTestWriter) so the typemap chain maps
	// postgres-source types to sqlite-target types.
	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(testCatalog(t),
		&dbconfig.TargetConfig{Type: "sqlite", Database: path, ChunkSize: 1000}, 1,
		driver.WriterOptions{BatchSize: 1000, SourceType: "postgres", TypeMapper: tm})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx := context.Background()

	// Simulate a PostgreSQL source table.
	src := &driver.Table{
		Schema: "public",
		Name:   "orders",
		Columns: []driver.Column{
			{Name: "id", DataType: "int4", IsNullable: false, IsIdentity: true},
			{Name: "customer_id", DataType: "uuid", IsNullable: false},
			{Name: "total", DataType: "numeric", Precision: 10, Scale: 2, IsNullable: false},
			{Name: "notes", DataType: "text", IsNullable: true},
			{Name: "created_at", DataType: "timestamp", IsNullable: false},
		},
		PrimaryKey: []string{"id"},
	}
	src.PopulatePKColumns()

	if err := w.CreateTable(ctx, src, ""); err != nil {
		t.Fatalf("CreateTable (pg→sqlite): %v", err)
	}

	// Write a row using the column shapes the source would have produced.
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "total", "notes", "created_at"},
		Rows: [][]any{
			{int64(1), "11111111-1111-1111-1111-111111111111", "199.99", "first", "2026-01-01 00:00:00"},
		},
	}); err != nil {
		t.Fatalf("WriteBatch (pg→sqlite): %v", err)
	}

	count, _ := w.GetRowCount(ctx, "", "orders")
	if count != 1 {
		t.Errorf("expected 1 row; got %d", count)
	}
}

// ---------- PRAGMA type parameter parsing ----------

func TestParseSqliteType(t *testing.T) {
	tests := []struct {
		in        string
		wantBase  string
		wantMax   int
		wantPrec  int
		wantScale int
	}{
		{"INTEGER", "integer", 0, 0, 0},
		{"VARCHAR(255)", "varchar", 255, 0, 0},
		{"varchar (100)", "varchar", 100, 0, 0},
		{"NUMERIC(10, 2)", "numeric", 10, 10, 2},
		{"DECIMAL(18,4)", "decimal", 18, 18, 4},
		{"TEXT", "text", 0, 0, 0},
		{"", "", 0, 0, 0},
		{"BLOB(garbage)", "blob", 0, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			b, m, p, s := parseDeclaredType(tc.in)
			if b != tc.wantBase || m != tc.wantMax || p != tc.wantPrec || s != tc.wantScale {
				t.Errorf("parseDeclaredType(%q) = (%q, %d, %d, %d); want (%q, %d, %d, %d)",
					tc.in, b, m, p, s, tc.wantBase, tc.wantMax, tc.wantPrec, tc.wantScale)
			}
		})
	}
}

// SQLite source with bounded VARCHAR and NUMERIC should preserve length
// and precision when mapped to a postgres target. Pre-fix, the reader
// stored only the raw type string and the typemap collapsed bounded
// types to unbounded (VARCHAR(255) → TEXT).
func TestReader_BoundedTypesPreserved(t *testing.T) {
	path := memoryDBPath(t, "bounded_types")

	dsn := (NewDialect(testCatalog(t))).BuildDSN("", 0, path, "", "", nil)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			label VARCHAR(50) NOT NULL,
			price NUMERIC(10, 2) NOT NULL
		)
	`); err != nil {
		t.Fatalf("create: %v", err)
	}

	r := newTestReader(t, path)
	defer r.Close()
	tables, err := r.ExtractSchema(context.Background(), "")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table; got %d", len(tables))
	}
	cols := tables[0].Columns
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns; got %d", len(cols))
	}

	var label, price *driver.Column
	for i := range cols {
		switch cols[i].Name {
		case "label":
			label = &cols[i]
		case "price":
			price = &cols[i]
		}
	}
	if label == nil || price == nil {
		t.Fatal("label or price column missing")
	}
	if label.MaxLength != 50 {
		t.Errorf("label.MaxLength: got %d, want 50", label.MaxLength)
	}
	if price.Precision != 10 || price.Scale != 2 {
		t.Errorf("price (Precision, Scale): got (%d, %d), want (10, 2)", price.Precision, price.Scale)
	}

	// Now confirm the deterministic mapper preserves the bounds across
	// engines (sqlite → postgres).
	m := driver.NewDeterministicMapper()
	gotLabel := m.MapType(driver.TypeInfo{
		SourceDBType: "sqlite", TargetDBType: "postgres",
		DataType: label.DataType, MaxLength: label.MaxLength,
	})
	if gotLabel != "VARCHAR(50)" {
		t.Errorf("sqlite VARCHAR(50) → postgres: got %q, want VARCHAR(50)", gotLabel)
	}
	gotPrice := m.MapType(driver.TypeInfo{
		SourceDBType: "sqlite", TargetDBType: "postgres",
		DataType: price.DataType, Precision: price.Precision, Scale: price.Scale,
	})
	if gotPrice != "NUMERIC(10, 2)" {
		t.Errorf("sqlite NUMERIC(10,2) → postgres: got %q, want NUMERIC(10, 2)", gotPrice)
	}
}

// ---------- helpers ----------

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && stringIndex(haystack, needle) >= 0
}

func stringIndex(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
