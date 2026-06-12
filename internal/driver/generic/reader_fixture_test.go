package generic

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// Fixture DDL exercises the introspection surface: AUTOINCREMENT
// identity, parameterized types, composite PK with non-declaration
// ordinal order, defaults, plain + unique + composite indexes, and a
// multi-column FK with actions.
const fixtureDDL = `
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(120) NOT NULL,
    bio TEXT,
    balance NUMERIC(10, 2) DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_users_name ON users(name);
CREATE UNIQUE INDEX idx_users_name_created ON users(name, created_at);

CREATE TABLE orders (
    region VARCHAR(10) NOT NULL,
    order_no INTEGER NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL,
    note TEXT,
    PRIMARY KEY (order_no, region)
);

CREATE TABLE line_items (
    region VARCHAR(10) NOT NULL,
    order_no INTEGER NOT NULL,
    line INTEGER NOT NULL,
    qty INTEGER NOT NULL DEFAULT 1,
    updated_at DATETIME,
    PRIMARY KEY (region, order_no, line),
    FOREIGN KEY (order_no, region) REFERENCES orders(order_no, region) ON DELETE CASCADE
);

INSERT INTO users (name, balance) VALUES ('a', 1.5), ('b', 2.5), ('c', 0);
INSERT INTO orders (region, order_no, user_id) VALUES ('us', 1, 1), ('eu', 2, 2);
INSERT INTO line_items (region, order_no, line, updated_at) VALUES
    ('us', 1, 1, '2024-06-15 10:30:00'), ('us', 1, 2, NULL), ('eu', 2, 1, '2024-06-16 11:00:00');
`

func fixtureDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fixtureDDL); err != nil {
		t.Fatal(err)
	}
	return path
}

func openReader(t *testing.T, path string) driver.Reader {
	t.Helper()
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewReader(cat, &dbconfig.SourceConfig{Type: "sqlite", Database: path}, 4)
	if err != nil {
		t.Fatalf("generic NewReader: %v", err)
	}
	t.Cleanup(func() { gen.Close() })
	return gen
}

// Literal expectations on the composite-PK/identity/FK fixture. These
// pinned values were proven deep-equal to the hand-written sqlite
// reader by the differential test that ran until #506 removed it.
func TestSQLiteCatalogReaderFixture(t *testing.T) {
	ctx := context.Background()
	gen := openReader(t, fixtureDB(t))

	tables, err := gen.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatalf("ExtractSchema: %v", err)
	}
	if len(tables) != 3 {
		t.Fatalf("table count = %d, want 3", len(tables))
	}
	byName := map[string]*driver.Table{}
	for i := range tables {
		tbl := &tables[i]
		byName[tbl.Name] = tbl
		if err := gen.LoadIndexes(ctx, tbl); err != nil {
			t.Fatal(err)
		}
		if err := gen.LoadForeignKeys(ctx, tbl); err != nil {
			t.Fatal(err)
		}
		if err := gen.LoadCheckConstraints(ctx, tbl); err != nil {
			t.Fatal(err)
		}
	}

	users := byName["users"]
	if got := users.PrimaryKey; !reflect.DeepEqual(got, []string{"id"}) {
		t.Errorf("users PK = %v", got)
	}
	if !users.Columns[0].IsIdentity {
		t.Error("users.id must be flagged identity (AUTOINCREMENT)")
	}
	name := users.Columns[1]
	if name.DataType != "varchar" || name.MaxLength != 120 || name.IsNullable {
		t.Errorf("users.name = %+v, want varchar(120) NOT NULL", name)
	}
	balance := users.Columns[3]
	if balance.DataType != "numeric" || balance.Precision != 10 || balance.Scale != 2 {
		t.Errorf("users.balance = %+v, want numeric(10,2)", balance)
	}
	idxNames := map[string]bool{}
	for _, idx := range users.Indexes {
		idxNames[idx.Name] = idx.IsUnique
	}
	if uniq, ok := idxNames["idx_users_name_created"]; !ok || !uniq {
		t.Errorf("users indexes = %v, want unique idx_users_name_created", idxNames)
	}

	// Composite PK in pk-ordinal order (declaration order of the
	// PRIMARY KEY clause, not column order).
	if got := byName["orders"].PrimaryKey; !reflect.DeepEqual(got, []string{"order_no", "region"}) {
		t.Errorf("orders PK = %v, want [order_no region]", got)
	}
	li := byName["line_items"]
	if got := li.PrimaryKey; !reflect.DeepEqual(got, []string{"region", "order_no", "line"}) {
		t.Errorf("line_items PK = %v", got)
	}
	// Multi-column FK grouped into one constraint with its action.
	if len(li.ForeignKeys) != 1 {
		t.Fatalf("line_items FKs = %+v, want 1", li.ForeignKeys)
	}
	fk := li.ForeignKeys[0]
	if fk.RefTable != "orders" || !reflect.DeepEqual(fk.Columns, []string{"order_no", "region"}) ||
		!reflect.DeepEqual(fk.RefColumns, []string{"order_no", "region"}) || fk.OnDelete != "CASCADE" {
		t.Errorf("line_items FK = %+v", fk)
	}

	t.Run("row counts and partitions", func(t *testing.T) {
		wantCounts := map[string]int64{"users": 3, "orders": 2, "line_items": 3}
		for name, want := range wantCounts {
			if got, err := gen.GetRowCount(ctx, "", name); err != nil || got != want {
				t.Errorf("GetRowCount(%s) = %d, %v; want %d", name, got, err, want)
			}
		}
		parts, err := gen.GetPartitionBoundaries(ctx, users, 8)
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != 1 || parts[0].PartitionID != 1 || !parts[0].IsFirstPartition || parts[0].RowCount != 3 {
			t.Errorf("partitions = %+v, want single first partition of 3 rows", parts)
		}
	})

	t.Run("incremental dates", func(t *testing.T) {
		dates, ok := gen.(driver.IncrementalDateReader)
		if !ok {
			t.Fatal("reader must expose IncrementalDateReader (catalog declares it)")
		}
		col, typ, found := dates.GetDateColumnInfo(ctx, "", "line_items", []string{"missing_col", "updated_at"})
		if !found || col != "updated_at" || typ != "datetime" {
			t.Errorf("GetDateColumnInfo = (%q,%q,%v)", col, typ, found)
		}
		max, err := dates.GetMaxDateColumnValue(ctx, "", "line_items", "updated_at")
		if err != nil || max == nil {
			t.Fatalf("GetMaxDateColumnValue = %v, %v", max, err)
		}
		if got := max.Format("2006-01-02 15:04:05"); got != "2024-06-16 11:00:00" {
			t.Errorf("max updated_at = %q", got)
		}
	})

	if gen.DBType() != "sqlite" || gen.MaxConns() != 4 || gen.PoolStats().DBType != "sqlite" {
		t.Errorf("identity: DBType=%q MaxConns=%d", gen.DBType(), gen.MaxConns())
	}
}
