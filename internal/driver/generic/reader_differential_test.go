package generic

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/sqlite"
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

func openBoth(t *testing.T, path string) (gen, ref driver.Reader) {
	t.Helper()
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &dbconfig.SourceConfig{Type: "sqlite", Database: path}
	gen, err = NewReader(cat, cfg, 4)
	if err != nil {
		t.Fatalf("generic NewReader: %v", err)
	}
	t.Cleanup(func() { gen.Close() })
	refReader, err := sqlite.NewReader(cfg, 4)
	if err != nil {
		t.Fatalf("sqlite NewReader: %v", err)
	}
	t.Cleanup(func() { refReader.Close() })
	return gen, refReader
}

// The PR-2 acceptance bar: the catalog-driven Reader's view of a real
// database is deep-equal to the hand-written sqlite Reader's, with the
// hand-written implementation as the oracle.
func TestSQLiteCatalogMatchesHandWrittenReader(t *testing.T) {
	ctx := context.Background()
	gen, ref := openBoth(t, fixtureDB(t))

	genTables, err := gen.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatalf("generic ExtractSchema: %v", err)
	}
	refTables, err := ref.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatalf("sqlite ExtractSchema: %v", err)
	}
	if len(genTables) != len(refTables) {
		t.Fatalf("table count: %d != %d", len(genTables), len(refTables))
	}

	for i := range refTables {
		genT, refT := &genTables[i], &refTables[i]
		if err := gen.LoadIndexes(ctx, genT); err != nil {
			t.Fatalf("generic LoadIndexes(%s): %v", genT.Name, err)
		}
		if err := ref.LoadIndexes(ctx, refT); err != nil {
			t.Fatal(err)
		}
		if err := gen.LoadForeignKeys(ctx, genT); err != nil {
			t.Fatalf("generic LoadForeignKeys(%s): %v", genT.Name, err)
		}
		if err := ref.LoadForeignKeys(ctx, refT); err != nil {
			t.Fatal(err)
		}
		if err := gen.LoadCheckConstraints(ctx, genT); err != nil {
			t.Fatal(err)
		}
		if err := ref.LoadCheckConstraints(ctx, refT); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(genT, refT) {
			t.Errorf("table %s diverges:\n  generic: %+v\n  sqlite:  %+v", refT.Name, genT, refT)
		}
	}

	t.Run("row counts", func(t *testing.T) {
		for _, tbl := range refTables {
			for _, fn := range []string{"GetRowCount", "GetRowCountFast", "GetRowCountExact"} {
				got, want := countVia(t, fn, gen, tbl.Name), countVia(t, fn, ref, tbl.Name)
				if got != want {
					t.Errorf("%s(%s): %d != %d", fn, tbl.Name, got, want)
				}
			}
		}
	})

	t.Run("partition boundaries", func(t *testing.T) {
		for i := range refTables {
			got, err := gen.GetPartitionBoundaries(ctx, &refTables[i], 8)
			if err != nil {
				t.Fatalf("generic GetPartitionBoundaries(%s): %v", refTables[i].Name, err)
			}
			want, err := ref.GetPartitionBoundaries(ctx, &refTables[i], 8)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("partitions(%s): %#v != %#v", refTables[i].Name, got, want)
			}
		}
	})

	t.Run("incremental date capability", func(t *testing.T) {
		genDates, ok := gen.(driver.IncrementalDateReader)
		if !ok {
			t.Fatal("generic reader must expose IncrementalDateReader (catalog declares it)")
		}
		refDates := ref.(driver.IncrementalDateReader)

		candidates := []string{"missing_col", "updated_at", "created_at"}
		gotCol, gotType, gotFound := genDates.GetDateColumnInfo(ctx, "", "line_items", candidates)
		wantCol, wantType, wantFound := refDates.GetDateColumnInfo(ctx, "", "line_items", candidates)
		if gotCol != wantCol || gotType != wantType || gotFound != wantFound {
			t.Errorf("GetDateColumnInfo: (%q,%q,%v) != (%q,%q,%v)", gotCol, gotType, gotFound, wantCol, wantType, wantFound)
		}

		gotMax, err := genDates.GetMaxDateColumnValue(ctx, "", "line_items", "updated_at")
		if err != nil {
			t.Fatal(err)
		}
		wantMax, err := refDates.GetMaxDateColumnValue(ctx, "", "line_items", "updated_at")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(gotMax, wantMax) {
			t.Errorf("GetMaxDateColumnValue: %v != %v", gotMax, wantMax)
		}
	})

	t.Run("pool identity", func(t *testing.T) {
		if gen.DBType() != ref.DBType() {
			t.Errorf("DBType: %q != %q", gen.DBType(), ref.DBType())
		}
		if gen.MaxConns() != ref.MaxConns() {
			t.Errorf("MaxConns: %d != %d", gen.MaxConns(), ref.MaxConns())
		}
		if gen.PoolStats().DBType != ref.PoolStats().DBType {
			t.Errorf("PoolStats.DBType: %q != %q", gen.PoolStats().DBType, ref.PoolStats().DBType)
		}
	})
}

func countVia(t *testing.T, fn string, r driver.Reader, table string) int64 {
	t.Helper()
	ctx := context.Background()
	var n int64
	var err error
	switch fn {
	case "GetRowCount":
		n, err = r.GetRowCount(ctx, "", table)
	case "GetRowCountFast":
		n, err = r.GetRowCountFast(ctx, "", table)
	case "GetRowCountExact":
		n, err = r.GetRowCountExact(ctx, "", table, false)
	}
	if err != nil {
		t.Fatalf("%s(%s): %v", fn, table, err)
	}
	return n
}
