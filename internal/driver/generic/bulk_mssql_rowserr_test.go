package generic

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

var erroringRowsDriverSeq atomic.Uint64

// openErroringRowsDB returns a *sql.DB backed by a fake driver whose queries
// yield one row and then fail, so the consumer's rows.Err() is non-nil after
// iteration. Used to pin the rows.Err() checks in the mssql staging/spatial
// introspection (#567).
func openErroringRowsDB(t *testing.T, ncols int) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("erroring_rows_%d", erroringRowsDriverSeq.Add(1))
	sql.Register(name, &erroringRowsDriver{ncols: ncols})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open fake driver: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var errInjectedMidResultSet = errors.New("injected mid-result-set read failure")

type erroringRowsDriver struct{ ncols int }

func (d *erroringRowsDriver) Open(string) (sqldriver.Conn, error) {
	return &erroringConn{ncols: d.ncols}, nil
}

type erroringConn struct{ ncols int }

func (c *erroringConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}
func (c *erroringConn) Close() error                 { return nil }
func (c *erroringConn) Begin() (sqldriver.Tx, error) { return nil, errors.New("no tx") }

func (c *erroringConn) QueryContext(_ context.Context, _ string, _ []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return &erroringRows{ncols: c.ncols}, nil
}

type erroringRows struct {
	ncols   int
	yielded bool
}

func (r *erroringRows) Columns() []string {
	cols := make([]string, r.ncols)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d", i)
	}
	return cols
}

func (r *erroringRows) Close() error { return nil }

func (r *erroringRows) Next(dest []sqldriver.Value) error {
	if !r.yielded {
		r.yielded = true
		for i := range dest {
			dest[i] = "x"
		}
		return nil
	}
	// After one row, fail with a non-EOF error — database/sql surfaces this
	// via rows.Err() and stops the consumer's Next() loop.
	return errInjectedMidResultSet
}

var _ io.Closer = (*erroringRows)(nil)

func TestMssqlGetStagingTableColumns_SurfacesRowsErr(t *testing.T) {
	db := openErroringRowsDB(t, 1) // one column: colName
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	_, err = mssqlGetStagingTableColumns(context.Background(), conn, "#stg")
	if err == nil {
		t.Fatal("expected the mid-result-set read error, got nil (truncated column list swallowed)")
	}
	if !strings.Contains(err.Error(), "injected mid-result-set read failure") {
		t.Fatalf("expected the injected read error, got: %v", err)
	}
}

func TestMssqlGetSpatialColumns_SurfacesRowsErr(t *testing.T) {
	db := openErroringRowsDB(t, 2) // two columns: name, type_name
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	_, err = mssqlGetSpatialColumns(context.Background(), conn, "#stg")
	if err == nil {
		t.Fatal("expected the mid-result-set read error, got nil (missed spatial column swallowed)")
	}
	if !strings.Contains(err.Error(), "injected mid-result-set read failure") {
		t.Fatalf("expected the injected read error, got: %v", err)
	}
}
