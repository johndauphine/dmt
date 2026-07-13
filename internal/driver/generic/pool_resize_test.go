package generic

import (
	"context"
	"database/sql"
	"testing"
)

func TestReaderResizeConnectionPoolUpdatesLiveAndCachedLimits(t *testing.T) {
	db := openPoolResizeDB(t)
	reader := &Reader{db: db, cat: &Catalog{Name: "test"}}

	if got := reader.ResizeConnectionPool(8); got != 8 {
		t.Fatalf("ResizeConnectionPool() = %d, want 8", got)
	}
	if got := reader.MaxConns(); got != 8 {
		t.Fatalf("MaxConns() = %d, want 8", got)
	}
	if got := db.Stats().MaxOpenConnections; got != 8 {
		t.Fatalf("database/sql max open = %d, want 8", got)
	}

	fillIdlePool(t, db, 5)
	if got := db.Stats().Idle; got != 2 {
		t.Fatalf("reader idle connections = %d, want 2 for max-open 8", got)
	}

	if got := reader.ResizeConnectionPool(3); got != 3 {
		t.Fatalf("shrinking ResizeConnectionPool() = %d, want 3", got)
	}
	if got := reader.MaxConns(); got != 3 {
		t.Fatalf("MaxConns() after shrink = %d, want 3", got)
	}
	if got := db.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("database/sql max open after shrink = %d, want 3", got)
	}
}

func TestWriterResizeConnectionPoolUpdatesLiveAndCachedLimits(t *testing.T) {
	db := openPoolResizeDB(t)
	writer := &Writer{db: db, cat: &Catalog{Name: "test"}}

	if got := writer.ResizeConnectionPool(6); got != 6 {
		t.Fatalf("ResizeConnectionPool() = %d, want 6", got)
	}
	if got := writer.MaxConns(); got != 6 {
		t.Fatalf("MaxConns() = %d, want 6", got)
	}
	if got := db.Stats().MaxOpenConnections; got != 6 {
		t.Fatalf("database/sql max open = %d, want 6", got)
	}

	fillIdlePool(t, db, 4)
	if got := db.Stats().Idle; got != 4 {
		t.Fatalf("writer idle connections = %d, want 4", got)
	}
}

func TestSingleWriterResizeConnectionPoolRemainsOne(t *testing.T) {
	db := openPoolResizeDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	writer := &Writer{
		db:       db,
		cat:      &Catalog{Name: "sqlite", Connection: ConnectionSpec{SingleWriter: true}},
		maxConns: 1,
	}

	if got := writer.ResizeConnectionPool(32); got != 1 {
		t.Fatalf("ResizeConnectionPool() = %d, want SQLite clamp 1", got)
	}
	if got := writer.MaxConns(); got != 1 {
		t.Fatalf("MaxConns() = %d, want SQLite clamp 1", got)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("database/sql max open = %d, want SQLite clamp 1", got)
	}
}

func openPoolResizeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing database: %v", err)
		}
	})
	return db
}

func fillIdlePool(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	conns := make([]*sql.Conn, 0, count)
	for range count {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("db.Conn: %v", err)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			t.Fatalf("closing connection: %v", err)
		}
	}
}
