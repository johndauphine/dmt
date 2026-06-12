package generic

import (
	"context"
	"database/sql"
	"testing"
)

// The packet-aware chunk cap depends on ProbeTarget surfacing
// @@max_allowed_packet (the #516 flip silently dropped the hand-written
// mysql probe; this pins the catalog-driven replacement).
func TestProbeTargetMaxAllowedPacket(t *testing.T) {
	for _, name := range []string{"sqlite", "clickhouse", "mysql", "postgres", "mssql"} {
		cat, err := LoadCatalog(name)
		if err != nil {
			t.Fatal(err)
		}
		d := NewDriver(cat)
		// nil DB never panics and returns an empty probe.
		if p := d.ProbeTarget(context.Background(), nil); p.MaxAllowedPacket != 0 {
			t.Errorf("%s: nil-db probe = %d, want 0", name, p.MaxAllowedPacket)
		}
		if name == "mysql" && cat.Queries.ProbeMaxAllowedPacket == "" {
			t.Error("mysql catalog must declare probe_max_allowed_packet")
		}
	}
}

func TestProbeTargetLiveMySQL(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	db, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}
	var want int64
	if err := db.QueryRow("SELECT @@max_allowed_packet").Scan(&want); err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	got := NewDriver(cat).ProbeTarget(context.Background(), db).MaxAllowedPacket
	if got != want || got <= 0 {
		t.Errorf("probe = %d, want %d (>0)", got, want)
	}
}
