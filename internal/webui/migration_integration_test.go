package webui

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // database/sql driver "sqlite" (pure Go)
)

// TestMigrationThroughAPI drives a full SQLite→SQLite migration over the HTTP
// API and asserts the SSE stream and final state — the #583 end-to-end check.
// Skipped in -short (it exercises the whole orchestrator pipeline).
func TestMigrationThroughAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("integration: full migration pipeline")
	}
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const wantRows = 50
	seedSource(t, srcPath, wantRows)
	cfgPath := writeMigrationConfig(t, dir, srcPath, dstPath, stateDir)

	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfgPath})
	srv := httptest.NewServer(s.buildHandler())
	defer srv.Close()

	// Collect SSE event types in the background until the run ends.
	sseTypes := make(chan []string, 1)
	go collectSSE(srv.URL, sseTypes)
	time.Sleep(150 * time.Millisecond) // let the subscription attach

	// Start the migration.
	if code, _ := apiPost(t, srv, "/api/run", `{}`); code != http.StatusAccepted {
		t.Fatalf("POST /api/run = %d, want 202", code)
	}

	// Poll until the run finishes.
	final := waitForRun(t, srv, 30*time.Second)
	if final != "completed" {
		t.Fatalf("run ended in %q, want completed", final)
	}

	// Target must hold every source row.
	if got := rowCount(t, dstPath, "widgets"); got != wantRows {
		t.Errorf("target rows = %d, want %d", got, wantRows)
	}

	// SSE must have seen the lifecycle: started → progress → done.
	select {
	case types := <-sseTypes:
		joined := strings.Join(types, ",")
		for _, want := range []string{"started", "done"} {
			if !strings.Contains(joined, want) {
				t.Errorf("SSE stream missing %q event; saw [%s]", want, joined)
			}
		}
	case <-time.After(2 * time.Second):
		t.Error("SSE collector did not finish")
	}
}

func seedSource(t *testing.T, path string, n int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)`); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	for i := 1; i <= n; i++ {
		if _, err := tx.Exec(`INSERT INTO widgets(id,name,qty) VALUES(?,?,?)`, i, fmt.Sprintf("w-%d", i), i*3); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func writeMigrationConfig(t *testing.T, dir, src, dst, stateDir string) string {
	t.Helper()
	p := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf(`source:
  type: sqlite
  database: %s
target:
  type: sqlite
  database: %s
migration:
  create_indexes: false
  create_foreign_keys: false
  create_check_constraints: false
  target_mode: drop_recreate
  runtime_tuning: false
  chunk_size: 10
  data_dir: %s
`, src, dst, stateDir)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func rowCount(t *testing.T, path, table string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

func apiPost(t *testing.T, srv *httptest.Server, path, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	respBody, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(respBody)
}

func waitForRun(t *testing.T, srv *httptest.Server, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/run", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		res, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Active bool `json:"active"`
			Run    struct {
				Status string `json:"status"`
			} `json:"run"`
		}
		json.NewDecoder(res.Body).Decode(&body)
		res.Body.Close()
		if body.Run.Status != "" && body.Run.Status != "running" {
			return body.Run.Status
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "timeout"
}

// collectSSE reads the event stream until a terminal event and reports the
// ordered list of event types it saw. It must not touch *testing.T — it runs
// on a separate goroutine, so it signals only through the channel.
func collectSSE(base string, out chan<- []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		out <- nil
		return
	}
	defer res.Body.Close()
	var types []string
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			et := strings.TrimPrefix(line, "event: ")
			types = append(types, et)
			if et == "done" || et == "failed" || et == "cancelled" {
				out <- types
				return
			}
		}
	}
	out <- types
}
