package main

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/secrets"
)

func TestAnalyzeCommandSupportsSQLiteSchemaStats(t *testing.T) {
	tmp := t.TempDir()
	sourcePath := filepath.Join(tmp, "source.db")
	targetPath := filepath.Join(tmp, "target.db")
	configPath := filepath.Join(tmp, "config.yaml")
	secretsPath := filepath.Join(tmp, "secrets.yaml")

	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open sqlite fixture: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			updated_at DATETIME,
			payload TEXT
		);
		INSERT INTO events (id, updated_at, payload) VALUES
			(1, '2026-07-12T10:00:00Z', 'one'),
			(2, '2026-07-12T11:00:00Z', 'two');
	`); err != nil {
		_ = db.Close()
		t.Fatalf("seed sqlite fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite fixture: %v", err)
	}

	configBody := "source:\n" +
		"  type: sqlite\n" +
		"  database: " + sourcePath + "\n" +
		"target:\n" +
		"  type: sqlite\n" +
		"  database: " + targetPath + "\n" +
		"migration:\n" +
		"  target_mode: drop_recreate\n" +
		"  data_dir: " + filepath.Join(tmp, "state") + "\n"
	if err := os.WriteFile(configPath, []byte(configBody), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(secretsPath, []byte("ai:\n  default_provider: \"\"\n"), 0600); err != nil {
		t.Fatalf("write secrets: %v", err)
	}
	t.Setenv(secrets.SecretsFileEnvVar, secretsPath)
	secrets.Reset()
	t.Cleanup(secrets.Reset)

	readOut, writeOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("capture stdout: %v", err)
	}
	originalOut := os.Stdout
	os.Stdout = writeOut
	defer func() { os.Stdout = originalOut }()

	runErr := newApp().Run([]string{"dmt", "--config", configPath, "analyze"})
	if err := writeOut.Close(); err != nil {
		t.Fatalf("close captured stdout: %v", err)
	}
	os.Stdout = originalOut
	output, err := io.ReadAll(readOut)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	if err := readOut.Close(); err != nil {
		t.Fatalf("close captured reader: %v", err)
	}
	if runErr != nil {
		t.Fatalf("dmt analyze SQLite: %v\noutput:\n%s", runErr, output)
	}

	text := string(output)
	for _, want := range []string{
		"# Database: 1 tables, 2 rows",
		"  workers:",
		"  chunk_size:",
		"  date_updated_columns:",
		"    - updated_at",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("analyze output missing %q:\n%s", want, text)
		}
	}
}
