package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRawExpandsTemplatedNonStringScalars(t *testing.T) {
	t.Setenv("DMT_RAW_SOURCE_PORT", "15432")
	t.Setenv("DMT_RAW_WORKERS", "7")
	t.Setenv("DMT_RAW_CREATE_INDEXES", "false")
	t.Setenv("DMT_RAW_RUNTIME_TUNING", "true")
	t.Setenv("DMT_RAW_SAMPLE_PERCENT", "2.5")

	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`source:
  port: ${env:DMT_RAW_SOURCE_PORT}
migration:
  workers: ${env:DMT_RAW_WORKERS}
  create_indexes: ${env:DMT_RAW_CREATE_INDEXES}
  runtime_tuning: ${env:DMT_RAW_RUNTIME_TUNING}
  validation:
    sample_rows_percent: ${env:DMT_RAW_SAMPLE_PERCENT}
`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadRaw(path)
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}

	if cfg.Source.Port != 15432 {
		t.Fatalf("Source.Port = %d, want 15432", cfg.Source.Port)
	}
	if cfg.Migration.Workers != 7 {
		t.Fatalf("Migration.Workers = %d, want 7", cfg.Migration.Workers)
	}
	if cfg.Migration.CreateIndexes == nil || *cfg.Migration.CreateIndexes {
		t.Fatalf("CreateIndexes = %v, want explicit false", cfg.Migration.CreateIndexes)
	}
	if cfg.Migration.RuntimeTuning == nil || !*cfg.Migration.RuntimeTuning {
		t.Fatalf("RuntimeTuning = %v, want explicit true", cfg.Migration.RuntimeTuning)
	}
	if cfg.Migration.Validation.SampleRowsPercent != 2.5 {
		t.Fatalf("SampleRowsPercent = %v, want 2.5", cfg.Migration.Validation.SampleRowsPercent)
	}
}

func TestLoadRawPreservesTemplatedStringFields(t *testing.T) {
	t.Setenv("DMT_RAW_PASSWORD", "secret")
	t.Setenv("DMT_RAW_HOST", "db.example.test")

	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`source:
  host: ${env:DMT_RAW_HOST}
  password: ${env:DMT_RAW_PASSWORD}
target:
  password: ${file:/run/secrets/dmt-target-password}
`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadRaw(path)
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}

	if cfg.Source.Host != "${env:DMT_RAW_HOST}" {
		t.Fatalf("Source.Host = %q, want placeholder preserved", cfg.Source.Host)
	}
	if cfg.Source.Password != "${env:DMT_RAW_PASSWORD}" {
		t.Fatalf("Source.Password = %q, want placeholder preserved", cfg.Source.Password)
	}
	if cfg.Target.Password != "${file:/run/secrets/dmt-target-password}" {
		t.Fatalf("Target.Password = %q, want placeholder preserved", cfg.Target.Password)
	}
}
