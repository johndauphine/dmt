package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRawPreservesCreateIndexesOmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`migration:
  target_mode: drop_recreate
`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadRaw(path)
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}

	if cfg.Migration.CreateIndexes != nil {
		t.Fatalf("omitted create_indexes should stay nil, got %v", *cfg.Migration.CreateIndexes)
	}
	if cfg.Migration.CreateForeignKeys != nil {
		t.Fatalf("omitted create_foreign_keys should stay nil, got %v", *cfg.Migration.CreateForeignKeys)
	}
	if !cfg.Migration.CreateIndexesEnabled() {
		t.Fatal("effective omitted create_indexes default should be true")
	}
	if !cfg.Migration.CreateForeignKeysEnabled() {
		t.Fatal("effective omitted create_foreign_keys default should be true")
	}
}

func TestLoadRawPreservesExplicitFalseCreateIndexes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`migration:
  create_indexes: false
  create_foreign_keys: false
`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadRaw(path)
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}

	if cfg.Migration.CreateIndexes == nil || *cfg.Migration.CreateIndexes {
		t.Fatalf("explicit create_indexes: false should stay false, got %v", cfg.Migration.CreateIndexes)
	}
	if cfg.Migration.CreateForeignKeys == nil || *cfg.Migration.CreateForeignKeys {
		t.Fatalf("explicit create_foreign_keys: false should stay false, got %v", cfg.Migration.CreateForeignKeys)
	}
}
