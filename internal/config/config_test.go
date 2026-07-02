package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
	"gopkg.in/yaml.v3"
)

func TestMSSQLDSNURLEncoding(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		password string
		database string
		wantUser string
		wantPass string
		wantDB   string
	}{
		{
			name:     "plain credentials",
			user:     "admin",
			password: "secret",
			database: "mydb",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "password with @",
			user:     "admin",
			password: "pass@word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%40word",
			wantDB:   "mydb",
		},
		{
			name:     "password with colon",
			user:     "admin",
			password: "pass:word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%3Aword",
			wantDB:   "mydb",
		},
		{
			name:     "password with slash",
			user:     "admin",
			password: "pass/word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%2Fword",
			wantDB:   "mydb",
		},
		{
			name:     "user with @",
			user:     "user@domain",
			password: "secret",
			database: "mydb",
			wantUser: "user%40domain",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "database with spaces",
			user:     "admin",
			password: "secret",
			database: "my database",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "my+database", // QueryEscape uses + for spaces
		},
		{
			name:     "complex password",
			user:     "admin",
			password: "P@ss:w/rd?123",
			database: "mydb",
			wantUser: "admin",
			wantPass: "P%40ss%3Aw%2Frd%3F123",
			wantDB:   "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			dsn := cfg.buildMSSQLDSN("localhost", 1433, tt.database, tt.user, tt.password,
				true, false, 0, "", "", "", "", "")

			// Check that encoded values appear in DSN
			if !strings.Contains(dsn, tt.wantUser+":") {
				t.Errorf("MSSQL DSN missing encoded user %q in %q", tt.wantUser, dsn)
			}
			if !strings.Contains(dsn, ":"+tt.wantPass+"@") {
				t.Errorf("MSSQL DSN missing encoded password %q in %q", tt.wantPass, dsn)
			}
			if !strings.Contains(dsn, "database="+tt.wantDB) {
				t.Errorf("MSSQL DSN missing encoded database %q in %q", tt.wantDB, dsn)
			}
		})
	}
}

func TestPostgresDSNURLEncoding(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		password string
		database string
		wantUser string
		wantPass string
		wantDB   string
	}{
		{
			name:     "plain credentials",
			user:     "admin",
			password: "secret",
			database: "mydb",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "password with @",
			user:     "admin",
			password: "pass@word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%40word",
			wantDB:   "mydb",
		},
		{
			name:     "password with colon",
			user:     "admin",
			password: "pass:word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%3Aword",
			wantDB:   "mydb",
		},
		{
			name:     "password with slash",
			user:     "admin",
			password: "pass/word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%2Fword",
			wantDB:   "mydb",
		},
		{
			name:     "user with @",
			user:     "user@domain",
			password: "secret",
			database: "mydb",
			wantUser: "user%40domain",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "database with spaces",
			user:     "admin",
			password: "secret",
			database: "my database",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "my%20database", // PathEscape uses %20 for spaces
		},
		{
			name:     "complex password",
			user:     "admin",
			password: "P@ss:w/rd?123",
			database: "mydb",
			wantUser: "admin",
			wantPass: "P%40ss%3Aw%2Frd%3F123",
			wantDB:   "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			dsn := cfg.buildPostgresDSN("localhost", 5432, tt.database, tt.user, tt.password,
				"disable", "", "")

			// Check that encoded values appear in DSN
			if !strings.Contains(dsn, tt.wantUser+":") {
				t.Errorf("Postgres DSN missing encoded user %q in %q", tt.wantUser, dsn)
			}
			if !strings.Contains(dsn, ":"+tt.wantPass+"@") {
				t.Errorf("Postgres DSN missing encoded password %q in %q", tt.wantPass, dsn)
			}
			if !strings.Contains(dsn, "/"+tt.wantDB+"?") {
				t.Errorf("Postgres DSN missing encoded database %q in %q", tt.wantDB, dsn)
			}
		})
	}
}

func TestMSSQLKerberosEncoding(t *testing.T) {
	cfg := &Config{}

	// Test MSSQL Kerberos with special chars
	dsn := cfg.buildMSSQLDSN("localhost", 1433, "my database", "user@REALM.COM", "",
		true, false, 0, "kerberos", "/path/to/krb5.conf", "", "REALM.COM", "MSSQLSvc/host:1433")

	// database is QueryEscaped (+ for spaces)
	if !strings.Contains(dsn, "database=my+database") {
		t.Errorf("MSSQL Kerberos DSN missing encoded database in %q", dsn)
	}
	// username in query param is QueryEscaped
	if !strings.Contains(dsn, "krb5-username=user%40REALM.COM") {
		t.Errorf("MSSQL Kerberos DSN missing encoded username in %q", dsn)
	}
	// SPN with special chars
	if !strings.Contains(dsn, "ServerSPN=MSSQLSvc%2Fhost%3A1433") {
		t.Errorf("MSSQL Kerberos DSN missing encoded SPN in %q", dsn)
	}
}

func TestPostgresKerberosEncoding(t *testing.T) {
	cfg := &Config{}

	// Test Postgres Kerberos with special chars
	dsn := cfg.buildPostgresDSN("localhost", 5432, "my database", "user@REALM.COM", "",
		"disable", "kerberos", "prefer")

	// database is PathEscaped (%20 for spaces)
	if !strings.Contains(dsn, "/my%20database?") {
		t.Errorf("Postgres Kerberos DSN missing encoded database in %q", dsn)
	}
	// user in userinfo is QueryEscaped
	if !strings.Contains(dsn, "user%40REALM.COM@") {
		t.Errorf("Postgres Kerberos DSN missing encoded user in %q", dsn)
	}
}

func TestSameEngineValidation(t *testing.T) {
	tests := []struct {
		name        string
		sourceType  string
		targetType  string
		targetMode  string
		sourceHost  string
		targetHost  string
		sourcePort  int
		targetPort  int
		sourceDB    string
		targetDB    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "cross-engine allowed",
			sourceType:  "mssql",
			targetType:  "postgres",
			targetMode:  "drop_recreate",
			sourceHost:  "localhost",
			targetHost:  "localhost",
			sourcePort:  1433,
			targetPort:  5432,
			sourceDB:    "source",
			targetDB:    "target",
			expectError: false,
		},
		{
			name:        "same-engine with drop_recreate allowed (different hosts)",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "drop_recreate",
			sourceHost:  "host1",
			targetHost:  "host2",
			sourcePort:  5432,
			targetPort:  5432,
			sourceDB:    "source",
			targetDB:    "target",
			expectError: false,
		},
		{
			name:        "same-engine with upsert allowed",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "upsert",
			sourceHost:  "host1",
			targetHost:  "host2",
			sourcePort:  5432,
			targetPort:  5432,
			sourceDB:    "source",
			targetDB:    "target",
			expectError: false,
		},
		{
			name:        "same database blocked",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "upsert",
			sourceHost:  "localhost",
			targetHost:  "localhost",
			sourcePort:  5432,
			targetPort:  5432,
			sourceDB:    "mydb",
			targetDB:    "mydb",
			expectError: true,
			errorMsg:    "source and target cannot be the same database",
		},
		{
			name:        "same host different database allowed",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "upsert",
			sourceHost:  "localhost",
			targetHost:  "localhost",
			sourcePort:  5432,
			targetPort:  5432,
			sourceDB:    "source",
			targetDB:    "target",
			expectError: false,
		},
		{
			name:        "same host different port allowed",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "upsert",
			sourceHost:  "localhost",
			targetHost:  "localhost",
			sourcePort:  5432,
			targetPort:  5433,
			sourceDB:    "mydb",
			targetDB:    "mydb",
			expectError: false,
		},
		{
			name:        "same database blocked (case-insensitive host)",
			sourceType:  "postgres",
			targetType:  "postgres",
			targetMode:  "upsert",
			sourceHost:  "LOCALHOST",
			targetHost:  "localhost",
			sourcePort:  5432,
			targetPort:  5432,
			sourceDB:    "mydb",
			targetDB:    "mydb",
			expectError: true,
			errorMsg:    "source and target cannot be the same database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Source: SourceConfig{
					Type:     tt.sourceType,
					Host:     tt.sourceHost,
					Port:     tt.sourcePort,
					Database: tt.sourceDB,
					User:     "user",
					Password: "pass",
				},
				Target: TargetConfig{
					Type:     tt.targetType,
					Host:     tt.targetHost,
					Port:     tt.targetPort,
					Database: tt.targetDB,
					User:     "user",
					Password: "pass",
				},
				Migration: MigrationConfig{
					TargetMode: tt.targetMode,
				},
			}

			err := cfg.validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestValidate_RejectsKerberosAuth guards #251's descope: the
// DSN-building code in this package emits correct Kerberos DSNs, but
// the runtime drivers use Dialect.BuildDSN(..., cfg.DSNOptions()) and
// DSNOptions doesn't carry auth/keytab/etc — so an `auth: kerberos`
// config silently falls back to password auth. Validation must reject
// it explicitly until #251 lands a verified wiring.
func TestValidate_RejectsKerberosAuth(t *testing.T) {
	base := func() *Config {
		return &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
	}

	for _, side := range []string{"source", "target"} {
		t.Run(side, func(t *testing.T) {
			cfg := base()
			if side == "source" {
				cfg.Source.Auth = "kerberos"
			} else {
				cfg.Target.Auth = "kerberos"
			}
			err := cfg.validate()
			if err == nil {
				t.Fatalf("%s.auth=kerberos validated successfully; expected rejection", side)
			}
			if !strings.Contains(err.Error(), "#251") {
				t.Errorf("error doesn't reference the tracking issue (#251): %v", err)
			}
			if !strings.Contains(err.Error(), side+".auth") {
				t.Errorf("error doesn't name the offending field (%s.auth): %v", side, err)
			}
		})
	}

	t.Run("case_insensitive", func(t *testing.T) {
		cfg := base()
		cfg.Source.Auth = "Kerberos"
		if err := cfg.validate(); err == nil {
			t.Error("validate accepted Kerberos (capital K); should be case-insensitive")
		}
	})

	t.Run("password_still_works", func(t *testing.T) {
		cfg := base()
		cfg.Source.Auth = "password"
		if err := cfg.validate(); err != nil {
			t.Errorf("validate rejected auth=password: %v", err)
		}
	})
}

// TestValidate_FileBasedDriverSkipsHost locks in the isFileBasedDriver
// carve-out added so sqlite configs validate without `host`. Without
// this test, a future refactor of canonicalDriverName or
// isFileBasedDriver could silently re-reject sqlite configs (the
// integration test would catch it, but unit tests catch it on every
// `go test` invocation, much earlier). The companion assertion
// (non-file drivers still require host) prevents the inverse
// regression: an over-eager relaxation that accidentally lets
// host-less postgres/mssql/mysql configs through.
func TestValidate_FileBasedDriverSkipsHost(t *testing.T) {
	t.Run("sqlite_source_no_host_validates", func(t *testing.T) {
		cfg := &Config{
			Source: SourceConfig{
				Type:     "sqlite",
				Database: "./testdata/source.db",
			},
			Target: TargetConfig{
				Type: "postgres", Host: "tgt", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
		if err := cfg.validate(); err != nil {
			t.Errorf("sqlite source with empty Host should validate; got error: %v", err)
		}
	})

	t.Run("sqlite_target_no_host_validates", func(t *testing.T) {
		cfg := &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type:     "sqlite",
				Database: "./testdata/target.db",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
		if err := cfg.validate(); err != nil {
			t.Errorf("sqlite target with empty Host should validate; got error: %v", err)
		}
	})

	t.Run("sqlite_alias_no_host_validates", func(t *testing.T) {
		// canonicalDriverName collapses "sqlite3" → "sqlite", so the
		// carve-out must apply to alias forms too.
		cfg := &Config{
			Source: SourceConfig{
				Type:     "sqlite3",
				Database: "./testdata/source.db",
			},
			Target: TargetConfig{
				Type: "postgres", Host: "tgt", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
		if err := cfg.validate(); err != nil {
			t.Errorf("sqlite3 alias source should also skip host; got error: %v", err)
		}
	})

	t.Run("postgres_source_no_host_still_rejected", func(t *testing.T) {
		// Inverse: the carve-out must NOT have relaxed host validation
		// for network drivers.
		cfg := &Config{
			Source: SourceConfig{
				Type:     "postgres",
				Database: "d",
				User:     "u",
				Password: "p",
				// Host intentionally empty.
			},
			Target: TargetConfig{
				Type: "postgres", Host: "tgt", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
		err := cfg.validate()
		if err == nil {
			t.Fatal("postgres source with empty Host validated; expected source.host required error")
		}
		if !strings.Contains(err.Error(), "source.host is required") {
			t.Errorf("expected 'source.host is required'; got %v", err)
		}
	})

	t.Run("mssql_target_no_host_still_rejected", func(t *testing.T) {
		cfg := &Config{
			Source: SourceConfig{
				Type: "sqlite", Database: "./src.db",
			},
			Target: TargetConfig{
				Type:     "mssql",
				Database: "d",
				User:     "u",
				Password: "p",
				// Host intentionally empty.
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
		err := cfg.validate()
		if err == nil {
			t.Fatal("mssql target with empty Host validated; expected target.host required error")
		}
		if !strings.Contains(err.Error(), "target.host is required") {
			t.Errorf("expected 'target.host is required'; got %v", err)
		}
	})
}

func TestAutoTuneWriteAheadWriters(t *testing.T) {
	// Test that write-ahead writers get set to a reasonable value
	// (may be from auto-tuning or global defaults)
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "source",
			User:     "user",
			Password: "pass",
		},
		Target: TargetConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "target",
			User:     "user",
			Password: "pass",
		},
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}

	// Should have a reasonable value (at least 2)
	if cfg.Migration.WriteAheadWriters < 2 {
		t.Errorf("WriteAheadWriters should be at least 2, got %d", cfg.Migration.WriteAheadWriters)
	}
}

func TestAutoTuneParallelReaders(t *testing.T) {
	// Test that parallel readers get set to a reasonable value
	// (may be from auto-tuning or global defaults)
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "source",
			User:     "user",
			Password: "pass",
		},
		Target: TargetConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5433,
			Database: "target",
			User:     "user",
			Password: "pass",
		},
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}

	// Should have a reasonable value (at least 2)
	if cfg.Migration.ParallelReaders < 2 {
		t.Errorf("ParallelReaders should be at least 2, got %d", cfg.Migration.ParallelReaders)
	}
}

func TestDateUpdatedColumnsConfig(t *testing.T) {
	configYAML := `
source:
  type: mssql
  host: localhost
  port: 1433
  database: source
  user: user
  password: pass
target:
  type: postgres
  host: localhost
  port: 5432
  database: target
  schema: public
  user: user
  password: pass
migration:
  target_mode: upsert
  date_updated_columns:
    - ModifiedDate
    - UpdatedAt
    - LastUpdated
`
	// Create temp config file
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Verify DateUpdatedColumns parsed correctly
	expected := []string{"ModifiedDate", "UpdatedAt", "LastUpdated"}
	if len(cfg.Migration.DateUpdatedColumns) != len(expected) {
		t.Fatalf("DateUpdatedColumns length mismatch: got %d, want %d",
			len(cfg.Migration.DateUpdatedColumns), len(expected))
	}

	for i, col := range expected {
		if cfg.Migration.DateUpdatedColumns[i] != col {
			t.Errorf("DateUpdatedColumns[%d] mismatch: got %s, want %s",
				i, cfg.Migration.DateUpdatedColumns[i], col)
		}
	}
}

func TestDateUpdatedColumnsEmptyConfig(t *testing.T) {
	configYAML := `
source:
  type: mssql
  host: localhost
  port: 1433
  database: source
  user: user
  password: pass
target:
  type: postgres
  host: localhost
  port: 5432
  database: target
  schema: public
  user: user
  password: pass
migration:
  target_mode: upsert
`
	// Create temp config file
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// DateUpdatedColumns should be empty when not configured
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		t.Errorf("Expected empty DateUpdatedColumns, got %v", cfg.Migration.DateUpdatedColumns)
	}
}

func TestAutoTuneUserOverride(t *testing.T) {
	// User-specified values should not be overridden by auto-tuning
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "source",
			User:     "user",
			Password: "pass",
		},
		Target: TargetConfig{
			Type:     "mssql",
			Host:     "localhost",
			Port:     1433,
			Database: "target",
			User:     "user",
			Password: "pass",
		},
		Migration: MigrationConfig{
			WriteAheadWriters: 8, // User-specified
			ParallelReaders:   6, // User-specified
		},
	}
	cfg.autoConfig.CPUCores = 16
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}

	// User values should be preserved
	if cfg.Migration.WriteAheadWriters != 8 {
		t.Errorf("expected user-specified 8 writers, got %d", cfg.Migration.WriteAheadWriters)
	}
	if cfg.Migration.ParallelReaders != 6 {
		t.Errorf("expected user-specified 6 readers, got %d", cfg.Migration.ParallelReaders)
	}
}

// TestApplyAISuggestions_UserOverridesPRRAB (#219) pins the
// user-override mechanism for parallel_readers and read_ahead_buffers
// across the full round-trip: user sets explicit values in YAML →
// applyDefaults snapshots them as Original* → tuner produces different
// suggestions → ApplyTunerSuggestions must keep the user's values, NOT the
// tuner's.
//
// Pre-#219 the tuner ignored these axes entirely so the override path
// was never exercised for them in tests. Now that the tuner actively
// recommends PR/RAB values, this test guards the gate.
func TestApplyAISuggestions_UserOverridesPRRAB(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{Type: "postgres", Host: "localhost", Port: 5432, Database: "s", User: "u", Password: "p"},
		Target: TargetConfig{Type: "mssql", Host: "localhost", Port: 1433, Database: "t", User: "u", Password: "p"},
		Migration: MigrationConfig{
			ParallelReaders:  6,  // user-specified
			ReadAheadBuffers: 12, // user-specified
		},
	}
	cfg.autoConfig.CPUCores = 16
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}

	// Sanity: applyDefaults preserved the user's values and snapshotted
	// them as Original* so the override gate downstream can fire.
	if cfg.autoConfig.OriginalParallelReaders != 6 {
		t.Fatalf("OriginalParallelReaders snapshot=%d, want 6", cfg.autoConfig.OriginalParallelReaders)
	}
	if cfg.autoConfig.OriginalReadAheadBuffers != 12 {
		t.Fatalf("OriginalReadAheadBuffers snapshot=%d, want 12", cfg.autoConfig.OriginalReadAheadBuffers)
	}

	// Tuner suggests something different on both axes.
	suggestions := &driver.SmartConfigSuggestions{
		ParallelReaders:  2,
		ReadAheadBuffers: 4,
	}
	cfg.ApplyTunerSuggestions(suggestions)

	if cfg.Migration.ParallelReaders != 6 {
		t.Errorf("user override clobbered: ParallelReaders=%d, want 6 (user value)", cfg.Migration.ParallelReaders)
	}
	if cfg.Migration.ReadAheadBuffers != 12 {
		t.Errorf("user override clobbered: ReadAheadBuffers=%d, want 12 (user value)", cfg.Migration.ReadAheadBuffers)
	}
}

// TestApplyAISuggestions_AppliesPRRABWhenUnset (#219) is the
// complementary case: when the user did NOT set PR/RAB in YAML, the
// tuner's suggestion must take effect (Original*==0 gate).
func TestApplyAISuggestions_AppliesPRRABWhenUnset(t *testing.T) {
	cfg := &Config{
		Source:    SourceConfig{Type: "postgres", Host: "localhost", Port: 5432, Database: "s", User: "u", Password: "p"},
		Target:    TargetConfig{Type: "mssql", Host: "localhost", Port: 1433, Database: "t", User: "u", Password: "p"},
		Migration: MigrationConfig{}, // user left PR/RAB unset
	}
	cfg.autoConfig.CPUCores = 16
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}
	if cfg.autoConfig.OriginalParallelReaders != 0 {
		t.Fatalf("OriginalParallelReaders should be 0 when user didn't set; got %d", cfg.autoConfig.OriginalParallelReaders)
	}
	if cfg.autoConfig.OriginalReadAheadBuffers != 0 {
		t.Fatalf("OriginalReadAheadBuffers should be 0 when user didn't set; got %d", cfg.autoConfig.OriginalReadAheadBuffers)
	}

	suggestions := &driver.SmartConfigSuggestions{
		ParallelReaders:  4,
		ReadAheadBuffers: 8,
	}
	cfg.ApplyTunerSuggestions(suggestions)

	if cfg.Migration.ParallelReaders != 4 {
		t.Errorf("tuner suggestion not applied: ParallelReaders=%d, want 4 (suggested)", cfg.Migration.ParallelReaders)
	}
	if cfg.Migration.ReadAheadBuffers != 8 {
		t.Errorf("tuner suggestion not applied: ReadAheadBuffers=%d, want 8 (suggested)", cfg.Migration.ReadAheadBuffers)
	}
}

func TestApplyTuningToConfigFileUpdatesMigrationOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := []byte(`source:
  type: mssql
  host: ${env:SRC_HOST}
  port: 1433
  database: so2010
  user: ${env:SRC_USER}
  password: ${env:SRC_PASS}

# migration settings
migration: # workload tuning
  target_mode: upsert
  max_memory_mb: 8192

target:
  type: postgres
  host: ${env:TGT_HOST}
  port: 5432
  database: so2010_pg
  user: ${env:TGT_USER}
  password: ${env:TGT_PASS}

notifications:
  slack:
    webhook_url: ${env:SLACK_WEBHOOK}
`)
	if err := os.WriteFile(path, original, 0640); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatalf("chmod config: %v", err)
	}

	suggestions := testTuningSuggestions()
	if err := ApplyTuningToConfigFile(path, suggestions); err != nil {
		t.Fatalf("ApplyTuningToConfigFile: %v", err)
	}

	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read updated config: %v", err)
	}
	if got, want := topLevelYAMLBlock(updated, "source"), topLevelYAMLBlock(original, "source"); got != want {
		t.Fatalf("source block changed:\ngot:\n%s\nwant:\n%s", got, want)
	}
	if got, want := topLevelYAMLBlock(updated, "target"), topLevelYAMLBlock(original, "target"); got != want {
		t.Fatalf("target block changed:\ngot:\n%s\nwant:\n%s", got, want)
	}
	if got, want := topLevelYAMLBlock(updated, "notifications"), topLevelYAMLBlock(original, "notifications"); got != want {
		t.Fatalf("notifications block changed:\ngot:\n%s\nwant:\n%s", got, want)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat updated config: %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0640 {
		t.Fatalf("file mode = %v, want 0640", got)
	}

	var parsed map[string]interface{}
	if err := yaml.Unmarshal(updated, &parsed); err != nil {
		t.Fatalf("parse updated config: %v", err)
	}
	migration, ok := parsed["migration"].(map[string]interface{})
	if !ok {
		t.Fatalf("migration section has type %T, want map", parsed["migration"])
	}
	want := map[string]int64{
		"workers":                 8,
		"max_source_connections":  12,
		"max_target_connections":  10,
		"max_memory_mb":           2049,
		"chunk_size":              50000,
		"max_partitions":          16,
		"large_table_threshold":   1000000,
		"upsert_merge_chunk_size": 7000,
		"read_ahead_buffers":      6,
		"write_ahead_writers":     3,
		"parallel_readers":        4,
		"checkpoint_frequency":    20,
		"max_retries":             5,
	}
	for key, value := range want {
		if got := yamlInt64(t, migration[key]); got != value {
			t.Fatalf("migration.%s = %d, want %d", key, got, value)
		}
	}
	if !strings.Contains(string(updated), "migration: # workload tuning\n") {
		t.Fatalf("migration line comment was not preserved:\n%s", updated)
	}
	if count := strings.Count(string(updated), "# migration settings"); count != 1 {
		t.Fatalf("migration head comment count = %d, want 1:\n%s", count, updated)
	}
	if !strings.Contains(string(updated), "\n\ntarget:\n") {
		t.Fatalf("blank line before target block was not preserved:\n%s", updated)
	}
	if strings.Contains(string(updated), "    workers:") {
		t.Fatalf("migration block was rendered with 4-space child indentation:\n%s", updated)
	}
}

func TestAppliedMaxMemoryMBCeilsFlooredEstimate(t *testing.T) {
	t.Run("positive_estimate_gets_one_mb_headroom", func(t *testing.T) {
		got := appliedMaxMemoryMB(&driver.SmartConfigSuggestions{EstimatedMemMB: 2048})
		if got != 2049 {
			t.Fatalf("appliedMaxMemoryMB() = %d, want 2049", got)
		}
	})

	t.Run("non_positive_estimate_stays_unset", func(t *testing.T) {
		got := appliedMaxMemoryMB(&driver.SmartConfigSuggestions{EstimatedMemMB: 0})
		if got != 0 {
			t.Fatalf("appliedMaxMemoryMB() = %d, want 0", got)
		}
	})
}

func TestApplyTuningToConfigFileTreatsNullMigrationAsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := []byte(`source:
  type: postgres

migration:

target:
  type: sqlite
`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := ApplyTuningToConfigFile(path, testTuningSuggestions()); err != nil {
		t.Fatalf("ApplyTuningToConfigFile: %v", err)
	}

	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read updated config: %v", err)
	}
	if !strings.Contains(string(updated), "migration:\n  workers: 8\n") {
		t.Fatalf("null migration section was not replaced with mapping:\n%s", updated)
	}
}

func TestApplyTuningToConfigFileWarnsOnOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`migration:
  workers: 2
  chunk_size: 1000
`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var logs bytes.Buffer
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	if err := ApplyTuningToConfigFile(path, testTuningSuggestions()); err != nil {
		t.Fatalf("ApplyTuningToConfigFile: %v", err)
	}

	got := logs.String()
	for _, want := range []string{
		"migration.workers: 2 -> 8",
		"migration.chunk_size: 1000 -> 50000",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("warning log missing %q in:\n%s", want, got)
		}
	}
}

func testTuningSuggestions() *driver.SmartConfigSuggestions {
	return &driver.SmartConfigSuggestions{
		Workers:                 8,
		MaxSourceConnections:    12,
		MaxTargetConnections:    10,
		EstimatedMemMB:          2048,
		ChunkSizeRecommendation: 50000,
		MaxPartitions:           16,
		LargeTableThreshold:     1000000,
		UpsertMergeChunkSize:    7000,
		ReadAheadBuffers:        6,
		WriteAheadWriters:       3,
		ParallelReaders:         4,
		CheckpointFrequency:     20,
		MaxRetries:              5,
	}
}

func topLevelYAMLBlock(data []byte, name string) string {
	lines := strings.SplitAfter(string(data), "\n")
	start := -1
	prefix := name + ":"
	for i, line := range lines {
		if strings.TrimSpace(line) == prefix {
			start = i
			break
		}
	}
	if start == -1 {
		return ""
	}

	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "")
}

func yamlInt64(t *testing.T, value interface{}) int64 {
	t.Helper()
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case uint64:
		return int64(v)
	default:
		t.Fatalf("value %v has type %T, want integer", value, value)
		return 0
	}
}

// withEmptySecretsFile points secrets loading at a tmp file with no
// migration_defaults so the dev machine's real
// ~/.secrets/dmt-config.yaml (which may carry `ai_adjust: true` or
// the renamed field) doesn't leak into tests that need a known
// baseline.
func withEmptySecretsFile(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	secretsPath := filepath.Join(tmp, "dmt-config.yaml")
	if err := os.WriteFile(secretsPath, []byte("ai:\n  default_provider: \"\"\n"), 0600); err != nil {
		t.Fatalf("write empty secrets: %v", err)
	}
	t.Setenv("DMT_SECRETS_FILE", secretsPath)
	secrets.Reset()
	t.Cleanup(secrets.Reset)
}

func withSecretsFile(t *testing.T, body string) {
	t.Helper()
	tmp := t.TempDir()
	secretsPath := filepath.Join(tmp, "dmt-config.yaml")
	if err := os.WriteFile(secretsPath, []byte(body), 0600); err != nil {
		t.Fatalf("write secrets: %v", err)
	}
	t.Setenv("DMT_SECRETS_FILE", secretsPath)
	secrets.Reset()
	t.Cleanup(secrets.Reset)
}

func minConfigYAML(migration string) []byte {
	return []byte(`
source:
  type: mssql
  host: localhost
  database: source
  user: user
  password: pass
target:
  type: postgres
  host: localhost
  database: target
  user: user
  password: pass
migration:
` + migration)
}

func TestDebugDumpDistinguishesUserConfigFromSecretsDefault(t *testing.T) {
	withSecretsFile(t, `
migration_defaults:
  workers: 8
`)

	inherited, err := LoadBytes(minConfigYAML("  target_mode: drop_recreate\n"))
	if err != nil {
		t.Fatalf("LoadBytes inherited config: %v", err)
	}
	inheritedDump := inherited.DebugDump()
	if !strings.Contains(inheritedDump, "Workers: 8 (source: secrets default)") {
		t.Fatalf("debug dump should label inherited workers as secrets default:\n%s", inheritedDump)
	}

	explicit, err := LoadBytes(minConfigYAML("  target_mode: drop_recreate\n  workers: 4\n"))
	if err != nil {
		t.Fatalf("LoadBytes explicit config: %v", err)
	}
	explicitDump := explicit.DebugDump()
	if !strings.Contains(explicitDump, "Workers: 4 (source: config)") {
		t.Fatalf("debug dump should label explicit workers as config:\n%s", explicitDump)
	}
}

func TestApplyAISuggestionsPinsSecretsDefaultsAndOverridesGeneratedDefaults(t *testing.T) {
	withSecretsFile(t, `
migration_defaults:
  workers: 8
`)

	cfg, err := LoadBytes(minConfigYAML("  target_mode: drop_recreate\n"))
	if err != nil {
		t.Fatalf("LoadBytes config: %v", err)
	}

	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		Workers:                 4,
		ChunkSizeRecommendation: 12345,
	})

	if cfg.Migration.Workers != 8 {
		t.Fatalf("workers = %d, want pinned secrets default 8", cfg.Migration.Workers)
	}
	if got := cfg.tunableProvenance(provenanceMigrationWorkers); got != ProvenanceSecretsDefault {
		t.Fatalf("workers provenance = %q, want %q", got, ProvenanceSecretsDefault)
	}
	if cfg.Migration.ChunkSize != 12345 {
		t.Fatalf("chunk_size = %d, want smartconfig override 12345", cfg.Migration.ChunkSize)
	}
	if got := cfg.tunableProvenance(provenanceMigrationChunkSize); got != ProvenanceSmartConfig {
		t.Fatalf("chunk_size provenance = %q, want %q", got, ProvenanceSmartConfig)
	}
}

// TestRuntimeTuningExplicitFalseRespected pins issue #149: setting
// `migration.runtime_tuning: false` (or its deprecated alias) in a
// per-migration YAML config must not be silently flipped back to true
// by the auto-enable logic. Pre-fix, the field was a plain `bool` and
// the parser couldn't distinguish "explicit false" from "unset", so
// the auto-enable code always overrode false→true.
func TestRuntimeTuningExplicitFalseRespected(t *testing.T) {
	withEmptySecretsFile(t)

	enabled := false
	cfg := minConfigWithAI()
	cfg.Migration.RuntimeTuning = &enabled
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil {
		t.Fatal("RuntimeTuning was nil after applyDefaults — auto-enable should preserve explicit pointer")
	}
	if *cfg.Migration.RuntimeTuning != false {
		t.Error("explicit runtime_tuning: false was overridden to true (issue #149 regression)")
	}
}

// TestRuntimeTuningExplicitTrueRespected verifies the symmetric case
// — explicit `runtime_tuning: true` must also stick (no special-case
// behavior).
func TestRuntimeTuningExplicitTrueRespected(t *testing.T) {
	withEmptySecretsFile(t)

	enabled := true
	cfg := minConfigWithAI()
	cfg.Migration.RuntimeTuning = &enabled
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil || *cfg.Migration.RuntimeTuning != true {
		t.Errorf("explicit runtime_tuning: true should stick; got %v", cfg.Migration.RuntimeTuning)
	}
}

// TestRuntimeTuningUnsetAutoEnabled covers the third state — the
// field is unset (nil) and the user has AI configured, so the auto-
// enable kicks in.
func TestRuntimeTuningUnsetAutoEnabled(t *testing.T) {
	withEmptySecretsFile(t)

	cfg := minConfigWithAI()
	// Migration.RuntimeTuning left nil (zero value for *bool).
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil {
		t.Fatal("RuntimeTuning still nil after auto-enable — auto-enable should have populated it")
	}
	if *cfg.Migration.RuntimeTuning != true {
		t.Error("auto-enable should set RuntimeTuning to true when unset and AI is configured")
	}
}

// TestRuntimeTuningInheritsSecretsFalse pins the second-tier
// precedence from PR #150 review: when the per-migration YAML doesn't
// set runtime_tuning but the secrets file's migration_defaults sets
// it explicitly to false, the per-migration field must inherit that
// false (not get auto-enabled to true). Pre-Copilot-fix this was
// broken — applyGlobalDefaults didn't copy the field, then the
// auto-enable site flipped nil → true, silently overriding the
// secrets default.
func TestRuntimeTuningInheritsSecretsFalse(t *testing.T) {
	tmp := t.TempDir()
	secretsPath := filepath.Join(tmp, "dmt-config.yaml")
	if err := os.WriteFile(secretsPath, []byte(`
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "sk-ant-test"

migration_defaults:
  runtime_tuning: false
  runtime_tuning_interval: "60s"
`), 0600); err != nil {
		t.Fatalf("write secrets: %v", err)
	}
	t.Setenv("DMT_SECRETS_FILE", secretsPath)
	secrets.Reset()
	t.Cleanup(secrets.Reset)

	cfg := minConfigWithoutAI()
	// Migration.RuntimeTuning left nil; secrets defaults should fill it with false.
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil {
		t.Fatal("RuntimeTuning still nil — applyGlobalDefaults should have inherited the secrets value")
	}
	if *cfg.Migration.RuntimeTuning != false {
		t.Errorf("RuntimeTuning = true; expected false (from migration_defaults.runtime_tuning). Auto-enable site clobbered the secrets default.")
	}
	if cfg.Migration.RuntimeTuningInterval != "60s" {
		t.Errorf("RuntimeTuningInterval = %q; expected \"60s\" (from migration_defaults)", cfg.Migration.RuntimeTuningInterval)
	}
}

// TestAIAdjustLegacyAliasMigratesToRuntimeTuning pins the #211
// deprecation cycle: a per-migration config that still uses the old
// `ai_adjust` name has its value silently migrated into the new
// RuntimeTuning field by normalizeRuntimeTuningFields. The deprecated
// field is cleared so no downstream reader can pick up the stale
// value.
//
// Uses a tmp secrets file so the dev machine's real
// ~/.secrets/dmt-config.yaml doesn't pre-populate RuntimeTuning via
// the secrets inheritance path and mask the per-migration migration.
func TestAIAdjustLegacyAliasMigratesToRuntimeTuning(t *testing.T) {
	withEmptySecretsFile(t)

	enabled := false
	cfg := minConfigWithAI()
	cfg.Migration.AIAdjust = &enabled
	cfg.Migration.AIAdjustInterval = "42s"
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil || *cfg.Migration.RuntimeTuning != false {
		t.Errorf("legacy ai_adjust=false should migrate to runtime_tuning=false; got %v",
			cfg.Migration.RuntimeTuning)
	}
	if cfg.Migration.RuntimeTuningInterval != "42s" {
		t.Errorf("legacy ai_adjust_interval should migrate to runtime_tuning_interval; got %q",
			cfg.Migration.RuntimeTuningInterval)
	}
	if cfg.Migration.AIAdjust != nil {
		t.Errorf("legacy AIAdjust field should be cleared after normalization; got %v",
			cfg.Migration.AIAdjust)
	}
	if cfg.Migration.AIAdjustInterval != "" {
		t.Errorf("legacy AIAdjustInterval should be cleared after normalization; got %q",
			cfg.Migration.AIAdjustInterval)
	}
}

// TestRuntimeTuningWinsOverLegacyAlias covers the simultaneous-set
// case with CONFLICTING values: when both names are present in YAML
// with different values, the new (canonical) name wins and a
// conflict-naming WARN fires (acceptance criteria for #211).
func TestRuntimeTuningWinsOverLegacyAlias(t *testing.T) {
	withEmptySecretsFile(t)

	var logs strings.Builder
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	legacy := true
	canonical := false
	cfg := minConfigWithAI()
	cfg.Migration.AIAdjust = &legacy
	cfg.Migration.AIAdjustInterval = "30s"
	cfg.Migration.RuntimeTuning = &canonical
	cfg.Migration.RuntimeTuningInterval = "10s"
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil || *cfg.Migration.RuntimeTuning != false {
		t.Errorf("runtime_tuning=false should win over conflicting ai_adjust=true; got %v",
			cfg.Migration.RuntimeTuning)
	}
	if cfg.Migration.RuntimeTuningInterval != "10s" {
		t.Errorf("runtime_tuning_interval=10s should win over ai_adjust_interval=30s; got %q",
			cfg.Migration.RuntimeTuningInterval)
	}
	if cfg.Migration.AIAdjust != nil || cfg.Migration.AIAdjustInterval != "" {
		t.Errorf("legacy fields must be cleared after normalization; got AIAdjust=%v AIAdjustInterval=%q",
			cfg.Migration.AIAdjust, cfg.Migration.AIAdjustInterval)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `conflicts with "runtime_tuning"`) {
		t.Errorf("expected conflict WARN naming runtime_tuning; got logs:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, `conflicts with "runtime_tuning_interval"`) {
		t.Errorf("expected conflict WARN naming runtime_tuning_interval; got logs:\n%s", logOutput)
	}
}

// TestRuntimeTuningBothSetMatchingValuesWarnsOnce covers the
// simultaneous-set case with MATCHING values: a user who has
// duplicated the same value under both names (a clean migration in
// progress, or a copy/paste) gets one deprecation WARN per field,
// not a conflict WARN (the acceptance criteria carve this out
// specifically: silent on value agreement, just the rename signal).
func TestRuntimeTuningBothSetMatchingValuesWarnsOnce(t *testing.T) {
	withEmptySecretsFile(t)

	var logs strings.Builder
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	same := true
	cfg := minConfigWithAI()
	cfg.Migration.AIAdjust = &same
	cfg.Migration.AIAdjustInterval = "5s"
	cfg.Migration.RuntimeTuning = &same
	cfg.Migration.RuntimeTuningInterval = "5s"
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil || *cfg.Migration.RuntimeTuning != true {
		t.Errorf("matching ai_adjust + runtime_tuning should resolve to true; got %v",
			cfg.Migration.RuntimeTuning)
	}
	logOutput := logs.String()
	if strings.Contains(logOutput, "conflicts with") {
		t.Errorf("matching-value case must not emit a conflict WARN; got logs:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, `field "ai_adjust" is deprecated`) {
		t.Errorf("expected basic deprecation WARN for ai_adjust; got logs:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, `field "ai_adjust_interval" is deprecated`) {
		t.Errorf("expected basic deprecation WARN for ai_adjust_interval; got logs:\n%s", logOutput)
	}
}

// TestNormalizeRuntimeTuningFieldsIsIdempotent pins that a second
// call to normalize after the legacy fields are cleared emits no
// further warnings — important if a future caller ever runs
// normalize twice (e.g., a config reload path), so the user doesn't
// see duplicate deprecation noise from a single config-on-disk.
func TestNormalizeRuntimeTuningFieldsIsIdempotent(t *testing.T) {
	v := false
	cfg := &Config{}
	cfg.Migration.AIAdjust = &v
	cfg.Migration.AIAdjustInterval = "3s"
	cfg.normalizeRuntimeTuningFields()

	var logs strings.Builder
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	cfg.normalizeRuntimeTuningFields() // second call should be silent
	if got := logs.String(); strings.Contains(got, "deprecated") {
		t.Errorf("idempotent call should emit no deprecation logs; got:\n%s", got)
	}
}

// TestRuntimeTuningResumeHashStableAcrossRename pins the #211 codex-
// review fix: the resume config hash (json.Marshal(cfg.Sanitized()))
// must produce identical output regardless of which YAML field name
// the user wrote — otherwise an in-flight migration started before
// the upgrade can't be resumed after it without --force-resume. The
// JSON tags on RuntimeTuning / RuntimeTuningInterval pin the legacy
// wire names (AIAdjust / AIAdjustInterval) for exactly this reason.
//
// Compares JSON shape directly (no applyDefaults) so the test isn't
// dependent on system-state-driven auto-tune values that vary
// between calls.
func TestRuntimeTuningResumeHashStableAcrossRename(t *testing.T) {
	v := false

	legacy := &Config{}
	legacy.Migration.AIAdjust = &v
	legacy.Migration.AIAdjustInterval = "7s"
	legacy.normalizeRuntimeTuningFields()

	renamed := &Config{}
	v2 := false
	renamed.Migration.RuntimeTuning = &v2
	renamed.Migration.RuntimeTuningInterval = "7s"
	renamed.normalizeRuntimeTuningFields()

	legacyJSON, err := json.Marshal(legacy.Migration)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	renamedJSON, err := json.Marshal(renamed.Migration)
	if err != nil {
		t.Fatalf("marshal renamed: %v", err)
	}
	if string(legacyJSON) != string(renamedJSON) {
		t.Errorf("resume hash JSON diverges across rename — pre-#211 migrations would fail to resume after upgrade.\n  legacy:  %s\n  renamed: %s",
			legacyJSON, renamedJSON)
	}
	// Belt-and-suspenders: the JSON must use the legacy wire names so
	// stored hashes from pre-#211 runs continue to match.
	if !strings.Contains(string(renamedJSON), `"AIAdjust":false`) {
		t.Errorf("renamed JSON missing legacy AIAdjust wire name; resume hash will break for pre-#211 users.\n  got: %s", renamedJSON)
	}
	if !strings.Contains(string(renamedJSON), `"AIAdjustInterval":"7s"`) {
		t.Errorf("renamed JSON missing legacy AIAdjustInterval wire name; resume hash will break for pre-#211 users.\n  got: %s", renamedJSON)
	}
}

// TestAIAdjustLegacyAliasInSecrets covers the secrets-layer side of
// the deprecation cycle: a global secrets file using the legacy
// `ai_adjust` field name continues to be honored by
// applyGlobalDefaults until the user migrates the file, AND emits a
// per-migration deprecation warning so the user sees the same signal
// they'd see if the legacy field were in their per-migration YAML
// (acceptance criteria for #211 — "emit a WARN per migration when
// set" applies to either layer).
func TestAIAdjustLegacyAliasInSecrets(t *testing.T) {
	tmp := t.TempDir()
	secretsPath := filepath.Join(tmp, "dmt-config.yaml")
	if err := os.WriteFile(secretsPath, []byte(`
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "sk-ant-test"

migration_defaults:
  ai_adjust: false
  ai_adjust_interval: "45s"
`), 0600); err != nil {
		t.Fatalf("write secrets: %v", err)
	}
	t.Setenv("DMT_SECRETS_FILE", secretsPath)
	secrets.Reset()
	t.Cleanup(secrets.Reset)

	var logs strings.Builder
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	cfg := minConfigWithoutAI()
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}
	if cfg.Migration.RuntimeTuning == nil || *cfg.Migration.RuntimeTuning != false {
		t.Errorf("legacy migration_defaults.ai_adjust=false should inherit into RuntimeTuning; got %v",
			cfg.Migration.RuntimeTuning)
	}
	if cfg.Migration.RuntimeTuningInterval != "45s" {
		t.Errorf("legacy migration_defaults.ai_adjust_interval should inherit; got %q",
			cfg.Migration.RuntimeTuningInterval)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `"migration_defaults.ai_adjust"`) {
		t.Errorf("expected WARN about deprecated migration_defaults.ai_adjust; got logs:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, `"migration_defaults.ai_adjust_interval"`) {
		t.Errorf("expected WARN about deprecated migration_defaults.ai_adjust_interval; got logs:\n%s", logOutput)
	}
}

// minConfigWithoutAI is identical to minConfigWithAI but without the AI block.
// Used for the inheritance test — the secrets file provides AI in that case.
func minConfigWithoutAI() *Config {
	cfg := &Config{
		Source: SourceConfig{
			Type: "postgres", Host: "localhost", Port: 5432,
			Database: "source", User: "user", Password: "pass",
		},
		Target: TargetConfig{
			Type: "mssql", Host: "localhost", Port: 1433,
			Database: "target", User: "user", Password: "pass",
		},
	}
	cfg.autoConfig.CPUCores = 16
	return cfg
}

// minConfigWithAI returns the minimal Config that triggers the AI auto-enable
// branch in applyDefaults.
func minConfigWithAI() *Config {
	cfg := &Config{
		Source: SourceConfig{
			Type: "postgres", Host: "localhost", Port: 5432,
			Database: "source", User: "user", Password: "pass",
		},
		Target: TargetConfig{
			Type: "mssql", Host: "localhost", Port: 1433,
			Database: "target", User: "user", Password: "pass",
		},
		AI: &AIConfig{
			APIKey:   "sk-ant-test",
			Provider: "anthropic",
		},
	}
	cfg.autoConfig.CPUCores = 16
	return cfg
}

func TestAutoTuneConnectionPoolSizing(t *testing.T) {
	// Test that connection pools get reasonable values
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "source",
			User:     "user",
			Password: "pass",
		},
		Target: TargetConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5433,
			Database: "target",
			User:     "user",
			Password: "pass",
		},
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() failed: %v", err)
	}

	// With 8 cores: readers=2, writers=2
	// Source connections: workers * readers + 4 = 4 * 2 + 4 = 12
	// Target connections: workers * writers + 4 = 4 * 2 + 4 = 12
	expectedSourceConns := cfg.Migration.Workers*cfg.Migration.ParallelReaders + 4
	expectedTargetConns := cfg.Migration.Workers*cfg.Migration.WriteAheadWriters + 4

	if cfg.Migration.MaxSourceConnections < expectedSourceConns {
		t.Errorf("insufficient source connections: got %d, need at least %d",
			cfg.Migration.MaxSourceConnections, expectedSourceConns)
	}
	if cfg.Migration.MaxTargetConnections < expectedTargetConns {
		t.Errorf("insufficient target connections: got %d, need at least %d",
			cfg.Migration.MaxTargetConnections, expectedTargetConns)
	}
}

func TestApplyDefaultsMemoryDetectionFallback(t *testing.T) {
	// Test that applyDefaults succeeds when max_memory_mb is set,
	// even on platforms where memory detection might fail.
	// The 70% hard cap is always applied to EffectiveMaxMemoryMB.
	cfg := &Config{
		Source: SourceConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "source",
			User:     "user",
			Password: "pass",
		},
		Target: TargetConfig{
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "target",
			User:     "user",
			Password: "pass",
		},
		Migration: MigrationConfig{
			MaxMemoryMB: 8192,
		},
	}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() should succeed with max_memory_mb set: %v", err)
	}

	if cfg.autoConfig.AvailableMemoryMB == 0 {
		t.Error("AvailableMemoryMB should not be 0")
	}

	// EffectiveMaxMemoryMB should never exceed 70% of available memory
	hardCap := cfg.autoConfig.AvailableMemoryMB * 70 / 100
	if cfg.autoConfig.EffectiveMaxMemoryMB > hardCap {
		t.Errorf("EffectiveMaxMemoryMB %d exceeds 70%% hard cap %d",
			cfg.autoConfig.EffectiveMaxMemoryMB, hardCap)
	}

	// When max_memory_mb < hard cap, it should be used directly
	if cfg.Migration.MaxMemoryMB < hardCap && cfg.autoConfig.EffectiveMaxMemoryMB != cfg.Migration.MaxMemoryMB {
		t.Errorf("EffectiveMaxMemoryMB should equal MaxMemoryMB (%d) when below hard cap, got %d",
			cfg.Migration.MaxMemoryMB, cfg.autoConfig.EffectiveMaxMemoryMB)
	}
}

func TestExpandTemplateValue(t *testing.T) {
	// Create a temp file with a secret
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "secret.txt")
	if err := os.WriteFile(secretFile, []byte("  my-secret-password  \n"), 0600); err != nil {
		t.Fatalf("failed to create secret file: %v", err)
	}

	// Set an env var for testing
	os.Setenv("TEST_SECRET_VAR", "env-secret-value")
	defer os.Unsetenv("TEST_SECRET_VAR")

	tests := []struct {
		name      string
		input     string
		expected  string
		expectErr bool
	}{
		{
			name:     "cleartext password",
			input:    "my-plain-password",
			expected: "my-plain-password",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "file template",
			input:    "${file:" + secretFile + "}",
			expected: "my-secret-password", // Whitespace trimmed
		},
		{
			name:     "env template",
			input:    "${env:TEST_SECRET_VAR}",
			expected: "env-secret-value",
		},
		{
			name:     "env template missing var",
			input:    "${env:NONEXISTENT_VAR_12345}",
			expected: "", // Empty, no error
		},
		{
			name:      "file template missing file",
			input:     "${file:/nonexistent/path/to/secret}",
			expectErr: true,
		},
		{
			name:     "not a template - dollar sign without braces",
			input:    "$file:/path",
			expected: "$file:/path",
		},
		{
			name:     "not a template - partial pattern",
			input:    "${file:}",
			expected: "${file:}", // Empty path, treated as literal
		},
		{
			name:     "legacy env var syntax expands",
			input:    "${TEST_SECRET_VAR}",
			expected: "env-secret-value", // Legacy ${VAR} expands like ${env:VAR}
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := expandTemplateValue(tt.input)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestExpandTemplateValueFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permission bits do not reliably represent Windows ACLs")
	}

	tmpDir := t.TempDir()
	tests := []struct {
		name      string
		mode      os.FileMode
		expectErr bool
	}{
		{name: "0600 accepted", mode: 0600},
		{name: "0400 accepted", mode: 0400},
		{name: "group-readable rejected", mode: 0640, expectErr: true},
		{name: "world-readable rejected", mode: 0604, expectErr: true},
		{name: "group-and-world-readable rejected", mode: 0644, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secretFile := filepath.Join(tmpDir, strings.ReplaceAll(tt.name, " ", "_"))
			if err := os.WriteFile(secretFile, []byte("file-secret"), 0600); err != nil {
				t.Fatalf("write secret file: %v", err)
			}
			if err := os.Chmod(secretFile, tt.mode); err != nil {
				t.Fatalf("chmod secret file: %v", err)
			}

			got, err := expandTemplateValue("${file:" + secretFile + "}")
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected permission error, got nil")
				}
				if !strings.Contains(err.Error(), "insecure permissions") {
					t.Fatalf("error = %q, want insecure permissions message", err)
				}
				if !strings.Contains(err.Error(), "group/world permissions") {
					t.Fatalf("error = %q, want group/world permissions message", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != "file-secret" {
				t.Fatalf("expanded secret = %q, want file-secret", got)
			}
		})
	}
}

func TestLoadBytesWithSecretTemplates(t *testing.T) {
	// Create temp files with secrets
	tmpDir := t.TempDir()
	mssqlPwdFile := filepath.Join(tmpDir, "mssql_password")
	pgPwdFile := filepath.Join(tmpDir, "pg_password")

	if err := os.WriteFile(mssqlPwdFile, []byte("mssql-secret-123"), 0600); err != nil {
		t.Fatalf("failed to create mssql password file: %v", err)
	}
	if err := os.WriteFile(pgPwdFile, []byte("pg-secret-456"), 0600); err != nil {
		t.Fatalf("failed to create pg password file: %v", err)
	}

	// Set env var for testing
	os.Setenv("TEST_PG_PASSWORD", "env-pg-password")
	defer os.Unsetenv("TEST_PG_PASSWORD")

	tests := []struct {
		name           string
		yaml           string
		expectedSource string
		expectedTarget string
		expectErr      bool
	}{
		{
			name: "file-based secrets",
			yaml: `
source:
  type: mssql
  host: mssql-server
  database: sourcedb
  user: sa
  password: ${file:` + mssqlPwdFile + `}
target:
  type: postgres
  host: pg-server
  database: targetdb
  user: postgres
  password: ${file:` + pgPwdFile + `}
`,
			expectedSource: "mssql-secret-123",
			expectedTarget: "pg-secret-456",
		},
		{
			name: "env-based secrets",
			yaml: `
source:
  type: mssql
  host: mssql-server
  database: sourcedb
  user: sa
  password: cleartext-source
target:
  type: postgres
  host: pg-server
  database: targetdb
  user: postgres
  password: ${env:TEST_PG_PASSWORD}
`,
			expectedSource: "cleartext-source",
			expectedTarget: "env-pg-password",
		},
		{
			name: "mixed - cleartext and file",
			yaml: `
source:
  type: mssql
  host: mssql-server
  database: sourcedb
  user: sa
  password: plain-password
target:
  type: postgres
  host: pg-server
  database: targetdb
  user: postgres
  password: ${file:` + pgPwdFile + `}
`,
			expectedSource: "plain-password",
			expectedTarget: "pg-secret-456",
		},
		{
			name: "missing file should error",
			yaml: `
source:
  type: mssql
  host: mssql-server
  database: sourcedb
  user: sa
  password: ${file:/nonexistent/secret}
target:
  type: postgres
  host: pg-server
  database: targetdb
  user: postgres
  password: test
`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadBytes([]byte(tt.yaml))

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if cfg.Source.Password != tt.expectedSource {
				t.Errorf("source password: expected %q, got %q", tt.expectedSource, cfg.Source.Password)
			}
			if cfg.Target.Password != tt.expectedTarget {
				t.Errorf("target password: expected %q, got %q", tt.expectedTarget, cfg.Target.Password)
			}
		})
	}
}

func TestExpandSecretsWithTilde(t *testing.T) {
	// Create a secret file in temp dir and use tilde expansion
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("cannot get home directory")
	}

	// Create a temp secret in a known location
	tmpDir := t.TempDir()
	secretFile := filepath.Join(tmpDir, "test-secret")
	if err := os.WriteFile(secretFile, []byte("tilde-secret"), 0600); err != nil {
		t.Fatalf("failed to create secret file: %v", err)
	}

	// Test that tilde expansion works in file paths
	// We can't easily test ~ directly, but we can test the expandTilde function
	result := expandTilde("~/some/path")
	expected := filepath.Join(home, "some/path")
	if result != expected {
		t.Errorf("expandTilde: expected %q, got %q", expected, result)
	}
}

func TestSecretsWithSpecialCharacters(t *testing.T) {
	// Test that secrets containing YAML special characters work correctly
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		secretContent  string
		expectedSource string
	}{
		{
			name:           "password with colon",
			secretContent:  "pass:word",
			expectedSource: "pass:word",
		},
		{
			name:           "password with quotes",
			secretContent:  `pass"word'test`,
			expectedSource: `pass"word'test`,
		},
		{
			name:           "password with special chars",
			secretContent:  "p@ss#w0rd!$%^&*()",
			expectedSource: "p@ss#w0rd!$%^&*()",
		},
		{
			name:           "password with spaces",
			secretContent:  "pass word with spaces",
			expectedSource: "pass word with spaces",
		},
		{
			name:           "password with newline gets trimmed",
			secretContent:  "password\n",
			expectedSource: "password",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create secret file
			secretFile := filepath.Join(tmpDir, "secret-"+tt.name)
			if err := os.WriteFile(secretFile, []byte(tt.secretContent), 0600); err != nil {
				t.Fatalf("failed to create secret file: %v", err)
			}

			// Test via LoadBytes - password field is quoted in YAML so special chars are safe
			yaml := `
source:
  type: mssql
  host: mssql-server
  database: sourcedb
  user: sa
  password: ${file:` + secretFile + `}
target:
  type: postgres
  host: pg-server
  database: targetdb
  user: postgres
  password: cleartext
`
			cfg, err := LoadBytes([]byte(yaml))
			if err != nil {
				t.Fatalf("LoadBytes failed: %v", err)
			}

			if cfg.Source.Password != tt.expectedSource {
				t.Errorf("expected password %q, got %q", tt.expectedSource, cfg.Source.Password)
			}
		})
	}
}

func TestInvalidEnvVarNames(t *testing.T) {
	// Test that invalid env var names are treated as literals (not expanded)
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "env var starting with number",
			input:    "${env:1INVALID}",
			expected: "${env:1INVALID}", // Not a valid env var name, treated as literal
		},
		{
			name:     "env var with hyphen",
			input:    "${env:INVALID-VAR}",
			expected: "${env:INVALID-VAR}", // Hyphen not allowed, treated as literal
		},
		{
			name:     "legacy var starting with number",
			input:    "${1INVALID}",
			expected: "${1INVALID}", // Not a valid env var name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := expandTemplateValue(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestCanonicalDriverName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"mssql", "mssql"},
		{"sqlserver", "mssql"},
		{"sql-server", "mssql"},
		{"MSSQL", "mssql"},
		{"SQLSERVER", "mssql"},
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"pg", "postgres"},
		{"POSTGRES", "postgres"},
		{"PG", "postgres"},
		{"unknown", "unknown"}, // Unknown types return unchanged
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := canonicalDriverName(tt.input)
			if result != tt.expected {
				t.Errorf("canonicalDriverName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsValidDriverType(t *testing.T) {
	validTypes := []string{
		"mssql", "sqlserver", "sql-server",
		"postgres", "postgresql", "pg",
		"mysql", "mariadb", "maria",
		"sqlite", "sqlite3", "sqlitedb",
		"MSSQL", "PG", "MYSQL", "SQLITE", // Case insensitive
	}
	invalidTypes := []string{
		"oracle", "unknown", "",
	}

	for _, dbType := range validTypes {
		t.Run("valid_"+dbType, func(t *testing.T) {
			if !isValidDriverType(dbType) {
				t.Errorf("isValidDriverType(%q) = false, want true", dbType)
			}
		})
	}

	for _, dbType := range invalidTypes {
		t.Run("invalid_"+dbType, func(t *testing.T) {
			if isValidDriverType(dbType) {
				t.Errorf("isValidDriverType(%q) = true, want false", dbType)
			}
		})
	}
}

func TestValidateSchemaEvolutionAddedColumnPolicy(t *testing.T) {
	base := func(policy SchemaEvolutionPolicy) *Config {
		return &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{
				TargetMode: "upsert",
				SchemaEvolution: &SchemaEvolutionConfig{
					AddedColumn: policy,
				},
			},
		}
	}

	for _, policy := range []SchemaEvolutionPolicy{
		"",
		SchemaEvolutionAuto,
		SchemaEvolutionLog,
		SchemaEvolutionFail,
		SchemaEvolutionDiscard,
		SchemaEvolutionDiscardValue,
	} {
		t.Run(string(policy), func(t *testing.T) {
			if err := base(policy).validate(); err != nil {
				t.Fatalf("validate returned error: %v", err)
			}
		})
	}

	cfg := base("oops")
	if err := cfg.validate(); err == nil {
		t.Fatal("validate returned nil error for invalid policy")
	}
}

func TestValidateSchemaEvolutionNullabilityChangePolicy(t *testing.T) {
	base := func(policy SchemaEvolutionPolicy) *Config {
		return &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{
				TargetMode: "upsert",
				SchemaEvolution: &SchemaEvolutionConfig{
					NullabilityChange: policy,
				},
			},
		}
	}

	for _, policy := range []SchemaEvolutionPolicy{"", SchemaEvolutionAuto, SchemaEvolutionLog, SchemaEvolutionFail} {
		t.Run(string(policy), func(t *testing.T) {
			if err := base(policy).validate(); err != nil {
				t.Fatalf("validate returned error: %v", err)
			}
		})
	}

	cfg := base("oops")
	if err := cfg.validate(); err == nil {
		t.Fatal("validate returned nil error for invalid policy")
	}
}

func TestValidateSchemaEvolutionTypeChangePolicy(t *testing.T) {
	base := func(policy SchemaEvolutionPolicy) *Config {
		return &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{
				TargetMode: "upsert",
				SchemaEvolution: &SchemaEvolutionConfig{
					TypeChange: policy,
				},
			},
		}
	}

	for _, policy := range []SchemaEvolutionPolicy{"", SchemaEvolutionAuto, SchemaEvolutionLog, SchemaEvolutionFail} {
		t.Run(string(policy), func(t *testing.T) {
			if err := base(policy).validate(); err != nil {
				t.Fatalf("validate returned error: %v", err)
			}
		})
	}

	cfg := base("oops")
	if err := cfg.validate(); err == nil {
		t.Fatal("validate returned nil error for invalid policy")
	}
}

func TestSchemaEvolutionPolicyDefaults(t *testing.T) {
	disabled := MigrationConfig{}
	if disabled.SchemaEvolutionEnabled() {
		t.Fatal("SchemaEvolutionEnabled() = true, want false")
	}
	if got := disabled.SchemaEvolutionDeprecationWarning(); got != "" {
		t.Fatalf("disabled deprecation warning = %q, want empty", got)
	}
	if got := disabled.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("disabled added-column policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := disabled.NullabilityChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("disabled nullability-change policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := disabled.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("disabled type-change policy = %q, want %q", got, SchemaEvolutionLog)
	}

	enabled := MigrationConfig{SchemaEvolution: &SchemaEvolutionConfig{}}
	if !enabled.SchemaEvolutionEnabled() {
		t.Fatal("SchemaEvolutionEnabled() = false, want true")
	}
	warning := enabled.SchemaEvolutionDeprecationWarning()
	for _, want := range []string{
		"migration.schema_evolution is deprecated",
		"migration.schema_contract",
		"Existing schema_evolution behavior still runs for now",
		"#403",
	} {
		if !strings.Contains(warning, want) {
			t.Fatalf("deprecation warning missing %q in %q", want, warning)
		}
	}
	if got := enabled.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionAuto {
		t.Fatalf("enabled default added-column policy = %q, want %q", got, SchemaEvolutionAuto)
	}
	if got := enabled.NullabilityChangeSchemaEvolutionPolicy(); got != SchemaEvolutionAuto {
		t.Fatalf("enabled default nullability-change policy = %q, want %q", got, SchemaEvolutionAuto)
	}
	if got := enabled.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("enabled default type-change policy = %q, want %q", got, SchemaEvolutionLog)
	}

	typeAuto := MigrationConfig{SchemaEvolution: &SchemaEvolutionConfig{TypeChange: SchemaEvolutionAuto}}
	if got := typeAuto.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionAuto {
		t.Fatalf("explicit type-change policy = %q, want %q", got, SchemaEvolutionAuto)
	}

	discardAlias := MigrationConfig{SchemaEvolution: &SchemaEvolutionConfig{AddedColumn: SchemaEvolutionDiscard}}
	if got := discardAlias.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionDiscardValue {
		t.Fatalf("discard alias added-column policy = %q, want %q", got, SchemaEvolutionDiscardValue)
	}
}

func TestSchemaContractShorthandReportMode(t *testing.T) {
	withEmptySecretsFile(t)

	var logs strings.Builder
	logging.SetOutput(&logs)
	logging.SetFormat("text")
	t.Cleanup(func() {
		logging.SetOutput(os.Stdout)
		logging.SetFormat("text")
	})

	cfg, err := LoadBytes(minConfigYAML(`  target_mode: upsert
  schema_contract: report
`))
	if err != nil {
		t.Fatalf("LoadBytes() error: %v", err)
	}
	if cfg.Migration.SchemaContract == nil {
		t.Fatal("SchemaContract was nil")
	}
	if got := cfg.Migration.SchemaContractTablesMode(); got != SchemaContractReport {
		t.Fatalf("tables mode = %q, want %q", got, SchemaContractReport)
	}
	if got := cfg.Migration.SchemaContractColumnsMode(); got != SchemaContractReport {
		t.Fatalf("columns mode = %q, want %q", got, SchemaContractReport)
	}
	if got := cfg.Migration.SchemaContractDataTypeMode(); got != SchemaContractReport {
		t.Fatalf("data_type mode = %q, want %q", got, SchemaContractReport)
	}
	if got := cfg.Migration.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("added-column policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := cfg.Migration.NullabilityChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("nullability policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := cfg.Migration.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("type policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if strings.Contains(logs.String(), "migration.schema_evolution is deprecated") {
		t.Fatalf("schema_contract should not emit schema_evolution deprecation warning:\n%s", logs.String())
	}
}

func TestSchemaContractMappingDefaultsOmittedEntitiesToEvolve(t *testing.T) {
	withEmptySecretsFile(t)

	cfg, err := LoadBytes(minConfigYAML(`  target_mode: upsert
  schema_contract:
    columns: discard_value
`))
	if err != nil {
		t.Fatalf("LoadBytes() error: %v", err)
	}
	if got := cfg.Migration.SchemaContractTablesMode(); got != SchemaContractEvolve {
		t.Fatalf("tables mode = %q, want %q", got, SchemaContractEvolve)
	}
	if got := cfg.Migration.SchemaContractColumnsMode(); got != SchemaContractDiscardValue {
		t.Fatalf("columns mode = %q, want %q", got, SchemaContractDiscardValue)
	}
	if got := cfg.Migration.SchemaContractDataTypeMode(); got != SchemaContractEvolve {
		t.Fatalf("data_type mode = %q, want %q", got, SchemaContractEvolve)
	}
	if got := cfg.Migration.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionDiscardValue {
		t.Fatalf("added-column policy = %q, want %q", got, SchemaEvolutionDiscardValue)
	}
	if got := cfg.Migration.NullabilityChangeSchemaEvolutionPolicy(); got != SchemaEvolutionAuto {
		t.Fatalf("nullability policy = %q, want %q", got, SchemaEvolutionAuto)
	}
	if got := cfg.Migration.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionAuto {
		t.Fatalf("type policy = %q, want %q", got, SchemaEvolutionAuto)
	}
}

func TestSchemaContractDiscardModes(t *testing.T) {
	withEmptySecretsFile(t)

	cfg, err := LoadBytes(minConfigYAML(`  target_mode: upsert
  schema_contract:
    tables: discard_row
    columns: discard_row
    data_type: discard_value
`))
	if err != nil {
		t.Fatalf("LoadBytes() error: %v", err)
	}
	if got := cfg.Migration.SchemaContractTablesMode(); got != SchemaContractDiscardRow {
		t.Fatalf("tables mode = %q, want %q", got, SchemaContractDiscardRow)
	}
	if got := cfg.Migration.SchemaContractColumnsMode(); got != SchemaContractDiscardRow {
		t.Fatalf("columns mode = %q, want %q", got, SchemaContractDiscardRow)
	}
	if got := cfg.Migration.AddedColumnSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("added-column policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := cfg.Migration.SchemaContractDataTypeMode(); got != SchemaContractDiscardValue {
		t.Fatalf("data_type mode = %q, want %q", got, SchemaContractDiscardValue)
	}
	if got := cfg.Migration.NullabilityChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("nullability policy = %q, want %q", got, SchemaEvolutionLog)
	}
	if got := cfg.Migration.TypeChangeSchemaEvolutionPolicy(); got != SchemaEvolutionLog {
		t.Fatalf("type policy = %q, want %q", got, SchemaEvolutionLog)
	}
}

func TestValidateSchemaContractRejectsAmbiguousOrUnsupportedModes(t *testing.T) {
	withEmptySecretsFile(t)

	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "cannot combine with legacy schema evolution",
			yaml: `  schema_contract: report
  schema_evolution:
    added_column: log
`,
			want: "cannot be combined",
		},
		{
			name: "invalid mode",
			yaml: "  schema_contract: noisy\n",
			want: "migration.schema_contract.tables must be one of",
		},
		{
			name: "table discard value unsupported",
			yaml: `  schema_contract:
    tables: discard_value
`,
			want: "migration.schema_contract.tables must be one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadBytes(minConfigYAML(tt.yaml))
			if err == nil {
				t.Fatal("LoadBytes() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestLoadBytesWarnsForDeprecatedSchemaEvolution(t *testing.T) {
	withEmptySecretsFile(t)

	var logs strings.Builder
	logging.SetOutput(&logs)
	logging.SetFormat("text")
	oldLevel := logging.GetLevel()
	logging.SetLevel(logging.LevelInfo)
	t.Cleanup(func() {
		logging.SetOutput(os.Stdout)
		logging.SetFormat("text")
		logging.SetLevel(oldLevel)
	})

	_, err := LoadBytes(minConfigYAML(`  target_mode: upsert
  schema_evolution:
    added_column: auto
`))
	if err != nil {
		t.Fatalf("LoadBytes() error: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, "migration.schema_evolution is deprecated") {
		t.Fatalf("expected schema evolution deprecation warning, got:\n%s", got)
	} else if !strings.Contains(got, "config_key=migration.schema_evolution") {
		t.Fatalf("expected structured config key on warning, got:\n%s", got)
	}
}

func TestNotifyPolicyDefaults(t *testing.T) {
	cfg := MigrationConfig{}
	if !cfg.NotifyOnSuccess() {
		t.Fatal("NotifyOnSuccess() = false, want true")
	}
	if !cfg.NotifyOnFailure() {
		t.Fatal("NotifyOnFailure() = false, want true")
	}
	if !cfg.NotifyOnStart() {
		t.Fatal("NotifyOnStart() = false, want true")
	}

	disabledSuccess := false
	enabledFailure := true
	cfg.Notify = NotifyConfig{
		OnSuccess: &disabledSuccess,
		OnFailure: &enabledFailure,
	}
	if cfg.NotifyOnSuccess() {
		t.Fatal("NotifyOnSuccess() = true, want false")
	}
	if !cfg.NotifyOnFailure() {
		t.Fatal("NotifyOnFailure() = false, want true")
	}
	if !cfg.NotifyOnStart() {
		t.Fatal("NotifyOnStart() = false when failure alerts are enabled")
	}

	cfg.Notify.OnFailure = &disabledSuccess
	if cfg.NotifyOnStart() {
		t.Fatal("NotifyOnStart() = true, want false when both completion policies are disabled")
	}
}

func TestNotifyPolicyOmittedFromResumeHashJSON(t *testing.T) {
	onSuccess := false
	onFailure := false
	cfg := &Config{
		Migration: MigrationConfig{
			Notify: NotifyConfig{
				OnSuccess: &onSuccess,
				OnFailure: &onFailure,
			},
		},
	}

	data, err := json.Marshal(cfg.Sanitized().Migration)
	if err != nil {
		t.Fatalf("marshal migration: %v", err)
	}
	if strings.Contains(string(data), "Notify") || strings.Contains(string(data), "on_success") {
		t.Fatalf("notify policy leaked into resume hash JSON: %s", data)
	}
}

func TestConfigValidationWithAliases(t *testing.T) {
	// Test that config validation accepts driver aliases
	tests := []struct {
		name       string
		sourceType string
		targetType string
		wantErr    bool
	}{
		{"mssql to postgres", "mssql", "postgres", false},
		{"sqlserver to pg", "sqlserver", "pg", false},
		{"sql-server to postgresql", "sql-server", "postgresql", false},
		{"pg to mssql", "pg", "mssql", false},
		{"postgres to sqlserver", "postgres", "sqlserver", false},
		{"mysql to postgres", "mysql", "postgres", false},
		{"mariadb to mssql", "mariadb", "mssql", false},
		{"invalid source", "oracle", "postgres", true},
		{"invalid target", "mssql", "oracle", true},
		{"both invalid", "oracle", "sqlite", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Source: SourceConfig{
					Type:     tt.sourceType,
					Host:     "localhost",
					Database: "test",
				},
				Target: TargetConfig{
					Type:     tt.targetType,
					Host:     "localhost",
					Database: "test",
				},
				Migration: MigrationConfig{
					TargetMode: "drop_recreate",
				},
			}
			err := cfg.validate()
			if tt.wantErr && err == nil {
				t.Errorf("validate() expected error for source=%q, target=%q", tt.sourceType, tt.targetType)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validate() unexpected error: %v", err)
			}
		})
	}
}

func TestSanitizedRedactsPasswords(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Type:     "mssql",
			Host:     "localhost",
			Database: "test",
			Password: "secret-password",
		},
		Target: TargetConfig{
			Type:     "postgres",
			Host:     "localhost",
			Database: "test",
			Password: "another-secret",
		},
		Migration: MigrationConfig{
			TargetMode: "drop_recreate",
		},
		AI: &AIConfig{
			Provider: "openai",
			Model:    "gpt-test",
			APIKey:   "sk-secret",
		},
		Slack: &SlackConfig{
			WebhookURL: "https://hooks.slack.com/services/secret",
			Channel:    "#ops",
			Username:   "dmt",
			Enabled:    true,
		},
	}

	sanitized := cfg.Sanitized()

	// Verify all secrets are redacted
	if sanitized.Source.Password != "[REDACTED]" {
		t.Errorf("Source password not redacted: %s", sanitized.Source.Password)
	}
	if sanitized.Target.Password != "[REDACTED]" {
		t.Errorf("Target password not redacted: %s", sanitized.Target.Password)
	}
	if sanitized.AI == nil || sanitized.AI.APIKey != "[REDACTED]" {
		t.Fatalf("AI API key not redacted: %#v", sanitized.AI)
	}
	if sanitized.Slack == nil || sanitized.Slack.WebhookURL != "[REDACTED]" {
		t.Fatalf("Slack webhook not redacted: %#v", sanitized.Slack)
	}
	if sanitized.AI.Provider != "openai" || sanitized.AI.Model != "gpt-test" {
		t.Fatalf("AI non-secret fields changed: %#v", sanitized.AI)
	}
	if sanitized.Slack.Channel != "#ops" || sanitized.Slack.Username != "dmt" || !sanitized.Slack.Enabled {
		t.Fatalf("Slack non-secret fields changed: %#v", sanitized.Slack)
	}

	// Verify original is unchanged
	if cfg.Source.Password == "[REDACTED]" {
		t.Error("Original source password was modified")
	}
	if cfg.Target.Password == "[REDACTED]" {
		t.Error("Original target password was modified")
	}
	if cfg.AI.APIKey == "[REDACTED]" {
		t.Error("Original AI API key was modified")
	}
	if cfg.Slack.WebhookURL == "[REDACTED]" {
		t.Error("Original Slack webhook was modified")
	}
}

func TestBooleanGlobalDefaultsLogic(t *testing.T) {
	// This test documents the expected behavior of boolean global defaults.
	// The logic is: apply global default only when migration config value is false.
	//
	// Limitation: We cannot distinguish "user didn't set" from "user set false",
	// so global true always wins over migration false.

	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name           string
		globalDefault  *bool // nil = not set in global config
		migrationValue bool  // value in per-migration config
		expected       bool  // expected final value
	}{
		// Global not set - migration value preserved
		{"global nil, migration false", nil, false, false},
		{"global nil, migration true", nil, true, true},

		// Global true - wins unless migration is already true
		{"global true, migration false", boolPtr(true), false, true},
		{"global true, migration true", boolPtr(true), true, true},

		// Global false - applied when migration is false, migration true wins
		{"global false, migration false", boolPtr(false), false, false},
		{"global false, migration true", boolPtr(false), true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the logic from applyGlobalDefaults
			result := tt.migrationValue
			if tt.globalDefault != nil && !tt.migrationValue {
				result = *tt.globalDefault
			}

			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBooleanGlobalDefaultsDocumentedLimitation(t *testing.T) {
	// This test explicitly documents the limitation:
	// You CANNOT override a global "true" to "false" per-migration.

	boolPtr := func(b bool) *bool { return &b }

	globalDefault := boolPtr(true)
	migrationExplicitlyFalse := false // User wants false, but we can't tell

	// Apply the logic
	result := migrationExplicitlyFalse
	if globalDefault != nil && !migrationExplicitlyFalse {
		result = *globalDefault
	}

	// The limitation: global true overrides migration false
	if result != true {
		t.Error("Expected limitation: global true should override migration false")
	}

	// Document this is a known limitation, not a bug
	t.Log("Known limitation: cannot override global 'true' to 'false' per-migration")
}
