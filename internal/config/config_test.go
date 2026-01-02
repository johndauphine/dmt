package config

import (
	"strings"
	"testing"
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
				"true", false, "", "", "", "", "")

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
		"true", false, "kerberos", "/path/to/krb5.conf", "", "REALM.COM", "MSSQLSvc/host:1433")

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
			name:        "same-engine with truncate allowed",
			sourceType:  "mssql",
			targetType:  "mssql",
			targetMode:  "truncate",
			sourceHost:  "host1",
			targetHost:  "host2",
			sourcePort:  1433,
			targetPort:  1433,
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

func TestAutoTuneWriteAheadWriters(t *testing.T) {
	tests := []struct {
		name       string
		targetType string
		cpuCores   int
		expected   int
	}{
		{"MSSQL 16 cores - fixed at 2", "mssql", 16, 2},
		{"MSSQL 8 cores - fixed at 2", "mssql", 8, 2},
		{"MSSQL 2 cores - fixed at 2", "mssql", 2, 2},
		{"PostgreSQL 16 cores - capped at 4", "postgres", 16, 4},
		{"PostgreSQL 8 cores - cores/4=2", "postgres", 8, 2},
		{"PostgreSQL 4 cores - cores/4=1 clamped to 2", "postgres", 4, 2},
		{"PostgreSQL 2 cores - cores/4=0 clamped to 2", "postgres", 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
					Type:     tt.targetType,
					Host:     "localhost",
					Port:     5432,
					Database: "target",
					User:     "user",
					Password: "pass",
				},
			}
			cfg.autoConfig.CPUCores = tt.cpuCores
			cfg.applyDefaults()

			if cfg.Migration.WriteAheadWriters != tt.expected {
				t.Errorf("expected %d writers for %s with %d cores, got %d",
					tt.expected, tt.targetType, tt.cpuCores, cfg.Migration.WriteAheadWriters)
			}
		})
	}
}

func TestAutoTuneParallelReaders(t *testing.T) {
	tests := []struct {
		name     string
		cpuCores int
		expected int
	}{
		{"16 cores - capped at 4", 16, 4},
		{"8 cores - cores/4=2", 8, 2},
		{"4 cores - cores/4=1 clamped to 2", 4, 2},
		{"2 cores - cores/4=0 clamped to 2", 2, 2},
		{"1 core - clamped to 2", 1, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			cfg.autoConfig.CPUCores = tt.cpuCores
			cfg.applyDefaults()

			if cfg.Migration.ParallelReaders != tt.expected {
				t.Errorf("expected %d readers for %d cores, got %d",
					tt.expected, tt.cpuCores, cfg.Migration.ParallelReaders)
			}
		})
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
	cfg.applyDefaults()

	// User values should be preserved
	if cfg.Migration.WriteAheadWriters != 8 {
		t.Errorf("expected user-specified 8 writers, got %d", cfg.Migration.WriteAheadWriters)
	}
	if cfg.Migration.ParallelReaders != 6 {
		t.Errorf("expected user-specified 6 readers, got %d", cfg.Migration.ParallelReaders)
	}
}

func TestAutoTuneConnectionPoolSizing(t *testing.T) {
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
		Migration: MigrationConfig{
			Workers: 4,
		},
	}
	cfg.autoConfig.CPUCores = 8
	cfg.autoConfig.AvailableMemoryMB = 8192
	cfg.applyDefaults()

	// With 8 cores: readers=2, writers=2
	// Source connections: workers * readers + 4 = 4 * 2 + 4 = 12
	// Target connections: workers * writers + 4 = 4 * 2 + 4 = 12
	expectedMSSQLConns := cfg.Migration.Workers*cfg.Migration.ParallelReaders + 4
	expectedPGConns := cfg.Migration.Workers*cfg.Migration.WriteAheadWriters + 4

	if cfg.Migration.MaxMssqlConnections < expectedMSSQLConns {
		t.Errorf("insufficient MSSQL connections: got %d, need at least %d",
			cfg.Migration.MaxMssqlConnections, expectedMSSQLConns)
	}
	if cfg.Migration.MaxPgConnections < expectedPGConns {
		t.Errorf("insufficient PG connections: got %d, need at least %d",
			cfg.Migration.MaxPgConnections, expectedPGConns)
	}
}
