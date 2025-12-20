package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// getAvailableMemoryMB returns available system memory in MB (Linux only, falls back to 4GB)
func getAvailableMemoryMB() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 4096 // Default 4GB if can't read
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb / 1024 // Convert KB to MB
				}
			}
		}
	}
	return 4096 // Default 4GB
}

// Config holds all configuration for the migration tool
type Config struct {
	Source    SourceConfig    `yaml:"source"`
	Target    TargetConfig    `yaml:"target"`
	Migration MigrationConfig `yaml:"migration"`
	Slack     SlackConfig     `yaml:"slack"`
}

// SlackConfig holds Slack notification settings
type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	Channel    string `yaml:"channel"`
	Username   string `yaml:"username"`
	Enabled    bool   `yaml:"enabled"`
}

// SourceConfig holds source database connection settings
type SourceConfig struct {
	Type            string `yaml:"type"`              // "mssql" or "postgres" (default: mssql)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         string `yaml:"encrypt"`           // MSSQL: disable, false, true (default: true)
}

// TargetConfig holds target database connection settings
type TargetConfig struct {
	Type            string `yaml:"type"`              // "postgres" or "mssql" (default: postgres)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         string `yaml:"encrypt"`           // MSSQL: disable, false, true (default: true)
}

// MigrationConfig holds migration behavior settings
type MigrationConfig struct {
	MaxConnections         int      `yaml:"max_connections"`          // Deprecated: use max_mssql_connections and max_pg_connections
	MaxMssqlConnections    int      `yaml:"max_mssql_connections"`    // Max SQL Server connections
	MaxPgConnections       int      `yaml:"max_pg_connections"`       // Max PostgreSQL connections
	ChunkSize              int      `yaml:"chunk_size"`
	MaxPartitions          int      `yaml:"max_partitions"`
	Workers                int      `yaml:"workers"`
	LargeTableThreshold    int64    `yaml:"large_table_threshold"`
	IncludeTables          []string `yaml:"include_tables"`           // Only migrate these tables (glob patterns)
	ExcludeTables          []string `yaml:"exclude_tables"`           // Skip these tables (glob patterns)
	DataDir                string   `yaml:"data_dir"`
	TargetMode             string   `yaml:"target_mode"`              // "drop_recreate" (default) or "truncate"
	StrictConsistency      bool     `yaml:"strict_consistency"`       // Use table locks instead of NOLOCK
	CreateIndexes          bool     `yaml:"create_indexes"`           // Create non-PK indexes
	CreateForeignKeys      bool     `yaml:"create_foreign_keys"`      // Create foreign key constraints
	CreateCheckConstraints bool     `yaml:"create_check_constraints"` // Create CHECK constraints
	SampleValidation       bool     `yaml:"sample_validation"`        // Enable sample data validation
	SampleSize             int      `yaml:"sample_size"`              // Number of rows to sample for validation
	ReadAheadBuffers       int      `yaml:"read_ahead_buffers"`       // Number of chunks to read ahead (default=8)
	WriteAheadWriters      int      `yaml:"write_ahead_writers"`      // Number of parallel writers per job (default=2)
	ParallelReaders        int      `yaml:"parallel_readers"`         // Number of parallel readers per job (default=2)
	MSSQLRowsPerBatch      int      `yaml:"mssql_rows_per_batch"`     // MSSQL bulk copy hint (default=chunk_size)
}

// Load reads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	// Apply defaults
	cfg.applyDefaults()

	// Validate
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) applyDefaults() {
	// Source defaults
	if c.Source.Type == "" {
		c.Source.Type = "mssql" // Default source is SQL Server for backward compat
	}
	if c.Source.Port == 0 {
		if c.Source.Type == "postgres" {
			c.Source.Port = 5432
		} else {
			c.Source.Port = 1433
		}
	}
	if c.Source.Schema == "" {
		if c.Source.Type == "postgres" {
			c.Source.Schema = "public"
		} else {
			c.Source.Schema = "dbo"
		}
	}
	// SSL defaults for source
	if c.Source.SSLMode == "" {
		c.Source.SSLMode = "require" // Secure default for PostgreSQL
	}
	if c.Source.Encrypt == "" {
		c.Source.Encrypt = "true" // Secure default for MSSQL
	}

	// Target defaults
	if c.Target.Type == "" {
		c.Target.Type = "postgres" // Default target is PostgreSQL for backward compat
	}
	if c.Target.Port == 0 {
		if c.Target.Type == "mssql" {
			c.Target.Port = 1433
		} else {
			c.Target.Port = 5432
		}
	}
	if c.Target.Schema == "" {
		if c.Target.Type == "mssql" {
			c.Target.Schema = "dbo"
		} else {
			c.Target.Schema = "public"
		}
	}
	// SSL defaults for target
	if c.Target.SSLMode == "" {
		c.Target.SSLMode = "require" // Secure default for PostgreSQL
	}
	if c.Target.Encrypt == "" {
		c.Target.Encrypt = "true" // Secure default for MSSQL
	}

	// Handle backwards compatibility: if max_connections is set but new options aren't
	if c.Migration.MaxConnections == 0 {
		c.Migration.MaxConnections = 12
	}
	if c.Migration.MaxMssqlConnections == 0 {
		c.Migration.MaxMssqlConnections = c.Migration.MaxConnections
	}
	if c.Migration.MaxPgConnections == 0 {
		c.Migration.MaxPgConnections = c.Migration.MaxConnections
	}
	// Auto-detect CPU cores for workers (leave 2 cores for OS/DB overhead)
	if c.Migration.Workers == 0 {
		cores := runtime.NumCPU()
		c.Migration.Workers = cores - 2
		if c.Migration.Workers < 2 {
			c.Migration.Workers = 2
		}
		if c.Migration.Workers > 32 {
			c.Migration.Workers = 32 // Cap at 32 workers
		}
	}
	if c.Migration.MaxPartitions == 0 {
		c.Migration.MaxPartitions = c.Migration.Workers // Match workers
	}
	// Auto-tune chunk size and buffers based on available memory
	// Target: use ~50% of available RAM for buffering
	// Each worker buffers: read_ahead_buffers × chunk_size × ~500 bytes/row average
	availableMB := getAvailableMemoryMB()
	targetMemoryMB := availableMB / 2 // Use 50% of available RAM
	if c.Migration.ChunkSize == 0 {
		// Scale chunk size with available memory: 100K-500K range
		c.Migration.ChunkSize = int(targetMemoryMB * 50) // ~50 rows per MB
		if c.Migration.ChunkSize < 100000 {
			c.Migration.ChunkSize = 100000
		}
		if c.Migration.ChunkSize > 500000 {
			c.Migration.ChunkSize = 500000
		}
	}
	if c.Migration.LargeTableThreshold == 0 {
		c.Migration.LargeTableThreshold = 5000000
	}
	if c.Migration.DataDir == "" {
		home, _ := os.UserHomeDir()
		c.Migration.DataDir = filepath.Join(home, ".mssql-pg-migrate")
	}
	if c.Migration.TargetMode == "" {
		c.Migration.TargetMode = "drop_recreate" // Default: drop and recreate tables
	}
	if c.Migration.SampleSize == 0 {
		c.Migration.SampleSize = 100 // Default sample size for validation
	}
	if c.Migration.WriteAheadWriters == 0 {
		c.Migration.WriteAheadWriters = 2 // Default: 2 parallel writers per job
	}
	if c.Migration.ParallelReaders == 0 {
		c.Migration.ParallelReaders = 2 // Default: 2 parallel readers per job
	}
	// MSSQLRowsPerBatch defaults to chunk_size (set after chunk_size is finalized)
	// This is applied later since it depends on ChunkSize being set
	if c.Migration.ReadAheadBuffers == 0 {
		// Scale buffers: enough to keep writers fed, but within memory limits
		// Formula: targetMemoryMB / workers / (chunkSize * 500 bytes avg)
		bytesPerChunk := int64(c.Migration.ChunkSize) * 500 // ~500 bytes per row average
		buffersPerWorker := (targetMemoryMB * 1024 * 1024) / int64(c.Migration.Workers) / bytesPerChunk
		c.Migration.ReadAheadBuffers = int(buffersPerWorker)
		if c.Migration.ReadAheadBuffers < 4 {
			c.Migration.ReadAheadBuffers = 4
		}
		if c.Migration.ReadAheadBuffers > 32 {
			c.Migration.ReadAheadBuffers = 32 // Cap to avoid excessive memory
		}
	}

	// Auto-size connection pools based on workers, readers, and writers
	// Each worker needs: parallel_readers source connections + write_ahead_writers target connections
	minSourceConns := c.Migration.Workers * c.Migration.ParallelReaders
	minTargetConns := c.Migration.Workers * c.Migration.WriteAheadWriters
	if c.Migration.MaxConnections < minTargetConns {
		c.Migration.MaxConnections = minTargetConns + 4 // Add headroom
	}
	if c.Migration.MaxMssqlConnections < minSourceConns {
		c.Migration.MaxMssqlConnections = minSourceConns + 4
	}
	if c.Migration.MaxPgConnections < minTargetConns {
		c.Migration.MaxPgConnections = minTargetConns + 4
	}

	// Default MSSQLRowsPerBatch to chunk_size if not specified
	if c.Migration.MSSQLRowsPerBatch == 0 {
		c.Migration.MSSQLRowsPerBatch = c.Migration.ChunkSize
	}
}

func (c *Config) validate() error {
	// Validate source
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if c.Source.Type != "mssql" && c.Source.Type != "postgres" {
		return fmt.Errorf("source.type must be 'mssql' or 'postgres', got '%s'", c.Source.Type)
	}

	// Validate target
	if c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if c.Target.Type != "mssql" && c.Target.Type != "postgres" {
		return fmt.Errorf("target.type must be 'mssql' or 'postgres', got '%s'", c.Target.Type)
	}

	// Ensure cross-database migration only (no same-to-same)
	if c.Source.Type == c.Target.Type {
		return fmt.Errorf("same-database migration not supported: %s -> %s (use native tools instead)", c.Source.Type, c.Target.Type)
	}

	// Validate migration settings
	if c.Migration.TargetMode != "drop_recreate" && c.Migration.TargetMode != "truncate" {
		return fmt.Errorf("migration.target_mode must be 'drop_recreate' or 'truncate'")
	}
	return nil
}

// SourceDSN returns the source database connection string
func (c *Config) SourceDSN() string {
	if c.Source.Type == "postgres" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database, c.Source.SSLMode)
	}
	// Default: MSSQL
	trustCert := "false"
	if c.Source.TrustServerCert {
		trustCert = "true"
	}
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database, c.Source.Encrypt, trustCert)
}

// TargetDSN returns the target database connection string
func (c *Config) TargetDSN() string {
	if c.Target.Type == "mssql" {
		trustCert := "false"
		if c.Target.TrustServerCert {
			trustCert = "true"
		}
		return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
			c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.Database, c.Target.Encrypt, trustCert)
	}
	// Default: PostgreSQL
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.Database, c.Target.SSLMode)
}

// Sanitized returns a copy of the config with sensitive fields redacted
func (c *Config) Sanitized() *Config {
	sanitized := *c // shallow copy

	// Redact source credentials
	sanitized.Source.Password = "[REDACTED]"

	// Redact target credentials
	sanitized.Target.Password = "[REDACTED]"

	// Redact Slack webhook
	if sanitized.Slack.WebhookURL != "" {
		sanitized.Slack.WebhookURL = "[REDACTED]"
	}

	return &sanitized
}
