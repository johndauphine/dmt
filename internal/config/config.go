package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

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
	Type     string `yaml:"type"`     // "mssql" or "postgres" (default: mssql)
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
}

// TargetConfig holds target database connection settings
type TargetConfig struct {
	Type              string `yaml:"type"`                 // "postgres" or "mssql" (default: postgres)
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	Database          string `yaml:"database"`
	User              string `yaml:"user"`
	Password          string `yaml:"password"`
	Schema            string `yaml:"schema"`
	BulkInsertTempDir string `yaml:"bulk_insert_temp_dir"` // For MSSQL target: local path where CSV files are written
	BulkInsertSQLPath string `yaml:"bulk_insert_sql_path"` // For MSSQL target: path as seen by SQL Server (if different from bulk_insert_temp_dir)
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
	if c.Migration.ChunkSize == 0 {
		c.Migration.ChunkSize = 200000
	}
	if c.Migration.MaxPartitions == 0 {
		c.Migration.MaxPartitions = 8
	}
	if c.Migration.Workers == 0 {
		c.Migration.Workers = 8
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
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database)
	}
	// Default: MSSQL
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
		c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database)
}

// TargetDSN returns the target database connection string
func (c *Config) TargetDSN() string {
	if c.Target.Type == "mssql" {
		return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
			c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.Database)
	}
	// Default: PostgreSQL
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.Database)
}
