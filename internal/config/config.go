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
}

// SourceConfig holds MSSQL connection settings
type SourceConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
}

// TargetConfig holds PostgreSQL connection settings
type TargetConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
}

// MigrationConfig holds migration behavior settings
type MigrationConfig struct {
	MaxConnections      int      `yaml:"max_connections"`
	ChunkSize           int      `yaml:"chunk_size"`
	MaxPartitions       int      `yaml:"max_partitions"`
	Workers             int      `yaml:"workers"`
	LargeTableThreshold int64    `yaml:"large_table_threshold"`
	ExcludeTables       []string `yaml:"exclude_tables"`
	DataDir             string   `yaml:"data_dir"`
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
	if c.Source.Port == 0 {
		c.Source.Port = 1433
	}
	if c.Source.Schema == "" {
		c.Source.Schema = "dbo"
	}
	if c.Target.Port == 0 {
		c.Target.Port = 5432
	}
	if c.Target.Schema == "" {
		c.Target.Schema = "public"
	}
	if c.Migration.MaxConnections == 0 {
		c.Migration.MaxConnections = 12
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
}

func (c *Config) validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	return nil
}

// SourceDSN returns the MSSQL connection string
func (c *Config) SourceDSN() string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
		c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database)
}

// TargetDSN returns the PostgreSQL connection string
func (c *Config) TargetDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.Target.User, c.Target.Password, c.Target.Host, c.Target.Port, c.Target.Database)
}
