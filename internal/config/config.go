package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"
)

// expandTilde expands ~ or ~/ at the start of a path to the user's home directory
func expandTilde(path string) string {
	if path == "" {
		return path
	}
	if path == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}

// AutoConfig tracks which values were auto-configured and why
type AutoConfig struct {
	// System resources detected
	AvailableMemoryMB int64
	CPUCores          int

	// Original values (before auto-tuning)
	OriginalWorkers            int
	OriginalChunkSize          int
	OriginalReadAheadBuffers   int
	OriginalMaxPartitions      int
	OriginalMaxMssqlConns      int
	OriginalMaxPgConns         int
	OriginalWriteAheadWriters  int
	OriginalParallelReaders    int
	OriginalMSSQLRowsPerBatch  int
	OriginalLargeTableThresh   int64
	OriginalSampleSize         int

	// Target memory used for calculations
	TargetMemoryMB int64
}

// Config holds all configuration for the migration tool
type Config struct {
	Source    SourceConfig    `yaml:"source"`
	Target    TargetConfig    `yaml:"target"`
	Migration MigrationConfig `yaml:"migration"`
	Slack     SlackConfig     `yaml:"slack"`
	Profile   ProfileConfig   `yaml:"profile,omitempty"`

	// AutoConfig stores auto-tuning metadata (not serialized to YAML)
	autoConfig AutoConfig
}

// ProfileConfig holds optional profile metadata.
type ProfileConfig struct {
	Name        string `yaml:"name,omitempty"`
	Description string `yaml:"description,omitempty"`
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
	Type            string `yaml:"type"` // "mssql" or "postgres" (default: mssql)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         string `yaml:"encrypt"`           // MSSQL: disable, false, true (default: true)
	// Kerberos authentication (alternative to user/password)
	Auth       string `yaml:"auth"`       // "password" (default) or "kerberos"
	Krb5Conf   string `yaml:"krb5_conf"`  // Path to krb5.conf (optional, uses system default)
	Keytab     string `yaml:"keytab"`     // Path to keytab file (optional, uses credential cache)
	Realm      string `yaml:"realm"`      // Kerberos realm (optional, auto-detected)
	SPN        string `yaml:"spn"`        // Service Principal Name for MSSQL (optional)
	GSSEncMode string `yaml:"gssencmode"` // PostgreSQL GSSAPI encryption: disable, prefer, require (default: prefer)
}

// TargetConfig holds target database connection settings
type TargetConfig struct {
	Type            string `yaml:"type"` // "postgres" or "mssql" (default: postgres)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         string `yaml:"encrypt"`           // MSSQL: disable, false, true (default: true)
	// Kerberos authentication (alternative to user/password)
	Auth       string `yaml:"auth"`       // "password" (default) or "kerberos"
	Krb5Conf   string `yaml:"krb5_conf"`  // Path to krb5.conf (optional, uses system default)
	Keytab     string `yaml:"keytab"`     // Path to keytab file (optional, uses credential cache)
	Realm      string `yaml:"realm"`      // Kerberos realm (optional, auto-detected)
	SPN        string `yaml:"spn"`        // Service Principal Name for MSSQL (optional)
	GSSEncMode string `yaml:"gssencmode"` // PostgreSQL GSSAPI encryption: disable, prefer, require (default: prefer)
}

// MigrationConfig holds migration behavior settings
type MigrationConfig struct {
	MaxConnections         int      `yaml:"max_connections"`       // Deprecated: use max_mssql_connections and max_pg_connections
	MaxMssqlConnections    int      `yaml:"max_mssql_connections"` // Max SQL Server connections
	MaxPgConnections       int      `yaml:"max_pg_connections"`    // Max PostgreSQL connections
	ChunkSize              int      `yaml:"chunk_size"`
	MaxPartitions          int      `yaml:"max_partitions"`
	Workers                int      `yaml:"workers"`
	LargeTableThreshold    int64    `yaml:"large_table_threshold"`
	IncludeTables          []string `yaml:"include_tables"` // Only migrate these tables (glob patterns)
	ExcludeTables          []string `yaml:"exclude_tables"` // Skip these tables (glob patterns)
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

// LoadOptions controls configuration loading behavior.
type LoadOptions struct {
	SuppressWarnings bool
}

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	return LoadWithOptions(path, LoadOptions{})
}

// LoadWithOptions reads configuration from a YAML file with options.
func LoadWithOptions(path string, opts LoadOptions) (*Config, error) {
	// Check file permissions before reading (warns if insecure)
	if warning := checkFilePermissions(path); warning != "" && !opts.SuppressWarnings {
		fmt.Fprint(os.Stderr, warning)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	return LoadBytes(data)
}

// LoadBytes reads configuration from YAML bytes.
func LoadBytes(data []byte) (*Config, error) {
	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Apply defaults
	cfg.applyDefaults()

	// Validate
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// DefaultDataDir returns the default data directory for state storage.
func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".mssql-pg-migrate")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

func (c *Config) applyDefaults() {
	// Capture original values before auto-tuning
	c.autoConfig.OriginalWorkers = c.Migration.Workers
	c.autoConfig.OriginalChunkSize = c.Migration.ChunkSize
	c.autoConfig.OriginalReadAheadBuffers = c.Migration.ReadAheadBuffers
	c.autoConfig.OriginalMaxPartitions = c.Migration.MaxPartitions
	c.autoConfig.OriginalMaxMssqlConns = c.Migration.MaxMssqlConnections
	c.autoConfig.OriginalMaxPgConns = c.Migration.MaxPgConnections
	c.autoConfig.OriginalWriteAheadWriters = c.Migration.WriteAheadWriters
	c.autoConfig.OriginalParallelReaders = c.Migration.ParallelReaders
	c.autoConfig.OriginalMSSQLRowsPerBatch = c.Migration.MSSQLRowsPerBatch
	c.autoConfig.OriginalLargeTableThresh = c.Migration.LargeTableThreshold
	c.autoConfig.OriginalSampleSize = c.Migration.SampleSize

	// Detect system resources
	c.autoConfig.CPUCores = runtime.NumCPU()
	c.autoConfig.AvailableMemoryMB = getAvailableMemoryMB()
	c.autoConfig.TargetMemoryMB = c.autoConfig.AvailableMemoryMB / 2 // Use 50% of available RAM

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
	targetMemoryMB := c.autoConfig.TargetMemoryMB
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
	} else {
		c.Migration.DataDir = expandTilde(c.Migration.DataDir)
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
		return c.buildPostgresDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, c.Source.SSLMode,
			c.Source.Auth, c.Source.GSSEncMode)
	}
	// Default: MSSQL
	return c.buildMSSQLDSN(c.Source.Host, c.Source.Port, c.Source.Database,
		c.Source.User, c.Source.Password, c.Source.Encrypt, c.Source.TrustServerCert,
		c.Source.Auth, c.Source.Krb5Conf, c.Source.Keytab, c.Source.Realm, c.Source.SPN)
}

// TargetDSN returns the target database connection string
func (c *Config) TargetDSN() string {
	if c.Target.Type == "mssql" {
		return c.buildMSSQLDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, c.Target.Encrypt, c.Target.TrustServerCert,
			c.Target.Auth, c.Target.Krb5Conf, c.Target.Keytab, c.Target.Realm, c.Target.SPN)
	}
	// Default: PostgreSQL
	return c.buildPostgresDSN(c.Target.Host, c.Target.Port, c.Target.Database,
		c.Target.User, c.Target.Password, c.Target.SSLMode,
		c.Target.Auth, c.Target.GSSEncMode)
}

// buildMSSQLDSN builds an MSSQL connection string with optional Kerberos auth
func (c *Config) buildMSSQLDSN(host string, port int, database, user, password, encrypt string,
	trustServerCert bool, auth, krb5Conf, keytab, realm, spn string) string {

	trustCert := "false"
	if trustServerCert {
		trustCert = "true"
	}

	// Kerberos authentication
	if auth == "kerberos" {
		dsn := fmt.Sprintf("sqlserver://%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s&authenticator=krb5",
			host, port, database, encrypt, trustCert)

		// Optional Kerberos parameters
		if krb5Conf != "" {
			dsn += "&krb5-configfile=" + krb5Conf
		}
		if keytab != "" {
			dsn += "&krb5-keytabfile=" + keytab
		}
		if realm != "" {
			dsn += "&krb5-realm=" + realm
		}
		if spn != "" {
			dsn += "&ServerSPN=" + spn
		}
		// If user specified, use it as the principal
		if user != "" {
			dsn += "&krb5-username=" + user
		}
		return dsn
	}

	// Password authentication (default)
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		user, password, host, port, database, encrypt, trustCert)
}

// buildPostgresDSN builds a PostgreSQL connection string with optional Kerberos auth
func (c *Config) buildPostgresDSN(host string, port int, database, user, password, sslMode,
	auth, gssEncMode string) string {

	// Kerberos/GSSAPI authentication
	if auth == "kerberos" {
		gssEnc := "prefer"
		if gssEncMode != "" {
			gssEnc = gssEncMode
		}
		// For Kerberos, we don't include password in the DSN
		if user != "" {
			return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&gssencmode=%s",
				user, host, port, database, sslMode, gssEnc)
		}
		return fmt.Sprintf("postgres://%s:%d/%s?sslmode=%s&gssencmode=%s",
			host, port, database, sslMode, gssEnc)
	}

	// Password authentication (default)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		user, password, host, port, database, sslMode)
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

// SanitizedYAML returns the config as YAML with sensitive fields redacted
func (c *Config) SanitizedYAML() string {
	sanitized := c.Sanitized()
	data, err := yaml.Marshal(sanitized)
	if err != nil {
		return fmt.Sprintf("error marshaling config: %v", err)
	}
	return string(data)
}

// formatAutoValue formats a value with auto-tuning explanation if applicable
func formatAutoValue(current, original int, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatAutoValue64 formats an int64 value with auto-tuning explanation if applicable
func formatAutoValue64(current, original int64, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatMemorySize formats bytes as a human-readable size
func formatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// DebugDump returns a comprehensive configuration dump with auto-tuning explanations
func (c *Config) DebugDump() string {
	var b strings.Builder
	ac := c.autoConfig

	b.WriteString("\n=== Configuration ===\n\n")

	// System Resources
	b.WriteString("System Resources:\n")
	b.WriteString(fmt.Sprintf("  Available Memory: %d MB\n", ac.AvailableMemoryMB))
	b.WriteString(fmt.Sprintf("  Target Memory (50%%): %d MB\n", ac.TargetMemoryMB))
	b.WriteString(fmt.Sprintf("  CPU Cores: %d\n", ac.CPUCores))

	// Source Database
	b.WriteString(fmt.Sprintf("\nSource (%s):\n", c.Source.Type))
	b.WriteString(fmt.Sprintf("  Host: %s\n", c.Source.Host))
	b.WriteString(fmt.Sprintf("  Port: %d\n", c.Source.Port))
	b.WriteString(fmt.Sprintf("  Database: %s\n", c.Source.Database))
	b.WriteString(fmt.Sprintf("  Schema: %s\n", c.Source.Schema))
	b.WriteString(fmt.Sprintf("  User: %s\n", c.Source.User))
	b.WriteString("  Password: [REDACTED]\n")
	if c.Source.Type == "mssql" {
		b.WriteString(fmt.Sprintf("  Encrypt: %s\n", c.Source.Encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Source.TrustServerCert))
	} else {
		b.WriteString(fmt.Sprintf("  SSLMode: %s\n", c.Source.SSLMode))
	}
	auth := c.Source.Auth
	if auth == "" {
		auth = "password"
	}
	b.WriteString(fmt.Sprintf("  Auth: %s\n", auth))
	if c.Source.Auth == "kerberos" {
		if c.Source.Krb5Conf != "" {
			b.WriteString(fmt.Sprintf("  Krb5Conf: %s\n", c.Source.Krb5Conf))
		}
		if c.Source.Realm != "" {
			b.WriteString(fmt.Sprintf("  Realm: %s\n", c.Source.Realm))
		}
	}

	// Target Database
	b.WriteString(fmt.Sprintf("\nTarget (%s):\n", c.Target.Type))
	b.WriteString(fmt.Sprintf("  Host: %s\n", c.Target.Host))
	b.WriteString(fmt.Sprintf("  Port: %d\n", c.Target.Port))
	b.WriteString(fmt.Sprintf("  Database: %s\n", c.Target.Database))
	b.WriteString(fmt.Sprintf("  Schema: %s\n", c.Target.Schema))
	b.WriteString(fmt.Sprintf("  User: %s\n", c.Target.User))
	b.WriteString("  Password: [REDACTED]\n")
	if c.Target.Type == "mssql" {
		b.WriteString(fmt.Sprintf("  Encrypt: %s\n", c.Target.Encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Target.TrustServerCert))
	} else {
		b.WriteString(fmt.Sprintf("  SSLMode: %s\n", c.Target.SSLMode))
	}
	auth = c.Target.Auth
	if auth == "" {
		auth = "password"
	}
	b.WriteString(fmt.Sprintf("  Auth: %s\n", auth))
	if c.Target.Auth == "kerberos" {
		if c.Target.Krb5Conf != "" {
			b.WriteString(fmt.Sprintf("  Krb5Conf: %s\n", c.Target.Krb5Conf))
		}
		if c.Target.Realm != "" {
			b.WriteString(fmt.Sprintf("  Realm: %s\n", c.Target.Realm))
		}
	}

	// Migration Settings
	b.WriteString("\nMigration Settings:\n")

	// Workers
	workersExpl := fmt.Sprintf("%d cores - 2", ac.CPUCores)
	b.WriteString(fmt.Sprintf("  Workers: %s\n", formatAutoValue(c.Migration.Workers, ac.OriginalWorkers, workersExpl)))

	// ChunkSize
	chunkExpl := fmt.Sprintf("%d MB * 50 rows/MB", ac.TargetMemoryMB)
	b.WriteString(fmt.Sprintf("  ChunkSize: %s\n", formatAutoValue(c.Migration.ChunkSize, ac.OriginalChunkSize, chunkExpl)))

	// ReadAheadBuffers
	buffersExpl := fmt.Sprintf("memory/%d workers/chunk bytes", c.Migration.Workers)
	b.WriteString(fmt.Sprintf("  ReadAheadBuffers: %s\n", formatAutoValue(c.Migration.ReadAheadBuffers, ac.OriginalReadAheadBuffers, buffersExpl)))

	// MaxPartitions
	partitionsExpl := "matches workers"
	b.WriteString(fmt.Sprintf("  MaxPartitions: %s\n", formatAutoValue(c.Migration.MaxPartitions, ac.OriginalMaxPartitions, partitionsExpl)))

	// Connection pools
	mssqlConnsExpl := fmt.Sprintf("%d workers * %d readers + 4", c.Migration.Workers, c.Migration.ParallelReaders)
	b.WriteString(fmt.Sprintf("  MaxMssqlConnections: %s\n", formatAutoValue(c.Migration.MaxMssqlConnections, ac.OriginalMaxMssqlConns, mssqlConnsExpl)))

	pgConnsExpl := fmt.Sprintf("%d workers * %d writers + 4", c.Migration.Workers, c.Migration.WriteAheadWriters)
	b.WriteString(fmt.Sprintf("  MaxPgConnections: %s\n", formatAutoValue(c.Migration.MaxPgConnections, ac.OriginalMaxPgConns, pgConnsExpl)))

	// WriteAheadWriters
	b.WriteString(fmt.Sprintf("  WriteAheadWriters: %s\n", formatAutoValue(c.Migration.WriteAheadWriters, ac.OriginalWriteAheadWriters, "default 2")))

	// ParallelReaders
	b.WriteString(fmt.Sprintf("  ParallelReaders: %s\n", formatAutoValue(c.Migration.ParallelReaders, ac.OriginalParallelReaders, "default 2")))

	// LargeTableThreshold
	b.WriteString(fmt.Sprintf("  LargeTableThreshold: %s\n", formatAutoValue64(c.Migration.LargeTableThreshold, ac.OriginalLargeTableThresh, "default 5M")))

	// MSSQLRowsPerBatch
	batchExpl := "matches chunk_size"
	b.WriteString(fmt.Sprintf("  MSSQLRowsPerBatch: %s\n", formatAutoValue(c.Migration.MSSQLRowsPerBatch, ac.OriginalMSSQLRowsPerBatch, batchExpl)))

	// Other settings
	b.WriteString(fmt.Sprintf("  TargetMode: %s\n", c.Migration.TargetMode))
	b.WriteString(fmt.Sprintf("  StrictConsistency: %v\n", c.Migration.StrictConsistency))
	b.WriteString(fmt.Sprintf("  CreateIndexes: %v\n", c.Migration.CreateIndexes))
	b.WriteString(fmt.Sprintf("  CreateForeignKeys: %v\n", c.Migration.CreateForeignKeys))
	b.WriteString(fmt.Sprintf("  CreateCheckConstraints: %v\n", c.Migration.CreateCheckConstraints))
	b.WriteString(fmt.Sprintf("  SampleValidation: %v\n", c.Migration.SampleValidation))
	b.WriteString(fmt.Sprintf("  SampleSize: %s\n", formatAutoValue(c.Migration.SampleSize, ac.OriginalSampleSize, "default 100")))
	b.WriteString(fmt.Sprintf("  DataDir: %s\n", c.Migration.DataDir))

	// Table Filters
	b.WriteString("\nTable Filters:\n")
	if len(c.Migration.IncludeTables) > 0 {
		b.WriteString(fmt.Sprintf("  IncludeTables: %v\n", c.Migration.IncludeTables))
	} else {
		b.WriteString("  IncludeTables: [all]\n")
	}
	if len(c.Migration.ExcludeTables) > 0 {
		b.WriteString(fmt.Sprintf("  ExcludeTables: %v\n", c.Migration.ExcludeTables))
	} else {
		b.WriteString("  ExcludeTables: [none]\n")
	}

	// Memory Estimate
	b.WriteString("\nMemory Estimate:\n")
	bytesPerRow := int64(500) // average row size estimate
	bufferMemory := int64(c.Migration.Workers) * int64(c.Migration.ReadAheadBuffers) * int64(c.Migration.ChunkSize) * bytesPerRow
	b.WriteString(fmt.Sprintf("  Buffer Memory: ~%s (%d workers * %d buffers * %d rows * %d bytes/row)\n",
		formatMemorySize(bufferMemory),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.ChunkSize,
		bytesPerRow))

	// Profile (if set)
	if c.Profile.Name != "" || c.Profile.Description != "" {
		b.WriteString("\nProfile:\n")
		if c.Profile.Name != "" {
			b.WriteString(fmt.Sprintf("  Name: %s\n", c.Profile.Name))
		}
		if c.Profile.Description != "" {
			b.WriteString(fmt.Sprintf("  Description: %s\n", c.Profile.Description))
		}
	}

	// Slack Notifications
	b.WriteString("\nNotifications:\n")
	if c.Slack.Enabled {
		b.WriteString("  Slack: enabled\n")
		if c.Slack.Channel != "" {
			b.WriteString(fmt.Sprintf("  Channel: %s\n", c.Slack.Channel))
		}
		if c.Slack.Username != "" {
			b.WriteString(fmt.Sprintf("  Username: %s\n", c.Slack.Username))
		}
		b.WriteString("  WebhookURL: [REDACTED]\n")
	} else {
		b.WriteString("  Slack: disabled\n")
	}

	return b.String()
}
