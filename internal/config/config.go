package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/dbconfig"
	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"gopkg.in/yaml.v3"

	// Import driver packages to trigger init() registration before validation
	_ "github.com/johndauphine/mssql-pg-migrate/internal/driver/mssql"
	_ "github.com/johndauphine/mssql-pg-migrate/internal/driver/postgres"
)

// Type aliases for database configuration types.
// These are defined in dbconfig package to break circular imports with driver package.
type SourceConfig = dbconfig.SourceConfig
type TargetConfig = dbconfig.TargetConfig

// canonicalDriverName returns the canonical driver name for a given type.
// For example, "pg" -> "postgres", "sqlserver" -> "mssql".
// Uses the driver registry for lookup.
func canonicalDriverName(dbType string) string {
	if d, err := driver.Get(dbType); err == nil {
		return d.Name()
	}
	return dbType
}

// isValidDriverType returns true if the type is a valid driver type or alias.
// Uses the driver registry for validation.
func isValidDriverType(dbType string) bool {
	return driver.IsRegistered(dbType)
}

// availableDriverTypes returns a list of supported driver types.
// Uses the driver registry to get available drivers.
func availableDriverTypes() []string {
	return driver.Available()
}

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

// Template patterns for secret expansion:
// - filePattern: ${file:/path/to/file} - any path characters allowed
// - envPattern: ${env:VAR_NAME} - valid env var names only [A-Za-z_][A-Za-z0-9_]*
// - legacyEnvPattern: ${VAR_NAME} - legacy shorthand for ${env:VAR_NAME}
var filePattern = regexp.MustCompile(`^\$\{file:(.+)\}$`)
var envPattern = regexp.MustCompile(`^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)
var legacyEnvPattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

// expandTemplateValue expands template patterns in a string value.
// Supported patterns:
//   - ${file:/path/to/file} - reads value from file (trimmed of whitespace)
//   - ${env:VAR_NAME} - reads value from environment variable (explicit)
//   - ${VAR_NAME} - reads value from environment variable (legacy shorthand)
//   - Any other value is returned as-is (cleartext password)
//
// Returns the expanded value and any error encountered.
func expandTemplateValue(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	// Check for ${file:...} pattern
	if matches := filePattern.FindStringSubmatch(value); matches != nil {
		filePath := expandTilde(matches[1])
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("reading secret from file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	// Check for ${env:VAR_NAME} pattern (explicit, restricted to valid env var names)
	if matches := envPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - allows optional env vars
		// but may cause silent auth failures if variable name is misspelled.
		return os.Getenv(matches[1]), nil
	}

	// Check for legacy ${VAR_NAME} pattern (shorthand for ${env:VAR_NAME})
	if matches := legacyEnvPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - matches ${env:VAR} behavior.
		// This allows optional env vars but may cause silent auth failures
		// if the variable name is misspelled. Use explicit ${env:VAR} for clarity.
		return os.Getenv(matches[1]), nil
	}

	// Not a template pattern - return as-is (cleartext)
	return value, nil
}

// AutoConfig tracks which values were auto-configured and why
type AutoConfig struct {
	// System resources detected
	AvailableMemoryMB    int64
	EffectiveMaxMemoryMB int64 // After applying user limit and 70% cap
	CPUCores             int

	// Original values (before auto-tuning)
	OriginalWorkers              int
	OriginalChunkSize            int
	OriginalReadAheadBuffers     int
	OriginalMaxPartitions        int
	OriginalMaxMssqlConns        int
	OriginalMaxPgConns           int
	OriginalWriteAheadWriters    int
	OriginalParallelReaders      int
	OriginalMSSQLRowsPerBatch    int
	OriginalLargeTableThresh     int64
	OriginalSampleSize           int
	OriginalUpsertMergeChunkSize int

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
	TargetMode             string   `yaml:"target_mode"`              // "drop_recreate" (default) or "upsert"
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
	UpsertMergeChunkSize   int      `yaml:"upsert_merge_chunk_size"`  // Chunk size for upsert UPDATE+INSERT (default=5000, auto-tuned)
	MaxMemoryMB            int64    `yaml:"max_memory_mb"`            // Max memory to use (default=70% of available, hard cap at 70%)
	// Restartability settings
	CheckpointFrequency  int `yaml:"checkpoint_frequency"`   // Save progress every N chunks (default=10)
	MaxRetries           int `yaml:"max_retries"`            // Retry failed tables N times (default=3)
	HistoryRetentionDays int `yaml:"history_retention_days"` // Keep run history for N days (default=30)
	// Date-based incremental sync (upsert mode only)
	DateUpdatedColumns []string `yaml:"date_updated_columns"` // Column names to check for last-modified date (tries each in order)
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

// expandYAMLTemplates expands ${file:path} and ${env:VAR} templates in YAML string.
// Also supports legacy ${VAR} syntax for backward compatibility.
// This runs before YAML parsing to allow templates in any field.
//
// Security note: File paths are not restricted - users should only use trusted paths
// like /run/secrets/ for Docker secrets. Avoid user-controlled paths.
func expandYAMLTemplates(yamlStr string) (string, error) {
	// Pattern to match ${file:path}, ${env:VAR}, or ${VAR} in YAML
	// - file: allows any path characters (user responsibility to use safe paths)
	// - env: restricted to valid env var names [A-Za-z_][A-Za-z0-9_]*
	// - legacy ${VAR}: also restricted to valid env var names
	pattern := regexp.MustCompile(`\$\{file:([^}]+)\}|\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

	var firstErr error
	result := pattern.ReplaceAllStringFunc(yamlStr, func(match string) string {
		// If we've already encountered an error, leave subsequent matches unchanged
		if firstErr != nil {
			return match
		}

		expanded, err := expandTemplateValue(match)
		if err != nil {
			firstErr = err
			return match // Keep original on error
		}
		return expanded
	})

	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}

// LoadBytes reads configuration from YAML bytes.
func LoadBytes(data []byte) (*Config, error) {
	// Expand templates (${file:path}, ${env:VAR}, and legacy ${VAR} syntax)
	expanded, err := expandYAMLTemplates(string(data))
	if err != nil {
		return nil, fmt.Errorf("expanding templates: %w", err)
	}

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
	c.autoConfig.OriginalUpsertMergeChunkSize = c.Migration.UpsertMergeChunkSize

	// Detect system resources (only if not already set, for testing)
	if c.autoConfig.CPUCores == 0 {
		c.autoConfig.CPUCores = runtime.NumCPU()
	}
	c.autoConfig.AvailableMemoryMB = getAvailableMemoryMB()

	// Calculate target memory for auto-tuning (50% of limit)
	// If user specified max_memory_mb, use that as the base
	// Otherwise use available memory
	baseMemoryMB := c.autoConfig.AvailableMemoryMB
	if c.Migration.MaxMemoryMB > 0 && c.Migration.MaxMemoryMB < baseMemoryMB {
		baseMemoryMB = c.Migration.MaxMemoryMB
	}
	c.autoConfig.TargetMemoryMB = baseMemoryMB / 2

	// Source defaults - use driver registry for pluggable defaults
	if c.Source.Type == "" {
		c.Source.Type = "mssql" // Default source is SQL Server for backward compat
	}
	if sourceDriver, err := driver.Get(c.Source.Type); err == nil {
		defaults := sourceDriver.Defaults()
		if c.Source.Port == 0 && defaults.Port > 0 {
			c.Source.Port = defaults.Port
		}
		if c.Source.Schema == "" && defaults.Schema != "" {
			c.Source.Schema = defaults.Schema
		}
		if c.Source.SSLMode == "" && defaults.SSLMode != "" {
			c.Source.SSLMode = defaults.SSLMode
		}
		if c.Source.Encrypt == nil {
			c.Source.Encrypt = &defaults.Encrypt
		}
		if c.Source.PacketSize == 0 && defaults.PacketSize > 0 {
			c.Source.PacketSize = defaults.PacketSize
		}
	}

	// Target defaults - use driver registry for pluggable defaults
	if c.Target.Type == "" {
		c.Target.Type = "postgres" // Default target is PostgreSQL for backward compat
	}
	if targetDriver, err := driver.Get(c.Target.Type); err == nil {
		defaults := targetDriver.Defaults()
		if c.Target.Port == 0 && defaults.Port > 0 {
			c.Target.Port = defaults.Port
		}
		if c.Target.Schema == "" && defaults.Schema != "" {
			c.Target.Schema = defaults.Schema
		}
		if c.Target.SSLMode == "" && defaults.SSLMode != "" {
			c.Target.SSLMode = defaults.SSLMode
		}
		if c.Target.Encrypt == nil {
			c.Target.Encrypt = &defaults.Encrypt
		}
		if c.Target.PacketSize == 0 && defaults.PacketSize > 0 {
			c.Target.PacketSize = defaults.PacketSize
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
	// Auto-detect CPU cores for workers
	// Formula: (cores - 2), clamped to 4-12 for optimal performance
	// This aligns with Rust implementation for consistent behavior
	if c.Migration.Workers == 0 {
		cores := runtime.NumCPU()
		c.Migration.Workers = cores - 2
		if c.Migration.Workers < 4 {
			c.Migration.Workers = 4
		}
		if c.Migration.Workers > 12 {
			c.Migration.Workers = 12 // Cap at 12 workers (diminishing returns beyond)
		}
	}
	if c.Migration.MaxPartitions == 0 {
		c.Migration.MaxPartitions = c.Migration.Workers // Match workers
	}
	// Auto-tune chunk size based on available RAM
	// Formula: 75K base + 25K per 8GB RAM, clamped to 50K-200K
	// This matches the Rust implementation for consistent behavior
	targetMemoryMB := c.autoConfig.TargetMemoryMB
	if c.Migration.ChunkSize == 0 {
		ramGB := float64(c.autoConfig.AvailableMemoryMB) / 1024.0
		chunkSize := 75000 + int(ramGB*25000.0/8.0)
		if chunkSize < 50000 {
			chunkSize = 50000
		}
		if chunkSize > 200000 {
			chunkSize = 200000
		}
		c.Migration.ChunkSize = chunkSize
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
	// Auto-tune parallel writers based on target driver defaults
	if c.Migration.WriteAheadWriters == 0 {
		if targetDriver, err := driver.Get(c.Target.Type); err == nil {
			defaults := targetDriver.Defaults()
			if defaults.ScaleWritersWithCores {
				// Scale with CPU cores (e.g., PostgreSQL COPY handles parallelism well)
				cores := c.autoConfig.CPUCores
				writers := cores / 4
				if writers < defaults.WriteAheadWriters {
					writers = defaults.WriteAheadWriters
				}
				if writers > 4 {
					writers = 4
				}
				c.Migration.WriteAheadWriters = writers
			} else {
				// Use fixed value (e.g., MSSQL TABLOCK serializes writes)
				c.Migration.WriteAheadWriters = defaults.WriteAheadWriters
			}
		} else {
			// Fallback for unknown drivers - log warning as this may indicate a config issue
			logging.Warn("Unknown target driver type '%s', using fallback WriteAheadWriters=2", c.Target.Type)
			c.Migration.WriteAheadWriters = 2
		}
	}
	// Auto-tune parallel readers based on CPU cores
	// Conservative defaults to avoid overwhelming source database
	if c.Migration.ParallelReaders == 0 {
		cores := c.autoConfig.CPUCores
		readers := cores / 4
		if readers < 2 {
			readers = 2
		}
		if readers > 4 {
			readers = 4
		}
		c.Migration.ParallelReaders = readers
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

	// Calculate effective memory limit for Go GC soft limit
	// Hard cap at 70% of available memory to leave room for OS and other processes
	hardCapMB := c.autoConfig.AvailableMemoryMB * 70 / 100
	effectiveMaxMB := hardCapMB
	if c.Migration.MaxMemoryMB > 0 {
		// User specified a limit - use it, but enforce hard cap
		if c.Migration.MaxMemoryMB < hardCapMB {
			effectiveMaxMB = c.Migration.MaxMemoryMB
		}
	}
	c.autoConfig.EffectiveMaxMemoryMB = effectiveMaxMB

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

	// Auto-tune UpsertMergeChunkSize for upsert mode
	// This controls UPDATE+INSERT chunk size for MSSQL target
	// Smaller chunks reduce SQL Server memory pressure during merge operations
	if c.Migration.UpsertMergeChunkSize == 0 {
		if c.Migration.TargetMode == "upsert" {
			// For upsert mode, use smaller chunks based on available memory
			// Base: 5000 rows, scale up with memory (max 20000)
			memoryFactor := targetMemoryMB / 1024 // Scale factor per GB
			if memoryFactor < 1 {
				memoryFactor = 1
			}
			c.Migration.UpsertMergeChunkSize = int(5000 * memoryFactor)
			if c.Migration.UpsertMergeChunkSize > 20000 {
				c.Migration.UpsertMergeChunkSize = 20000
			}
			if c.Migration.UpsertMergeChunkSize < 2000 {
				c.Migration.UpsertMergeChunkSize = 2000
			}
		} else {
			// Not in upsert mode - set a sensible default anyway
			c.Migration.UpsertMergeChunkSize = 10000
		}
	}

	// Restartability defaults
	if c.Migration.CheckpointFrequency == 0 {
		c.Migration.CheckpointFrequency = 10 // Save progress every 10 chunks
	}
	if c.Migration.MaxRetries == 0 {
		c.Migration.MaxRetries = 3 // Retry failed tables 3 times
	}
	if c.Migration.HistoryRetentionDays == 0 {
		c.Migration.HistoryRetentionDays = 30 // Keep run history for 30 days
	}
}

// TableRowSize holds row size info for a table
type TableRowSize struct {
	Name             string
	RowCount         int64
	EstimatedRowSize int64
}

// RefineSettingsForRowSizes reports actual table row sizes for informational purposes.
// Previously this function would reduce chunk_size/workers based on memory estimates,
// but this caused performance regressions. The Go GC soft limit handles memory pressure.
// Returns false (no adjustments made) and a description of row sizes.
func (c *Config) RefineSettingsForRowSizes(tables []TableRowSize) (adjusted bool, changes string) {
	if len(tables) == 0 {
		return false, ""
	}

	// Calculate weighted average row size based on row counts (for informational purposes)
	var totalRows int64
	var weightedSum int64
	var maxRowSize int64
	var maxRowSizeTable string

	for _, t := range tables {
		if t.EstimatedRowSize > 0 {
			totalRows += t.RowCount
			weightedSum += t.RowCount * t.EstimatedRowSize
			if t.EstimatedRowSize > maxRowSize {
				maxRowSize = t.EstimatedRowSize
				maxRowSizeTable = t.Name
			}
		}
	}

	if totalRows == 0 || weightedSum == 0 {
		return false, ""
	}

	weightedAvgRowSize := weightedSum / totalRows

	// No adjustments made - just report row sizes for visibility
	return false, fmt.Sprintf("Row sizes: weighted avg %s, max %s in %s",
		FormatMemorySize(weightedAvgRowSize), FormatMemorySize(maxRowSize), maxRowSizeTable)
}

func (c *Config) validate() error {
	// Validate source
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if !isValidDriverType(c.Source.Type) {
		return fmt.Errorf("source.type '%s' is not a valid driver type (supported: %v)", c.Source.Type, availableDriverTypes())
	}

	// Validate target
	if c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if !isValidDriverType(c.Target.Type) {
		return fmt.Errorf("target.type '%s' is not a valid driver type (supported: %v)", c.Target.Type, availableDriverTypes())
	}

	// Same-engine migration validation: prevent migration to the exact same database
	// Compare canonical driver names to handle aliases (e.g., "mssql" == "sqlserver")
	if canonicalDriverName(c.Source.Type) == canonicalDriverName(c.Target.Type) {
		// Use case-insensitive comparison for hostnames (RFC 1035)
		sameHost := strings.EqualFold(c.Source.Host, c.Target.Host)
		samePort := c.Source.Port == c.Target.Port
		sameDB := c.Source.Database == c.Target.Database
		if sameHost && samePort && sameDB {
			return fmt.Errorf("source and target cannot be the same database (%s:%d/%s)",
				c.Source.Host, c.Source.Port, c.Source.Database)
		}
	}

	// Validate migration settings
	if c.Migration.TargetMode != "drop_recreate" && c.Migration.TargetMode != "upsert" {
		return fmt.Errorf("migration.target_mode must be 'drop_recreate' or 'upsert'")
	}
	return nil
}

// SourceDSN returns the source database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) SourceDSN() string {
	// Use driver registry to get canonical name (e.g., "pg" -> "postgres")
	driverName := canonicalDriverName(c.Source.Type)
	switch driverName {
	case "postgres":
		return c.buildPostgresDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, c.Source.SSLMode,
			c.Source.Auth, c.Source.GSSEncMode)
	case "mssql":
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		return c.buildMSSQLDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, encrypt, c.Source.TrustServerCert,
			c.Source.PacketSize, c.Source.Auth, c.Source.Krb5Conf, c.Source.Keytab, c.Source.Realm, c.Source.SPN)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// TargetDSN returns the target database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) TargetDSN() string {
	// Use driver registry to get canonical name (e.g., "sqlserver" -> "mssql")
	driverName := canonicalDriverName(c.Target.Type)
	switch driverName {
	case "mssql":
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		return c.buildMSSQLDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, encrypt, c.Target.TrustServerCert,
			c.Target.PacketSize, c.Target.Auth, c.Target.Krb5Conf, c.Target.Keytab, c.Target.Realm, c.Target.SPN)
	case "postgres":
		return c.buildPostgresDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, c.Target.SSLMode,
			c.Target.Auth, c.Target.GSSEncMode)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// buildMSSQLDSN builds an MSSQL connection string with optional Kerberos auth
func (c *Config) buildMSSQLDSN(host string, port int, database, user, password string, encrypt bool,
	trustServerCert bool, packetSize int, auth, krb5Conf, keytab, realm, spn string) string {

	encryptStr := "false"
	if encrypt {
		encryptStr = "true"
	}
	trustCert := "false"
	if trustServerCert {
		trustCert = "true"
	}

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	encodedDB := url.QueryEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos authentication
	if auth == "kerberos" {
		dsn := fmt.Sprintf("sqlserver://%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s&authenticator=krb5",
			host, port, encodedDB, encryptStr, trustCert)

		// Add packet size for better throughput (default 4KB is too small)
		if packetSize > 0 {
			dsn += fmt.Sprintf("&packet+size=%d", packetSize)
		}

		// Optional Kerberos parameters
		if krb5Conf != "" {
			dsn += "&krb5-configfile=" + url.QueryEscape(krb5Conf)
		}
		if keytab != "" {
			dsn += "&krb5-keytabfile=" + url.QueryEscape(keytab)
		}
		if realm != "" {
			dsn += "&krb5-realm=" + url.QueryEscape(realm)
		}
		if spn != "" {
			dsn += "&ServerSPN=" + url.QueryEscape(spn)
		}
		// If user specified, use it as the principal
		if user != "" {
			dsn += "&krb5-username=" + url.QueryEscape(user)
		}
		return dsn
	}

	// Password authentication (default)
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		encodedUser, encodedPass, host, port, encodedDB, encryptStr, trustCert)

	// Add packet size for better throughput (default 4KB is too small)
	if packetSize > 0 {
		dsn += fmt.Sprintf("&packet+size=%d", packetSize)
	}

	return dsn
}

// buildPostgresDSN builds a PostgreSQL connection string with optional Kerberos auth
func (c *Config) buildPostgresDSN(host string, port int, database, user, password, sslMode,
	auth, gssEncMode string) string {

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	// Use PathEscape for database since it's in the URL path
	encodedDB := url.PathEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos/GSSAPI authentication
	if auth == "kerberos" {
		gssEnc := "prefer"
		if gssEncMode != "" {
			gssEnc = gssEncMode
		}
		// For Kerberos, we don't include password in the DSN
		if user != "" {
			return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&gssencmode=%s",
				encodedUser, host, port, encodedDB, sslMode, gssEnc)
		}
		return fmt.Sprintf("postgres://%s:%d/%s?sslmode=%s&gssencmode=%s",
			host, port, encodedDB, sslMode, gssEnc)
	}

	// Password authentication (default)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		encodedUser, encodedPass, host, port, encodedDB, sslMode)
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

// EstimateMemoryUsage calculates expected memory usage given actual row sizes.
// avgRowSize should be the weighted average row size across all tables.
// Returns estimated bytes.
func (c *Config) EstimateMemoryUsage(avgRowSize int64) int64 {
	if avgRowSize <= 0 {
		avgRowSize = 500
	}
	// Formula: workers * (readers * buffers + writers * buffers) * chunk_size * avg_row_size
	// Simplified: workers * total_buffers * chunk_size * avg_row_size
	// Each worker has read-ahead buffers + pending writes
	totalBuffers := int64(c.Migration.ReadAheadBuffers) * 2 // read + write queues
	return int64(c.Migration.Workers) * totalBuffers * int64(c.Migration.ChunkSize) * avgRowSize
}

// FormatMemoryEstimate returns a human-readable memory estimate string.
func (c *Config) FormatMemoryEstimate(avgRowSize int64) string {
	mem := c.EstimateMemoryUsage(avgRowSize)
	return fmt.Sprintf("~%s (%d workers * %d buffers * %d chunk * %d bytes/row)",
		formatMemorySize(mem),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers*2,
		c.Migration.ChunkSize,
		avgRowSize)
}

// FormatMemorySize exports the formatMemorySize function for use by other packages.
func FormatMemorySize(bytes int64) string {
	return formatMemorySize(bytes)
}

// AutoConfig returns the auto-configuration metadata.
func (c *Config) AutoConfig() AutoConfig {
	return c.autoConfig
}

// DebugDump returns a comprehensive configuration dump with auto-tuning explanations
func (c *Config) DebugDump() string {
	var b strings.Builder
	ac := c.autoConfig

	b.WriteString("\n=== Configuration ===\n\n")

	// System Resources
	b.WriteString("System Resources:\n")
	b.WriteString(fmt.Sprintf("  Available Memory: %d MB\n", ac.AvailableMemoryMB))
	if c.Migration.MaxMemoryMB > 0 {
		b.WriteString(fmt.Sprintf("  Max Memory Limit: %d MB (user configured)\n", c.Migration.MaxMemoryMB))
	}
	b.WriteString(fmt.Sprintf("  Effective Max Memory: %d MB (hard cap 70%%)\n", ac.EffectiveMaxMemoryMB))
	b.WriteString(fmt.Sprintf("  CPU Cores: %d\n", ac.CPUCores))

	// Source Database
	b.WriteString(fmt.Sprintf("\nSource (%s):\n", c.Source.Type))
	b.WriteString(fmt.Sprintf("  Host: %s\n", c.Source.Host))
	b.WriteString(fmt.Sprintf("  Port: %d\n", c.Source.Port))
	b.WriteString(fmt.Sprintf("  Database: %s\n", c.Source.Database))
	b.WriteString(fmt.Sprintf("  Schema: %s\n", c.Source.Schema))
	b.WriteString(fmt.Sprintf("  User: %s\n", c.Source.User))
	b.WriteString("  Password: [REDACTED]\n")
	if canonicalDriverName(c.Source.Type) == "mssql" {
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		b.WriteString(fmt.Sprintf("  Encrypt: %v\n", encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Source.TrustServerCert))
		b.WriteString(fmt.Sprintf("  PacketSize: %d\n", c.Source.PacketSize))
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
	if canonicalDriverName(c.Target.Type) == "mssql" {
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		b.WriteString(fmt.Sprintf("  Encrypt: %v\n", encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Target.TrustServerCert))
		b.WriteString(fmt.Sprintf("  PacketSize: %d\n", c.Target.PacketSize))
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
	workersExpl := fmt.Sprintf("(cores-2) clamped 4-12, %d cores", ac.CPUCores)
	b.WriteString(fmt.Sprintf("  Workers: %s\n", formatAutoValue(c.Migration.Workers, ac.OriginalWorkers, workersExpl)))

	// ChunkSize
	ramGB := float64(ac.AvailableMemoryMB) / 1024.0
	chunkExpl := fmt.Sprintf("75K + %.1fGB*3.1K", ramGB)
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

	// WriteAheadWriters - use driver defaults for explanation
	var writersExpl string
	if targetDriver, err := driver.Get(c.Target.Type); err == nil {
		defaults := targetDriver.Defaults()
		if defaults.ScaleWritersWithCores {
			writersExpl = fmt.Sprintf("driver default scaled with cores (%d cores)", ac.CPUCores)
		} else {
			writersExpl = fmt.Sprintf("driver default fixed at %d", defaults.WriteAheadWriters)
		}
	} else {
		writersExpl = "fallback default"
	}
	b.WriteString(fmt.Sprintf("  WriteAheadWriters: %s\n", formatAutoValue(c.Migration.WriteAheadWriters, ac.OriginalWriteAheadWriters, writersExpl)))

	// ParallelReaders
	readersExpl := fmt.Sprintf("cores/4 clamped 2-4, %d cores", ac.CPUCores)
	b.WriteString(fmt.Sprintf("  ParallelReaders: %s\n", formatAutoValue(c.Migration.ParallelReaders, ac.OriginalParallelReaders, readersExpl)))

	// LargeTableThreshold
	b.WriteString(fmt.Sprintf("  LargeTableThreshold: %s\n", formatAutoValue64(c.Migration.LargeTableThreshold, ac.OriginalLargeTableThresh, "default 5M")))

	// MSSQLRowsPerBatch
	batchExpl := "matches chunk_size"
	b.WriteString(fmt.Sprintf("  MSSQLRowsPerBatch: %s\n", formatAutoValue(c.Migration.MSSQLRowsPerBatch, ac.OriginalMSSQLRowsPerBatch, batchExpl)))

	// Other settings
	b.WriteString(fmt.Sprintf("  TargetMode: %s\n", c.Migration.TargetMode))

	// UpsertMergeChunkSize - only show in upsert mode
	if c.Migration.TargetMode == "upsert" {
		upsertExpl := "auto: memory-scaled 5K-20K"
		b.WriteString(fmt.Sprintf("  UpsertMergeChunkSize: %s\n", formatAutoValue(c.Migration.UpsertMergeChunkSize, ac.OriginalUpsertMergeChunkSize, upsertExpl)))
		// DateUpdatedColumns - only show in upsert mode if configured
		if len(c.Migration.DateUpdatedColumns) > 0 {
			b.WriteString(fmt.Sprintf("  DateUpdatedColumns: %v\n", c.Migration.DateUpdatedColumns))
		}
	}
	b.WriteString(fmt.Sprintf("  StrictConsistency: %v\n", c.Migration.StrictConsistency))
	b.WriteString(fmt.Sprintf("  CreateIndexes: %v\n", c.Migration.CreateIndexes))
	b.WriteString(fmt.Sprintf("  CreateForeignKeys: %v\n", c.Migration.CreateForeignKeys))
	b.WriteString(fmt.Sprintf("  CreateCheckConstraints: %v\n", c.Migration.CreateCheckConstraints))
	b.WriteString(fmt.Sprintf("  SampleValidation: %v\n", c.Migration.SampleValidation))
	b.WriteString(fmt.Sprintf("  SampleSize: %s\n", formatAutoValue(c.Migration.SampleSize, ac.OriginalSampleSize, "default 100")))
	b.WriteString(fmt.Sprintf("  DataDir: %s\n", c.Migration.DataDir))

	// Restartability Settings
	b.WriteString("\nRestartability:\n")
	b.WriteString(fmt.Sprintf("  CheckpointFrequency: %d chunks\n", c.Migration.CheckpointFrequency))
	b.WriteString(fmt.Sprintf("  MaxRetries: %d\n", c.Migration.MaxRetries))
	b.WriteString(fmt.Sprintf("  HistoryRetentionDays: %d\n", c.Migration.HistoryRetentionDays))

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

	// Memory Estimate (conservative estimate, actual may vary based on row content)
	b.WriteString("\nMemory Estimate:\n")
	bytesPerRow := int64(500) // conservative default - actual sizes queried during schema extraction
	bufferMemory := int64(c.Migration.Workers) * int64(c.Migration.ReadAheadBuffers) * int64(c.Migration.ChunkSize) * bytesPerRow
	b.WriteString(fmt.Sprintf("  Buffer Memory: ~%s (%d workers * %d buffers * %d rows * %d bytes/row)\n",
		formatMemorySize(bufferMemory),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.ChunkSize,
		bytesPerRow))
	b.WriteString("  Note: Actual memory depends on row sizes. Tables with large text columns use more.\n")

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
