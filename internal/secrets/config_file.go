package secrets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	globalConfig *Config
	configOnce   sync.Once
	configErr    error
)

// Load loads the secrets configuration from the default or override location.
// It caches the result and returns the same config on subsequent calls.
func Load() (*Config, error) {
	configOnce.Do(func() {
		globalConfig, configErr = loadFromFile()
	})
	return globalConfig, configErr
}

// Reset clears the cached config (useful for testing)
func Reset() {
	configOnce = sync.Once{}
	globalConfig = nil
	configErr = nil
}

// GetSecretsPath returns the path to the secrets file
func GetSecretsPath() string {
	if envPath := os.Getenv(SecretsFileEnvVar); envPath != "" {
		return envPath
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", DefaultSecretsDir, DefaultSecretsFile)
	}
	return filepath.Join(homeDir, DefaultSecretsDir, DefaultSecretsFile)
}

// EnsureSecretsDir creates the secrets directory with secure permissions if it doesn't exist
func EnsureSecretsDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %w", err)
	}

	secretsDir := filepath.Join(homeDir, DefaultSecretsDir)

	// Check if directory exists
	info, err := os.Stat(secretsDir)
	if os.IsNotExist(err) {
		// Create directory with secure permissions
		if err := os.MkdirAll(secretsDir, SecureDirMode); err != nil {
			return "", fmt.Errorf("creating secrets directory: %w", err)
		}
		return secretsDir, nil
	} else if err != nil {
		return "", fmt.Errorf("checking secrets directory: %w", err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%s exists but is not a directory", secretsDir)
	}

	return secretsDir, nil
}

func loadFromFile() (*Config, error) {
	path := GetSecretsPath()

	// Check file permissions before reading - reject if too permissive.
	if err := ValidateFilePermissions(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &SecretsNotFoundError{Path: path}
		}
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &SecretsNotFoundError{Path: path}
		}
		return nil, fmt.Errorf("reading secrets file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing secrets file: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

// Save writes the config to the secrets file, preserving existing fields.
// It loads the current file first to preserve any fields not in the new config.
func Save(updates *Config) error {
	path := GetSecretsPath()

	// Load existing config if it exists
	existing := &Config{}
	data, err := os.ReadFile(path)
	if err == nil {
		// Surface parse errors instead of swallowing them. A malformed
		// existing file would otherwise leave `existing` as the zero
		// Config, and the merge+write below would clobber the encryption
		// master key, other AI providers, the Slack webhook, and
		// migration defaults with just the new section — silently
		// destroying every AES-GCM-encrypted profile. Mirror
		// SaveSlackWebhook: refuse to write, caller hand-fixes the YAML.
		if err := yaml.Unmarshal(data, existing); err != nil {
			return fmt.Errorf("parsing existing secrets file (refusing to overwrite): %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading existing secrets file: %w", err)
	}

	// Merge updates into existing config
	mergeConfig(existing, updates)

	// Marshal the merged config
	newData, err := yaml.Marshal(existing)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	// Ensure the directory exists
	if _, err := EnsureSecretsDir(); err != nil {
		return err
	}

	// Write atomically (temp file + rename) so a crash mid-write can't
	// truncate the secrets file and lose the master key.
	if err := atomicWriteFile(path, newData, SecureFileMode); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	// Reset the cached config so next Load() picks up changes
	Reset()

	return nil
}

// SaveSlackWebhook sets the Slack webhook URL in the secrets file,
// preserving all other sections (AI, Encryption, MigrationDefaults).
// Unlike Save's merge semantics, an empty url explicitly clears the
// existing value — the setup wizard needs this so users can disable
// notifications by clearing the webhook in the prompt.
func SaveSlackWebhook(url string) error {
	path := GetSecretsPath()

	existing := &Config{}
	data, err := os.ReadFile(path)
	if err == nil {
		// Surface parse errors instead of swallowing them — a malformed
		// existing file would otherwise leave `existing` as the zero
		// Config and the subsequent write would clobber AI keys,
		// encryption key, and migration defaults with just the Slack
		// section. Caller should hand-fix the YAML.
		if err := yaml.Unmarshal(data, existing); err != nil {
			return fmt.Errorf("parsing existing secrets file (refusing to overwrite): %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading existing secrets file: %w", err)
	}

	existing.Notifications.Slack.WebhookURL = url

	newData, err := yaml.Marshal(existing)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if _, err := EnsureSecretsDir(); err != nil {
		return err
	}

	if err := atomicWriteFile(path, newData, SecureFileMode); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	Reset()
	return nil
}

// atomicWriteFile writes data to path atomically: it stages the content
// in a temp file in the same directory, fsyncs it, renames it into place,
// then fsyncs the parent directory so the rename survives power loss. A
// crash at any point leaves the original secrets file intact rather than
// truncated — losing the encryption master key is unrecoverable, so the
// write must be all-or-nothing. The temp is created in the same directory
// as the target so the rename stays within one filesystem (cross-fs
// rename is not atomic).
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, SecureDirMode); err != nil {
		return fmt.Errorf("creating secrets dir: %w", err)
	}

	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp.")
	if err != nil {
		return fmt.Errorf("creating temp secrets file: %w", err)
	}
	tmpName := f.Name()
	// No-op on the happy path (rename removes tmpName); cleans up the
	// partial file on every error path so we don't leak temps.
	defer os.Remove(tmpName)

	// Nail down the mode so the temp never widens permissions during the
	// brief window before the rename.
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return fmt.Errorf("chmod temp secrets file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("writing temp secrets file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp secrets file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp secrets file: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming secrets file into place: %w", err)
	}

	// fsync the parent dir on POSIX so the rename itself is durable.
	// Skipped on Windows, where Sync on a directory handle is unsupported
	// and would fail every save (see checkpoint.atomicWriteFile).
	if runtime.GOOS != "windows" {
		d, err := os.Open(dir)
		if err != nil {
			return fmt.Errorf("opening secrets dir for fsync: %w", err)
		}
		syncErr := d.Sync()
		_ = d.Close()
		if syncErr != nil {
			return fmt.Errorf("fsync secrets dir: %w", syncErr)
		}
	}
	return nil
}

// mergeConfig merges updates into existing config, only overwriting non-zero values.
func mergeConfig(existing, updates *Config) {
	mergeAIConfig(&existing.AI, &updates.AI)
	mergeMigrationDefaults(&existing.MigrationDefaults, &updates.MigrationDefaults)
}

// mergeAIConfig merges AI configuration updates into existing config.
func mergeAIConfig(existing, updates *AIConfig) {
	if updates.DefaultProvider != "" {
		existing.DefaultProvider = updates.DefaultProvider
	}
	if len(updates.Providers) > 0 {
		if existing.Providers == nil {
			existing.Providers = make(map[string]*Provider)
		}
		for name, provider := range updates.Providers {
			existing.Providers[name] = provider
		}
	}
}

// mergeMigrationDefaults merges non-zero migration defaults.
func mergeMigrationDefaults(existing, updates *MigrationDefaults) {
	if updates.Workers > 0 {
		existing.Workers = updates.Workers
	}
	if updates.MaxSourceConnections > 0 {
		existing.MaxSourceConnections = updates.MaxSourceConnections
	}
	if updates.MaxTargetConnections > 0 {
		existing.MaxTargetConnections = updates.MaxTargetConnections
	}
	if updates.MaxMemoryMB > 0 {
		existing.MaxMemoryMB = updates.MaxMemoryMB
	}
	if updates.ReadAheadBuffers > 0 {
		existing.ReadAheadBuffers = updates.ReadAheadBuffers
	}
	if updates.WriteAheadWriters > 0 {
		existing.WriteAheadWriters = updates.WriteAheadWriters
	}
	if updates.ParallelReaders > 0 {
		existing.ParallelReaders = updates.ParallelReaders
	}
	if updates.CheckpointFrequency > 0 {
		existing.CheckpointFrequency = updates.CheckpointFrequency
	}
	if updates.MaxRetries > 0 {
		existing.MaxRetries = updates.MaxRetries
	}
	if updates.SampleSize > 0 {
		existing.SampleSize = updates.SampleSize
	}
	if updates.HistoryRetentionDays > 0 {
		existing.HistoryRetentionDays = updates.HistoryRetentionDays
	}
	if updates.DataDir != "" {
		existing.DataDir = updates.DataDir
	}
	if updates.AIAdjustInterval != "" {
		existing.AIAdjustInterval = updates.AIAdjustInterval
	}
	if updates.RuntimeTuningInterval != "" {
		existing.RuntimeTuningInterval = updates.RuntimeTuningInterval
	}
	// Boolean pointers - only update if explicitly set
	if updates.CreateIndexes != nil {
		existing.CreateIndexes = updates.CreateIndexes
	}
	if updates.CreateForeignKeys != nil {
		existing.CreateForeignKeys = updates.CreateForeignKeys
	}
	if updates.CreateCheckConstraints != nil {
		existing.CreateCheckConstraints = updates.CreateCheckConstraints
	}
	if updates.StrictConsistency != nil {
		existing.StrictConsistency = updates.StrictConsistency
	}
	if updates.SampleValidation != nil {
		existing.SampleValidation = updates.SampleValidation
	}
	if updates.AIAdjust != nil {
		existing.AIAdjust = updates.AIAdjust
	}
	if updates.RuntimeTuning != nil {
		existing.RuntimeTuning = updates.RuntimeTuning
	}
}
