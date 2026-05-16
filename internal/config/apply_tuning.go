package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"gopkg.in/yaml.v3"
)

type tuningFileSetting struct {
	key   string
	value int64
}

// ApplyTuningToConfigFile writes analyze-mode tuning recommendations into the
// analyzed config file's migration section. Non-migration sections are left
// byte-for-byte unchanged so secrets placeholders and unrelated config comments
// are not rewritten by yaml marshaling.
func ApplyTuningToConfigFile(path string, suggestions *driver.SmartConfigSuggestions) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("config path is required")
	}
	if suggestions == nil {
		return fmt.Errorf("tuning suggestions are required")
	}

	path = expandTilde(path)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading config file %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat config file %s: %w", path, err)
	}

	updated, err := applyTuningToConfigYAML(data, suggestions)
	if err != nil {
		return fmt.Errorf("updating config file %s: %w", path, err)
	}

	return atomicWriteFile(path, updated, info.Mode().Perm())
}

func applyTuningToConfigYAML(data []byte, suggestions *driver.SmartConfigSuggestions) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		doc.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
	}

	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("top-level YAML document must be a mapping")
	}

	migrationKey, migration := findMappingValue(root, "migration")
	if migration == nil {
		migrationKey = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "migration"}
		migration = &yaml.Node{Kind: yaml.MappingNode}
		root.Content = append(root.Content, migrationKey, migration)
	} else if migration.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("migration section must be a mapping")
	}

	for _, setting := range tuningFileSettings(suggestions) {
		setMigrationScalar(migration, setting.key, strconv.FormatInt(setting.value, 10))
	}

	renderedMigration, err := renderMigrationBlock(migration)
	if err != nil {
		return nil, err
	}

	if migrationKey.Line == 0 {
		return appendMigrationBlock(data, renderedMigration), nil
	}
	return replaceTopLevelBlock(data, migrationKey.Line, renderedMigration)
}

func tuningFileSettings(s *driver.SmartConfigSuggestions) []tuningFileSetting {
	return []tuningFileSetting{
		{key: "workers", value: int64(s.Workers)},
		{key: "max_source_connections", value: int64(s.MaxSourceConnections)},
		{key: "max_target_connections", value: int64(s.MaxTargetConnections)},
		{key: "max_memory_mb", value: s.EstimatedMemMB},
		{key: "chunk_size", value: int64(s.ChunkSizeRecommendation)},
		{key: "max_partitions", value: int64(s.MaxPartitions)},
		{key: "large_table_threshold", value: s.LargeTableThreshold},
		{key: "upsert_merge_chunk_size", value: int64(s.UpsertMergeChunkSize)},
		{key: "read_ahead_buffers", value: int64(s.ReadAheadBuffers)},
		{key: "write_ahead_writers", value: int64(s.WriteAheadWriters)},
		{key: "parallel_readers", value: int64(s.ParallelReaders)},
		{key: "checkpoint_frequency", value: int64(s.CheckpointFrequency)},
		{key: "max_retries", value: int64(s.MaxRetries)},
	}
}

func findMappingValue(mapping *yaml.Node, key string) (*yaml.Node, *yaml.Node) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i], mapping.Content[i+1]
		}
	}
	return nil, nil
}

func setMigrationScalar(mapping *yaml.Node, key, value string) {
	_, valueNode := findMappingValue(mapping, key)
	if valueNode != nil {
		if valueNode.Kind == yaml.ScalarNode && valueNode.Value != value {
			logging.Warn("analyze --apply overwriting migration.%s: %s -> %s", key, valueNode.Value, value)
		} else if valueNode.Kind != yaml.ScalarNode {
			logging.Warn("analyze --apply overwriting migration.%s with scalar value %s", key, value)
		}
		valueNode.Kind = yaml.ScalarNode
		valueNode.Tag = "!!int"
		valueNode.Value = value
		valueNode.Style = 0
		return
	}

	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: value},
	)
}

func renderMigrationBlock(migration *yaml.Node) ([]byte, error) {
	doc := &yaml.Node{
		Kind: yaml.DocumentNode,
		Content: []*yaml.Node{{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Tag: "!!str", Value: "migration"},
				migration,
			},
		}},
	}
	return yaml.Marshal(doc)
}

func replaceTopLevelBlock(data []byte, startLine int, replacement []byte) ([]byte, error) {
	if startLine <= 0 {
		return nil, fmt.Errorf("invalid migration section line %d", startLine)
	}

	lines := strings.SplitAfter(string(data), "\n")
	start := startLine - 1
	if start >= len(lines) {
		return nil, fmt.Errorf("migration section line %d is outside the file", startLine)
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

	var buf bytes.Buffer
	for _, line := range lines[:start] {
		buf.WriteString(line)
	}
	buf.Write(replacement)
	for _, line := range lines[end:] {
		buf.WriteString(line)
	}
	return buf.Bytes(), nil
}

func appendMigrationBlock(data, block []byte) []byte {
	var buf bytes.Buffer
	buf.Write(data)
	if len(data) > 0 && data[len(data)-1] != '\n' {
		buf.WriteByte('\n')
	}
	if len(data) > 0 {
		buf.WriteByte('\n')
	}
	buf.Write(block)
	return buf.Bytes()
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("setting temp file mode: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}
	return nil
}
