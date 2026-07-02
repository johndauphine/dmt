package driver

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

// CacheSource is the provenance tag persisted with every cache entry
// (#177). Used by the on-disk format and by `dmt cache clear --ai-only`
// to invalidate AI entries on model upgrade without touching anything
// else. The read path defensively bypasses entries tagged
// SourceDeterministic — the deterministic mapper is the source of truth
// for those mappings and nothing should be persisting them, but if a
// legacy run did, ignore them rather than honoring stale state.
type CacheSource string

const (
	// SourceAI marks entries produced by an AI fallback. Today the only
	// writer; the only kind that benefits from caching (round-trip cost).
	SourceAI CacheSource = "ai"

	// SourceDeterministic is reserved for entries claiming to come from
	// the deterministic mapper. Nothing currently writes this; the read
	// path skips it as defense-in-depth.
	SourceDeterministic CacheSource = "deterministic"
)

// cacheFileFormatVersion is the version stamp on the on-disk cache
// payload (#177). Bump when changing the structured envelope; the load
// path always tries v2+ first and falls back to the unversioned legacy
// flat map.
const cacheFileFormatVersion = 2

// CacheEntry is one stored type-mapping with provenance metadata (#177).
// Result is the cached mapping string (varchar(255), full CREATE TABLE,
// etc.). Source / Model / CachedAt are best-effort and may be empty on
// legacy entries migrated from the pre-#177 flat-map format.
type CacheEntry struct {
	Result     string      `json:"result"`
	Source     CacheSource `json:"source,omitempty"`
	Provider   string      `json:"provider,omitempty"`
	Model      string      `json:"model,omitempty"`
	SchemaHash string      `json:"schema_hash,omitempty"`
	CachedAt   time.Time   `json:"cached_at,omitempty"`
}

// TypeMappingCache stores type mappings keyed by stable cache keys
// (see cacheKey). Pre-#177 this held a `map[string]string`; #177
// switched the storage to provenance-tagged CacheEntry while keeping
// the Get/Set/All/Load surface stable so existing call sites compile
// unchanged. New AI write sites should use SetAI to record full
// provenance.
type TypeMappingCache struct {
	mu       sync.RWMutex
	mappings map[string]CacheEntry
}

// NewTypeMappingCache creates a new empty cache.
func NewTypeMappingCache() *TypeMappingCache {
	return &TypeMappingCache{
		mappings: make(map[string]CacheEntry),
	}
}

// Get retrieves a cached mapping. Entries tagged SourceDeterministic
// are bypassed defensively (see CacheSource doc); callers see them as
// cache misses and fall through to the live deterministic mapper.
func (c *TypeMappingCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.mappings[key]
	if !ok {
		return "", false
	}
	if e.Source == SourceDeterministic {
		return "", false
	}
	return e.Result, true
}

// GetAI retrieves an AI cache entry only when provider/model metadata are
// compatible with the current mapper. Legacy entries without metadata remain
// readable, but newly written entries do not cross provider/model boundaries.
func (c *TypeMappingCache) GetAI(key, provider, model string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.mappings[key]
	if !ok || e.Source == SourceDeterministic {
		return "", false
	}
	if e.Provider != "" && e.Provider != provider {
		return "", false
	}
	if e.Model != "" && e.Model != model {
		return "", false
	}
	return e.Result, true
}

// Set stores a mapping without provenance metadata. Kept for backward
// compatibility with tests and pre-#177 callers; production AI write
// sites use SetAI so `dmt cache clear --ai-only` can find them.
func (c *TypeMappingCache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mappings[key] = CacheEntry{Result: value}
}

// SetAI stores a mapping tagged with full AI provenance (model + time).
// Used by the AITypeMapper's column- and table-level write paths so the
// cache CLI's --ai-only filter can find them on model upgrade.
func (c *TypeMappingCache) SetAI(key, value, model string) {
	c.SetAIWithMetadata(key, value, "", model)
}

// SetAIWithMetadata stores a mapping tagged with AI provider/model provenance
// and a stable hash of the input signature encoded in the cache key.
func (c *TypeMappingCache) SetAIWithMetadata(key, value, provider, model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mappings[key] = CacheEntry{
		Result:     value,
		Source:     SourceAI,
		Provider:   provider,
		Model:      model,
		SchemaHash: cacheKeyHash(key),
		CachedAt:   time.Now().UTC(),
	}
}

// All returns the flat (key → result string) view used by callers that
// don't need provenance (ExportCache, debug counts). Deterministic-
// tagged entries are excluded for consistency with Get.
func (c *TypeMappingCache) All() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]string, len(c.mappings))
	for k, e := range c.mappings {
		if e.Source == SourceDeterministic {
			continue
		}
		result[k] = e.Result
	}
	return result
}

// AllEntries returns the full provenance-tagged map. Used by saveCache
// to serialize and by ClearAI to count what gets cleared.
func (c *TypeMappingCache) AllEntries() map[string]CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]CacheEntry, len(c.mappings))
	for k, v := range c.mappings {
		out[k] = v
	}
	return out
}

// Load populates the cache from a flat string map. Used by the legacy
// (pre-#177) on-disk format migration path: every entry is tagged
// SourceAI because that was the only writer before this issue. Kept on
// the public surface so callers building a cache from a legacy export
// don't have to wrap each entry.
func (c *TypeMappingCache) Load(mappings map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range mappings {
		c.mappings[k] = CacheEntry{Result: v, Source: SourceAI}
	}
}

// LoadEntries populates the cache from a provenance-tagged map. Used by
// the v2+ on-disk format load path.
func (c *TypeMappingCache) LoadEntries(entries map[string]CacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range entries {
		c.mappings[k] = v
	}
}

// ClearAI removes every entry tagged SourceAI and returns the count
// cleared. Other entries (including SourceDeterministic and untagged)
// are preserved.
func (c *TypeMappingCache) ClearAI() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cleared := 0
	for k, v := range c.mappings {
		if v.Source == SourceAI {
			delete(c.mappings, k)
			cleared++
		}
	}
	return cleared
}

// cacheFilePayload is the on-disk envelope for the v2+ cache format
// (#177). Wraps Mappings in a versioned struct so future shape changes
// can branch on Version cleanly. Read path tries this format first and
// falls back to the legacy flat-map format below.
type cacheFilePayload struct {
	Version  int                   `json:"version"`
	Checksum string                `json:"checksum,omitempty"`
	Mappings map[string]CacheEntry `json:"mappings"`
}

func (m *AITypeMapper) loadCache() error {
	data, err := os.ReadFile(m.cacheFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading cache file: %w", err)
	}

	// Try the v2+ provenance-tagged format. json.Unmarshal silently
	// ignores top-level keys it doesn't recognize, so this also succeeds
	// (with Mappings == nil) on a legacy flat-map file — the nil check
	// below triggers the legacy migration path.
	var payload cacheFilePayload
	if err := json.Unmarshal(data, &payload); err == nil && payload.Mappings != nil {
		if payload.Checksum != "" {
			checksum, err := cachePayloadChecksum(payload.Version, payload.Mappings)
			if err != nil {
				return fmt.Errorf("checksumming cache file: %w", err)
			}
			if checksum != payload.Checksum {
				return fmt.Errorf("cache integrity check failed: checksum mismatch")
			}
		}
		m.cache.LoadEntries(payload.Mappings)
		logging.Debug("Loaded %d type mappings from cache (format v%d)", len(payload.Mappings), payload.Version)
		return nil
	}

	// Legacy pre-#177 format: flat `{key: result-string}`. The old code
	// only ever wrote AI mappings here, so migrate every entry as
	// SourceAI (defense-in-depth — if a non-AI writer ever did exist
	// under the old code, tagging it SourceAI surfaces it via
	// --ai-only clear, which is the safer default).
	var legacy map[string]string
	if err := json.Unmarshal(data, &legacy); err != nil {
		return fmt.Errorf("parsing cache file: %w", err)
	}
	m.cache.Load(legacy)
	logging.Debug("Loaded %d legacy AI type mappings from cache (pre-#177 format)", len(legacy))
	return nil
}

func (m *AITypeMapper) saveCache() error {
	// Serialize saves. Callers release cacheMu before calling saveCache, so
	// without a dedicated lock two goroutines could interleave truncate/write
	// and tear the file. A torn or partial file fails the checksum on load and
	// the whole cache is discarded, re-billing every previously-paid mapping
	// (#563).
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	payload := cacheFilePayload{
		Version:  cacheFileFormatVersion,
		Mappings: m.cache.AllEntries(),
	}
	checksum, err := cachePayloadChecksum(payload.Version, payload.Mappings)
	if err != nil {
		return fmt.Errorf("checksumming cache: %w", err)
	}
	payload.Checksum = checksum
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}

	return atomicWriteCacheFile(m.cacheFile, data, 0600)
}

// atomicWriteCacheFile writes data to path atomically: it stages the content in
// a temp file in the same directory, fsyncs it, then renames it over the target.
// A reader therefore never observes a partial file, and a crash or interleaved
// write can't corrupt the cache — a torn file would fail the checksum on load
// and discard every previously-paid mapping (#563).
func atomicWriteCacheFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp.")
	if err != nil {
		return fmt.Errorf("creating temp cache file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename; cleans up on error

	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp cache file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temp cache file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp cache file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp cache file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming cache file into place: %w", err)
	}
	return nil
}

// CacheSize returns the number of cached mappings.
func (m *AITypeMapper) CacheSize() int {
	return len(m.cache.All())
}

// ClearCache removes all cached mappings.
func (m *AITypeMapper) ClearCache() error {
	m.cacheMu.Lock()
	m.cache = NewTypeMappingCache()
	m.cacheMu.Unlock()

	if err := os.Remove(m.cacheFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing cache file: %w", err)
	}
	return nil
}

// ClearAICacheEntries drops every SourceAI entry from the on-disk cache
// at the given path (#177). Used by `dmt cache clear --ai-only` after a
// model upgrade — invalidates AI mappings without touching anything
// else. Removes the cache file entirely if nothing else remains; leaves
// it alone if there were no AI entries to clear. Operates directly on
// the file so the CLI doesn't need a live AITypeMapper (which would
// require a provider/API key just to invalidate stale cache).
func ClearAICacheEntries(cacheFile string) (int, error) {
	data, err := os.ReadFile(cacheFile)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading cache file: %w", err)
	}

	cache := NewTypeMappingCache()
	var payload cacheFilePayload
	if err := json.Unmarshal(data, &payload); err == nil && payload.Mappings != nil {
		if payload.Checksum != "" {
			checksum, err := cachePayloadChecksum(payload.Version, payload.Mappings)
			if err != nil {
				return 0, fmt.Errorf("checksumming cache file: %w", err)
			}
			if checksum != payload.Checksum {
				return 0, fmt.Errorf("cache integrity check failed: checksum mismatch")
			}
		}
		cache.LoadEntries(payload.Mappings)
	} else {
		var legacy map[string]string
		if err := json.Unmarshal(data, &legacy); err != nil {
			return 0, fmt.Errorf("parsing cache file: %w", err)
		}
		cache.Load(legacy)
	}

	cleared := cache.ClearAI()
	if cleared == 0 {
		return 0, nil
	}

	remaining := cache.AllEntries()
	if len(remaining) == 0 {
		if err := os.Remove(cacheFile); err != nil && !os.IsNotExist(err) {
			return cleared, fmt.Errorf("removing cache file: %w", err)
		}
		return cleared, nil
	}

	out := cacheFilePayload{Version: cacheFileFormatVersion, Mappings: remaining}
	checksum, err := cachePayloadChecksum(out.Version, out.Mappings)
	if err != nil {
		return cleared, fmt.Errorf("checksumming cache: %w", err)
	}
	out.Checksum = checksum
	rewritten, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return cleared, fmt.Errorf("marshaling cache: %w", err)
	}
	// Same file, same atomicity requirement as saveCache — `dmt cache clear`
	// must not tear the cache (#563).
	if err := atomicWriteCacheFile(cacheFile, rewritten, 0600); err != nil {
		return cleared, err
	}
	return cleared, nil
}

func cacheKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", sum[:])
}

func cachePayloadChecksum(version int, mappings map[string]CacheEntry) (string, error) {
	raw, err := json.Marshal(struct {
		Version  int                   `json:"version"`
		Mappings map[string]CacheEntry `json:"mappings"`
	}{
		Version:  version,
		Mappings: mappings,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

// DefaultCacheFilePath is the canonical on-disk location for the type
// cache. Used by the cache CLI subcommand so its --ai-only path can
// operate on the file without constructing an AITypeMapper.
func DefaultCacheFilePath() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".dmt", "type-cache.json")
}

// ExportCache exports cached mappings for review or sharing.
func (m *AITypeMapper) ExportCache(w io.Writer) error {
	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}
	_, err = w.Write(data)
	return err
}
