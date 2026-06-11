// Type-cache management shared by the CLI and the TUI (#443).
package command

import (
	"fmt"
	"os"

	"github.com/johndauphine/dmt/internal/driver"
)

// CacheClearDescription states exactly what ClearTypeCache(aiOnly)
// would remove — the TUI shows this before asking for --confirm.
func CacheClearDescription(aiOnly bool) string {
	cacheFile := driver.DefaultCacheFilePath()
	if aiOnly {
		return fmt.Sprintf("AI-sourced entries in %s (deterministic typemap entries are kept)", cacheFile)
	}
	return fmt.Sprintf("the entire type cache file %s", cacheFile)
}

// ClearTypeCache removes the local type-mapping cache (or only its
// AI-sourced entries) and returns the human-readable result.
func ClearTypeCache(aiOnly bool) (string, error) {
	cacheFile := driver.DefaultCacheFilePath()
	if aiOnly {
		cleared, err := driver.ClearAICacheEntries(cacheFile)
		if err != nil {
			return "", fmt.Errorf("clearing AI cache entries: %w", err)
		}
		return fmt.Sprintf("Cleared %d AI cache entries from %s\n", cleared, cacheFile), nil
	}

	if err := os.Remove(cacheFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Sprintf("No cache file to clear at %s\n", cacheFile), nil
		}
		return "", fmt.Errorf("removing cache file: %w", err)
	}
	return fmt.Sprintf("Removed cache file %s\n", cacheFile), nil
}
