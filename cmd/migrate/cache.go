package main

import (
	"fmt"
	"os"

	"github.com/johndauphine/dmt/internal/driver"

	"github.com/urfave/cli/v2"
)

func cacheClear(c *cli.Context) error {
	cacheFile := driver.DefaultCacheFilePath()
	if c.Bool("ai-only") {
		cleared, err := driver.ClearAICacheEntries(cacheFile)
		if err != nil {
			return fmt.Errorf("clearing AI cache entries: %w", err)
		}
		fmt.Printf("Cleared %d AI cache entries from %s\n", cleared, cacheFile)
		return nil
	}

	if err := os.Remove(cacheFile); err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("No cache file to clear at %s\n", cacheFile)
			return nil
		}
		return fmt.Errorf("removing cache file: %w", err)
	}
	fmt.Printf("Removed cache file %s\n", cacheFile)
	return nil
}
