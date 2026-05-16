//go:build !windows

package config

import (
	"fmt"
	"os"
	"syscall"
)

func preserveFileOwnership(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if err := os.Chown(path, int(stat.Uid), int(stat.Gid)); err != nil {
		return fmt.Errorf("preserving config file ownership: %w", err)
	}
	return nil
}
