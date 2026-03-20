//go:build !linux && !darwin && !windows

package config

import "fmt"

// getAvailableMemoryMB is not implemented on this platform.
// Use max_memory_mb in the config file to set the memory budget explicitly.
func getAvailableMemoryMB() (int64, error) {
	return 0, fmt.Errorf("memory detection not supported on this platform; set max_memory_mb in config")
}
