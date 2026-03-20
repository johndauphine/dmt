//go:build linux

package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// getAvailableMemoryMB returns available system memory in MB on Linux.
// Uses MemAvailable from /proc/meminfo, which accounts for free memory,
// reclaimable caches, and buffers. Falls back to MemTotal if MemAvailable
// is not present (kernels < 3.14).
func getAvailableMemoryMB() (int64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc/meminfo: %w", err)
	}
	defer file.Close()

	var memAvailable, memTotal int64

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					memAvailable = kb / 1024
				}
			}
		} else if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					memTotal = kb / 1024
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("failed to scan /proc/meminfo: %w", err)
	}

	if memAvailable > 0 {
		return memAvailable, nil
	}
	if memTotal > 0 {
		return memTotal, nil
	}
	return 0, fmt.Errorf("failed to parse MemAvailable or MemTotal from /proc/meminfo")
}
