//go:build linux

package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// getAvailableMemoryMB returns available system memory in MB on Linux.
// Uses MemAvailable from /proc/meminfo, which accounts for free memory,
// reclaimable caches, and buffers. Falls back to MemTotal if MemAvailable
// is not present (kernels < 3.14).
func getAvailableMemoryMB() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 4096
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

	if memAvailable > 0 {
		return memAvailable
	}
	if memTotal > 0 {
		return memTotal
	}
	return 4096
}
