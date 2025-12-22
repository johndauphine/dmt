//go:build linux

package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// getAvailableMemoryMB returns available system memory in MB on Linux
func getAvailableMemoryMB() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 4096 // Default 4GB if can't read
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb / 1024 // Convert KB to MB
				}
			}
		}
	}
	return 4096 // Default 4GB
}
