//go:build darwin

package config

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// getAvailableMemoryMB returns available system memory in MB on macOS.
// Uses vm_stat to compute free + inactive + purgeable memory, which represents
// memory that can be reclaimed without swapping.
func getAvailableMemoryMB() (int64, error) {
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		return 0, fmt.Errorf("failed to run vm_stat: %w", err)
	}

	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("vm_stat returned unexpected output")
	}

	// First line: "Mach Virtual Memory Statistics: (page size of XXXX bytes)"
	pageSize := int64(0)
	if idx := strings.Index(lines[0], "page size of "); idx >= 0 {
		s := lines[0][idx+len("page size of "):]
		s = strings.TrimSuffix(s, " bytes)")
		if ps, err := strconv.ParseInt(s, 10, 64); err == nil {
			pageSize = ps
		}
	}
	if pageSize == 0 {
		return 0, fmt.Errorf("failed to parse page size from vm_stat")
	}

	// Parse page counts from vm_stat output
	values := make(map[string]int64)
	for _, line := range lines[1:] {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.TrimSuffix(val, ".")
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			values[key] = n
		}
	}

	// Available = free + inactive + purgeable (reclaimable without swapping)
	freePages := values["Pages free"]
	inactivePages := values["Pages inactive"]
	purgeablePages := values["Pages purgeable"]

	availablePages := freePages + inactivePages + purgeablePages
	if availablePages == 0 {
		return 0, fmt.Errorf("vm_stat reported 0 available pages (free=%d, inactive=%d, purgeable=%d)",
			freePages, inactivePages, purgeablePages)
	}

	return availablePages * pageSize / (1024 * 1024), nil
}
