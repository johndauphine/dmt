//go:build darwin

package config

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

// getAvailableMemoryMB returns available system memory in MB on macOS.
// Uses vm_stat to compute free + inactive + purgeable memory, which represents
// memory that can be reclaimed without swapping. Falls back to total memory
// if vm_stat parsing fails.
func getAvailableMemoryMB() int64 {
	available := getAvailableFromVMStat()
	if available > 0 {
		return available
	}
	// Fallback: total physical memory
	return getTotalMemoryMB()
}

// getAvailableFromVMStat parses vm_stat output to compute reclaimable memory.
// macOS reports memory in pages; page size is obtained from vm_stat header.
func getAvailableFromVMStat() int64 {
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		return 0
	}

	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return 0
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
		return 0
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
		return 0
	}

	return availablePages * pageSize / (1024 * 1024)
}

// getTotalMemoryMB returns total physical memory via sysctl hw.memsize.
func getTotalMemoryMB() int64 {
	var memSize uint64
	size := unsafe.Sizeof(memSize)

	mib := []int32{6, 24} // CTL_HW = 6, HW_MEMSIZE = 24
	_, _, errno := syscall.Syscall6(
		syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(unsafe.Pointer(&memSize)),
		uintptr(unsafe.Pointer(&size)),
		0,
		0,
	)

	if errno != 0 {
		return 4096
	}

	return int64(memSize / (1024 * 1024))
}
