//go:build windows

package config

import (
	"fmt"
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// getAvailableMemoryMB returns available system memory in MB on Windows.
// Uses GlobalMemoryStatusEx to get the amount of physical memory currently
// available (free + standby/cached that can be reclaimed).
func getAvailableMemoryMB() (int64, error) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")

	var memStatus memoryStatusEx
	memStatus.Length = uint32(unsafe.Sizeof(memStatus))

	ret, _, err := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0, fmt.Errorf("GlobalMemoryStatusEx failed: %w", err)
	}

	availMB := int64(memStatus.AvailPhys) / (1024 * 1024)
	if availMB == 0 {
		return 0, fmt.Errorf("GlobalMemoryStatusEx reported 0 available memory")
	}

	return availMB, nil
}
