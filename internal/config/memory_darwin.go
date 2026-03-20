//go:build darwin

package config

import (
	"syscall"
	"unsafe"
)

// getAvailableMemoryMB returns available system memory in MB on macOS
func getAvailableMemoryMB() int64 {
	// Use syscall to get hw.memsize (total physical memory)
	var memSize uint64
	size := unsafe.Sizeof(memSize)

	// sysctlbyname for "hw.memsize"
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
		return 4096 // Default 4GB if syscall fails
	}

	// Convert bytes to MB
	return int64(memSize / (1024 * 1024))
}
