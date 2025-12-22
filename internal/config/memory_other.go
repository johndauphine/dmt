//go:build !linux && !darwin

package config

// getAvailableMemoryMB returns a default value on unsupported platforms
func getAvailableMemoryMB() int64 {
	return 4096 // Default 4GB
}
