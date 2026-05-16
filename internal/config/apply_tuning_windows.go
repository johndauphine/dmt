//go:build windows

package config

import "os"

func preserveFileOwnership(path string, info os.FileInfo) error {
	return nil
}
