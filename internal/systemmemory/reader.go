package systemmemory

import (
	"fmt"

	"github.com/shirou/gopsutil/v3/mem"
)

// Reader returns the memory domains visible to the current process.
type Reader interface {
	Read() (Snapshot, error)
}

type hostMemory struct {
	capacityBytes  uint64
	availableBytes uint64
}

type hostReadFunc func() (hostMemory, error)
type readFileFunc func(string) ([]byte, error)

// NewReader returns the platform memory reader. Linux combines host memory
// with the current process's cgroup; other supported platforms report host
// memory only.
func NewReader() Reader {
	return newPlatformReader(readHostMemory)
}

func readHostMemory() (hostMemory, error) {
	stats, err := mem.VirtualMemory()
	if err != nil {
		return hostMemory{}, fmt.Errorf("reading host memory: %w", err)
	}
	if stats.Total == 0 {
		return hostMemory{}, fmt.Errorf("reading host memory: total memory is zero")
	}
	available := stats.Available
	if available > stats.Total {
		available = stats.Total
	}
	return hostMemory{capacityBytes: stats.Total, availableBytes: available}, nil
}

func hostSnapshot(host hostMemory) Snapshot {
	return Snapshot{
		CapacityMB:         bytesToMiB(host.capacityBytes),
		AvailableMB:        bytesToMiB(host.availableBytes),
		HostCapacityMB:     bytesToMiB(host.capacityBytes),
		HostAvailableMB:    bytesToMiB(host.availableBytes),
		Source:             "host",
		hostCapacityBytes:  host.capacityBytes,
		hostAvailableBytes: host.availableBytes,
	}
}
