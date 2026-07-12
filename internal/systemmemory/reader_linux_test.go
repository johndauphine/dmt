//go:build linux

package systemmemory

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestLinuxReaderNestedCgroupV2(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 16 * gib, availableBytes: 10 * gib},
		map[string]string{
			procSelfCgroupPath:                       "0::/docker/abc/workload\n",
			procSelfMountinfoPath:                    "36 29 0:32 /docker/abc /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/workload/memory.max":     fmt.Sprint(8 * gib),
			"/sys/fs/cgroup/workload/memory.current": fmt.Sprint(2 * gib),
		},
		nil,
	)

	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	assertSnapshot(t, got, Snapshot{
		CapacityMB: 8 * 1024, AvailableMB: 6 * 1024,
		HostCapacityMB: 16 * 1024, HostAvailableMB: 10 * 1024,
		CgroupLimitMB: 8 * 1024, CgroupCurrentMB: 2 * 1024,
		Source: "cgroup-v2",
	})
}

func TestLinuxReaderFiniteCgroupSourcePreservedWhenHostAvailabilityIsTighter(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 16 * gib, availableBytes: 3 * gib},
		map[string]string{
			procSelfCgroupPath:                       "0::/workload\n",
			procSelfMountinfoPath:                    "36 29 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/workload/memory.max":     fmt.Sprint(8 * gib),
			"/sys/fs/cgroup/workload/memory.current": fmt.Sprint(2 * gib),
		},
		nil,
	)

	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.CapacityMB != 8*1024 || got.AvailableMB != 3*1024 || got.Source != "cgroup-v2" {
		t.Fatalf("snapshot = %+v, want 8GiB capacity/3GiB host-limited availability with cgroup-v2 source", got)
	}
}

func TestLinuxReaderNestedCgroupV1(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 16 * gib, availableBytes: 12 * gib},
		map[string]string{
			procSelfCgroupPath: "9:cpuset:/machine.slice/container/child\n" +
				"5:memory,blkio:/machine.slice/container/child\n",
			procSelfMountinfoPath:                               "41 29 0:38 /machine.slice/container /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,memory\n",
			"/sys/fs/cgroup/memory/child/memory.limit_in_bytes": fmt.Sprint(4 * gib),
			"/sys/fs/cgroup/memory/child/memory.usage_in_bytes": fmt.Sprint(3 * gib),
		},
		nil,
	)

	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	assertSnapshot(t, got, Snapshot{
		CapacityMB: 4 * 1024, AvailableMB: 1024,
		HostCapacityMB: 16 * 1024, HostAvailableMB: 12 * 1024,
		CgroupLimitMB: 4 * 1024, CgroupCurrentMB: 3 * 1024,
		Source: "cgroup-v1",
	})
}

func TestLinuxReaderHybridPrefersV1MemoryController(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 16 * gib, availableBytes: 12 * gib},
		map[string]string{
			procSelfCgroupPath: "0::/unified/path\n5:memory:/legacy/path\n",
			procSelfMountinfoPath: "40 29 0:37 / /sys/fs/cgroup/unified rw - cgroup2 cgroup rw\n" +
				"41 29 0:38 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
			"/sys/fs/cgroup/memory/legacy/path/memory.limit_in_bytes": fmt.Sprint(4 * gib),
			"/sys/fs/cgroup/memory/legacy/path/memory.usage_in_bytes": fmt.Sprint(gib),
		},
		nil,
	)

	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Source != "cgroup-v1" || got.CgroupLimitMB != 4*1024 {
		t.Fatalf("snapshot = %+v, want cgroup-v1 4GiB limit", got)
	}
}

func TestLinuxReaderUnlimitedCgroupUsesHost(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name: "v2 max",
			files: map[string]string{
				procSelfCgroupPath:                   "0::/workload\n",
				procSelfMountinfoPath:                "36 29 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
				"/sys/fs/cgroup/workload/memory.max": "max\n",
			},
		},
		{
			name: "v1 sentinel",
			files: map[string]string{
				procSelfCgroupPath:    "5:memory:/workload\n",
				procSelfMountinfoPath: "41 29 0:38 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
				"/sys/fs/cgroup/memory/workload/memory.limit_in_bytes": fmt.Sprint(v1UnlimitedThreshold),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := fixtureLinuxReader(
				hostMemory{capacityBytes: 16 * gib, availableBytes: 12 * gib},
				test.files,
				nil,
			)
			got, err := reader.Read()
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			assertSnapshot(t, got, Snapshot{
				CapacityMB: 16 * 1024, AvailableMB: 12 * 1024,
				HostCapacityMB: 16 * 1024, HostAvailableMB: 12 * 1024,
				Source: "host",
			})
		})
	}
}

func TestLinuxReaderNoMemoryMembershipUsesHost(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 8 * gib, availableBytes: 5 * gib},
		map[string]string{procSelfCgroupPath: "7:cpu,cpuacct:/workload\n"},
		nil,
	)
	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Source != "host" || got.CapacityMB != 8*1024 || got.AvailableMB != 5*1024 {
		t.Fatalf("snapshot = %+v, want host-only 8GiB/5GiB", got)
	}
}

func TestLinuxReaderV2ControllerMetadataDistinguishesAbsenceFromFailure(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	base := map[string]string{
		procSelfCgroupPath:    "0::/workload\n",
		procSelfMountinfoPath: "36 29 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
	}

	t.Run("memory controller genuinely unavailable uses host", func(t *testing.T) {
		files := cloneStrings(base)
		files["/sys/fs/cgroup/workload/cgroup.controllers"] = "cpu io pids\n"
		reader := fixtureLinuxReader(hostMemory{capacityBytes: 8 * gib, availableBytes: 5 * gib}, files, nil)
		got, err := reader.Read()
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if got.Source != "host" || got.CapacityMB != 8*1024 || got.AvailableMB != 5*1024 {
			t.Fatalf("snapshot = %+v, want host-only", got)
		}
	})

	t.Run("advertised memory controller with missing limit fails", func(t *testing.T) {
		files := cloneStrings(base)
		files["/sys/fs/cgroup/workload/cgroup.controllers"] = "cpu memory pids\n"
		reader := fixtureLinuxReader(hostMemory{capacityBytes: 8 * gib, availableBytes: 5 * gib}, files, nil)
		_, err := reader.Read()
		if err == nil || !strings.Contains(err.Error(), "reading identified cgroup v2 memory limit") {
			t.Fatalf("Read error = %v, want missing identified memory limit", err)
		}
	})

	t.Run("missing controller metadata is ambiguous and fails", func(t *testing.T) {
		reader := fixtureLinuxReader(hostMemory{capacityBytes: 8 * gib, availableBytes: 5 * gib}, base, nil)
		_, err := reader.Read()
		if err == nil || !strings.Contains(err.Error(), "controller metadata") {
			t.Fatalf("Read error = %v, want controller-metadata failure", err)
		}
	})
}

func TestLinuxReaderIdentifiedControllerErrorsAreConservative(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	base := map[string]string{
		procSelfCgroupPath:                       "0::/workload\n",
		procSelfMountinfoPath:                    "36 29 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
		"/sys/fs/cgroup/workload/memory.max":     fmt.Sprint(4 * gib),
		"/sys/fs/cgroup/workload/memory.current": fmt.Sprint(gib),
	}
	tests := []struct {
		name      string
		mutate    func(map[string]string, map[string]error)
		wantError string
	}{
		{
			name: "limit permission error",
			mutate: func(_ map[string]string, fileErrors map[string]error) {
				fileErrors["/sys/fs/cgroup/workload/memory.max"] = errors.New("permission denied")
			},
			wantError: "permission denied",
		},
		{
			name: "malformed limit",
			mutate: func(files map[string]string, _ map[string]error) {
				files["/sys/fs/cgroup/workload/memory.max"] = "not-a-number"
			},
			wantError: "parsing identified cgroup v2 memory limit",
		},
		{
			name: "missing usage",
			mutate: func(files map[string]string, _ map[string]error) {
				delete(files, "/sys/fs/cgroup/workload/memory.current")
			},
			wantError: "reading identified cgroup v2 memory usage",
		},
		{
			name: "usage permission error",
			mutate: func(_ map[string]string, fileErrors map[string]error) {
				fileErrors["/sys/fs/cgroup/workload/memory.current"] = errors.New("permission denied")
			},
			wantError: "permission denied",
		},
		{
			name: "malformed usage",
			mutate: func(files map[string]string, _ map[string]error) {
				files["/sys/fs/cgroup/workload/memory.current"] = "not-a-number"
			},
			wantError: "parsing identified cgroup v2 memory usage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := cloneStrings(base)
			fileErrors := map[string]error{}
			test.mutate(files, fileErrors)
			reader := fixtureLinuxReader(
				hostMemory{capacityBytes: 16 * gib, availableBytes: 12 * gib},
				files,
				fileErrors,
			)
			_, err := reader.Read()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Read error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestLinuxReaderV1IdentifiedControllerErrorsAreConservative(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	const limitPath = "/sys/fs/cgroup/memory/workload/memory.limit_in_bytes"
	const usagePath = "/sys/fs/cgroup/memory/workload/memory.usage_in_bytes"
	base := map[string]string{
		procSelfCgroupPath:    "5:memory:/workload\n",
		procSelfMountinfoPath: "41 29 0:38 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
		limitPath:             fmt.Sprint(4 * gib),
		usagePath:             fmt.Sprint(gib),
	}
	tests := []struct {
		name      string
		mutate    func(map[string]string, map[string]error)
		wantError string
	}{
		{
			name: "limit permission error",
			mutate: func(_ map[string]string, fileErrors map[string]error) {
				fileErrors[limitPath] = errors.New("permission denied")
			},
			wantError: "permission denied",
		},
		{
			name: "malformed limit",
			mutate: func(files map[string]string, _ map[string]error) {
				files[limitPath] = "not-a-number"
			},
			wantError: "parsing identified cgroup v1 memory limit",
		},
		{
			name: "usage permission error",
			mutate: func(_ map[string]string, fileErrors map[string]error) {
				fileErrors[usagePath] = errors.New("permission denied")
			},
			wantError: "permission denied",
		},
		{
			name: "malformed usage",
			mutate: func(files map[string]string, _ map[string]error) {
				files[usagePath] = "not-a-number"
			},
			wantError: "parsing identified cgroup v1 memory usage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := cloneStrings(base)
			fileErrors := map[string]error{}
			test.mutate(files, fileErrors)
			reader := fixtureLinuxReader(
				hostMemory{capacityBytes: 16 * gib, availableBytes: 12 * gib},
				files,
				fileErrors,
			)
			_, err := reader.Read()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Read error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestLinuxReaderMissingMatchingMountUsesHost(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 8 * gib, availableBytes: 5 * gib},
		map[string]string{
			procSelfCgroupPath:    "0::/workload\n",
			procSelfMountinfoPath: "22 1 8:1 / / rw - ext4 /dev/root rw\n",
		},
		nil,
	)
	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Source != "host" || got.CapacityMB != 8*1024 || got.AvailableMB != 5*1024 {
		t.Fatalf("snapshot = %+v, want host-only fallback", got)
	}
}

func TestLinuxReaderCurrentAboveLimitHasNoAvailableMemory(t *testing.T) {
	const mib = uint64(1024 * 1024)
	reader := fixtureLinuxReader(
		hostMemory{capacityBytes: 2048 * mib, availableBytes: 1024 * mib},
		map[string]string{
			procSelfCgroupPath:                       "0::/workload\n",
			procSelfMountinfoPath:                    "36 29 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
			"/sys/fs/cgroup/workload/memory.max":     fmt.Sprint(512 * mib),
			"/sys/fs/cgroup/workload/memory.current": fmt.Sprint(600 * mib),
		},
		nil,
	)

	got, err := reader.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.CapacityMB != 512 || got.AvailableMB != 0 {
		t.Fatalf("effective memory = %dMB/%dMB available, want 512MB/0MB", got.CapacityMB, got.AvailableMB)
	}
	pressure, source, ok := got.EffectivePressure()
	if !ok || pressure != 100 || source != "cgroup-v2" {
		t.Fatalf("pressure = (%.1f, %q, %v), want (100, cgroup-v2, true)", pressure, source, ok)
	}
}

func fixtureLinuxReader(host hostMemory, files map[string]string, fileErrors map[string]error) *linuxReader {
	return &linuxReader{
		hostRead: func() (hostMemory, error) { return host, nil },
		readFile: func(name string) ([]byte, error) {
			if err := fileErrors[name]; err != nil {
				return nil, err
			}
			if value, ok := files[name]; ok {
				return []byte(value), nil
			}
			return nil, os.ErrNotExist
		},
	}
}

func assertSnapshot(t *testing.T, got, want Snapshot) {
	t.Helper()
	if got.CapacityMB != want.CapacityMB || got.AvailableMB != want.AvailableMB ||
		got.HostCapacityMB != want.HostCapacityMB || got.HostAvailableMB != want.HostAvailableMB ||
		got.CgroupLimitMB != want.CgroupLimitMB || got.CgroupCurrentMB != want.CgroupCurrentMB ||
		got.Source != want.Source {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func cloneStrings(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
