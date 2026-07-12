//go:build linux

package systemmemory

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	procSelfCgroupPath    = "/proc/self/cgroup"
	procSelfMountinfoPath = "/proc/self/mountinfo"

	// Linux cgroup v1 commonly represents an unlimited memory controller
	// with a page-aligned value just below MaxInt64. Treat values in that
	// sentinel range as non-limits while preserving ordinary limits that
	// merely happen to exceed host RAM.
	v1UnlimitedThreshold = uint64(1) << 60
)

type linuxReader struct {
	hostRead hostReadFunc
	readFile readFileFunc
}

type cgroupMembership struct {
	version int
	path    string
}

type cgroupMount struct {
	root       string
	mountPoint string
}

type cgroupMemory struct {
	limitBytes   uint64
	currentBytes uint64
	source       string
}

func newPlatformReader(hostRead hostReadFunc) Reader {
	return &linuxReader{hostRead: hostRead, readFile: os.ReadFile}
}

func (r *linuxReader) Read() (Snapshot, error) {
	host, err := r.hostRead()
	if err != nil {
		return Snapshot{}, err
	}
	if host.capacityBytes == 0 {
		return Snapshot{}, fmt.Errorf("reading host memory: total memory is zero")
	}
	if host.availableBytes > host.capacityBytes {
		host.availableBytes = host.capacityBytes
	}

	membershipData, err := r.readFile(procSelfCgroupPath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading %s: %w", procSelfCgroupPath, err)
	}
	membership, found, err := parseMemoryMembership(string(membershipData))
	if err != nil {
		return Snapshot{}, fmt.Errorf("parsing %s: %w", procSelfCgroupPath, err)
	}
	if !found {
		return hostSnapshot(host), nil
	}

	mountinfoData, err := r.readFile(procSelfMountinfoPath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading %s for identified cgroup v%d memory controller: %w",
			procSelfMountinfoPath, membership.version, err)
	}
	mount, found, err := findMemoryMount(string(mountinfoData), membership)
	if err != nil {
		return Snapshot{}, fmt.Errorf("parsing %s: %w", procSelfMountinfoPath, err)
	}
	if !found {
		// Membership can be visible through proc while the corresponding
		// controller mount is not present in this mount namespace (minimal
		// containers/chroots). With no matching mount/controller identified,
		// the contract is host-only rather than an ambiguous finite limit.
		return hostSnapshot(host), nil
	}

	dir, err := rebaseCgroupPath(mount.root, mount.mountPoint, membership.path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolving cgroup v%d memory path: %w", membership.version, err)
	}
	cgroup, finite, err := r.readCgroupMemory(membership.version, dir)
	if err != nil {
		return Snapshot{}, err
	}
	if !finite {
		return hostSnapshot(host), nil
	}

	effectiveCapacity := minUint64(host.capacityBytes, cgroup.limitBytes)
	cgroupAvailable := uint64(0)
	if cgroup.currentBytes < cgroup.limitBytes {
		cgroupAvailable = cgroup.limitBytes - cgroup.currentBytes
	}
	effectiveAvailable := minUint64(host.availableBytes, cgroupAvailable)

	return Snapshot{
		CapacityMB:         bytesToMiB(effectiveCapacity),
		AvailableMB:        bytesToMiB(effectiveAvailable),
		HostCapacityMB:     bytesToMiB(host.capacityBytes),
		HostAvailableMB:    bytesToMiB(host.availableBytes),
		CgroupLimitMB:      bytesToMiB(cgroup.limitBytes),
		CgroupCurrentMB:    bytesToMiB(cgroup.currentBytes),
		Source:             cgroup.source,
		hostCapacityBytes:  host.capacityBytes,
		hostAvailableBytes: host.availableBytes,
		cgroupLimitBytes:   cgroup.limitBytes,
		cgroupCurrentBytes: cgroup.currentBytes,
		cgroupSource:       cgroup.source,
	}, nil
}

func (r *linuxReader) readCgroupMemory(version int, dir string) (cgroupMemory, bool, error) {
	switch version {
	case 2:
		limitPath := path.Join(dir, "memory.max")
		limitData, err := r.readFile(limitPath)
		if err != nil {
			// A unified cgroup membership exists even when the kernel or
			// delegated hierarchy does not expose the memory controller. Only
			// an explicit controller-metadata check can classify that case as
			// host-only; a missing file while memory is advertised is an error.
			if errors.Is(err, os.ErrNotExist) {
				// Controller availability is delegated per subtree. The mount root
				// can advertise memory even when the branch containing this process
				// does not, so consult metadata at the rebased process cgroup.
				controllersPath := path.Join(dir, "cgroup.controllers")
				controllers, controllersErr := r.readFile(controllersPath)
				if controllersErr != nil {
					return cgroupMemory{}, false, fmt.Errorf("reading cgroup v2 controller metadata %s after missing memory limit: %w", controllersPath, controllersErr)
				}
				if !spaceListContains(string(controllers), "memory") {
					return cgroupMemory{}, false, nil
				}
			}
			return cgroupMemory{}, false, fmt.Errorf("reading identified cgroup v2 memory limit %s: %w", limitPath, err)
		}
		limitText := strings.TrimSpace(string(limitData))
		if limitText == "max" {
			return cgroupMemory{}, false, nil
		}
		limit, err := parsePositiveUint(limitText)
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("parsing identified cgroup v2 memory limit %s: %w", limitPath, err)
		}

		currentPath := path.Join(dir, "memory.current")
		currentData, err := r.readFile(currentPath)
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("reading identified cgroup v2 memory usage %s: %w", currentPath, err)
		}
		current, err := parseUint(strings.TrimSpace(string(currentData)))
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("parsing identified cgroup v2 memory usage %s: %w", currentPath, err)
		}
		return cgroupMemory{limitBytes: limit, currentBytes: current, source: "cgroup-v2"}, true, nil

	case 1:
		limitPath := path.Join(dir, "memory.limit_in_bytes")
		limitData, err := r.readFile(limitPath)
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("reading identified cgroup v1 memory limit %s: %w", limitPath, err)
		}
		limit, err := parsePositiveUint(strings.TrimSpace(string(limitData)))
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("parsing identified cgroup v1 memory limit %s: %w", limitPath, err)
		}
		if limit >= v1UnlimitedThreshold {
			return cgroupMemory{}, false, nil
		}

		currentPath := path.Join(dir, "memory.usage_in_bytes")
		currentData, err := r.readFile(currentPath)
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("reading identified cgroup v1 memory usage %s: %w", currentPath, err)
		}
		current, err := parseUint(strings.TrimSpace(string(currentData)))
		if err != nil {
			return cgroupMemory{}, false, fmt.Errorf("parsing identified cgroup v1 memory usage %s: %w", currentPath, err)
		}
		return cgroupMemory{limitBytes: limit, currentBytes: current, source: "cgroup-v1"}, true, nil

	default:
		return cgroupMemory{}, false, fmt.Errorf("unsupported cgroup version %d", version)
	}
}

func parseMemoryMembership(contents string) (cgroupMembership, bool, error) {
	var v2 *cgroupMembership
	var v1 *cgroupMembership
	for lineNumber, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return cgroupMembership{}, false, fmt.Errorf("line %d has %d fields, want 3", lineNumber+1, len(parts))
		}
		controllers := parts[1]
		membershipPath := parts[2]
		if membershipPath == "" || !strings.HasPrefix(membershipPath, "/") {
			return cgroupMembership{}, false, fmt.Errorf("line %d has invalid cgroup path %q", lineNumber+1, membershipPath)
		}
		if controllers == "" {
			entry := cgroupMembership{version: 2, path: path.Clean(membershipPath)}
			v2 = &entry
			continue
		}
		if commaListContains(controllers, "memory") {
			entry := cgroupMembership{version: 1, path: path.Clean(membershipPath)}
			v1 = &entry
		}
	}

	// In hybrid hierarchies the memory controller can remain on v1 even
	// though a v2 membership line is also present. Prefer the explicitly
	// named v1 memory controller in that case.
	if v1 != nil {
		return *v1, true, nil
	}
	if v2 != nil {
		return *v2, true, nil
	}
	return cgroupMembership{}, false, nil
}

func findMemoryMount(contents string, membership cgroupMembership) (cgroupMount, bool, error) {
	var best cgroupMount
	bestRootLength := -1
	for lineNumber, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if separator < 6 || separator+3 >= len(fields) {
			return cgroupMount{}, false, fmt.Errorf("line %d has invalid mountinfo layout", lineNumber+1)
		}

		fsType := fields[separator+1]
		matches := membership.version == 2 && fsType == "cgroup2"
		if membership.version == 1 && fsType == "cgroup" {
			matches = mountHasController(fields, separator, "memory")
		}
		if !matches {
			continue
		}

		root := path.Clean(unescapeMountField(fields[3]))
		mountPoint := path.Clean(unescapeMountField(fields[4]))
		if !pathContains(root, membership.path) {
			continue
		}
		if len(root) > bestRootLength {
			best = cgroupMount{root: root, mountPoint: mountPoint}
			bestRootLength = len(root)
		}
	}
	return best, bestRootLength >= 0, nil
}

func mountHasController(fields []string, separator int, controller string) bool {
	if separator+3 < len(fields) && commaListContains(fields[separator+3], controller) {
		return true
	}
	if len(fields) > 5 && commaListContains(fields[5], controller) {
		return true
	}
	for _, field := range fields[6:separator] {
		if commaListContains(field, controller) {
			return true
		}
	}
	return false
}

func rebaseCgroupPath(root, mountPoint, membershipPath string) (string, error) {
	root = path.Clean(root)
	mountPoint = path.Clean(mountPoint)
	membershipPath = path.Clean(membershipPath)
	if !pathContains(root, membershipPath) {
		return "", fmt.Errorf("membership path %q is outside mount root %q", membershipPath, root)
	}
	relative := strings.TrimPrefix(membershipPath, root)
	relative = strings.TrimPrefix(relative, "/")
	if relative == "" {
		return mountPoint, nil
	}
	return path.Join(mountPoint, relative), nil
}

func pathContains(root, candidate string) bool {
	root = path.Clean(root)
	candidate = path.Clean(candidate)
	return root == "/" || candidate == root || strings.HasPrefix(candidate, root+"/")
}

func commaListContains(list, target string) bool {
	for _, value := range strings.Split(list, ",") {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func spaceListContains(list, target string) bool {
	for _, value := range strings.Fields(list) {
		if value == target {
			return true
		}
	}
	return false
}

func unescapeMountField(value string) string {
	replacer := strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	)
	return replacer.Replace(value)
}

func parsePositiveUint(value string) (uint64, error) {
	parsed, err := parseUint(value)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("value must be positive")
	}
	return parsed, nil
}

func parseUint(value string) (uint64, error) {
	if value == "" {
		return 0, fmt.Errorf("value is empty")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
