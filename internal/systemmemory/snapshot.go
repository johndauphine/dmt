package systemmemory

import "strings"

const bytesPerMiB = uint64(1024 * 1024)

// Snapshot describes the host and cgroup memory domains visible to dmt.
// CapacityMB and AvailableMB are the more restrictive effective values used
// for budgeting. The component fields remain available so live pressure can
// be computed within each domain instead of combining unrelated minima.
type Snapshot struct {
	CapacityMB      int64
	AvailableMB     int64
	HostCapacityMB  int64
	HostAvailableMB int64
	CgroupLimitMB   int64
	CgroupCurrentMB int64
	Source          string

	hostCapacityBytes  uint64
	hostAvailableBytes uint64
	cgroupLimitBytes   uint64
	cgroupCurrentBytes uint64
	cgroupSource       string
}

// EffectivePressure returns the larger valid utilization percentage across
// the host and finite-cgroup domains. It deliberately does not derive pressure
// from CapacityMB and AvailableMB: those values may have been constrained by
// different domains and combining them would manufacture a percentage that
// exists in neither domain.
func (s Snapshot) EffectivePressure() (percent float64, source string, ok bool) {
	hostCapacity, hostAvailable := s.hostValues()
	if hostCapacity > 0 {
		if hostAvailable > hostCapacity {
			hostAvailable = hostCapacity
		}
		percent = clampPercent(float64(hostCapacity-hostAvailable) / float64(hostCapacity) * 100)
		source = "host"
		ok = true
	}

	cgroupLimit, cgroupCurrent := s.cgroupValues()
	if cgroupLimit > 0 {
		cgroupPercent := clampPercent(float64(cgroupCurrent) / float64(cgroupLimit) * 100)
		if !ok || cgroupPercent > percent {
			percent = cgroupPercent
			source = s.pressureCgroupSource()
			ok = true
		}
	}

	return percent, source, ok
}

func (s Snapshot) hostValues() (capacity, available uint64) {
	if s.hostCapacityBytes > 0 {
		return s.hostCapacityBytes, s.hostAvailableBytes
	}
	return positiveMiB(s.HostCapacityMB), positiveMiB(s.HostAvailableMB)
}

func (s Snapshot) cgroupValues() (limit, current uint64) {
	if s.cgroupLimitBytes > 0 {
		return s.cgroupLimitBytes, s.cgroupCurrentBytes
	}
	return positiveMiB(s.CgroupLimitMB), positiveMiB(s.CgroupCurrentMB)
}

func (s Snapshot) pressureCgroupSource() string {
	if s.cgroupSource != "" {
		return s.cgroupSource
	}
	if strings.HasPrefix(s.Source, "cgroup-") {
		return s.Source
	}
	return "cgroup"
}

func positiveMiB(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	if uint64(value) > ^uint64(0)/bytesPerMiB {
		return ^uint64(0)
	}
	return uint64(value) * bytesPerMiB
}

func clampPercent(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 100:
		return 100
	default:
		return value
	}
}

func bytesToMiB(value uint64) int64 {
	return int64(value / bytesPerMiB)
}
