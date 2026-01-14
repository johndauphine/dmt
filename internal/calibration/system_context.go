package calibration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

// SystemContext contains comprehensive information about the execution environment.
type SystemContext struct {
	// Host information
	Host HostInfo `json:"host"`

	// Source database information
	Source DatabaseInfo `json:"source"`

	// Target database information
	Target DatabaseInfo `json:"target"`

	// Network measurements
	Network NetworkInfo `json:"network"`
}

// HostInfo contains host system information.
type HostInfo struct {
	// OS information
	OS            string `json:"os"`
	OSVersion     string `json:"os_version"`
	Platform      string `json:"platform"`
	Hostname      string `json:"hostname"`
	Containerized bool   `json:"containerized"`

	// CPU information
	CPUCores       int     `json:"cpu_cores"`
	CPUModel       string  `json:"cpu_model"`
	CPUFrequencyMHz float64 `json:"cpu_frequency_mhz"`

	// Memory information
	MemoryTotalMB     int64 `json:"memory_total_mb"`
	MemoryAvailableMB int64 `json:"memory_available_mb"`
	MemoryUsedPercent float64 `json:"memory_used_percent"`

	// Disk information (working directory)
	DiskTotalGB     float64 `json:"disk_total_gb"`
	DiskFreeGB      float64 `json:"disk_free_gb"`
	DiskUsedPercent float64 `json:"disk_used_percent"`

	// Go runtime
	GoVersion    string `json:"go_version"`
	GoMaxProcs   int    `json:"go_max_procs"`
	NumGoroutine int    `json:"num_goroutine"`
}

// DatabaseInfo contains database system information.
type DatabaseInfo struct {
	Type    string `json:"type"`
	Version string `json:"version"`
	Host    string `json:"host"`
	Port    int    `json:"port"`

	// Server configuration (what we can detect)
	MaxConnections    int   `json:"max_connections,omitempty"`
	CurrentConnections int  `json:"current_connections,omitempty"`
	MaxMemoryMB       int64 `json:"max_memory_mb,omitempty"`

	// For source: table statistics
	TableCount        int   `json:"table_count,omitempty"`
	TotalRows         int64 `json:"total_rows,omitempty"`
	LargestTableRows  int64 `json:"largest_table_rows,omitempty"`
	LargestTableName  string `json:"largest_table_name,omitempty"`
	AvgRowWidthBytes  int   `json:"avg_row_width_bytes,omitempty"`

	// Network
	LatencyMs float64 `json:"latency_ms"`
}

// NetworkInfo contains network performance measurements.
type NetworkInfo struct {
	SourceLatencyMs    float64 `json:"source_latency_ms"`
	TargetLatencyMs    float64 `json:"target_latency_ms"`
	SourceLatencyP95Ms float64 `json:"source_latency_p95_ms"`
	TargetLatencyP95Ms float64 `json:"target_latency_p95_ms"`
}

// GatherHostInfo collects information about the host system.
func GatherHostInfo() HostInfo {
	info := HostInfo{
		CPUCores:   runtime.NumCPU(),
		GoVersion:  runtime.Version(),
		GoMaxProcs: runtime.GOMAXPROCS(0),
		NumGoroutine: runtime.NumGoroutine(),
	}

	// OS information
	if hostInfo, err := host.Info(); err == nil {
		info.OS = hostInfo.OS
		info.OSVersion = hostInfo.KernelVersion
		info.Platform = hostInfo.Platform
		info.Hostname = hostInfo.Hostname
	}

	// Check if containerized
	info.Containerized = isContainerized()

	// CPU information
	if cpuInfo, err := cpu.Info(); err == nil && len(cpuInfo) > 0 {
		info.CPUModel = cpuInfo[0].ModelName
		info.CPUFrequencyMHz = cpuInfo[0].Mhz
	}

	// Memory information
	if memInfo, err := mem.VirtualMemory(); err == nil {
		info.MemoryTotalMB = int64(memInfo.Total / 1024 / 1024)
		info.MemoryAvailableMB = int64(memInfo.Available / 1024 / 1024)
		info.MemoryUsedPercent = memInfo.UsedPercent
	}

	// Disk information for current directory
	if cwd, err := os.Getwd(); err == nil {
		if diskInfo, err := disk.Usage(cwd); err == nil {
			info.DiskTotalGB = float64(diskInfo.Total) / 1024 / 1024 / 1024
			info.DiskFreeGB = float64(diskInfo.Free) / 1024 / 1024 / 1024
			info.DiskUsedPercent = diskInfo.UsedPercent
		}
	}

	return info
}

// isContainerized checks if we're running in a container.
func isContainerized() bool {
	// Check for Docker
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	// Check for Kubernetes
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}

	// Check cgroup for container indicators
	if data, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		content := string(data)
		if strings.Contains(content, "docker") || strings.Contains(content, "kubepods") {
			return true
		}
	}

	return false
}

// GatherMSSQLInfo collects information about a SQL Server database.
func GatherMSSQLInfo(ctx context.Context, db *sql.DB, host string, port int) DatabaseInfo {
	info := DatabaseInfo{
		Type: "mssql",
		Host: host,
		Port: port,
	}

	// Get version
	row := db.QueryRowContext(ctx, "SELECT @@VERSION")
	var version string
	if err := row.Scan(&version); err == nil {
		// Extract first line
		if idx := strings.Index(version, "\n"); idx > 0 {
			version = version[:idx]
		}
		info.Version = strings.TrimSpace(version)
	}

	// Get max connections
	row = db.QueryRowContext(ctx, "SELECT @@MAX_CONNECTIONS")
	if err := row.Scan(&info.MaxConnections); err != nil {
		logging.Debug("Failed to get max connections: %v", err)
	}

	// Get current connections
	row = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1")
	if err := row.Scan(&info.CurrentConnections); err != nil {
		logging.Debug("Failed to get current connections: %v", err)
	}

	// Get max server memory
	row = db.QueryRowContext(ctx, `
		SELECT CAST(value_in_use AS BIGINT)
		FROM sys.configurations
		WHERE name = 'max server memory (MB)'
	`)
	if err := row.Scan(&info.MaxMemoryMB); err != nil {
		logging.Debug("Failed to get max memory: %v", err)
	}

	return info
}

// GatherPostgresInfo collects information about a PostgreSQL database.
func GatherPostgresInfo(ctx context.Context, db *sql.DB, host string, port int) DatabaseInfo {
	info := DatabaseInfo{
		Type: "postgres",
		Host: host,
		Port: port,
	}

	// Get version
	row := db.QueryRowContext(ctx, "SELECT version()")
	if err := row.Scan(&info.Version); err != nil {
		logging.Debug("Failed to get version: %v", err)
	}

	// Get max connections
	row = db.QueryRowContext(ctx, "SHOW max_connections")
	var maxConns string
	if err := row.Scan(&maxConns); err == nil {
		fmt.Sscanf(maxConns, "%d", &info.MaxConnections)
	}

	// Get current connections
	row = db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE state IS NOT NULL")
	if err := row.Scan(&info.CurrentConnections); err != nil {
		logging.Debug("Failed to get current connections: %v", err)
	}

	// Get shared_buffers (approximate max memory for PostgreSQL)
	row = db.QueryRowContext(ctx, "SELECT setting::bigint * 8192 / 1024 / 1024 FROM pg_settings WHERE name = 'shared_buffers'")
	if err := row.Scan(&info.MaxMemoryMB); err != nil {
		logging.Debug("Failed to get shared_buffers: %v", err)
	}

	return info
}

// WriterQueryer is an interface for querying a database via a Writer.
type WriterQueryer interface {
	QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error
	DBType() string
}

// GatherTargetDatabaseInfo collects database info using a Writer interface.
func GatherTargetDatabaseInfo(ctx context.Context, w WriterQueryer, host string, port int) DatabaseInfo {
	dbType := w.DBType()
	info := DatabaseInfo{
		Type: dbType,
		Host: host,
		Port: port,
	}

	switch dbType {
	case "postgres":
		// Get version
		if err := w.QueryRowRaw(ctx, "SELECT version()", &info.Version); err != nil {
			logging.Debug("Failed to get version: %v", err)
		}

		// Get max connections
		var maxConns string
		if err := w.QueryRowRaw(ctx, "SHOW max_connections", &maxConns); err == nil {
			fmt.Sscanf(maxConns, "%d", &info.MaxConnections)
		}

		// Get current connections
		if err := w.QueryRowRaw(ctx, "SELECT count(*) FROM pg_stat_activity WHERE state IS NOT NULL", &info.CurrentConnections); err != nil {
			logging.Debug("Failed to get current connections: %v", err)
		}

		// Get shared_buffers
		if err := w.QueryRowRaw(ctx, "SELECT setting::bigint * 8192 / 1024 / 1024 FROM pg_settings WHERE name = 'shared_buffers'", &info.MaxMemoryMB); err != nil {
			logging.Debug("Failed to get shared_buffers: %v", err)
		}

	case "mssql":
		// Get version
		if err := w.QueryRowRaw(ctx, "SELECT @@VERSION", &info.Version); err != nil {
			logging.Debug("Failed to get version: %v", err)
		} else if idx := strings.Index(info.Version, "\n"); idx > 0 {
			info.Version = strings.TrimSpace(info.Version[:idx])
		}

		// Get max connections
		if err := w.QueryRowRaw(ctx, "SELECT @@MAX_CONNECTIONS", &info.MaxConnections); err != nil {
			logging.Debug("Failed to get max connections: %v", err)
		}

		// Get current connections
		if err := w.QueryRowRaw(ctx, "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1", &info.CurrentConnections); err != nil {
			logging.Debug("Failed to get current connections: %v", err)
		}

		// Get max server memory
		if err := w.QueryRowRaw(ctx, "SELECT CAST(value_in_use AS BIGINT) FROM sys.configurations WHERE name = 'max server memory (MB)'", &info.MaxMemoryMB); err != nil {
			logging.Debug("Failed to get max memory: %v", err)
		}
	}

	return info
}

// GatherTableStats collects statistics about tables in the source schema.
func GatherTableStats(ctx context.Context, db *sql.DB, dbType, schema string) (tableCount int, totalRows, largestRows int64, largestName string, avgRowWidth int) {
	switch strings.ToLower(dbType) {
	case "mssql", "sqlserver":
		return gatherMSSQLTableStats(ctx, db, schema)
	case "postgres", "postgresql":
		return gatherPostgresTableStats(ctx, db, schema)
	}
	return 0, 0, 0, "", 0
}

func gatherMSSQLTableStats(ctx context.Context, db *sql.DB, schema string) (tableCount int, totalRows, largestRows int64, largestName string, avgRowWidth int) {
	// Get table count and row statistics
	rows, err := db.QueryContext(ctx, `
		SELECT
			t.name,
			SUM(p.rows) as row_count,
			ISNULL(SUM(a.total_pages) * 8 * 1024 / NULLIF(SUM(p.rows), 0), 0) as avg_row_size
		FROM sys.tables t
		JOIN sys.partitions p ON t.object_id = p.object_id
		JOIN sys.allocation_units a ON p.partition_id = a.container_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND p.index_id IN (0, 1)
		GROUP BY t.name
		ORDER BY row_count DESC
	`, sql.Named("schema", schema))
	if err != nil {
		logging.Debug("Failed to get table stats: %v", err)
		return
	}
	defer rows.Close()

	var totalRowWidth int64
	for rows.Next() {
		var name string
		var rowCount int64
		var rowSize int
		if err := rows.Scan(&name, &rowCount, &rowSize); err != nil {
			continue
		}
		tableCount++
		totalRows += rowCount
		totalRowWidth += int64(rowSize)
		if rowCount > largestRows {
			largestRows = rowCount
			largestName = name
		}
	}

	if tableCount > 0 {
		avgRowWidth = int(totalRowWidth / int64(tableCount))
	}

	return
}

func gatherPostgresTableStats(ctx context.Context, db *sql.DB, schema string) (tableCount int, totalRows, largestRows int64, largestName string, avgRowWidth int) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			relname,
			n_live_tup,
			COALESCE(
				(SELECT avg_width FROM pg_stats WHERE schemaname = $1 AND tablename = c.relname LIMIT 1),
				100
			) as avg_width
		FROM pg_stat_user_tables
		JOIN pg_class c ON c.relname = pg_stat_user_tables.relname
		WHERE schemaname = $1
		ORDER BY n_live_tup DESC
	`, schema)
	if err != nil {
		logging.Debug("Failed to get table stats: %v", err)
		return
	}
	defer rows.Close()

	var totalRowWidth int64
	for rows.Next() {
		var name string
		var rowCount int64
		var rowWidth int
		if err := rows.Scan(&name, &rowCount, &rowWidth); err != nil {
			continue
		}
		tableCount++
		totalRows += rowCount
		totalRowWidth += int64(rowWidth)
		if rowCount > largestRows {
			largestRows = rowCount
			largestName = name
		}
	}

	if tableCount > 0 {
		avgRowWidth = int(totalRowWidth / int64(tableCount))
	}

	return
}

// MeasureLatencyDetailed measures network latency with percentiles.
func MeasureLatencyDetailed(ctx context.Context, pingFunc func(context.Context) error) (avgMs, p95Ms float64) {
	const iterations = 20
	latencies := make([]time.Duration, 0, iterations)

	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := pingFunc(ctx); err != nil {
			continue
		}
		latencies = append(latencies, time.Since(start))
	}

	if len(latencies) == 0 {
		return 0, 0
	}

	// Calculate average
	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	avgMs = float64(total.Microseconds()) / float64(len(latencies)) / 1000

	// Sort for percentile
	for i := 0; i < len(latencies)-1; i++ {
		for j := i + 1; j < len(latencies); j++ {
			if latencies[j] < latencies[i] {
				latencies[i], latencies[j] = latencies[j], latencies[i]
			}
		}
	}

	// P95
	p95Idx := int(float64(len(latencies)) * 0.95)
	if p95Idx >= len(latencies) {
		p95Idx = len(latencies) - 1
	}
	p95Ms = float64(latencies[p95Idx].Microseconds()) / 1000

	return
}

// FormatSystemContext returns a human-readable summary of system context.
func (sc *SystemContext) FormatSummary() string {
	var sb strings.Builder

	sb.WriteString("=== System Context ===\n\n")

	// Host
	sb.WriteString("Host:\n")
	sb.WriteString(fmt.Sprintf("  OS: %s %s (%s)\n", sc.Host.OS, sc.Host.OSVersion, sc.Host.Platform))
	sb.WriteString(fmt.Sprintf("  CPU: %s (%d cores @ %.0f MHz)\n", sc.Host.CPUModel, sc.Host.CPUCores, sc.Host.CPUFrequencyMHz))
	sb.WriteString(fmt.Sprintf("  Memory: %d MB available / %d MB total (%.1f%% used)\n",
		sc.Host.MemoryAvailableMB, sc.Host.MemoryTotalMB, sc.Host.MemoryUsedPercent))
	sb.WriteString(fmt.Sprintf("  Disk: %.1f GB free / %.1f GB total (%.1f%% used)\n",
		sc.Host.DiskFreeGB, sc.Host.DiskTotalGB, sc.Host.DiskUsedPercent))
	if sc.Host.Containerized {
		sb.WriteString("  Environment: Containerized\n")
	}

	// Source
	sb.WriteString(fmt.Sprintf("\nSource (%s):\n", sc.Source.Type))
	sb.WriteString(fmt.Sprintf("  Version: %s\n", sc.Source.Version))
	sb.WriteString(fmt.Sprintf("  Connections: %d / %d max\n", sc.Source.CurrentConnections, sc.Source.MaxConnections))
	if sc.Source.MaxMemoryMB > 0 {
		sb.WriteString(fmt.Sprintf("  Max Memory: %d MB\n", sc.Source.MaxMemoryMB))
	}
	sb.WriteString(fmt.Sprintf("  Tables: %d (%.1fM total rows)\n", sc.Source.TableCount, float64(sc.Source.TotalRows)/1000000))
	if sc.Source.LargestTableName != "" {
		sb.WriteString(fmt.Sprintf("  Largest: %s (%.1fM rows)\n", sc.Source.LargestTableName, float64(sc.Source.LargestTableRows)/1000000))
	}
	if sc.Source.AvgRowWidthBytes > 0 {
		sb.WriteString(fmt.Sprintf("  Avg Row Width: %d bytes\n", sc.Source.AvgRowWidthBytes))
	}
	sb.WriteString(fmt.Sprintf("  Latency: %.1f ms (p95: %.1f ms)\n", sc.Network.SourceLatencyMs, sc.Network.SourceLatencyP95Ms))

	// Target
	sb.WriteString(fmt.Sprintf("\nTarget (%s):\n", sc.Target.Type))
	sb.WriteString(fmt.Sprintf("  Version: %s\n", sc.Target.Version))
	sb.WriteString(fmt.Sprintf("  Connections: %d / %d max\n", sc.Target.CurrentConnections, sc.Target.MaxConnections))
	if sc.Target.MaxMemoryMB > 0 {
		sb.WriteString(fmt.Sprintf("  Shared Buffers: %d MB\n", sc.Target.MaxMemoryMB))
	}
	sb.WriteString(fmt.Sprintf("  Latency: %.1f ms (p95: %.1f ms)\n", sc.Network.TargetLatencyMs, sc.Network.TargetLatencyP95Ms))

	return sb.String()
}
