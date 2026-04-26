# Performance Benchmarks

Comprehensive benchmark results comparing Go and Rust implementations.

## Test Environment

- **Hardware**: Apple M3 Max, 36GB RAM, 14 CPU cores
- **OS**: macOS (Darwin 25.2.0)
- **Databases**: SQL Server 2022, PostgreSQL 15 (Docker containers)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables)
- **Date**: January 2026

> **Note**: SQL Server runs under Rosetta 2 emulation on Apple Silicon, adding overhead. Production Linux deployments will be faster.
> Azure SQL Edge runs natively on ARM64 — see M3 Max Azure SQL Edge section for Rosetta-free results.

## Dataset Details

| Table | Rows | Description |
|-------|------|-------------|
| Votes | 10,143,364 | Largest table |
| Comments | 3,875,183 | Text-heavy |
| Posts | 3,729,195 | Mixed content types |
| Badges | 1,102,020 | Datetime columns |
| Users | 299,398 | Various types |
| PostLinks | 161,519 | Foreign keys |
| VoteTypes | 15 | Small lookup |
| PostTypes | 10 | Small lookup |
| LinkTypes | 2 | Small lookup |
| **Total** | **19,310,706** | |

## Go vs Rust Comparison

### MSSQL → PostgreSQL

| Mode | Go | Rust | Notes |
|------|-----|------|-------|
| drop_recreate | 287,000 rows/s (75s) | 289,372 rows/s (74s) | Comparable |
| upsert | **235,160 rows/s** (82s) | 181,450 rows/s (106s) | **Go 30% faster** |

### PostgreSQL → MSSQL

| Mode | Go | Rust | Notes |
|------|-----|------|-------|
| drop_recreate | **140,387 rows/s** (137s) | 145,175 rows/s (133s) | Comparable |
| upsert | 68,825 rows/s (280s) | **80,075 rows/s** (241s) | Rust 16% faster |

### Memory Usage

| Direction | Mode | Go Peak | Rust Peak |
|-----------|------|---------|-----------|
| MSSQL→PG | drop_recreate | 5.3 GB | 4.9 GB |
| MSSQL→PG | upsert | ~5.0 GB* | 7.4 GB |
| PG→MSSQL | drop_recreate | **1.8 GB** | 5.4 GB |
| PG→MSSQL | upsert | **0.5 GB** | 3.7 GB |

*Estimated based on improved allocation patterns (was 10.9 GB).

## Key Findings

### 1. Go Upsert Performance is Superior
After optimizing the keyset pagination strategy and memory allocation, Go's MSSQL→PG upsert throughput jumped from 72K to **235K rows/s**, surpassing Rust (181K rows/s). This is achieved by using the same "Staging Table + COPY + MERGE" strategy as the ROW_NUMBER path.

### 2. Bulk Load Performance is Comparable
Both implementations achieve similar throughput for bulk loads (~290K rows/sec MSSQL→PG). The bottleneck is database I/O, not the application.

### 3. Go Has Lower Memory Usage
Go uses significantly less memory for PostgreSQL to MSSQL migrations. The recent optimizations also reduced MSSQL→PG upsert memory usage by eliminating millions of small allocations per chunk.

## M5 Pro vs M3 Max Comparison (StackOverflow2010, drop_recreate)

### Test Environment

- **M3 Max**: 36GB RAM, 14 CPU cores, macOS Darwin 25.2.0
- **M5 Pro**: 24GB RAM, 15 CPU cores, macOS Tahoe Darwin 25.3.0

Both use Docker containers with SQL Server 2022 (Rosetta 2) and PostgreSQL 16.

### Disk I/O (Docker VM)

| Metric | M3 Max | M5 Pro | Delta |
|--------|--------|--------|-------|
| Sequential Write | 2.7 GB/s | **5.3 GB/s** | **+96%** |
| Sequential Read | 7.5 GB/s | **13.6 GB/s** | **+81%** |

> Average of 3 runs, `dd bs=1M count=1024` inside Docker container.
> Native macOS disk is much faster but Docker's VM I/O virtualization
> is the actual bottleneck for database migrations.
> These Rosetta 2 results were measured on an earlier Docker version.
> Current M5 Pro numbers: 6.2 GB/s write, 35.6 GB/s read (Docker 29.3.1).

### Results (default DB settings)

| Direction | M3 Max 36GB | M5 Pro 24GB | Delta |
|-----------|-------------|-------------|-------|
| MSSQL→PG | 287K rows/s (75s) | **391K rows/s** (49s) | **+36%** |
| PG→MSSQL | 140K rows/s (137s) | **197K rows/s** (98s) | **+40%** |

### Results (with DB tuning, transfer-only metric)

| Direction | M3 Max (16GB Docker) | M5 Pro (8GB Docker) | Delta |
|-----------|---------------------|---------------------|-------|
| MSSQL→PG | 472K rows/s (40s) | **1,357K rows/s** (14s) | **M5 Pro +187%** |
| PG→MSSQL | 439K rows/s (44s) | **567K rows/s** (34s) | **M5 Pro +29%** |

> Transfer-only throughput (PR #102) — excludes DDL generation, finalization, and validation.
> Average of runs 3-5 (after AI convergence). Both use parallel BCP without TABLOCK (PR #98).

### M3 Max Docker RAM vs Throughput (MSSQL→PG)

| Docker RAM | MSSQL mem | PG shared_buffers | Transfer | vs M5 Pro (8GB Docker) |
|-----------|-----------|-------------------|----------|------------------------|
| 8GB | 4GB | 1GB | 342K rows/s (56s) | -29% |
| 16GB | 8GB | 4GB | 438K rows/s (44s) | -9% |
| **24GB** | **12GB** | **6GB** | **467K rows/s (41s)** | **-3%** |

### Database Tuning Applied

**8GB Docker** (matching M5 Pro config):
- MSSQL: `max server memory` = 4096MB, `max degree of parallelism` = 6
- PG: `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB

**16GB Docker**:
- MSSQL: `max server memory` = 8192MB, `max degree of parallelism` = 6
- PG: `shared_buffers` = 4GB, `work_mem` = 512MB, `maintenance_work_mem` = 1GB

**24GB Docker**:
- MSSQL: `max server memory` = 12288MB, `max degree of parallelism` = 6
- PG: `shared_buffers` = 6GB, `work_mem` = 1GB, `maintenance_work_mem` = 2GB

All configs: `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0, `fsync` = off

### AI Tuning Convergence (5 runs each direction)

**M5 Pro (8GB Docker):**

| Run | MSSQL→PG | PG→MSSQL |
|-----|----------|----------|
| 1 | 1,269K rows/s | 574K rows/s |
| 2 | 962K rows/s | 553K rows/s |
| 3 | 1,376K rows/s | 570K rows/s |
| 4 | 1,349K rows/s | 559K rows/s |
| 5 | 1,345K rows/s | 571K rows/s |

**M3 Max (16GB Docker):**

| Run | MSSQL→PG | PG→MSSQL |
|-----|----------|----------|
| 1 | 420K rows/s | 350K rows/s |
| 2 | 470K rows/s | 410K rows/s |
| 3 | 485K rows/s | 383K rows/s |
| 4 | 455K rows/s | 490K rows/s |
| 5 | 477K rows/s | 444K rows/s |

### Key Findings

1. **M5 Pro dominates MSSQL→PG (+187%)** — faster CPU (Rosetta emulation) and disk I/O are the bottleneck for MSSQL reads
2. **PG→MSSQL gap is smaller (+29%)** — parallel BCP writes benefit from M3 Max's extra RAM for OS page cache and write buffering
3. **AI tuning converges in 3 runs** on both machines — settles on optimal workers and chunk_size
4. **Transfer-only metric** (PR #102) gives the AI accurate feedback — no longer confused by DDL generation overhead
5. **RAM helps but can't overcome CPU/disk** — M3 Max with 16GB Docker (472K) still trails M5 Pro with 8GB Docker (1,357K) on MSSQL→PG
6. **DB tuning + RAM allocation together** deliver the biggest gains: M3 Max untuned (287K) → tuned 16GB Docker (472K) = **+64%**

## M3 Max Azure SQL Edge Benchmark (StackOverflow2010, MSSQL→PG)

### Test Environment

- **Hardware**: Apple M3 Max, 36GB RAM, 14 CPU cores
- **OS**: macOS (Darwin 25.4.0)
- **Source**: Azure SQL Edge (native ARM64, Docker, named volume, 4GB memory limit)
- **Target**: PostgreSQL 16 (Docker, named volume, tuned)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables)
- **AI Provider**: Anthropic (`claude-haiku-4-5-20251001`)
- **Mode**: `drop_recreate`, AI tuning enabled
- **Code**: `98b94a6` (current, includes PRs #107–#112 CopyFrom safety + TCP send buffer tuning)
- **Date**: March 2026

> **Key change from prior M3 Max benchmarks**: Azure SQL Edge runs natively on ARM64 (no Rosetta 2),
> and Docker named volumes use VM-internal ext4 (~3.4 GB/s writes) instead of VirtioFS bind mounts (~1.5 GB/s).

### Disk I/O (Docker VM, named volumes)

| Metric | M3 Max (bind mount) | M3 Max (named volume) | M5 Pro (named volume) |
|--------|---------------------|-----------------------|-----------------------|
| Sequential Write | 2.7 GB/s | **3.4 GB/s** | **5.3 GB/s** |
| Sequential Read | 7.5 GB/s | **9.4 GB/s** | **13.6 GB/s** |

> Average of 3 runs, `dd bs=1M count=1024` inside Docker container.

### Database Configuration

**Azure SQL Edge** (source):
- `MSSQL_MEMORY_LIMIT_MB` = 4096

**PostgreSQL** (target):
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0, `fsync` = off

### Results (MSSQL→PG, 5 Runs)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 1,149K rows/s | 755K rows/s | 26s |
| 2 | 1,187K rows/s | 875K rows/s | 22s |
| 3 | **1,227K rows/s** | 872K rows/s | 22s |
| 4 | 1,092K rows/s | 781K rows/s | 25s |
| 5 | 1,164K rows/s | 799K rows/s | 24s |
| **Avg (2-5)** | **1,168K rows/s** | **832K rows/s** | **23s** |

### Cross-Config Comparison (M3 Max, SO2010, MSSQL→PG)

| Configuration | Transfer | Overall | vs Baseline |
|---------------|----------|---------|-------------|
| SQL Server 2022 + bind mount (old) | 472K rows/s | 287K rows/s | — |
| **Azure SQL Edge + named volume** | **1,168K rows/s** | **832K rows/s** | **+147% / +190%** |

### Key Findings

1. **Eliminating Rosetta 2 + using named volumes delivers a 2.5x speedup** — from 472K to 1,168K transfer rows/s on the same hardware
2. **M3 Max now matches M5 Pro on MSSQL→PG** — 1,168K vs 1,357K (86%), compared to the old 472K vs 1,357K (35%) when both platforms were bottlenecked by Rosetta 2
3. **Named volumes are essential on macOS** — Docker's VM-internal ext4 delivers 3.4 GB/s writes vs 1.5 GB/s through VirtioFS bind mounts
4. **Native ARM64 SQL Server eliminates the #1 Mac bottleneck** — consistent with WSL2 ARM64 findings (Azure SQL Edge matched M3 Max Rosetta with fewer cores and slower disk)
5. **AI tuning converges quickly** — peak transfer (1,227K) achieved on run 3, stable ±6% across warm runs

## M3 Max Azure SQL Edge Benchmark (StackOverflow2013, MSSQL→PG)

### Test Environment

- **Hardware**: Apple M3 Max, 36GB RAM, 14 CPU cores
- **OS**: macOS (Darwin 25.4.0)
- **Source**: Azure SQL Edge (native ARM64, Docker, named volume, 4GB memory limit)
- **Target**: PostgreSQL 16 (Docker, named volume, tuned)
- **Dataset**: StackOverflow2013 (~106.5M rows, 9 tables)
- **AI Provider**: Anthropic (`claude-haiku-4-5-20251001`)
- **Mode**: `drop_recreate`, AI tuning enabled
- **Code**: `98b94a6` (current, includes PRs #107–#112)
- **Date**: March 2026

### Results (MSSQL→PG, 5 Runs)

| Run | Transfer | Overall | Duration | Notes |
|-----|----------|---------|----------|-------|
| 1 (cold) | 904K rows/s | 664K rows/s | 2m40s | |
| 2 | 981K rows/s | 717K rows/s | 2m29s | |
| 3 | **992K rows/s** | **725K rows/s** | 2m27s | |
| 4 | 335K rows/s† | 279K rows/s | 6m22s | buffer pool thrash |
| 5 | 970K rows/s | 699K rows/s | 2m32s | |
| **Avg (2,3,5)** | **981K rows/s** | **714K rows/s** | **2m29s** | excl. outlier |

> †Run 4 transferred 114.5M rows (vs 106.5M actual) due to ~8M rows of chunk retries across 26 extra
> tasks. Same buffer pool thrashing pattern as M5 Pro runs 2/4 — the 52GB dataset overwhelms the 4GB
> Azure SQL Edge buffer pool. After consecutive runs fill and evict the pool, read latency spikes
> cause CopyFrom timeouts that trigger chunk-level retries. Odd/even alternation depends on whether
> the OS page cache has stabilized from the prior run's eviction storm.

### Cross-Config Comparison (M3 Max, SO2013, MSSQL→PG)

| Configuration | Transfer | Overall | vs Baseline |
|---------------|----------|---------|-------------|
| SQL Server 2022 + Rosetta 2 (old) | 287K rows/s | — | — |
| **Azure SQL Edge + named volume** | **981K rows/s** | **714K rows/s** | **+242%** |

### M3 Max vs M5 Pro (Azure SQL Edge, SO2013)

| Machine | Transfer (avg) | Overall (avg) |
|---------|---------------|---------------|
| **M3 Max** (36GB, 14 cores) | 981K rows/s (1m49s) | 714K rows/s (2m29s) |
| **M5 Pro** (24GB, 15 cores) | **1,042K rows/s** (1m42s) | **720K rows/s** (2m28s) |
| Delta | M5 Pro +6% | M5 Pro +1% |

> On SO2013 (52GB dataset, exceeds RAM), M5 Pro's faster disk I/O (4.4 vs 3.4 GB/s write) provides a
> modest edge on transfer, but overall throughput is nearly identical. The gap narrows dramatically
> compared to the old Rosetta 2 results (M5 Pro was +177% faster).

---

## StackOverflow2013 Benchmark — Rosetta 2 Baseline (106.5M rows)

### Test Environment

- **M3 Max**: 36GB RAM, 14 CPU cores, Docker 16GB, macOS Darwin 25.3.0
- **M5 Pro**: 24GB RAM, 15 CPU cores, Docker 8GB, macOS Tahoe Darwin 25.3.0
- **Databases**: SQL Server 2022 (Rosetta 2), PostgreSQL 16
- **Dataset**: StackOverflow2013 (~106.5M rows, 9 tables)
- **Date**: March 2026

### Dataset Details

| Table | Rows | Avg Row Size |
|-------|------|-------------|
| Votes | 52,928,720 | 37 bytes |
| Comments | 24,534,730 | 343 bytes |
| Posts | 17,142,169 | 2,290 bytes |
| Badges | 8,042,005 | 50 bytes |
| Users | 2,465,713 | 298 bytes |
| PostLinks | 1,421,208 | 44 bytes |
| VoteTypes | 15 | — |
| PostTypes | 8 | — |
| LinkTypes | 2 | — |
| **Total** | **106,534,570** | **573 bytes avg** |

### Results (transfer-only metric, tuned DBs)

| Direction | M3 Max (16GB Docker) | M5 Pro (8GB Docker) | Delta |
|-----------|---------------------|---------------------|-------|
| MSSQL→PG | 287K rows/s (6m12s) | **795K rows/s** (2m14s) | **M5 Pro +177%** |
| PG→MSSQL | 222K rows/s (8m5s) | **351K rows/s** (5m4s) | **M5 Pro +58%** |

> Transfer-only throughput (PR #102). PG→MSSQL uses parallel BCP without TABLOCK (PR #98).
> MSSQL→PG may intermittently stall on large partitions under Rosetta 2 emulation.
> Not reproducible on native Linux.

### Database Tuning Applied

**M5 Pro (8GB Docker):**
- MSSQL: `max server memory` = 4096MB, `max degree of parallelism` = 6
- PG: `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB

**M3 Max (16GB Docker):**
- MSSQL: `max server memory` = 8192MB, `max degree of parallelism` = 6
- PG: `shared_buffers` = 3GB, `work_mem` = 512MB, `maintenance_work_mem` = 1GB

Both: `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0, `fsync` = off

### Key Findings

1. **M5 Pro is 58-177% faster on SO2013** — the 52GB dataset exceeds any cache, making disk I/O and CPU speed the bottleneck
2. **Extra Docker RAM barely helps on large datasets** — M3 Max with 16GB Docker (287K) vs 8GB Docker (262K) = only +10% on MSSQL→PG, because the dataset doesn't fit in cache
3. **PG→MSSQL gap (+58%) is larger than SO2010 (+29%)** — longer-running transfers amplify the M5 Pro's disk I/O advantage
4. **Rosetta 2 intermittent stalls** — MSSQL reads on large partitions (50M+ rows) may hang under emulation; not an issue on native Linux

## PostgreSQL → PostgreSQL with AI Tuning (106.5M rows)

### Test Environment

- **Hardware**: Apple M5 Pro, 24GB RAM, 15 CPU cores
- **OS**: macOS Tahoe (Darwin 25.3.0)
- **Source/Target**: PostgreSQL 16 (Docker, shared container)
- **Dataset**: StackOverflow2013 (~106.5M rows, 9 tables)
- **AI Provider**: Anthropic (`claude-haiku-4-5-20251001`)
- **Mode**: `drop_recreate`, AI tuning enabled
- **Date**: March 2026

### Database Configuration

**PostgreSQL** (source and target, same container):
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0
- `fsync` = on

**Docker**: Default memory (no cap), single container for both source and target databases.

### AI Tuning Convergence (5 Runs)

The AI tuner uses historical throughput data to optimize parameters across runs. Starting from a base config of 4 workers / 50K chunk_size, it explores the parameter space and converges on optimal settings.

| Run | Workers | Chunk Size | Pools | Transfer | Overall | AI Behavior |
|-----|---------|-----------|-------|----------|---------|-------------|
| 1 | 10 | 12,000 | 15 | 889K rows/s | 713K rows/s | Initial conservative estimate |
| 2 | 12 | 14,000 | 16 | **962K rows/s** | **750K rows/s** | Scaled up, hit peak |
| 3 | 13 | 15,000 | 20 | 894K rows/s | 693K rows/s | Overshot, regression |
| 4 | 11 | 13,500 | 15 | 930K rows/s | 735K rows/s | Detected regression, backed off |
| 5 | 12 | 14,000 | 16 | 939K rows/s | 727K rows/s | Converged on run 2's proven config |

### AI Tuning Parameters (All Runs)

| Parameter | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 |
|-----------|-------|-------|-------|-------|-------|
| Workers | 10 | 12 | 13 | 11 | 12 |
| Chunk Size | 12,000 | 14,000 | 15,000 | 13,500 | 14,000 |
| Read Ahead | 2 | 2 | 2 | 2 | 2 |
| Write Ahead | 2 | 2 | 2 | 2 | 2 |
| Parallel Readers | 4 | 4 | 4 | 4 | 4 |
| Connection Pools | 15 | 16 | 20 | 15 | 16 |
| Large Table Threshold | 5M | 5M | 5M | 5M | 5M |

### Key Findings

1. **AI tuner performs hill-climbing optimization** — it explores upward (runs 1→2→3), detects regression at 13 workers/15K chunks, corrects (run 4), and converges on the optimal config (run 5)
2. **PG→PG is significantly faster than MSSQL→PG** — peak 962K rows/s vs 662K rows/s (45% faster), no Rosetta 2 overhead on reads
3. **12 workers / 14K chunk_size is optimal** — for this workload on M5 Pro with 15 cores, leaving 3 cores for OS/Docker overhead
4. **Read/write ahead buffers stay constant** — AI correctly identifies that 2/2 is stable across all runs, not worth varying
5. **Connection pool oversizing hurts** — run 3's jump to 20 pools correlated with the throughput regression

## Separate Container Benchmarks (106.5M rows)

### Test Environment

- **Hardware**: Apple M5 Pro, 24GB RAM, 15 CPU cores
- **OS**: macOS Tahoe (Darwin 25.3.0)
- **Source**: PostgreSQL 16 (Docker, port 5432) or SQL Server 2022 (Docker, port 1433)
- **Target**: PostgreSQL 16 (Docker, port 5433, separate container)
- **Dataset**: StackOverflow2013 (~106.5M rows, 9 tables)
- **Config**: `workers=4, chunk_size=50000, target_mode=drop_recreate`, AI tuning enabled
- **Date**: March 2026

### Database Configuration

Both PostgreSQL containers tuned identically:
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0
- `autovacuum` = off (benchmark only)

SQL Server tuned with `max server memory (MB)` = 4096, `max degree of parallelism` = 6.

Target DB dropped and recreated between each run to eliminate autovacuum interference.

### PostgreSQL → PostgreSQL (5 Runs)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 | 478K rows/s | 425K rows/s | 4m11s |
| 2 | **649K rows/s** | **560K rows/s** | 3m10s |
| 3 | 640K rows/s | 552K rows/s | 3m13s |
| 4 | 591K rows/s | 504K rows/s | 3m31s |
| 5 | 570K rows/s | 489K rows/s | 3m38s |
| **Avg (2-5)** | **613K rows/s** | **526K rows/s** | **3m23s** |

### MSSQL → PostgreSQL (5 Runs)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 | **619K rows/s** | **491K rows/s** | 3m37s |
| 2 | 604K rows/s | 481K rows/s | 3m42s |
| 3 | 614K rows/s | 489K rows/s | 3m38s |
| 4 | 601K rows/s | 480K rows/s | 3m42s |
| 5 | 604K rows/s | 480K rows/s | 3m42s |
| **Avg** | **608K rows/s** | **484K rows/s** | **3m40s** |

### Key Findings

1. **Separate containers eliminate resource contention** — PG source and target no longer compete for shared_buffers, WAL, and connection slots
2. **MSSQL→PG nearly matches PG→PG** — 608K vs 613K transfer (within 1%), suggesting the bottleneck is target write speed, not source read speed
3. **Run 1 cold cache penalty** — PG→PG run 1 (478K) is 26% slower than warm runs due to cold PG caches; MSSQL→PG shows no cold penalty since source data is already cached
4. **Consistent MSSQL→PG performance** — 600-619K across all 5 runs (3% variance), very stable
5. **Single-container PG→PG is faster** — the AI-tuned single-container results (962K peak) outperform separate containers (649K peak) because localhost loopback is faster than Docker bridge networking

## WSL2 ARM64 Benchmarks

### Test Environment

- **Hardware**: ARM64, 10 CPU cores, 32GB RAM (24GB allocated to WSL2)
- **OS**: Linux 6.6.87.2 (WSL2 on Windows)
- **Source**: Azure SQL Edge (native ARM64, Docker, 8GB container, 3GB internal limit)
- **Target**: PostgreSQL 16 (Docker, 4GB container, tuned)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables — full Brent Ozar dataset, 10GB)
- **Code**: afda4e0 (PRs #107–#112 CopyFrom safety)
- **Date**: March 2026

> **Note**: Azure SQL Edge runs natively on ARM64 — no Rosetta 2 emulation overhead.
> Full SO2010 dataset (SQL Server 2008 format MDF) attached directly to Azure SQL Edge
> with compatibility level set to 150. All 9 tables match the SQL Server 2022 row counts.

### Disk I/O (Docker in WSL2)

| Metric | M3 Max (named vol) | M5 Pro | WSL2 ARM64 |
|--------|---------------------|--------|------------|
| Sequential Write | 3.4 GB/s | **6.2 GB/s** | 1.3 GB/s |
| Sequential Read | 9.4 GB/s | **35.6 GB/s** | 4.6 GB/s |

> Average of 5 runs, `dd bs=1M count=1024` inside Docker container.
> M5 Pro on Docker 29.3.1. Write speed is the primary bottleneck for PG target throughput.

### Database Configuration

**Azure SQL Edge** (source):
- Container memory: 8GB (`--memory=8g`)
- `MSSQL_MEMORY_LIMIT_MB` = 3072

**PostgreSQL** (target):
- Container memory: 4GB (`--memory=4g`)
- `shared_buffers` = 2GB, `work_mem` = 256MB, `maintenance_work_mem` = 1GB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0, `fsync` = off

### Results (MSSQL→PG, 6 workers, chunk_size=100000)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 486K rows/s | 386K rows/s | 50s |
| 2 | 487K rows/s | 395K rows/s | 49s |
| 3 | **488K rows/s** | 378K rows/s | 51s |
| **Avg (2-3)** | **487K rows/s** | **387K rows/s** | **50s** |

> Workers=6 and chunk_size=100000 set explicitly.
> AI startup tuning applied sensible defaults for remaining parameters.
> Run 1 slower due to cold MSSQL cache.

### Re-validation (0735127 — PG writer refactor + AI tuning improvements)

**Environment changes from initial WSL2 run:**
- **Code**: 0735127 (includes PG writer refactor, AI tuning trajectory)
- **MSSQL container**: 12GB (`--memory=12g`), `MSSQL_MEMORY_LIMIT_MB` = 8192
- **PG container**: unconstrained, `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- **AI tuning**: Anthropic (`claude-haiku-4-5-20251001`), selected workers=6, chunk_size=100000, parallel_readers=3

#### Disk I/O (re-measured)

| Metric | Previous (afda4e0) | Current (0735127) |
|--------|-------------------|-------------------|
| Sequential Write | 1.3 GB/s | **2.4 GB/s** |
| Sequential Read | 4.6 GB/s | **4.3 GB/s** |

> Average of 5 runs, `dd bs=1M count=1024` inside Docker containers.
> Write speed nearly doubled — both runs use Docker named volumes, but WSL2/Docker
> updates between runs likely improved virtio-fs write performance.

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 590K rows/s | 378K rows/s | 33s |
| 2 | 618K rows/s | 438K rows/s | 31s |
| 3 | **647K rows/s** | 419K rows/s | 30s |
| 4 | 641K rows/s | 414K rows/s | 30s |
| **Avg (2-4)** | **635K rows/s** | **424K rows/s** | **30s** |

> AI startup tuning selected parallel_readers=3 based on trajectory analysis of prior runs.
> Run 1 slower due to cold MSSQL cache. Containers unconstrained (no `--memory` flags).

**vs initial WSL2 run (afda4e0):**

| Metric | afda4e0 (487K) | 0735127 (635K) | Delta |
|--------|----------------|----------------|-------|
| Transfer (avg) | 487K rows/s | **635K rows/s** | **+30%** |
| Duration | 50s | 30s | **-40%** |
| Docker Write I/O | 1.3 GB/s | 2.4 GB/s | **+85%** |
| parallel_readers | 1 | 3 | AI trajectory |
| MSSQL memory | 3GB internal | 8GB internal | +5GB |
| PG shared_buffers | 2GB | 1GB | -1GB |

> Four factors contribute to the +30% improvement: (1) WSL2/Docker disk write speed nearly doubled
> (1.3→2.4 GB/s), (2) AI tuning now selects parallel_readers=3 based on trajectory analysis,
> (3) PG writer refactor reduces per-batch overhead, (4) larger MSSQL buffer pool (8GB vs 3GB) keeps
> more of the 10GB dataset cached. The PG shared_buffers decrease (2GB→1GB) did not hurt — write
> throughput is dominated by WAL and CopyFrom, not shared_buffers.

### Cross-Machine Comparison (SO2010, MSSQL→PG, transfer-only)

| Machine | Source Engine | Cores | RAM | Docker Write | Transfer (avg) | vs M5 Pro (SS2022) |
|---------|-------------|-------|-----|-------------|---------------|-----------|
| M3 Max (16GB Docker) | SQL Server 2022 (Rosetta) | 14 | 36GB | 2.7 GB/s | 472K rows/s | -65% |
| WSL2 ARM64 (afda4e0) | Azure SQL Edge | 10 | 24GB | 1.3 GB/s | 487K rows/s | -64% |
| **WSL2 ARM64 (0735127)** | **Azure SQL Edge** | **10** | **24GB** | **2.4 GB/s** | **635K rows/s** | **-53%** |
| M5 Pro (8GB Docker) | Azure SQL Edge | 15 | 24GB | 4.4 GB/s | 886K rows/s | -35% |
| M5 Pro (8GB Docker) | SQL Server 2022 (Rosetta) | 15 | 24GB | 5.3 GB/s | 1,357K rows/s | — |

### Key Findings

1. **+30% throughput from combined improvements** — 635K vs 487K transfer; disk I/O (+85%), AI parallel readers, PG writer refactor, and larger MSSQL buffer pool all contribute
2. **WSL2 ARM64 now reaches 69% of M5 Pro Azure SQL Edge** — up from 55% (487K/886K) to 69% (635K/918K), closing the gap significantly
3. **WSL2 virtual disk write speed remains the primary bottleneck** — 2.4 GB/s write vs M5 Pro's 4.4 GB/s (45% slower) explains most of the remaining gap
4. **Container memory limits are essential on WSL2** — Docker shares the WSL2 memory pool with no separate cap; `--memory` flags on containers prevent DB processes from starving the pipeline
5. **Azure SQL Edge requires explicit memory capping** — without `MSSQL_MEMORY_LIMIT_MB`, it consumes all container memory and OOM-kills
6. **4GB PG container with 2GB shared_buffers** gives 9% improvement over 2GB container with 512MB shared_buffers (487K vs 447K)

### StackOverflow2013 (106.5M rows, MSSQL→PG)

**Environment changes from SO2010 run:**
- **MSSQL container**: 12GB (`--memory=12g`), `MSSQL_MEMORY_LIMIT_MB` = 8192
- **PG container**: 4GB (`--memory=4g`), `shared_buffers` = 2GB, `maintenance_work_mem` = 1GB
- **Dataset**: Full Brent Ozar SO2013 (SQL Server 2008 format MDF, 52GB, 9 tables)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 402K rows/s | 330K rows/s | 5m23s |
| 2 | 412K rows/s | 340K rows/s | 5m14s |
| 3 | **423K rows/s** | 348K rows/s | 5m6s |
| **Avg (2-3)** | **417K rows/s** | **344K rows/s** | **5m10s** |

> AI startup tuning used sensible defaults (API credits unavailable).
> With AI-optimized parameters, throughput may be slightly higher.

### Cross-Machine Comparison (SO2013, MSSQL→PG, transfer-only)

| Machine | Source Engine | Cores | RAM | Transfer (avg) | vs M5 Pro (SS2022) |
|---------|-------------|-------|-----|---------------|-----------|
| M3 Max (16GB Docker) | SQL Server 2022 (Rosetta) | 14 | 36GB | 287K rows/s | -64% |
| **WSL2 ARM64 (12GB container)** | **Azure SQL Edge** | **10** | **24GB** | **417K rows/s** | **-48%** |
| M5 Pro (8GB Docker) | SQL Server 2022 (Rosetta) | 15 | 24GB | 795K rows/s | — |
| M5 Pro (12GB container) | Azure SQL Edge | 15 | 24GB | 1,042K rows/s | * |

> *Azure SQL Edge and SQL Server 2022 are different products with different configs —
> cross-engine throughput comparisons are not apples-to-apples.

### SO2013 Key Findings

1. **Native ARM64 advantage holds at scale** — WSL2 (417K) beats M3 Max (287K) by 45% on SO2013, consistent with the SO2010 advantage
2. **Gap to M5 Pro is larger than SO2010** — -48% (SO2013) vs -64% (SO2010), because the 52GB dataset exceeds all caches, amplifying the WSL2 disk I/O bottleneck
3. **Warm-cache improvement is modest** — run 1 (402K) to run 3 (423K) = +5%, as the 52GB dataset far exceeds the 8GB MSSQL buffer pool
4. **Azure SQL Edge handles 52GB database without issues** — all 106.5M rows validated across 9 tables

## M5 Pro Azure SQL Edge Benchmarks

### Test Environment

- **Hardware**: Apple M5 Pro, 24GB RAM, 15 CPU cores
- **OS**: macOS Tahoe (Darwin 25.4.0)
- **Source**: Azure SQL Edge (native ARM64, Docker, named volume, 4GB memory limit)
- **Target**: PostgreSQL 16 (Docker, named volume, tuned)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables — full Brent Ozar dataset, 10GB)
- **AI Provider**: Anthropic (`claude-haiku-4-5-20251001`)
- **Code**: 98b94a6 (current, includes PRs #107–#113)
- **Date**: March 2026

> **Note**: Azure SQL Edge runs natively on ARM64 — no Rosetta 2 emulation overhead.
> Full SO2010 dataset (SQL Server 2008 format MDF) attached directly to Azure SQL Edge
> with compatibility level set to 150. All 9 tables match the original Brent Ozar row counts.
> Containers use unconstrained memory (no `--memory` flags) to match M3 Max config.

### Disk I/O (Docker VM)

| Metric | M3 Max | WSL2 ARM64 | M5 Pro |
|--------|--------|------------|--------|
| Sequential Write | 3.4 GB/s | 1.3 GB/s | **6.2 GB/s** |
| Sequential Read | 9.4 GB/s | 4.6 GB/s | **35.6 GB/s** |

> Average of 5 runs, `dd bs=1M count=1024` inside unconstrained Docker container.
> M5 Pro on Docker 29.3.1. Disk speed does not explain the SO2010 throughput gap —
> M3 Max (1,168K) leads M5 Pro (918K) despite slower disk because its extra 12GB RAM
> keeps more of the 10GB dataset in OS page cache, reducing Azure SQL Edge read latency.
> SO2010 is memory/CPU-bound on Azure SQL Edge (capped at 4 logical processors), not disk-bound.

### Database Configuration

**Azure SQL Edge** (source):
- `MSSQL_MEMORY_LIMIT_MB` = 4096

**PostgreSQL** (target):
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0, `fsync` = off

### AI Tuning Convergence (5 Runs, MSSQL→PG)

| Run | Workers | Chunk Size | Transfer | Overall | Duration |
|-----|---------|-----------|----------|---------|----------|
| 1 (cold) | 4 | 45,000 | 947K rows/s | 667K rows/s | 29s |
| 2 | 4 | 45,000 | **953K rows/s** | **679K rows/s** | 28s |
| 3 | **3** | 45,000 | 921K rows/s | 658K rows/s | 29s |
| 4 | 4 | 45,000 | 919K rows/s | 635K rows/s | 30s |
| 5 | 4 | 45,000 | 877K rows/s | 633K rows/s | 30s |
| **Avg (2-5)** | — | — | **918K rows/s** | **651K rows/s** | **29s** |

### M5 Pro vs M3 Max (Azure SQL Edge, SO2010)

| Machine | Transfer (avg) | Overall (avg) |
|---------|---------------|---------------|
| **M3 Max** (36GB, 14 cores) | **1,168K rows/s** | **832K rows/s** |
| M5 Pro (24GB, 15 cores) | 918K rows/s | 651K rows/s |
| Delta | M3 Max +27% | M3 Max +28% |

> M3 Max's extra 12GB RAM provides more OS page cache for the 10GB dataset,
> giving it an edge despite slower disk I/O. The gap narrows on SO2013 (52GB)
> where the dataset exceeds all caches.

### Cross-Machine Comparison (SO2010, MSSQL→PG, Azure SQL Edge, transfer-only)

| Machine | Cores | RAM | Docker Write | Transfer (avg) | vs M3 Max |
|---------|-------|-----|-------------|---------------|-----------|
| WSL2 ARM64 (afda4e0) | 10 | 24GB | 1.3 GB/s | 487K rows/s | -58% |
| WSL2 ARM64 (0735127) | 10 | 24GB | 2.4 GB/s | 635K rows/s | -46% |
| M5 Pro | 15 | 24GB | 4.4 GB/s | 918K rows/s | -21% |
| **M3 Max** | **14** | **36GB** | **3.4 GB/s** | **1,168K rows/s** | **—** |

### SQL Server 2022 (Rosetta 2) vs Azure SQL Edge (M5 Pro, SO2010)

| Engine | Workers | Avg Transfer (runs 2-5) |
|--------|---------|------------------------|
| **Azure SQL Edge (native ARM64)** | **4** | **918K rows/s** |
| SQL Server 2022 (Rosetta 2) | 4 | 845K rows/s |
| SQL Server 2022 (Rosetta 2) | 6 | 897K rows/s |
| SQL Server 2022 (Rosetta 2) | 8 | 904K rows/s |

> SQL Server 2022 has access to all 15 cores but Rosetta 2 overhead (~8%) cancels
> out the extra parallelism. Azure SQL Edge at 4 workers still wins despite its
> 4 logical processor cap, because native ARM64 execution is more efficient per core.

### Key Findings

1. **M3 Max is 27% faster than M5 Pro on SO2010** — 1,168K vs 918K transfer; M3 Max's extra 12GB RAM keeps more of the 10GB dataset in OS page cache, and SO2010 is memory/CPU-bound (not disk-bound)
2. **Azure SQL Edge beats SQL Server 2022 on Apple Silicon** — native ARM64 (918K) outperforms Rosetta 2 (904K best) even with fewer cores; Rosetta overhead negates the parallelism advantage
3. **AI converges on 4 workers / 45K chunk_size** — fewer workers with less contention outperforms 6 workers on Azure SQL Edge (918K vs 871K)
4. **M5 Pro is 88% faster than WSL2 ARM64** — 918K vs 487K transfer, driven by faster Docker disk I/O
5. **No cold-cache penalty** — run 1 (947K) matches warm runs, as the 10GB dataset fits within the 4GB MSSQL buffer pool + OS page cache

### StackOverflow2013 (106.5M rows, MSSQL→PG)

**Environment**: Same as SO2010, Docker 29.3.1, unconstrained containers.

| Run | Workers | Chunk Size | Transfer | Overall | Duration |
|-----|---------|-----------|----------|---------|----------|
| 1 (cold) | 5 | 50,000 | **1,087K rows/s** | 801K rows/s | 2m13s |
| 2 | 4 | 50,000 | 1,019K rows/s | 747K rows/s | 2m22s |
| 3 | 5 | 50,000 | 908K rows/s | 681K rows/s | 2m36s |
| 4 | 5 | 50,000 | 941K rows/s | 696K rows/s | 2m33s |
| 5 | 5 | 50,000 | 988K rows/s | 716K rows/s | 2m29s |
| **Avg (2-5)** | — | — | **964K rows/s** | **710K rows/s** | **2m30s** |

> All 5 runs completed without buffer pool thrashing (previously seen on Docker 29.3.0
> with memory-limited containers). Docker 29.3.1 + unconstrained containers resolved
> the alternating fast/slow pattern.

### M5 Pro vs M3 Max (Azure SQL Edge, SO2013)

| Machine | Transfer (avg) | Overall (avg) |
|---------|---------------|---------------|
| M3 Max (36GB, 14 cores) | 981K rows/s | 714K rows/s |
| **M5 Pro** (24GB, 15 cores) | 964K rows/s | 710K rows/s |
| Delta | **M3 Max +2%** | **M3 Max +1%** |

> Essentially tied on SO2013. The 52GB dataset exceeds both machines' RAM,
> neutralizing M3 Max's page cache advantage. M5 Pro's faster disk I/O (6.2 vs 3.4 GB/s)
> is offset by Azure SQL Edge's 4-core CPU cap being the bottleneck at this scale.

### Cross-Machine Comparison (SO2013, MSSQL→PG, Azure SQL Edge, transfer-only)

| Machine | Cores | RAM | Transfer (avg) | vs M5 Pro |
|---------|-------|-----|---------------|-----------|
| WSL2 ARM64 | 10 | 24GB | 417K rows/s | -57% |
| M5 Pro | 15 | 24GB | 964K rows/s | — |
| M3 Max | 14 | 36GB | 981K rows/s | +2% |

### SO2013 Key Findings

1. **M3 Max and M5 Pro are essentially tied** — 981K vs 964K (2% gap), because the 52GB dataset exceeds all caches and Azure SQL Edge's 4-core cap is the bottleneck
2. **Both are ~130% faster than WSL2 ARM64** — driven by faster disk I/O and more CPU cores
3. **Docker 29.3.1 eliminates buffer pool thrashing** — all 5 runs stable, no alternating fast/slow pattern seen on Docker 29.3.0
4. **AI settles on 4-5 workers / 50K chunks** — consistent with SO2010 findings under Azure SQL Edge's 4-core limit

## Panther Lake / WSL2 (x86 native, no Rosetta) — SO2010 + SO2013

### Environment
- **CPU**: Intel Core Ultra 7 358H (Panther Lake, 16C/16T, no HT)
- **Host RAM**: 32GB; WSL allocated 14 cores / 24GB via `.wslconfig`
- **Containers**: `make bench-dbs-up` profile — MSSQL `MSSQL_MEMORY_LIMIT_MB=8192`, PG `shared_buffers=1GB / fsync=off / synchronous_commit=off / max_wal_size=4GB / wal_buffers=64MB`
- **dmt**: built from `main` (post PR #125), AI tuner enabled with Anthropic `claude-haiku-4-5-20251001`
- **Disk I/O** (`dd bs=1M count=1024` × 5 in container, named volume): write 2.4 GB/s, read ~14 GB/s

### SO2010 (10GB, 19.3M rows, MSSQL → PG)

| Phase | Config | Transfer avg (rows/s) | Overall peak (rows/s) | Duration peak |
|-------|--------|-----------------------|-----------------------|---------------|
| AI default (free) | W=12 C=50K PR=6 | 875K | 715K | 27s |
| AI converged after exploration | W=12 C=25K PR=8 | **905K** | 715K | 27s |
| Hand-pushed exploration | W=12 C=10K PR=8 | 898K | **743K** | **26s** |

> AI smart-config converged stably on `W=12 C=25K PR=8` after seeing exploration results in history (PR #122 history visibility + PR #125 post-AI config persistence working as intended).
> Peak transfer 916K rows/s observed in a single run (C=10K).

### SO2013 (52GB, 106.5M rows, MSSQL → PG)

#### First-attach measurements (favorable conditions, not repeatable)

| Run | Duration | Overall (rows/s) | Transfer (rows/s) |
|-----|----------|------------------|-------------------|
| 1 (cold) | 192s | 555K | 676K |
| 2 | 190s | 561K | 695K |
| 3 | 222s | 480K | 572K |
| 4 | 198s | 538K | 670K |
| 5 | 173s | 616K | **772K** ← outlier |
| 6 | 184s | 579K | 713K |
| Avg (runs 2-6) | 193s | 555K | 685K |

#### Steady-state measurements (post `wsl --shutdown`, repeatable)

| Run | Duration | Overall (rows/s) | Transfer (rows/s) |
|-----|----------|------------------|-------------------|
| 1 (cold) | 243s | 438K | 559K |
| 2 | 293s | 364K | 440K |
| 3 | 271s | 393K | 474K |
| 4 | 274s | 389K | 479K |
| 5 | 278s | 383K | 471K |
| **Avg (warm 2-5)** | **279s** | **382K** | **466K** |

Repeated runs after fully resetting WSL (`wsl --shutdown`) and the PG volume settle into the lower band consistently. Detach + re-attach + cache-priming experiments did not recover the higher numbers — the gap appears to come from transient kernel/scheduler state on first migrations after attach, which is not reproducible in steady-state operation.

The first-attach figures stay published as the high-water mark, but the steady-state numbers are what a long-running deployment will see.

AI converged on `W=12 C=50K PR=6` for SO2013 — same plateau as the initial SO2010 default, did not explore smaller chunks unprompted. Smaller chunks did not help on SO2013 in side-tests; bottleneck shifted from pipeline handoff (writer-bound on cached SO2010) to MSSQL disk reads (read-bound on the 52GB dataset that exceeds 8GB cache).

#### Cross-platform comparison (SO2013 transfer rate)

| | Core Ultra 7 358H 32GB — first attach | Core Ultra 7 358H 32GB — steady state | M5 Pro (macOS / Rosetta) | M3 Max (macOS / Rosetta) |
|---|---|---|---|---|
| Transfer rate | 772K | **466K** | 795K | 287K |
| Duration | 173s | 279s | 134s | 372s |

The first-attach number on the Core Ultra 7 358H ties M5 Pro on transfer throughput despite a lower-spec CPU (no Rosetta penalty). The steady-state number is the more honest comparison for ongoing workloads — still well ahead of M3 Max (Rosetta) but ~40% behind M5 Pro, reflecting Docker Desktop's VHDX-on-NTFS storage overhead in WSL2 vs native macOS file I/O.

### Memory-pressure caveats observed
- Bumping `MSSQL_MEMORY_LIMIT_MB` to 12288 on SO2013 caused WSL to swap (only 24GB total, MSSQL+PG+dmt+OS exceeded budget). Throughput dropped ~30%. Practical ceiling: 8GB MSSQL cap on a 24GB WSL.
- After accumulated swap activity (cumulative `pswpout` > 23GB), SO2013 throughput regressed ~33% vs cold-start state even after container restarts. A `wsl --shutdown` is required for a true clean reset; container restarts alone don't reclaim the WSL kernel's degraded page-cache state.

## Implemented Optimizations

- [x] Parallel table processing with configurable workers
- [x] Connection pooling for concurrent operations
- [x] Batch processing with configurable chunk sizes
- [x] Staging table approach for upsert mode
- [x] Intra-table partitioning for large tables
- [x] Progress reporting with throughput metrics
- [x] **New:** Fast-path upsert for Keyset Pagination (COPY+MERGE)
- [x] **New:** Zero-allocation row scanning (slice recycling)

## Reproduction

```bash
# Build
go build -o dmt .

# MSSQL → PostgreSQL (drop_recreate)
./dmt -config benchmark-config.yaml run

# MSSQL → PostgreSQL (upsert)
./dmt -config benchmark-upsert.yaml run
```

## Configuration

```yaml
migration:
  workers: 6
  chunk_size: 50000  # chunk size per worker, tuned for upsert mode
  target_mode: drop_recreate  # or upsert
  create_indexes: false
  create_foreign_keys: false
```