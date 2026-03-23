# Performance Benchmarks

Comprehensive benchmark results comparing Go and Rust implementations.

## Test Environment

- **Hardware**: Apple M3 Max, 36GB RAM, 14 CPU cores
- **OS**: macOS (Darwin 25.2.0)
- **Databases**: SQL Server 2022, PostgreSQL 15 (Docker containers)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables)
- **Date**: January 2026

> **Note**: SQL Server runs under Rosetta 2 emulation on Apple Silicon, adding overhead. Production Linux deployments will be faster.

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

- **M3 Max**: 36GB RAM, 14 CPU cores, macOS Darwin 25.2.0, January 2026
- **M5 Pro**: 24GB RAM, 15 CPU cores, macOS Tahoe Darwin 25.3.0, March 2026

Both use Docker containers with SQL Server 2022 (Rosetta 2) and PostgreSQL 16.

### Results (default DB settings)

| Direction | M3 Max 36GB | M5 Pro 24GB | Delta |
|-----------|-------------|-------------|-------|
| MSSQL→PG | 287K rows/s (75s) | **391K rows/s** (49s) | **+36%** |
| PG→MSSQL | 140K rows/s (137s) | **197K rows/s** (98s) | **+40%** |

### Results (with DB tuning, 8GB Docker, transfer-only metric)

| Direction | M5 Pro (tuned) | Duration | Notes |
|-----------|----------------|----------|-------|
| MSSQL→PG | **1,357K rows/s** | 14s | AI converged: workers=10, chunk=8192 |
| PG→MSSQL | **567K rows/s** | 34s | AI converged: workers=10, chunk=8192 |

> Transfer-only throughput (PR #102) — excludes DDL generation, finalization, and validation.
> Average of runs 3-5 (after AI convergence). Both use parallel BCP without TABLOCK (PR #98).
> M3 Max results need re-testing with transfer-only metric for fair comparison.

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

| Run | MSSQL→PG | PG→MSSQL |
|-----|----------|----------|
| 1 | 1,269K rows/s | 574K rows/s |
| 2 | 962K rows/s | 553K rows/s |
| 3 | 1,376K rows/s | 570K rows/s |
| 4 | 1,349K rows/s | 559K rows/s |
| 5 | 1,345K rows/s | 571K rows/s |

### Key Findings

1. **AI tuning converges in 3 runs** — settles on workers=10, chunk_size=8192 for both directions
2. **Transfer-only metric** (PR #102) gives the AI accurate feedback — no longer confused by DDL generation overhead
3. **1.36M rows/s MSSQL→PG** — 4.7x faster than original M3 Max baseline (287K) due to memory guardrail, parallel BCP, and AI tuning improvements
4. **567K rows/s PG→MSSQL** — 4x faster than original M3 Max baseline (140K) due to parallel BCP without TABLOCK
5. **RAM matters when you give it to the databases** — M3 Max closed the MSSQL→PG gap from -64% to -3% by increasing Docker RAM from 8GB to 24GB
6. **DB tuning + RAM allocation together** deliver the biggest gains

## StackOverflow2013 Benchmark (106.5M rows)

### Test Environment

- **Hardware**: Apple M5 Pro, 24GB RAM, 15 CPU cores
- **OS**: macOS Tahoe (Darwin 25.3.0)
- **Databases**: SQL Server 2022, PostgreSQL 16 (Docker 8GB)
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

### Results (transfer-only metric, tuned DBs, 8GB Docker)

| Direction | Transfer | Duration | Notes |
|-----------|----------|----------|-------|
| MSSQL→PG | **795K rows/s** | 2m14s | workers=8, chunk=8192 |
| PG→MSSQL | **351K rows/s** | 5m04s | workers=8, chunk=8192, parallel BCP |

> Transfer-only throughput (PR #102). PG→MSSQL uses parallel BCP without TABLOCK (PR #98).
> MSSQL→PG may intermittently stall on large partitions under Rosetta 2 emulation.
> Not reproducible on native Linux.

### Database Tuning Applied

**SQL Server**:
- `max server memory (MB)` = 4096, `max degree of parallelism` = 6

**PostgreSQL**:
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off, `wal_level` = minimal, `max_wal_senders` = 0

### Key Findings

1. **795K rows/s MSSQL→PG** — 2.8x faster than original baseline (287K) due to memory guardrail and AI tuning improvements
2. **351K rows/s PG→MSSQL** — 2.5x faster than original baseline (140K) due to parallel BCP
3. **Rosetta 2 intermittent stalls** — MSSQL reads on large partitions (50M+ rows) may hang under emulation; not an issue on native Linux
4. **AI tuning with transfer-only metric** converges on optimal parameters within 3 runs

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