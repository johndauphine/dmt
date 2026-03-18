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

### MSSQL → PostgreSQL (drop_recreate)

| Configuration | Transfer | Overall | Duration |
|--------------|----------|---------|----------|
| Default (no DB tuning) | 528K rows/s | 425K rows/s | 4m11s |
| DB tuning (fsync=on) | 662K rows/s | 510K rows/s | 3m29s |
| DB tuning (fsync=off) | 656K rows/s | 518K rows/s | 3m26s |

### Database Tuning Applied

**SQL Server** (source):
- `max server memory (MB)` = 4096 (prevents consuming all Docker RAM)
- `max degree of parallelism` = 6

**PostgreSQL** (target):
- `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB
- `max_wal_size` = 4GB, `wal_buffers` = 64MB, `checkpoint_completion_target` = 0.9
- `synchronous_commit` = off (safe — biggest single win for write throughput)
- `wal_level` = minimal, `max_wal_senders` = 0

### Docker RAM vs Throughput

| Docker RAM | Transfer (fsync=on) | Notes |
|-----------|-------------------|-------|
| 4GB | Stalled | MSSQL runs out of memory |
| 6GB | 528K rows/s | Good, but host memory constrained |
| **8GB** | **662K rows/s** | **Optimal** — best balance of DB cache and host pipeline memory |
| 12GB | 432K rows/s | Too much to DBs, starves dmt pipeline |
| 16GB | ~200-290K rows/s | Significantly degraded |

### Key Findings

1. **Database tuning matters more than Docker RAM** — tuning PG write settings on 8GB Docker (662K) outperformed untuned 6GB (528K) by 25%
2. **synchronous_commit=off is the biggest safe win** — fsync=on vs off makes minimal difference when synchronous_commit is already off
3. **More Docker RAM is not better** — the host needs memory for dmt's parallel pipeline buffers; 8GB is optimal for 24GB host
4. **AI startup tuning finds optimal chunk_size** — with throughput feedback, the AI correctly identifies chunk_size=8000 as optimal for this mixed-row-size workload
5. **Rosetta 2 is the remaining bottleneck** — ~30-40% overhead on MSSQL reads; native Linux would be significantly faster

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