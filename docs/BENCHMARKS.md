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

## Docker Host↔VM Proxy Ceiling (June 2026 — read this before comparing numbers)

Forensics from #527 settled why current host-side runs trail the published
high-water marks. Measured on M5 Pro, Docker Desktop (VM 18 CPUs), tuned
containers, fresh PG volume, all-catalog-engine main (`f5ed8a46`):

| dmt location | Transfer-only (MSSQL→PG) | Overall | Wall time |
|---|---|---|---|
| Host (via port-forward proxy) | 520–562K rows/s | ~495K rows/s | 34–36s |
| Inside the Docker network | **1,117–1,188K rows/s** | **878–920K rows/s** | **16–17s** |

The cause is Docker Desktop's host↔VM network path, which caps **aggregate**
throughput at ~222MB/s regardless of connection count:

| Test (Posts table, 3.28GB COPY text) | Throughput |
|---|---|
| 1 COPY stream, host → container via port-forward | 222MB/s |
| 4 parallel COPY streams via port-forward | 222MB/s aggregate (no scaling) |
| 1 COPY stream inside the container | 273MB/s |
| 4 parallel COPY streams inside the container | **678MB/s (near-linear)** |

Implications:

- The "Posts cliff" (wide-row tail at ~200K rows/s) is this byte ceiling
  expressed in rows/sec on 10×-wider rows — not a scheduling or code issue.
  Cost-ordered (LPT) job dispatch was implemented and measured **neutral**.
- Code is not a variable: the #509 oracle-vs-catalog gates measured parity,
  and the in-VM run reproduces the historical ~1.2M rows/s band on today's
  code.
- **The historical host-side numbers were real, on a faster proxy.** The
  900K–1,357K measurements above were ordinary host-side runs on engine
  29.3.1 — a 14s run implies ~450–470MB/s aggregate through the proxy,
  ~2× what engine 29.4.1 (Docker Desktop 4.71.0) delivers on the same
  machine. The port-forward path regressed with the Docker update; disk
  I/O (measured separately above) did not regress enough to explain it.
- **Methodology**: headline numbers should be measured with dmt running
  inside the Docker network (static linux build,
  `docker run --network <dbnet> -v ...`), which sidesteps proxy-version
  drift entirely. Host-side numbers are only comparable across identical
  Docker Desktop versions and must note the engine version.

## Strict Consistency: Parallel Readers (July 2026)

The original pre-change check used one relaxed/strict pair on StackOverflow2010
(19,310,703 rows), SQL Server to PostgreSQL, with identical settings except for
`strict_consistency`. It is a historical point estimate, not a sampled
distribution:

| Path | Relaxed | Strict (one reader) | Original point estimate |
|---|---|---|---|
| Host-side (Docker proxy-bound) | 598K rows/s | 614K rows/s | ~0% — the proxy ceiling masks the reader clamp |
| In-VM (headline methodology) | 1,008K rows/s (19s) | 877K rows/s (22s) | ~13% |

The post-change result was re-measured on 2026-07-11 with every observation
retained. The static Linux arm64 binary was built from `0f33ecf7`; Docker
Desktop Engine 29.5.3 ran in an arm64 VM with 18 CPUs and 25.2GB shared across
uncapped containers. The `dmt` and PostgreSQL processes were arm64. SQL Server
ran cross-architecture under emulation: its 2022-latest amd64 image reported
SQL Server 16.0.4250.1 X64. The image digests were
SQL Server
(`mcr.microsoft.com/mssql/server@sha256:2dca9ee5cd5316952d9b6ef4a0c088ac95b55e3502accdda0fc12ad6ede7b905`)
and PostgreSQL 16-alpine
(`postgres@sha256:4e6e670bb069649261c9c18031f0aded7bb249a5b6664ddec29c013a89310d50`).
`dmt` ran inside the Docker network with runtime tuning disabled and fixed
`workers: 8`, `write_ahead_writers: 4`, `chunk_size: 50000`,
`parallel_readers: 4`, `read_ahead_buffers: 4`, and `max_partitions: 8`.
Requested connection caps of 20 source / 30 target were normalized to the
runtime minimum of 36 in both modes.

Each observation used the same target database name, dropped and recreated
before the run. Odd pairs ran relaxed then strict; even pairs reversed the
order to counterbalance cache and time trends. The values below are the
transfer-phase metric: seconds are reconstructed as 19,310,703 divided by the
integer `rows/sec` emitted by `Transfer complete`, so validation and
finalization are excluded. All 20 runs completed successfully with exactly
19,310,703 transferred rows. Paired penalty is
`(strict_seconds / relaxed_seconds) - 1`; a negative value means strict was
faster.

| Pair | Order | Relaxed | Strict, table scope | Paired penalty |
|---:|---|---:|---:|---:|
| 1 | relaxed → strict | 913,122 rows/s (21.148s) | 977,195 rows/s (19.761s) | -6.56% |
| 2 | strict → relaxed | 1,005,780 rows/s (19.200s) | 962,782 rows/s (20.057s) | +4.46% |
| 3 | relaxed → strict | 968,398 rows/s (19.941s) | 976,866 rows/s (19.768s) | -0.87% |
| 4 | strict → relaxed | 1,010,537 rows/s (19.109s) | 1,003,230 rows/s (19.249s) | +0.73% |
| 5 | relaxed → strict | 981,203 rows/s (19.681s) | 980,180 rows/s (19.701s) | +0.10% |
| 6 | strict → relaxed | 978,423 rows/s (19.737s) | 989,533 rows/s (19.515s) | -1.12% |
| 7 | relaxed → strict | 967,231 rows/s (19.965s) | 946,033 rows/s (20.412s) | +2.24% |
| 8 | strict → relaxed | 903,120 rows/s (21.382s) | 901,084 rows/s (21.431s) | +0.23% |
| 9 | relaxed → strict | 885,489 rows/s (21.808s) | 882,381 rows/s (21.885s) | +0.35% |
| 10 | strict → relaxed | 872,159 rows/s (22.141s) | 873,606 rows/s (22.105s) | -0.16% |

In this sample, the median paired penalty is **+0.17%** and the mean is
**-0.06%**, with an observed range of **-6.56% to +4.46%**. The sign is not
stable across pairs, so no consistent directional difference was detected in
this sample. This is a descriptive result, not an equivalence test, and it does
not bound the true effect outside the recorded setup. The original ~13%
pre-change value is only one historical pair and does not support a statistical
before/after effect size. The isolated reader proofs below establish removal of
the reader clamp; the paired migration sample shows that no repeatable
whole-migration penalty was detected on the recorded setup.

The engine-specific live proofs isolate reader scaling from whole-migration
noise by scanning one million rows with per-query server parallelism disabled.
With four readers, MySQL lock-window sessions measured 19ms versus 74ms for
one strict reader (4.0×); SQL Server shared-table-lock readers measured 30ms
versus 206ms (7.0×); and SQL Server database-snapshot readers measured 35ms
versus 207ms (5.9×). The companion mutation tests prove every reader sees the
same frozen source view.

Environment attribution matters: a host-side run can saturate Docker's proxy
with one reader and hide both the old penalty and the new speedup, so host-side
observations are excluded from the post-change headline result. A schema with
one dominant table magnifies reader parallelism; on a multi-table schema,
concurrent table jobs already overlap some of the single-reader cost. Future
comparisons should predetermine their repetition count, counterbalance run
order, recreate the target for every observation, publish every pair, and
report descriptive statistics rather than promote one pair or a min/max range
to a steady-state estimate.

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
- **Code**: `98b94a6` for first-attach measurements; `79a3f41` (HEAD) for steady-state and aged-volume measurements
- **Date**: March 2026 (first-attach), April 2026 (steady-state and aged-volume)

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

### Results (MSSQL→PG)

#### First-attach measurements (favorable conditions, not repeatable)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 1,149K rows/s | 755K rows/s | 26s |
| 2 | 1,187K rows/s | 875K rows/s | 22s |
| 3 | **1,227K rows/s** | 872K rows/s | 22s |
| 4 | 1,092K rows/s | 781K rows/s | 25s |
| 5 | 1,164K rows/s | 799K rows/s | 24s |
| **Avg (2-5)** | **1,168K rows/s** | **832K rows/s** | **23s** |

#### Steady-state measurements (HEAD `79a3f41`, fresh PG volume, April 2026)

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 711K rows/s | 571K rows/s | 34s |
| 2 | **941K rows/s** | **733K rows/s** | 26s |
| 3 | 906K rows/s | 700K rows/s | 28s |
| 4 | 889K rows/s | 688K rows/s | 28s |
| 5 | 784K rows/s | 624K rows/s | 31s |
| **Avg (2-5)** | **880K rows/s** | **686K rows/s** | **28s** |

#### Aged-volume measurements (HEAD `79a3f41`, 2-month-old PG volume, April 2026)

| Source engine | Transfer (avg 2-5) | Overall (avg 2-5) |
|---|---|---|
| Azure SQL Edge (native ARM64) | 627K rows/s | 438K rows/s |
| SQL Server 2022 (Rosetta 2) | 615K rows/s | 464K rows/s |

The fresh-volume numbers were captured on a brand-new `pg-bench-data-fresh` volume; the aged numbers came from the same `pg-bench-data` volume that had been used across ~2 months of repeated benchmarks. The aged volume caps throughput on the wide-row Posts table at ~250K rows/s instantaneous, regardless of source engine — the per-tick trace shows 1.7-1.9M rows/s on small tables and 1.2-1.4M on Votes, but stalls on Posts. Recreating the PG volume restores most of the headroom.

The first-attach figures stay published as the high-water mark, but the steady-state numbers (fresh PG, code-equivalent to the original measurements) are what a clean redeploy will see.

#### Code-regression check

Bisected back to the original measurement commit (`98b94a6`, March) and an intermediate commit (`7a34d21`, pre-#123/#124). All three commits — March baseline, pre-#123/#124, and HEAD — produce within ±5% on the aged PG volume (March `98b94a6` measured 624K transfer / 458K overall; HEAD `79a3f41` aged-volume row in the table below shows 627K / 438K — same band, different commits/runs). PRs #115, #119, #120, #122, #123, #124 between March and April had **no measurable throughput effect** on this dataset. The gap between first-attach (1,168K) and today's fresh-PG (880K) is environmental — most likely Docker 29.3.1 → 29.4.0 (doc previously flagged Docker write regressions) and a Docker VM disk-write drift from 3.4 GB/s to 2.9 GB/s.

### Cross-Config Comparison (M3 Max, SO2010, MSSQL→PG)

| Configuration | Transfer | Overall | vs Baseline |
|---------------|----------|---------|-------------|
| SQL Server 2022 + bind mount (old) | 472K rows/s | 287K rows/s | — |
| Azure SQL Edge + named volume — first attach | **1,168K rows/s** | **832K rows/s** | **+147% / +190%** |
| Azure SQL Edge + named volume — steady-state (HEAD, fresh PG) | 880K rows/s | 686K rows/s | +86% / +139% |
| Azure SQL Edge + named volume — aged PG (HEAD) | 627K rows/s | 438K rows/s | +33% / +53% |

### Key Findings

1. **First-attach (1,168K) is a high-water mark, not a steady state** — repeating the same benchmark on a fresh PG volume with HEAD code lands at 880K transfer / 686K overall, ~25% below the published peak. The 1,168K figure is the achievable peak on a fresh container/disk; the 880K is the realistic warm-state ceiling on this hardware/Docker version.
2. **PG volume state is the dominant runtime variable** — the same code, hardware, and source engine runs at 627K on a 2-month-old PG volume vs 880K on a fresh one (+40% just from recreating the target volume). Drop-recreate of tables alone is not enough to recover the headroom; the volume itself must be recreated.
3. **Source-engine Rosetta penalty is small at this throughput band** — Azure SQL Edge (native ARM64, 627K) ties SQL Server 2022 under Rosetta 2 (615K) on the aged-volume runs. The original "+147% from eliminating Rosetta" claim conflated Rosetta removal with the move from bind mounts to named volumes; the named-volume change carried most of the speedup. On a fresh PG volume, neither configuration approaches first-attach numbers, so Rosetta is not the active bottleneck.
4. **The wide-row Posts table is the choke point** — instantaneous throughput drops from 1.7–1.9M rows/s on small tables to 250–580K rows/s during Posts (which contains nvarchar(MAX) Body and is materially wider than the small-table phases on either side). On the aged PG volume the dip is severe enough to dominate total runtime; on a fresh volume it recovers more gracefully.
5. **Code is unchanged March → April** — bisection at `98b94a6` (March), `7a34d21` (pre-#123/#124), and HEAD `79a3f41` all produce within ±5% of each other. PRs #115 (transactional CopyFrom), #123 (COPY batch sizing), and #124 (varchar(MAX) memory estimate) had no measurable impact on this benchmark.
6. **AI tuning converges quickly but warm runs vary ~17%** — peak transfer (941K) achieved on fresh-PG run 2, with later warm runs trending downward to 784K (run 5). The AI-applied parameter set was identical across all 5 runs (workers=10, chunk_size=45K, parallel_readers=5), so the spread reflects PG state drift across consecutive drop_recreate cycles within the same volume, not parameter exploration.

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

### Constrained Re-validation (582eb82 — 10GB WSL2 / 8 CPU)

**Environment changes from 24GB re-validation:**
- **Code**: 582eb82 (PRs #135–#137 LM Studio infrastructure fixes; same dmt logic as 0735127)
- **WSL2**: 10GB RAM, 8 CPUs (`.wslconfig` capped vs. 24GB / 10 CPU above)
- **MSSQL container**: 5GB (`--memory=5g`), `max server memory` = 3072MB (set via `sp_configure`, not env var)
- **PG container**: 3GB (`--memory=3g`), `shared_buffers` = 1GB, `work_mem` = 256MB, `maintenance_work_mem` = 512MB, durability-off (`fsync=off`, `synchronous_commit=off`, `full_page_writes=off`, `wal_level=minimal`)
- **AI tuning**: Anthropic (`claude-haiku-4-5-20251001`), selected workers=6, parallel_readers=4, chunk_size=50000, write_ahead_writers=1, read_ahead_buffers=4

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 (cold) | 462K rows/s | 301K rows/s | 1m4s |
| 2 | **581K rows/s** | **409K rows/s** | 47s |
| 3 | 546K rows/s | 408K rows/s | 47s |
| 4 | 527K rows/s | 375K rows/s | 52s |
| **Avg (2-4)** | **551K rows/s** | **398K rows/s** | **49s** |

> AI converged on `write_ahead_writers=1` from run 1 onward — the smartconfig retry-rate
> rule (PR #133) flagged `write_ahead_writers=2` as risky given the constrained target
> transport. Chunk size dropped to 50000 (vs 100000 in the 24GB run) to fit the smaller
> per-worker memory budget. Run 1 cold cache penalty is larger here (-16% vs warm) than
> in the 24GB run (-7%), because the 3GB MSSQL buffer pool can hold only ~30% of the
> 10GB dataset, so disk seeks dominate the first pass.

**vs 24GB re-validation:**

| Metric | 24GB / 10 CPU | 10GB / 8 CPU | Delta |
|--------|---------------|--------------|-------|
| Transfer (warm avg) | 635K rows/s | 551K rows/s | **-13%** |
| Overall (warm avg) | 424K rows/s | 398K rows/s | **-6%** |
| MSSQL buffer pool | 8GB | 3GB | -5GB |
| WSL2 cores | 10 | 8 | -2 |
| AI workers | 6 | 6 | — |
| AI parallel_readers | 3 | 4 | +1 |
| AI chunk_size | 100000 | 50000 | -50% |
| AI write_ahead_writers | 2 | 1 | -1 |

> The 13% transfer-side regression tracks the smaller MSSQL buffer pool (3GB vs 8GB
> can no longer keep the full 10GB dataset hot) and 2 fewer CPU cores. Overall throughput
> is hit less (-6%) because PG-side write speed is unchanged — durability is still off
> and `shared_buffers=1GB` matches the 24GB run.

### Resource-Headroom Re-validation (582eb82 — 11GB WSL2 / 16 CPU)

**Environment changes from 10GB / 8 CPU run:**
- **Code**: 582eb82 (same dmt logic; only `.wslconfig` changed)
- **WSL2**: 11GB RAM, 16 CPUs (`.wslconfig`: `memory=11GB processors=16 swap=4GB`) on a Snapdragon X2 Elite host with 16GB total RAM and 18 cores
- **Containers, AI provider, schema**: unchanged from the 10GB / 8 CPU run

| Run | Workers (AI) | Transfer | Overall | Duration |
|-----|--------------|----------|---------|----------|
| 1 (cold) | 14 | 506K rows/s | 354K rows/s | 54s |
| 2 | 6 | 421K rows/s | 323K rows/s | 1m0s |
| 3 | 6 | 451K rows/s | 339K rows/s | 57s |
| 4 | 6 | **707K rows/s** | **483K rows/s** | 40s |
| **Avg (2-4)** | — | **527K rows/s** | **382K rows/s** | **52s** |

> **AI anchored on prior history, not new resources.** Run 1 saw 16 cores and scaled to
> `workers=14` (`cpu_cores - 2`), measured 506K rows/s transfer, then in runs 2–4 the
> tuner observed that historical workers=6 runs (from the 8-CPU baseline) had higher
> median throughput and reverted to `workers=6`. Other AI-selected params were stable
> across runs 2–4: `chunk_size=50000`, `parallel_readers=4`, `write_ahead_writers=1`,
> `read_ahead_buffers=4` — identical to the 10GB / 8 CPU run.
>
> **Throughput regressed slightly vs. the 10GB / 8 CPU baseline** (527K vs 551K transfer
> warm avg; 382K vs 398K overall). With the AI parameters held constant, doubling cores
> (8 → 16) and adding 1GB of memory did **not** translate into more dmt throughput on
> this host — the migration was already CPU- and memory-bound by the database
> containers, not by dmt's pipeline. Run-to-run variance was also higher (warm range
> 421–707K vs 527–581K in the 10GB run), tracking a more crowded host.
>
> **WSL2 hard-crashed during the first attempt at run 3.** The `.wslconfig` allocates
> 11GB out of 16GB total host RAM, leaving only ~5GB for Windows + non-WSL processes.
> Mid-migration WSL2 went unresponsive and the host rebooted. After bringing the
> session back up, runs 1–2 were intact (cleanly written to disk before the crash) and
> runs 3–4 were re-executed cleanly. **Operationally: 11GB allocation on a 16GB host
> is fragile** — keep the WSL2 memory cap at no more than ~60–65% of host RAM if
> running real workloads.
>
> **Note on `--output-file` JSON correctness:** when dmt resumes a previously-crashed
> run from its SQLite checkpoint, the periodic `--output-file` writer continues to emit
> the *original* run's stale state (`status: running`, partial row count) instead of
> being replaced by the new run's results. The migration logs reflect the actual run.
> The numbers in this table are pulled from `Migration complete:` log lines, not
> from the JSON output files. Worth filing as a separate issue.

### Sweet-Spot Re-validation (5dbd5ab — 9GB WSL2 / 14 CPU)

**Environment changes from the 11GB / 16 CPU run:**
- **Code**: 5dbd5ab (same dmt logic as 582eb82; PR #138 was docs-only)
- **WSL2**: 9GB RAM, 14 CPUs (`.wslconfig`: `memory=9GB processors=14 swap=4GB`) — settled here after the 11GB cap crashed the host
- **MSSQL container**: 4GB (`--memory=4g`, trimmed from 5g via `docker update`), `max server memory` = 3072MB unchanged
- **PG container**: 2GB (`--memory=2g`, trimmed from 3g), durability/buffers unchanged
- **AI provider, schema, dataset**: unchanged

Two independent batches of 4 runs each were executed back-to-back to verify reproducibility, on the same `~/.dmt/state.db` (i.e. accumulated AI tuning history from prior runs).

| Batch | Run 1 (cold) | Run 2 | Run 3 | Run 4 | Warm Avg (2-4) |
|-------|--------------|-------|-------|-------|----------------|
| A — transfer | 610K rows/s | 780K | 792K | 816K | **796K rows/s** |
| A — overall  | 425K rows/s | 512K | 519K | 504K | **512K rows/s** |
| B — transfer | 727K rows/s | 797K | 817K | 792K | **802K rows/s** |
| B — overall  | 498K rows/s | 514K | 536K | 491K | **514K rows/s** |

> **The two batches reproduce within 1%** (796K vs 802K transfer; 512K vs 514K overall).
> Combined warm avg across the 6 warm runs: **799K transfer, 513K overall.** AI selected
> `workers=6, parallel_readers=4, chunk_size=50000, write_ahead_writers=1, read_ahead_buffers=4`
> on every iteration — identical to the 10GB and 11GB runs.
>
> **Why this configuration wins on this host.** The 9-table SO2010 migration is bottlenecked
> by the database containers (sqledge-so2010 / pg-so2010 CPU+memory), not by dmt's pipeline.
> Adding cores or RAM beyond what the containers consume just adds host-side noise. The
> 9GB/14CPU cap leaves more headroom for the Windows host (~7GB free vs ~5GB at the 11GB
> cap) and avoids the host-crash failure mode. Trimming containers to 4GB/2GB (vs 5GB/3GB
> at the higher caps) keeps the same 6GB combined DB budget inside a smaller WSL2 footprint.
>
> **Caveat on AI tuner warm-up.** The SQLite checkpoint at `~/.dmt/migrate.db` had 15+
> historical runs by the start of this re-test, so the tuner converged on the proven
> `workers=6` configuration immediately on run 1. With a fresh checkpoint, run 1 would
> re-explore. **The 799K/513K figure is the operational steady-state**, not a cold-start
> number — but see the cold-tuner verification below.

#### Cold-Tuner Verification (same hardware, fresh `migrate.db`, restarted DB containers)

To confirm the sweet-spot result was not a tuning artifact, we wiped `~/.dmt/migrate.db`,
restarted both DB containers (clearing their buffer pools — but OS page cache stayed hot,
no sudo for `vm.drop_caches`), and re-ran 4 iterations from scratch.

| Run | AI workers | Transfer | Overall |
|-----|-----------:|---------:|--------:|
| 1 (cold tuner, cold buffer pools) | 12 | 596K rows/s | 432K rows/s |
| 2 | 12 | 741K rows/s | 499K rows/s |
| 3 | 12 | 825K rows/s | 553K rows/s |
| 4 | 12 | **836K rows/s** | 540K rows/s |
| **Warm avg (2-4)** | 12 | **801K rows/s** | **531K rows/s** |

> **The cold-tuner warm avg (801K / 531K) matches the warm-tuner warm avg (799K / 513K)
> within 1% on transfer and is actually *higher* on overall (531K vs 513K).** The
> hardware genuinely sustains this throughput on the 9GB/14CPU cap — the warm-tuner
> result was not inflated by accumulated history.
>
> **Notable side finding:** the cold tuner explored `workers=12` (cpu_cores-2) and
> reached the same throughput as the warm tuner's preferred `workers=6`. The AI's
> warm-history preference for `workers=6` was driven by a single peak observation
> (run 10 of the original 10GB/8CPU batch at 707K rows/s) that wasn't structurally
> better — at this hardware scale, workers from 6 to 12 all hit the DB-container
> bottleneck at the same ceiling. `write_ahead_writers=1` is the load-bearing parameter,
> not the worker count.

**vs prior WSL2 ARM64 re-validations:**

| Metric              | 10GB / 8 CPU | 11GB / 16 CPU | 9GB / 14 CPU |
|---------------------|--------------|---------------|--------------|
| Transfer (warm avg) | 551K rows/s  | 527K rows/s   | **799K rows/s** |
| Overall (warm avg)  | 398K rows/s  | 382K rows/s   | **513K rows/s** |
| WSL2 cap            | 10GB         | 11GB          | 9GB          |
| Cores               | 8            | 16            | 14           |
| Windows host headroom | ~6GB       | ~5GB          | ~7GB         |
| Container budget    | 5GB+3GB      | 5GB+3GB       | 4GB+2GB      |
| AI workers          | 6            | 6             | 6            |
| AI history at run 1 | 0            | 4             | 15+          |
| Host crashes mid-batch | 0         | 1             | 0            |

> The 9GB/14CPU cap is the sweet spot for this hardware: highest sustained throughput,
> the lowest host-crash risk, and the same AI parameters as the other two configurations.
> ~45% faster transfer-side than 10GB/8CPU, ~52% faster than 11GB/16CPU.

### Unconstrained Re-validation (a151191 — 30GB WSL2 / 18 CPU, no container caps)

**Environment changes from the 9GB / 14 CPU sweet-spot run:**
- **Code**: a151191 (PR #140 — smartconfig retry-rule grounding; the AI's reasoning text is now data-cited rather than confabulated, but parameter choices are unchanged from prior runs)
- **WSL2**: 30GB RAM, 18 CPUs (no `.wslconfig` cap — full host allocation on a 31GB / 18-core class machine; `free -h` reports 31Gi total inside WSL2)
- **MSSQL container**: unconstrained, `max server memory` = 8192MB (set via `sp_configure`), `max degree of parallelism` = 4, `cost threshold for parallelism` = 50
- **PG container**: unconstrained, `shared_buffers` = 8GB, `effective_cache_size` = 16GB, `work_mem` = 64MB, `maintenance_work_mem` = 2GB, `max_wal_size` = 16GB, `min_wal_size` = 2GB, `checkpoint_timeout` = 30min, `synchronous_commit` = off, `wal_buffers` = 64MB, `max_connections` = 200, `max_worker_processes` = 16, `max_parallel_workers` = 16, `max_parallel_maintenance_workers` = 4 — durability mostly on (`fsync=on`, `full_page_writes=on`, `wal_level=replica`), only `synchronous_commit=off`
- **AI provider, schema, dataset**: unchanged

The intent of this run was to lift every container-side bottleneck the prior re-validations had documented (constrained MSSQL buffer pool, small PG `shared_buffers`, host-RAM headroom), while leaving the dmt logic and AI tuning identical. AI history at run 1 was 25+ — same warm-tuner steady state as the 9GB sweet-spot batches.

| Run | Transfer | Overall | Duration |
|-----|----------|---------|----------|
| 1 | 940K rows/s | 710K rows/s | 27s |
| 2 | 897K rows/s | 665K rows/s | 29s |
| 3 | 820K rows/s | 632K rows/s | 31s |
| 4 | 978K rows/s | 723K rows/s | 27s |
| 5 | 906K rows/s | 661K rows/s | 29s |
| 6 | 1,022K rows/s | 767K rows/s | 25s |
| 7 | **1,037K rows/s** | 728K rows/s | 27s |
| 8 | 914K rows/s | 621K rows/s | 31s |
| **Warm avg (1-8)** | **939K rows/s** | **688K rows/s** | **28s** |

> AI selected `workers=16`, `chunk_size=50000`, `read_ahead_buffers=4`,
> `write_ahead_writers=1`, `parallel_readers=4`, `max_partitions=16` on every
> iteration — same parameter choices as the 9GB / 10GB / 11GB runs. The
> additional cores and memory did not move the AI off `workers=16` (cpu_cores−2)
> or `chunk_size=50000`.

#### Forced-WAW Sensitivity (same hardware, AI smartconfig disabled mid-batch)

To verify that `write_ahead_writers=1` was load-bearing on this hardware (and not just AI inertia from prior history), three batches forced explicit waw values via config override (`ai_adjust: false`, AI smartconfig still picked the rest). Telemetry from `~/.dmt/migrate.db` `ai_tuning_history`:

| waw | Runs | Peak | Mean | Total chunk retries |
|----:|----:|----:|----:|----:|
| 1 (AI default) | 13 | 1,045K rows/s | 939K rows/s | **0** |
| 2 (forced) | 4 | 972K rows/s | 909K rows/s | **0** |
| 4 (forced) | 4 | 869K rows/s | 827K rows/s | **0** |

> No actual retries at any waw level — confirmed that the 12% throughput gap
> between waw=1 and waw=4 on this configuration is **not** the transport-saturation
> mechanism the prior smartconfig prompt asserted (the case that motivated #140).
> The real mechanism for the throughput delta on a tuned PG target is more likely
> heap-page contention or WAL-buffer contention from concurrent COPYs, not retries.

**vs prior WSL2 ARM64 re-validations:**

| Metric | 10GB / 8 CPU | 11GB / 16 CPU | 9GB / 14 CPU | **30GB / 18 CPU (this row)** |
|---------------------|--------------|---------------|--------------|------------------------|
| Transfer (warm avg) | 551K rows/s | 527K rows/s | 799K rows/s | **939K rows/s** |
| Transfer (peak) | 581K rows/s | 707K rows/s | 836K rows/s | **1,037K rows/s** |
| Overall (warm avg) | 398K rows/s | 382K rows/s | 513K rows/s | **688K rows/s** |
| WSL2 cap | 10GB | 11GB | 9GB | 30GB |
| Cores | 8 | 16 | 14 | 18 |
| Container budget | 5GB+3GB | 5GB+3GB | 4GB+2GB | unconstrained |
| MSSQL `max server memory` | 3GB | 3GB | 3GB | **8GB** |
| PG `shared_buffers` | 1GB | 1GB | 1GB | **8GB** |
| PG `max_wal_size` | (default 1GB) | (default 1GB) | (default 1GB) | **16GB** |
| AI workers | 6 | 6 | 6 | **16** |
| AI other params | identical | identical | identical | identical |
| Host crashes mid-batch | 0 | 1 | 0 | 0 |

> **+18% transfer / +34% overall vs the published 9GB sweet spot** (799K → 939K
> transfer; 513K → 688K overall). The 9GB/14CPU run was deliberately constrained
> for crash-safety on a 16GB host class; on a 31GB host the same dmt logic falls
> out at materially higher throughput once the container caps are removed and PG
> is given an 8GB `shared_buffers` + 16GB `max_wal_size`.
>
> **AI workers shift (6 → 16) is the largest visible parameter delta** — at this
> hardware scale the AI's prior preference for `workers=6` (driven by a single
> peak observation in the 10GB/8CPU history) finally gives way to
> `workers=cpu_cores−2`. Per the cold-tuner finding from the 9GB run, this
> parameter is not load-bearing in isolation; the change tracks `cpu_cores`,
> not `final_throughput`.
>
#### Cold-Tuner Verification (same hardware, fresh `migrate.db`, restarted DB containers)

To verify whether the warm-tuner result reflected hardware capability or accumulated AI bias, we ran 4 iterations against a fresh `data_dir` (the user's `~/.dmt/migrate.db` was left untouched; cold-tuner state was isolated under `/tmp/dmt-coldtuner/`). Both DB containers were restarted before run 1 to clear in-memory buffer pools (OS page cache stayed hot — no sudo for `vm.drop_caches`, matching the 9GB sweet-spot methodology).

| Run | AI workers | AI parallel_readers | AI waw | Transfer | Overall | Wall |
|-----|-----------:|--------------------:|-------:|---------:|--------:|-----:|
| 1 (cold tuner, cold pools) | 16 | 4 | 2 | 815K rows/s | 642K rows/s | 24s |
| 2 | 16 | 4 | 2 | **1,130K rows/s** | **819K rows/s** | 17s |
| 3 | 18 | 6 | 2 | 1,055K rows/s | 694K rows/s | 18s |
| 4 | 18 | 6 | 2 | 1,108K rows/s | 758K rows/s | 17s |
| **Warm avg (2-4)** | — | — | 2 | **1,098K rows/s** | **757K rows/s** | **17s** |

> **The cold-tuner result *exceeds* the warm-tuner result by +17% transfer / +10%
> overall** (1,098K / 757K vs 939K / 688K). This is the opposite of the 9GB
> sweet-spot section's finding (where cold and warm matched within 1%): on this
> hardware, the warm-tuner's accumulated history actively biased the AI toward
> a sub-optimal configuration.
>
> **Why the warm tuner under-performed.** The cold tuner saw zero history at
> run 1, fell back to the prompt's baseline default (`waw=2`), measured strong
> throughput, and stayed there. The warm tuner saw 21 prior runs at `waw=1`
> with peak 1,045K and 4 runs at `waw=2` with peak 972K — all from the
> *constrained* 9GB/10GB/11GB configurations where waw=1 had genuinely been
> optimal. Clause 4(b) of the post-#140 retry-rate rule correctly noted "every
> waw at 0% retry rate, rule does not apply, choose by throughput" — but the
> *throughput* evidence was from a different hardware regime, so the choice
> the model made (waw=1) was throughput-correct for the data it saw and
> wrong for the current hardware.
>
> **Cold tuner also explored other parameters.** Run 3 escalated `workers`
> 16→18 and `parallel_readers` 4→6 after seeing run 2's 1.13M r/s peak. The
> exploration didn't beat run 2's transfer peak but stayed within 5% — i.e.
> no regression. With no dominant-history anchor, the AI exercised the full
> hardware. See #144 for the broader pattern: history collected on a different
> hardware regime can mislead the smartconfig's throughput-based choice.
>
> **Reasoning quality (post-#140) held across the cold-tuner batch.** Every
> reasoning block correctly cited its data source. Run 2 said verbatim:
> *"With only one historical data point and zero retries at waw=2, the
> write_ahead_writers retry rule does not apply (condition c)"* — naming the
> rule clause and grounding in observed retries (0). No mechanism
> confabulation in any of the 4 runs.

### Cross-Machine Comparison (SO2010, MSSQL→PG, transfer-only)

| Machine | Source Engine | Cores | RAM | Docker Write | Transfer (avg) | vs M5 Pro (SS2022) |
|---------|-------------|-------|-----|-------------|---------------|-----------|
| M3 Max (16GB Docker) | SQL Server 2022 (Rosetta) | 14 | 36GB | 2.7 GB/s | 472K rows/s | -65% |
| WSL2 ARM64 (afda4e0) | Azure SQL Edge | 10 | 24GB | 1.3 GB/s | 487K rows/s | -64% |
| WSL2 ARM64 (582eb82, **constrained**) | Azure SQL Edge | 8 | 10GB | — | 551K rows/s | -59% |
| WSL2 ARM64 (582eb82, 16 CPU / 11GB) | Azure SQL Edge | 16 | 11GB | — | 527K rows/s | -60% |
| WSL2 ARM64 (5dbd5ab, **sweet spot** 9GB / 14 CPU) | Azure SQL Edge | 14 | 9GB | — | 799K rows/s | -41% |
| WSL2 ARM64 (0735127) | Azure SQL Edge | 10 | 24GB | 2.4 GB/s | 635K rows/s | -53% |
| **WSL2 ARM64 (a151191, *unconstrained* 30GB / 18 CPU, warm tuner)** | **Azure SQL Edge** | **18** | **30GB** | **—** | **939K rows/s (peak 1,037K)** | **-31%** |
| **WSL2 ARM64 (a151191, *unconstrained* 30GB / 18 CPU, cold tuner)** | **Azure SQL Edge** | **18** | **30GB** | **—** | **1,098K rows/s (peak 1,130K)** | **-19%** |
| M5 Pro (8GB Docker) | Azure SQL Edge | 15 | 24GB | 4.4 GB/s | 886K rows/s | -35% |
| M5 Pro (8GB Docker) | SQL Server 2022 (Rosetta) | 15 | 24GB | 5.3 GB/s | 1,357K rows/s | — |

### Key Findings

1. **WSL2 ARM64 unconstrained beats M5 Pro Azure SQL Edge** — 1,098K cold-tuner mean / 1,130K peak (and 939K warm-tuner mean / 1,037K peak) vs M5 Pro's 886K. The doc's prior "WSL2 disk I/O is the primary bottleneck" finding was load-bearing on the constrained runs but stops applying once PG `shared_buffers=8GB` keeps the working set off-disk for the inserts.
2. **Cold tuner outperformed warm tuner by +17% on this hardware** (1,098K vs 939K transfer mean) — the *opposite* of the 9GB sweet-spot finding (where cold and warm matched within 1%). The warm tuner's accumulated history was from a different hardware regime (constrained 9-11GB caps) and biased the AI toward a sub-optimal `waw=1` choice on the unconstrained 30GB host. See #144 for the proposed fix (regime-aware trajectory filtering or schema additions for target tuning state).
3. **+30% throughput from combined improvements (constrained)** — 635K vs 487K transfer; disk I/O (+85%), AI parallel readers, PG writer refactor, and larger MSSQL buffer pool all contribute
4. **+18-37% transfer / +34-48% overall from removing container caps** — 939K-1,098K transfer vs 9GB sweet-spot's 799K, on the same dmt logic. The constrained runs were tuned for crash-safety on 16GB hosts; a 31GB host doesn't need that and gets the throughput back.
5. **WSL2 virtual disk write speed remains a constraint, but not the bottleneck on tuned PG** — 2.4 GB/s write vs M5 Pro's 4.4 GB/s explains much of the gap on the *constrained* runs; on the unconstrained run, PG's `shared_buffers=8GB` and `synchronous_commit=off` keep the bulk-insert path off the slow virtio-fs writer for most of the migration
6. **Container memory limits are essential on WSL2 with constrained host RAM** — Docker shares the WSL2 memory pool with no separate cap; on a 16GB host, `--memory` flags prevent DB processes from starving the pipeline. On a 31GB+ host, removing the caps is faster.
7. **Azure SQL Edge requires explicit memory capping** — without `MSSQL_MEMORY_LIMIT_MB` (or `sp_configure 'max server memory'` on the unconstrained run), it consumes all container memory and OOM-kills
8. **4GB PG container with 2GB shared_buffers** gives 9% improvement over 2GB container with 512MB shared_buffers (487K vs 447K) on the constrained class
9. **AI smartconfig retry-rate rule does not fire on tuned PG targets** — 25 runs at waw=1 + 8 runs at waw=2 + 4 runs at waw=4 produced **0 chunk retries total**. The waw choice is now correctly attributed to throughput, not retry avoidance, after PR #140 grounded the rule's reasoning in the actual `chunk_retry_count` column. (The cold-tuner result above shows that throughput-driven choice is sensitive to which hardware regime the throughput data came from — a separate concern.)

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

### SO2013 on Unconstrained 30GB / 18 CPU (a151191 — five-knob experiment)

**Environment changes from the 12GB-container SO2013 run above:**
- **Code**: a151191 + #146 (post-PR #145 / #146 — bounded-history smartconfig + citation disambiguation)
- **WSL2**: 30GB RAM, 18 CPUs (matches the 30GB SO2010 unconstrained section above; same `.wslconfig`)
- **MSSQL container**: unconstrained, `max server memory` = 8192MB (one variant at 16384MB — see below), `maxdop` = 4, `cost threshold for parallelism` = 50
- **PG container**: unconstrained, baseline `shared_buffers=8GB, synchronous_commit=off, max_wal_size=16GB, fsync=on, full_page_writes=on, wal_level=replica`
- **AI provider**: `claude-haiku-4-5-20251001`
- **Dataset**: Full Brent Ozar SO2013 (SQL Server 2008 format MDF + 4 NDF data files, ~52GB attached, 9 tables, 106.5M rows). Compatibility level set to 150 for Azure SQL Edge.

Five experiments tested in sequence to isolate which knob actually moves the needle on this 5.5×-larger-than-SO2010 dataset:

| Config | mssql memory | PG durability | Trials | Mean transfer (warm) | Mean overall (warm) | Peak transfer | Δ vs warm-tuner baseline |
|---|---|---|---|---|---|---|---|
| Warm-tuner baseline (SO2010 history seeded) | 8GB | on | 1 | 469K rows/s | 364K rows/s | 469K | — |
| Cold-tuner (no history; AI escalated `pr` 4→6) | 8GB | on | 4 (warm 2-4) | 380K rows/s | 308K rows/s | 396K | **-19%** |
| Forced `chunk_size=100K` @ pr=4 | 8GB | on | 3 | 388K rows/s | 311K rows/s | 408K | **-17%** |
| Forced @ pr=4, `mssql max=16GB` | 16GB | on | 3 (warm 2-3) | 447K rows/s | 349K rows/s | 448K | **-5%** (within noise) |
| **Forced @ pr=4, PG durability-off triad** | **8GB** | **off** | **7 (warm 2-7)** | **535K rows/s** | **398K rows/s** | **576K** | **+14%** |

> **One clear winner: the PG durability-off triad** (`fsync=off`, `full_page_writes=off`,
> `wal_level=minimal`, plus `autovacuum=off`, `maintenance_work_mem=4GB`,
> `max_parallel_maintenance_workers=8`). +14% transfer / +9% overall on a clean
> A/B against the warm-tuner baseline. Six warm samples, range 491–576K, peak 576K.
> Run 5 hit `19310s @ 576K rows/s` transfer in 3m05s — best SO2013 result on this
> hardware. **Bench-only — `fsync=off` and `full_page_writes=off` lose crash safety;
> revert to durability-on for production data.**
>
> **The doc's published 9GB sweet-spot SO2010 result (799K)** also used the
> durability-off triad (see "Constrained Re-validation" environment section above).
> Today's earlier SO2010 unconstrained section measured **with durability ON**, so its
> published 939K mean is durability-on — apples-to-apples comparison with the prior
> 9GB sweet-spot is +18% from removing container caps even after subtracting the
> 14% durability-off was already worth.

#### Why the other four knobs didn't help

1. **Cold-tuner found a *worse* local optimum than the warm-tuner.** With no history, the AI's smartconfig escalated `parallel_readers` 4→6 after a single +5% improvement that turned out to be noise. Subsequent runs locked in that decision. The warm-tuner's "stale" SO2010 evidence happened to keep `pr=4` which is the genuinely better choice on a disk-bound source. **Opposite outcome from the SO2010 cold-tuner finding above** (where cold-tuner outperformed warm-tuner by +17%) — see #144 for the underlying regime-mismatch issue. On SO2013 the regime mismatch saved us; on SO2010 it cost us.

2. **`chunk_size=100K` rejected.** Hypothesis was that with 2,128 chunks at 50K rows the per-chunk overhead would amortize better at 100K (1,064 chunks). Empirical: 388K mean across 3 runs (-17% vs 469K baseline). Per-chunk overhead is NOT the binding constraint at the cs=50K floor on this dataset; PG COPY's per-chunk cost may actually scale super-linearly with chunk size in the ranges that matter, and bigger in-flight buffers add memory pressure that offsets any savings.

3. **`mssql max_server_memory=16GB` was within noise.** Hypothesis: doubling the buffer pool would push cache hit ratio from ~15% to ~31% on the 52GB dataset. Empirical: 447K mean across 2 warm samples (-5% vs 469K baseline — within run-to-run variance). The largest tables (Posts, Comments, Votes) are individually under 16GB but together push out the others as MSSQL sequentially scans them; cache size is not the binding constraint at this hardware class.

4. **`parallel_readers` escalation hurts at this hardware scale.** AI's pr=4→6 in the cold-tuner cost -18% vs warm-tuner baseline. More concurrent SELECTs against a disk-bound source amplify cache thrash at the OS page cache layer. Same direction of regression as the earlier SO2010 forced-waw=4 experiment (-19% vs the chosen waw=2).

#### Forced-config detail (the durability-off winner)

```yaml
migration:
  target_mode: drop_recreate
  workers: 16              # cpu_cores - 2
  chunk_size: 50000
  read_ahead_buffers: 4
  write_ahead_writers: 2
  parallel_readers: 4
  max_partitions: 16
```

PG settings flipped from PR #145-era baseline:
```sql
ALTER SYSTEM SET fsync = 'off';
ALTER SYSTEM SET full_page_writes = 'off';
ALTER SYSTEM SET wal_level = 'minimal';
ALTER SYSTEM SET max_wal_senders = 0;
ALTER SYSTEM SET autovacuum = 'off';
ALTER SYSTEM SET maintenance_work_mem = '4GB';
ALTER SYSTEM SET max_parallel_maintenance_workers = 8;
```

#### Per-run trajectory under durability-off

| Run | Cache | Transfer | Overall |
|-----|-------|----------|---------|
| 1 | cold mssql + cold pg | 396K | 317K |
| 2 | warm | 491K | 371K |
| 3 | warm | 547K | 407K |
| 4 | warm | 546K | 404K |
| 5 | warm | **576K** | **419K** |
| 6 | warm | 533K | 400K |
| 7 | warm | 517K | 389K |
| **Warm avg (runs 2-7)** | | **535K rows/s** | **398K rows/s** |

> The cold-cache penalty is small (Run 1 396K vs warm avg 535K = -26%); steady state
> is reached by Run 2. Run-to-run variance under steady state is ±8% which is consistent
> with the SO2010 30GB unconstrained variance.

#### Practical caveats

- The `migration.ai_adjust: false` flag in the per-migration YAML was intended for these forced experiments but is silently ignored by dmt's config parser — the runtime tuner ran throughout. Tracked in **#149**. The persisted run records confirm the user-explicit `chunk_size`, `parallel_readers`, etc. values stuck regardless, so the forced-config experiments are still informative.
- The smartconfig prompt's guideline 6 ("row count does not affect optimal parameters") was empirically validated on this dataset — `chunk_size` up did not help. The earlier hypothesis that the guideline should be loosened for high chunk counts is rejected.

### Cross-Machine Comparison (SO2013, MSSQL→PG, transfer-only)

| Machine | Source Engine | Cores | RAM | Transfer (avg) | vs M5 Pro (SS2022) |
|---------|-------------|-------|-----|---------------|-----------|
| M3 Max (16GB Docker) | SQL Server 2022 (Rosetta) | 14 | 36GB | 287K rows/s | -64% |
| WSL2 ARM64 (12GB container) | Azure SQL Edge | 10 | 24GB | 417K rows/s | -48% |
| **WSL2 ARM64 (a151191, *unconstrained* 30GB / 18 CPU, durability-on)** | **Azure SQL Edge** | **18** | **30GB** | **469K rows/s** | **-41%** |
| **WSL2 ARM64 (a151191, *unconstrained* 30GB / 18 CPU, durability-off)** | **Azure SQL Edge** | **18** | **30GB** | **535K rows/s (peak 576K)** | **-33%** |
| M5 Pro (8GB Docker) | SQL Server 2022 (Rosetta) | 15 | 24GB | 795K rows/s | — |
| M5 Pro (12GB container) | Azure SQL Edge | 15 | 24GB | 1,042K rows/s | * |

> *Azure SQL Edge and SQL Server 2022 are different products with different configs —
> cross-engine throughput comparisons are not apples-to-apples.

### SO2013 Key Findings

1. **PG durability-off triad is the dominant lever on SO2013, +14% transfer.** Six warm-sample mean of 535K rows/sec (peak 576K) on the same dmt config (`cs=50K, pr=4, waw=2`) that gets 469K under durability-on. The +14% comes from `fsync=off` + `full_page_writes=off` + `wal_level=minimal` skipping the per-COPY WAL safety overhead. Bench-only — these settings lose crash safety.
2. **mssql memory size is NOT the binding constraint at 8GB.** Doubling to 16GB gave ~447K mean — within run-to-run noise of the 469K 8GB baseline. The 52GB dataset cycles through any cache size that's smaller than itself; cache hit ratio matters less than absolute disk read bandwidth.
3. **chunk_size=100K rejected on SO2013.** 388K mean across 3 forced trials — actually worse than the cs=50K baseline. The smartconfig prompt's guideline "row count does not affect optimal parameters" is empirically validated.
4. **AI cold-tuner ALSO worse than warm-tuner on SO2013** — the *opposite* of the SO2010 cold-tuner finding. The cold tuner's free exploration converged on `parallel_readers=6` after a single noisy +5% gain, which underperforms the warm-tuner's `pr=4` by -18%. Issue #144 notes that history-regime mismatch can cut either way; on SO2013 the SO2010-derived priors saved us, on SO2010 they cost us.
5. **Native ARM64 advantage holds at scale** — even the durability-on baseline (469K) beats both the prior 12GB-container WSL2 ARM64 (417K, +12%) and M3 Max (287K, +63%).
6. **Gap to M5 Pro narrows under durability-off.** Durability-on is -41% vs M5 Pro SS2022; durability-off is -33%. Some of M5 Pro's published peak likely also benefits from less safety overhead.
7. **Warm-cache improvement is modest under durability-on** — run 1 (402K) to run 3 (423K) = +5%, as the 52GB dataset far exceeds the 8GB MSSQL buffer pool. Larger improvements come from PG-side durability or from fitting the dataset entirely in a buffer pool (impossible at 52GB on 30GB WSL).
8. **Azure SQL Edge handles 52GB database without issues** — all 106.5M rows validated across 9 tables in every run.

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

After fully resetting WSL (`wsl --shutdown`) and recreating the PG volume, repeated runs settle into the lower band consistently. Detach + re-attach + cache-priming experiments did not recover the higher numbers — the gap appears to come from transient kernel/scheduler state on first migrations after attach, which is not reproducible in steady-state operation.

The first-attach figures stay published as the high-water mark, but the steady-state numbers are what a long-running deployment will see.

AI converged on `W=12 C=50K PR=6` for SO2013 — same plateau as the initial SO2010 default, did not explore smaller chunks unprompted. Smaller chunks did not help on SO2013 in side-tests; bottleneck shifted from pipeline handoff (writer-bound on cached SO2010) to MSSQL disk reads (read-bound on the 52GB dataset that exceeds 8GB cache).

#### Cross-platform comparison (SO2013 transfer rate)

| | Core Ultra 7 358H 32GB — first attach | Core Ultra 7 358H 32GB — steady-state | M5 Pro (macOS / Rosetta) | M3 Max (macOS / Rosetta) |
|---|---|---|---|---|
| Transfer rate | 772K | **466K** | 795K | 287K |
| Duration | 173s | 279s | 134s | 372s |

The first-attach number on the Core Ultra 7 358H ties M5 Pro on transfer throughput despite a lower-spec CPU (no Rosetta penalty). The steady-state number is the more honest comparison for ongoing workloads — still well ahead of M3 Max (Rosetta) but ~40% behind M5 Pro, reflecting Docker Desktop's VHDX-on-NTFS storage overhead in WSL2 vs native macOS file I/O.

### Memory-pressure caveats observed
- Bumping `MSSQL_MEMORY_LIMIT_MB` to 12288 on SO2013 caused WSL to swap (only 24GB total, MSSQL+PG+dmt+OS exceeded budget). Throughput dropped ~30%. Practical ceiling: 8GB MSSQL cap on a 24GB WSL.
- After accumulated swap activity (cumulative `pswpout` > 23GB), SO2013 throughput regressed ~33% vs cold-start state even after container restarts. A `wsl --shutdown` is required for a true clean reset; container restarts alone don't reclaim the WSL kernel's degraded page-cache state.

## ASUS Windows Native DBs -- SO2010 Runtime Tuning A/B

### Environment

- **Host**: ASUS Windows laptop, performance power mode enabled for the final comparison
- **Source**: SQL Server 2022 Developer installed directly on Windows
- **Target**: PostgreSQL 17 installed directly on Windows
- **Dataset**: StackOverflow2010 full Brent Ozar fixture, 19,310,703 rows transferred
- **dmt**: `v5.2.0`, Windows binary, run directly from Windows
- **Mode**: `drop_recreate`, startup auto-tuning enabled, no fixed transfer parameters passed
- **Runtime controller A/B**: `runtime_tuning: true` vs `runtime_tuning: false`
- **Date**: June 2026

Database-level tuning was applied before the comparison:

- SQL Server: `max server memory = 6144 MB`, `MAXDOP = 4`, `cost threshold for parallelism = 50`, 8 tempdb files
- PostgreSQL: `shared_buffers = 2GB`, `work_mem = 32MB`, `maintenance_work_mem = 1GB`, `effective_cache_size = 8GB`, `wal_buffers = 64MB`, `max_wal_size = 8GB`, `checkpoint_timeout = 30min`, `synchronous_commit = off`

### Runtime tuning enabled vs disabled

Both series let the startup tuner choose parameters. The runtime-disabled series only turned off mid-run controller adjustments; it did not pin `workers`, `chunk_size`, `parallel_readers`, or writer counts.

| Series | n | Runtime adjustments | Transfer mean | Transfer median | Transfer CV | Overall mean | Overall median | Overall CV |
|--------|--:|--------------------:|--------------:|----------------:|------------:|-------------:|---------------:|-----------:|
| Runtime tuning enabled, all runs | 10 | 10 | 537,193 rows/s | 520,786 rows/s | 13.50% | 394,953 rows/s | 394,260 rows/s | 10.84% |
| Runtime tuning disabled, all runs | 10 | 0 | 362,154 rows/s | 315,359 rows/s | 28.75% | 236,679 rows/s | 212,230 rows/s | 28.58% |
| Runtime tuning enabled, runs 3-10 | 8 | 8 | 517,236 rows/s | 512,582 rows/s | 12.44% | 381,176 rows/s | 371,910 rows/s | 8.88% |
| Runtime tuning disabled, runs 3-10 | 8 | 0 | 313,788 rows/s | 306,460 rows/s | 6.30% | 209,849 rows/s | 208,770 rows/s | 5.38% |
| Runtime tuning enabled, runs 3-10 regression-tier only | 6 | 6 | 491,596 rows/s | 492,551 rows/s | 5.78% | 368,038 rows/s | 357,605 rows/s | 6.88% |

### Runtime-disabled run detail

The no-runtime series converged on the same startup shape for every run: `workers=14`, `chunk_size=24557`, `write_ahead_writers=1`, `parallel_readers=2`, `read_ahead_buffers=4`. No rows were inserted into `ai_adjustments` during the series.

| Run | Tuning tier | Transfer | Overall | Wall time |
|----:|-------------|---------:|--------:|----------:|
| 1 | regression | 580,716 rows/s | 419,797 rows/s | 45.996s |
| 2 | regression | 530,520 rows/s | 268,204 rows/s | 72.070s |
| 3 | regression | 310,535 rows/s | 201,153 rows/s | 96.923s |
| 4 | regression | 302,386 rows/s | 219,439 rows/s | 88.992s |
| 5 | regression | 296,742 rows/s | 201,153 rows/s | 97.368s |
| 6 | regression | 296,844 rows/s | 195,057 rows/s | 99.956s |
| 7 | regression | 344,532 rows/s | 207,641 rows/s | 93.141s |
| 8 | regression | 341,550 rows/s | 229,889 rows/s | 84.073s |
| 9 | regression | 320,183 rows/s | 209,898 rows/s | 93.484s |
| 10 | regression | 297,530 rows/s | 214,563 rows/s | 90.811s |

### Findings

1. **Runtime tuning improved performance substantially.** Against the full 10-run samples, runtime tuning raised transfer throughput from 362K to 537K rows/s (+48%) and overall throughput from 237K to 395K rows/s (+67%). On the settled runs 3-10, the lift was larger: transfer +65%, overall +82%.
2. **Runtime tuning did not make the environment quieter.** Whole-series CV was lower with runtime tuning enabled than disabled, but that was mostly because the disabled series had two high opening outliers. After dropping those opening runs, no-runtime CV tightened to 6.30% transfer / 5.38% overall, while runtime-enabled runs 3-10 remained at 12.44% transfer / 8.88% overall.
3. **The quieter no-runtime band was much slower.** Runtime-disabled runs 3-10 averaged only 314K transfer / 210K overall. Runtime-enabled regression-tier runs in the same performance-mode regime averaged 492K transfer / 368K overall with similar CV.
4. **The startup regression tuner had converged.** It repeatedly selected `WAW=1`, `chunk_size=24557`, `PR=2`, `RAB=4`, `workers=14`. The remaining spread is therefore not startup parameter churn.
5. **Mid-run adjustments were load-bearing.** The runtime-enabled series recorded 10 adjustments, mostly increasing `write_ahead_writers` from 1 to 2 and occasionally shrinking chunks under memory pressure. Disabling the controller removed those adjustments completely, but also removed the mechanism that rescued throughput on this host.
6. **Native Windows is faster than the earlier Docker/WSL2 laptop setup, but still noisy.** Native DBs remove Docker Desktop and WSL2 as bottlenecks, yet the laptop still shows warmup/regime effects and run-to-run variance. Repeated samples and outlier filtering remain necessary for convergence claims.

### Takeaway

Keep runtime tuning enabled for native Windows SO2010 runs. It materially improves throughput, and the observed noise is better handled by repeated runs, regression-tier filtering, and warmup/outlier handling than by disabling the runtime controller.

## M5 Pro 48GB — Host Memory Pressure Finding (SO2010 + SO2013, MSSQL→PG)

### Test Environment

- **Hardware**: Apple M5 Pro, **48GB RAM**, 18 CPU cores (distinct from prior M5 Pro 24GB host runs)
- **OS**: macOS (Darwin 25.4.0)
- **Source**: SQL Server 2022 (linux/amd64 image under Rosetta 2, Docker, named volume)
- **Target**: PostgreSQL 16 (Alpine, Docker, named volume, tuned)
- **Datasets**: StackOverflow2010 (~19.3M rows) and StackOverflow2013 (~106.5M rows), full Brent Ozar dumps attached and upgraded to compatibility level 160
- **AI Provider**: Anthropic (`claude-haiku-4-5-20251001`)
- **Mode**: `drop_recreate`, AI tuning enabled, `workers`/`chunk_size` removed from config so AI controls them
- **Code**: HEAD `08b1284`
- **Date**: April 2026

### Disk I/O (Docker VM, named volume, 24GB Docker allocation)

| Metric | Value |
|--------|------:|
| Sequential write | 2.7 GB/s |
| Sequential read (cached) | 11.0 GB/s |

> Average of 5 runs, `dd bs=1M count=1024` inside Docker container. `/proc/sys/vm/drop_caches`
> is read-only inside Docker Desktop, so the read number reflects cached reads.

### Database Configuration

Container sizing was scaled proportionally from the `Makefile` `bench-dbs-up` recipe (which targets 8GB Docker → 4GB MSSQL / 1GB PG `shared_buffers`). For 24GB Docker:

**SQL Server 2022** (source, Rosetta 2): `MSSQL_MEMORY_LIMIT_MB=12288` (12 GB)

**PostgreSQL 16** (target):
- `shared_buffers=3GB`, `effective_cache_size=12GB`
- `work_mem=384MB`, `maintenance_work_mem=768MB`
- `max_wal_size=6GB`, `min_wal_size=1500MB`, `wal_buffers=64MB`
- `synchronous_commit=off`, `fsync=off`, `wal_level=minimal`, `max_wal_senders=0`
- `--shm-size=3g`

### Host Memory Pressure: 32GB Docker vs 24GB Docker (Activity Monitor, mid-run)

Initial runs sized Docker at 32GB (the default suggestion when host has 48GB). Activity Monitor showed sustained pressure: dmt + Docker + browser + OS exceeded 48GB, macOS aggressively compressed memory, and throughput collapsed. Dropping Docker to 24GB recovered ~2.5x throughput on SO2013.

| Metric | 32GB Docker | 24GB Docker | Δ |
|--------|------------:|------------:|---|
| Docker VM (RSS) | 32.01 GB | 24.06 GB | −8 GB (the resize) |
| dmt process (RSS) | 9.46 GB | 5.55 GB | −41% |
| **Compressed memory** | **25.22 GB** | **1.88 GB** | **−93%** |
| Swap used | 3.89 GB | 1.24 GB | −68% |
| Cached files (host) | 4.05 GB | 6.86 GB | +69% |
| Memory Pressure chart | yellow (sustained) | **green** | — |

The mechanism: every page touch in compressed memory pays a decompression cost that steals CPU from PG and MSSQL, and the swapped pages compete with `.mdf` reads on the same NVMe. Dropping Docker to 24GB also frees the host page cache to speed `.mdf` source reads.

### SO2010 Results (5 Runs, 24GB Docker, fresh PG volume)

| Run | Transfer | Overall | Time | Notes |
|----:|---------:|--------:|-----:|-------|
| 1 (cold) | 426K rows/s | 358K rows/s | 54s | retry on Posts (3.4% rework) |
| 2 | 1,193K rows/s | 861K rows/s | 22s | clean |
| 3 | **1,451K rows/s** | **1,074K rows/s** | 18s | clean — best |
| 4 | 1,411K rows/s | 1,047K rows/s | 18s | clean |
| 5 | 501K rows/s | 416K rows/s | 46s | retry on Posts (5.3% rework) |
| **Clean avg (2,3,4)** | **1,352K rows/s** | **994K rows/s** | **19s** | excludes retries |

### SO2013 Results (5 Runs, 24GB Docker, fresh PG volume)

| Run | Transfer | Overall | Time | Notes |
|----:|---------:|--------:|-----:|-------|
| 1 (cold) | 1,141K rows/s | 858K rows/s | 2m04s | retry on Posts (0.7% rework) |
| 2 | 1,140K rows/s | 848K rows/s | 2m06s | clean — exact 106.5M |
| 3 | 962K rows/s | 751K rows/s | 2m22s | retry on Posts (1.4% rework) |
| 4 | 1,032K rows/s | 812K rows/s | 2m11s | clean — exact 106.5M |
| 5 | 988K rows/s | 724K rows/s | 2m27s | retries (3.6% rework) |
| **5-run avg** | **1,053K rows/s** | **799K rows/s** | **2m14s** | |
| **Clean avg (1,2,4)** | **1,104K rows/s** | **839K rows/s** | **2m07s** | |

### SO2013 Pressure Comparison (same hardware, same code, 32GB vs 24GB Docker)

| Configuration | Run 1 (cold) Overall | Run 2 Overall |
|---------------|---------------------:|---------------:|
| 32GB Docker (host pressured) | 341K rows/s (5m12s) | 399K rows/s (4m27s) |
| 24GB Docker (host healthy) | **858K rows/s (2m04s)** | **848K rows/s (2m06s)** |
| Δ | **+152%** | **+113%** |

> Two cold-start data points at 32GB Docker were captured before the batch was killed
> and Docker resized. The improvement at 24GB is unambiguous and reproduces across all 5 runs.

### Cross-Config Comparison (SO2010, MSSQL→PG)

| Configuration | Transfer (avg) | Overall (avg) |
|---------------|---------------:|--------------:|
| M3 Max + Rosetta 2 + bind mount, 8GB Docker (original) | 472K rows/s | 287K rows/s |
| M3 Max Azure SQL Edge + named volume, fresh-PG steady-state | 880K rows/s | 686K rows/s |
| M3 Max Azure SQL Edge first-attach (prior peak) | 1,168K rows/s | 832K rows/s |
| M5 Pro 24GB host + 8GB Docker, Azure SQL Edge | 918K rows/s | 651K rows/s |
| **M5 Pro 48GB host + 24GB Docker, SQL Server 2022 (Rosetta 2)** | **1,352K rows/s** | **994K rows/s** |
| **M5 Pro 48GB host + 24GB Docker, best run** | **1,451K rows/s** | **1,074K rows/s** |

### Cross-Config Comparison (SO2013, MSSQL→PG)

| Configuration | Transfer (avg) | Overall (avg) |
|---------------|---------------:|--------------:|
| M3 Max + Rosetta 2 + bind mount, 8GB Docker (original) | 287K rows/s | — |
| M3 Max Azure SQL Edge + named volume, run 2-3-5 avg | 981K rows/s | 714K rows/s |
| M5 Pro 24GB host + 8GB Docker, Azure SQL Edge | 964K rows/s | 710K rows/s |
| M5 Pro 48GB host + 32GB Docker (pressured) — single run | 406K rows/s | 341K rows/s |
| **M5 Pro 48GB host + 24GB Docker, 5-run avg** | **1,053K rows/s** | **799K rows/s** |
| **M5 Pro 48GB host + 24GB Docker, clean-run avg** | **1,104K rows/s** | **839K rows/s** |
| **M5 Pro 48GB host + 24GB Docker, best run** | **1,141K rows/s** | **858K rows/s** |

### Concurrent COPY Stalls — Docker VM Networking Limit (was: "Posts Wide-Row Retry Pattern")

Initial framing was wrong. The retries were attributed to Posts' `nvarchar(MAX) Body` rows producing batches that exceeded the COPY context deadline. A follow-up investigation falsified that hypothesis and identified a different root cause.

#### Evidence that wide rows are *not* the cause

- A side-test pinning `chunk_size=30000` halved retry rate vs `chunk_size=50000` but did not eliminate retries. The runtime tuner was already shrinking Posts chunks: failed batches were sub-chunks of ~2,000 rows, not full 30K/50K chunks.
- Posts.Body actual distribution (SO2010, measured): p50=988B, p90=2,954B, p99=7,956B, p999=19,955B, max=97,394B. The largest *row* is 95KB; even a sub-batch full of outliers totals only a few MB. At PG's normal local-Docker COPY throughput (50–100 MB/s), this should complete in tens to hundreds of milliseconds, not the 30+ seconds that timed out.
- Live `pg_stat_activity` capture during a stall showed the stuck backend in wait state **`Client / ClientRead`** with `pg_stat_progress_copy.bytes_processed` frozen at the same value for 100+ seconds. PG was not slow, locked, or checkpointing — it was sitting in `recv()` waiting for the client to send more bytes. The stall is *upstream* of PG.

#### Mitigation experiments (M5 Pro 48GB / 24GB Docker / SQL Server 2022 Rosetta / SO2010, AI tuning enabled, `target_mode=drop_recreate`, fresh PG volume per run)

| Configuration | Sample | Retry rate | Run-time band | Notes |
|---------------|-------:|-----------:|---------------|-------|
| 30s timeout floor (original) | 5 runs | 2/5 (40%) | 22s – 2m18s | Baseline. Retries cluster around marginal-timeout cases. |
| 120s timeout floor | 5 runs | 1/5 (20%) | 21s – 2m18s | Halves the rate by absorbing marginal-timeout sub-batches. The remaining stalls last the full 120s. |
| 120s floor + PG `tcp_keepalives_idle=10 tcp_keepalives_interval=5 tcp_keepalives_count=3` | 10 runs | 1/10 (10%) | 19s – 2m15s | Keepalives did not detect failure during the stall — the connection stayed established, it was just slow. Rules out packet loss / dead path. |
| 120s floor + keepalives + **`write_ahead_writers=1`** (down from AI default 2) | 10 runs | **0/10 (0%)** | **20s – 27s** | Halves total concurrent COPY connections from 36 (18 workers × 2) to 18. **All 10 runs clean.** |
| **`write_ahead_writers=1` alone (30s floor, no keepalives)** | 10 runs | **0/10 (0%)** | **22s – 27s** | The concurrency cap is the only change that matters — 120s floor and keepalives add no incremental value when paired with it. |

The `write_ahead_writers=1` runs dropped peak per-run throughput by ~10–15% (best run 924K overall vs 1,074K with `writers=2`), but eliminated 100% of stalls and shrank the runtime band to a tight 22–27s window with no 2m+ outliers.

#### Root cause

**Docker Desktop VM networking saturates under concurrent COPY connection load.** With `workers=18` and `write_ahead_writers=2`, dmt holds 36 concurrent COPY connections to PG. Docker Desktop on macOS uses a vsock + userspace network stack with documented per-flow throughput limits under heavy concurrent connection counts. Above some threshold a small subset of connections enters a degraded-throughput regime (data trickles through at KB/s instead of MB/s) without dropping — the connection stays established, but `pg_stat_progress_copy.bytes_processed` barely advances. dmt's 30s timeout fires; the chunk retries on a fresh connection and succeeds.

This is environment, not code. The same dmt code on Linux native (no Docker VM network layer) does not exhibit the pattern, and prior M3 Max / WSL2 benchmarks didn't surface it. dmt makes the issue visible on macOS by picking concurrency settings tuned for the underlying CPU (`workers=18` for 18 cores) without subtracting capacity for the VM network bottleneck.

#### Recommendations

- **macOS Docker Desktop deployments:** cap `write_ahead_writers=1` (not 2). Drops concurrent COPY count from `2 × workers` to `1 × workers`, eliminating the stalls in the test above. Cost: ~10–15% lower peak throughput, gained: 100% predictable runtime.
- **Native Linux deployments:** keep `write_ahead_writers=2` (or whatever AI picks). The Docker VM bottleneck doesn't apply.
- **AI smartconfig (`internal/driver/ai_smartconfig.go`)** should detect macOS host + Docker target and cap `write_ahead_writers=1`. Until then, override in config when running on Apple Silicon Docker Desktop.

### Key Findings

1. **Host memory pressure dominates throughput on Apple Silicon laptops with consumer RAM budgets.** On a 48GB host, allocating 32GB to Docker pushed dmt + Docker + browser + macOS over the physical limit and triggered ~25 GB of compressed memory; throughput dropped 2.5x on SO2013. Dropping Docker to 24GB (50% of host) restored full throughput. The AI tuner sized for "20.2 GB available" based on the 48GB host total but did not subtract the 32GB Docker allocation — reasonable on Linux, where Docker is host-resident, but wrong on macOS where Docker runs in a wired-memory hypervisor VM.
2. **Rosetta 2 is not visibly the bottleneck on this hardware.** SQL Server 2022 (Rosetta 2) on M5 Pro 48GB / 24GB Docker reaches **1,141K transfer / 858K overall on SO2013 and 1,451K / 1,074K on SO2010** — exceeding all prior published numbers including ARM-native Azure SQL Edge on M3 Max. Peak instantaneous on these runs hits ~2.0M rows/s, well above the M3 Max Azure SQL Edge first-attach peak of 1.17M.
3. **First time overall throughput exceeds 1M rows/s on this codebase** — SO2010 best run 1,074K overall, code unchanged from the M3 Max measurements.
4. **The "Posts wide-row choke point" was Docker VM networking, not wide rows.** Capping `write_ahead_writers=1` (halving concurrent COPY connections from 36 to 18) eliminates 100% of stalls in a 10-run sample, at a cost of ~10–15% peak throughput. PG-side `pg_stat_activity` evidence (`Client / ClientRead` wait, frozen `bytes_processed`) confirms the bottleneck is outside both PG and the row-size estimator.
5. **Sizing rule of thumb (Apple Silicon laptops):** allocate Docker no more than ~50% of host RAM if dmt + browser + IDE will run concurrently. The other 50% is needed for dmt's working set, the host page cache that backs `.mdf` reads, and OS overhead. For target writes specifically: cap `write_ahead_writers=1` to stay below the Docker VM concurrent-connection threshold.

## Smartconfig Memory-Budget Compliance — 6-Model Sweep (M5 Pro 48GB, SO2010, MSSQL→PG)

This sweep evaluates how well different LLMs respect the smartconfig prompt's `chunk_size` budget at varying `max_memory_mb`, and how often the Go-side post-AI clamp (PR #156) has to step in.

### Test Environment

- **Hardware**: Apple M5 Pro, 48GB RAM, 18 CPU cores
- **Source / Target**: SQL Server 2022 (Rosetta 2) → PostgreSQL 16, both Docker, named volumes
- **Dataset**: SO2010 (~19.3M rows)
- **Mode**: `drop_recreate`, `create_indexes=false`, `create_foreign_keys=false`, `create_check_constraints=false` (transfer-only — isolates AI behavior from DDL noise)
- **Runtime controller**: disabled (`migration.runtime_tuning: false`) — only initial smartconfig runs, no mid-migration adjustments
- **State directory**: wiped between every run, so smartconfig sees no historical tuning grounding
- **Instrumentation**: a temporary `time.Since(...)` wrapper around `analyzer.Analyze(...)` in `applyAITuning` captured AI prompt latency; reverted after the sweep
- **Code**: branch `fix/memory-budget-cliff` (`6064eb5`)
- **Date**: May 2026
- **Local LLM host**: LM Studio at `http://localhost:1234`, OpenAI-compat API, MLX where applicable
- **Cloud**: Anthropic API direct
- **Sweep values**: `max_memory_mb ∈ {256, 512, 1024, 2048, 4096, 8192}` — single run per (model, budget) cell, 6 budgets × 6 models = 36 runs

### chunk_size raw pick (before clamp); **bold** = Go-side clamp triggered

| Budget | Gemma-26B-MLX | Gemma-e4b-MLX | Qwen3-Coder-30B | gpt-oss-20B | Haiku 4.5 | Sonnet 4.6 |
|---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 256 MB | **50000** | **50000** | **50000** | **50000** | **50000** | 5000 |
| 512 MB | 10987 | **50000** | **50000** | **50000** | **50000** | 10000 |
| 1024 MB | 21974 | **50000** | **50000** | **50000** | **50000** | 21974 |
| 2048 MB | 43948 | **50000** | **50000** | **50000** | **50000** | 43000 |
| 4096 MB | 50000 | 50000 | 50000 | 50000 | 50000 | 50000 |
| 8192 MB | 50000 | 50000 | 50000 | 50000 | 50000 | 50000 |
| **Sub-budget runs needing clamp** | **1/4** | **4/4** | **4/4** | **4/4** | **4/4** | **0/4** |

After the post-AI clamp, the *applied* `chunk_size` is identical across all six models at every budget (5493 / 10987 / 21974 / 43948 / 50000 / 50000). The clamp is what guarantees correctness; the model only determines how often the clamp has to do that work.

### Quality ranking on the budget-respecting axis

1. **Sonnet 4.6** — 0 clamps. The only model in this matrix that produces a budget-aware `chunk_size` at every budget. At 256/512 MB it rounds down to a clean number (5000, 10000) just under the safe ceiling; at 1024/2048 it picks at-ceiling. Reasoning is genuine, not pattern-matched.
2. **Gemma 4 26B (MLX)** — 1 clamp (only at the 256 MB extreme). At 512–2048 MB it echoes the Go-computed safe ceiling exactly, doubling cleanly with the budget. Best local model in the matrix, by a wide margin.
3. **Tied at 4 clamps each** — Haiku 4.5, Qwen3 Coder 30B, gpt-oss-20B, Gemma 4 e4b (MLX). All produce a constant `chunk_size = 50000` regardless of budget. Without the clamp, all four would have shipped chunk_sizes 9.7×, 4.5×, 2.3×, and 1.1× over budget at 256/512/1024/2048 MB respectively.

That Haiku exhibits the same constant-50000 failure as the smaller local models is consistent with the claim in `internal/driver/ai_smartconfig.go:579–582` — soft prompt constraints do not bind reliably across model sizes/providers, *which is why the clamp exists.*

### AI prompt latency (single smartconfig call per run)

| Model | Range | Median |
|---|---|---:|
| Sonnet 4.6 | 8.4–11.4 s | 10.1 s |
| Gemma 4 26B (MLX) | 4.5–6.5 s | 4.8 s |
| Haiku 4.5 | 4.1–5.5 s | 4.8 s |
| Gemma 4 e4b (MLX) | 5.3–6.1 s | 5.8 s |
| Qwen3 Coder 30B | 3.9–4.6 s | 4.5 s |
| gpt-oss-20B | 3.7–5.7 s | 4.4 s |

Sonnet pays roughly 2× Haiku's latency to deliver genuine budget-aware reasoning. End-to-end migration time difference is small (Sonnet adds ~5 s on a 25–30 s migration); the AI call is a small slice of total run time.

A separate spot-test against `google/gemma-4-26b-a4b` (the **non-MLX** GGUF reasoning variant) at 512 MB picked a budget-aware **10500** — quality similar to the MLX 26B — but AI prompt latency was **78 s**, 17× slower than the MLX cut. Not pursued for the full sweep on cost grounds.

### Throughput note (single-run, included for completeness only)

End-to-end transfer throughput across the 36-run matrix sat in 299K–849K rows/s, with most cells in the 600K–800K band. With one run per cell, only ≥30% deltas are statistically meaningful here. Two robust patterns:

- **Gemma 4 e4b is consistently the slowest end-to-end performer** (always in the 299K–636K band; never breaks 700K). Its constant-50000 picks aren't the cause — the applied `chunk_size` is identical to the other models post-clamp — so the gap must come from second-order knobs (`parallel_readers`, `max_partitions`, `checkpoint_frequency`, `upsert_merge_chunk_size`) where its picks differ.
- **Haiku has the tightest run-to-run band** (665K–734K rows/s, only 1.1× spread across all 6 budgets) — a useful default for predictable timings.

Counterintuitively, **Sonnet's runs were consistently the lowest-throughput** (566K–655K) despite being the only model to pick a correct `chunk_size`. On SO2010 at this scale, `chunk_size = 50000` is not actually OOM-blowing the ~5 GB working set the model is told to respect — so the clamp's value is **safety on tight-budget hosts**, not throughput on this 48GB host. The clamp pays dividends where the model's mispick would actually exceed RAM.

## Smartconfig Memory-Budget Compliance — Re-Sweep After PR #163 Imperative-Prompt Change

PR #163 (issue #162) rewrote `buildMemoryBudgetBlock` to be imperative: leads with `**CRITICAL CONSTRAINT — chunk_size MUST NOT exceed N**`, names the post-AI clamp as the consequence of violation, and emits a concrete `**Default action: set chunk_size = <default>**` line where `default = min(50000, ceiling)`. Two conflicting anchors ("50000 is a strong default", "do not under-provision") were rescoped so the budget block isn't fighting the rest of the prompt.

Same matrix as the previous section (M5 Pro 48GB, SO2010, MSSQL→PG, `drop_recreate`, no DDL, runtime tuning disabled, state dir wiped per run, single run per cell). Code: `492aa08` (post-merge of #163, including the loose-budget-anchor fix from review).

### Sub-budget Go-clamp engagements per model (lower = better)

| Model | OLD prompt | NEW prompt | Δ |
|---|---:|---:|---|
| Sonnet 4.6 | 0 / 4 | **0 / 4** | unchanged (already perfect) |
| Gemma 4 26B-MLX | 1 / 4 | **0 / 4** | now perfect |
| gpt-oss-20B | 4 / 4 | **0 / 4** | full fix |
| Qwen3 Coder 30B | 4 / 4 | **0 / 4** | full fix |
| Gemma 4 e4b-MLX | 4 / 4 | **0 / 4** | full fix |
| Haiku 4.5 | 4 / 4 | **1 / 4** | partial — still clamps at 2048 MB only |

**5 of 6 models go from clamp-needing to perfect compliance on the new prompt.** Issue #162's success threshold ("≥4/6 models bind on all 4 sub-budget cells") is exceeded. Haiku is the lone holdout, clamping only at 2048 MB (raw pick rounds up past the 43948 ceiling).

### Per-budget detail — applied `chunk_size`

| Budget | Sonnet OLD→NEW | Haiku OLD→NEW | gpt-oss OLD→NEW | Qwen3-Coder OLD→NEW | Gemma 26B OLD→NEW | Gemma e4b OLD→NEW |
|---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 256 MB | 5000 → 5493 | 5493\* → 5493 | 5493\* → 5493 | 5493\* → 5493 | 5493\* → 5493 | 5493\* → 5493 |
| 512 MB | 10000 → 10987 | 10987\* → 10987 | 10987\* → 10987 | 10987\* → 10987 | 10987 → 10987 | 10987\* → 10987 |
| 1024 MB | 21974 → 21974 | 21974\* → 21974 | 21974\* → 21974 | 21974\* → 21974 | 21974 → 21974 | 21974\* → 21974 |
| 2048 MB | 43000 → 43948 | 43948\* → 43948\* | 43948\* → 43948 | 43948\* → 43948 | 43948 → 43948 | 43948\* → 43948 |
| 4096 MB | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 |
| 8192 MB | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 | 50000 → 50000 |

`*` = Go-side clamp fired (raw pick was 50000 in every starred cell, both prompts).

Two notes on cells that look interesting in the raw sweep:

- **Sonnet 4.6 at 256/512 MB** picks the *exact* safe ceiling (5493 / 10987) on the new prompt where it previously rounded down to clean numbers (5000 / 10000). Tighter binding to the printed value.
- **Loose budgets (4096 / 8192 MB)** were unstable mid-PR. An earlier draft of #163 caused Haiku, gpt-oss-20B, Qwen3-Coder, and Gemma 4 e4b to break through the 50000 anchor *upward* to 87896 / 175792 (the actual safe ceiling at those budgets). Copilot review flagged this as an unmeasured behavioral change conflicting with the well-tested 50000 default; the merged version pins `default = min(50000, ceiling)` to keep the loose-budget anchor at 50000. **Re-running this sweep against current `main` will see 50000 at 4096/8192 across all six models, not the upward jumps.**

### AI prompt latency — old vs new (median per model)

| Model | OLD median | NEW median | Δ |
|---|---:|---:|---:|
| Sonnet 4.6 | 10.1 s | 9.5 s | ~ same |
| Haiku 4.5 | 4.8 s | 5.1 s | +6% |
| Gemma 26B-MLX | 4.8 s | 5.1 s | +6% |
| Gemma e4b-MLX | 5.8 s | 6.4 s | +10% |
| Qwen3-Coder 30B | 4.5 s | 6.4 s | +42% |
| gpt-oss-20B | 4.4 s | 6.6 s | +50% |

Local models pay a noticeable latency tax (~10–50%) on the longer + more imperative prompt; cloud models are unchanged. Still well under the smartconfig timeout on every model (~30 s default).

### Takeaways

- **Prompt engineering is doing the work**: post-#163, the Go-side clamp engages at most once per six runs across the matrix (Haiku's 2048 MB cell). On the old prompt it engaged 17 times across the same six models × four sub-budget cells.
- **The clamp stays**: per `internal/driver/ai_smartconfig.go:579–582`, soft prompt constraints don't bind under sufficient pressure (the original cap=200 / cap=500 stress tests still hit pre-clamp), so removing the clamp is not on the table. It's just doing dramatically less work.
- **Throughput at this scale is unchanged**. Same caveat as the previous section: at 48 GB host RAM and SO2010 (~5 GB working set), `chunk_size = 50000` was never actually OOM-blowing — the clamp / prompt change is correctness-on-tight-budget-hosts, not a speed dial. End-to-end transfer throughput across the new sweep stayed in the 300K–800K rows/s band, same as the OLD prompt.

## Chunk Size vs Memory-Fit Ceiling — Direct Throughput Sweep (issue #164)

PR #163's review pinned the smartconfig default at `min(50000, ceiling)`. The empirical question — *would picking the memory-fit ceiling at loose budgets actually be faster?* — was deferred to issue #164. This section answers it.

### Methodology

Smartconfig and generated-default clamps are out of the loop. `chunk_size` is set directly in YAML and locked alongside every other tunable knob (`workers=16`, `read_ahead_buffers=4`, `write_ahead_writers=2`, `parallel_readers=2`, `max_partitions=16`, all connection pools and thresholds explicit). Runtime tuning is disabled so the controller doesn't move parameters during the run. State directory wiped between runs; `target_mode: drop_recreate` so each run starts from a fresh PG schema. AI mapper may still run for unsupported type/DDL fallback surfaces, but `ApplyTunerSuggestions` produces zero parameter changes because every tunable is user-set.

Sweep grid: `chunk_size ∈ {50000, 87896, 100000, 175792, 250000}`, **3 runs per cell** = 15 runs total. Same fixture as the rest of this section (M5 Pro 48GB, SO2010 mssql→pg, transfer-only).

### Results

| chunk_size | run 1 | run 2 | run 3 | median | mean | clean retries |
|---:|---:|---:|---:|---:|---:|:---:|
| 50000 | 577K | 654K | 596K | **596K** | 609K | 0/3 |
| 87896 | 636K | 660K | 654K | **654K** | 650K | 0/3 |
| 100000 | 628K | 641K | 663K | **641K** | 644K | 0/3 |
| 175792 | **338K**\* | 599K | 651K | **599K** | 529K | **1/3** |
| 250000 | 655K | 662K | 595K | **655K** | 637K | 0/3 |

`*` Posts-retry event (`copy batch [82388:84304]: timeout: context deadline exceeded`) — the documented Docker VM concurrent-COPY pattern from #132.

### Median deltas vs the 50000 anchor

| chunk_size | median | Δ vs 50000 |
|---:|---:|---:|
| 50000 | 596K | — |
| 87896 | 654K | +9.7% |
| 100000 | 641K | +7.6% |
| 175792 | 599K | +0.5% (mean −13% with the retry-event run included) |
| 250000 | 655K | +9.9% |

### Decision rule application

Per the issue:
- **"50000 anchor is correct"** if rows/sec at 50000 is within ±3% of every larger value, OR rows/sec drops at chunk_size > 100000.
- **"Breakthrough is correct"** if rows/sec at the ceiling is **≥ +10%** over 50000 with no retry/GC regression.
- **"Hybrid"** if rows/sec rises monotonically up to some inflection point.

Observed:
- No chunk_size cleared the **+10% breakthrough threshold** by either median or mean. 87896 (+9.7%) and 250000 (+9.9%) came closest but both sit just under.
- 175792 had a **3× higher retry rate** than every other size (1/3 vs 0/12). The Posts-retry event dropped that run by 43%. This is the documented Docker-VM COPY-saturation pattern, not noise — at higher chunk × concurrent-writer load, the COPY pipeline crosses a threshold and stalls.
- The pattern is not monotonic — 87896 > 100000 by median, 175792 dips, 250000 recovers — so "hybrid with an inflection point" doesn't fit cleanly either.

**Verdict: 50000 anchor is correct on this fixture.** Marginal +5–10% upside at larger sizes, but nothing decisive, and 175792 carries real tail-risk. PR #163's `min(50000, ceiling)` cap stays.

### Caveats

- **Single fixture.** SO2010 mssql→pg on this hardware. PG's bulk-insert pipeline is one path; MSSQL target parallel BCP without TABLOCK is a different path that this experiment doesn't cover. A measurement on PG→MSSQL or sqlserver target could land differently — particularly if `write_ahead_writers=1` is in play (which already prevents the Posts-retry pattern but caps peak throughput at ~85% of `write_ahead_writers=2`).
- **`avg_row_bytes` matters.** SO2010 averages around 248–500 bytes/row. On a fixture with much larger rows (BLOBs, JSON), the same chunk_size in rows means a much larger memory footprint per chunk and the throughput curve could shift left. Out of scope here.
- **Single-run-per-cell at 3 reps.** The +10% threshold sits inside the run-to-run noise band. A more rigorous answer would need ≥10 runs per cell; the current 3 are enough to rule out a *strong* breakthrough but not enough to confirm a marginal one. Acceptable given the decision rule's symmetric framing.

### Confirmation Sweep — 50000 vs 87896 at n=15 (interleaved)

The original n=3 sweep above showed 87896 with a +9.7% median advantage over 50000 — under the +10% breakthrough bar but suggestive enough that an arbitrary-threshold rule wasn't the right way to settle it. This subsection answers the question with significance testing instead of a fixed threshold.

**Methodology**: same fixed-knob config as above, but only 50000 and 87896. **n=15 runs each = 30 runs total**, **interleaved** (50K, 87K, 50K, 87K, …) so any time-correlated noise (PG cache warmth, host thermal state, transient background load) hits both groups equally rather than correlating with order.

**Result: the n=3 effect did not replicate. 87896 is not statistically faster than 50000.**

| dataset | n | mean(87896 − 50000) | Welch t | M-W z | p (one-sided H₁: 87896 > 50000) |
|---|---:|---:|---:|---:|---:|
| Full | 15 / 15 | −17.8K (−3.0%) | −0.39 | −1.35 | M-W: 0.91 |
| Drop warmup (seq 1–2) | 14 / 14 | −31.8K (−5.1%) | −0.74 | −1.61 | M-W: 0.95 |
| Drop warmup + transient (seq 1,2,17,18) | 13 / 13 | −35.6K (−5.6%) | −1.03 | −1.82 | M-W: 0.97 |
| Clean only (also drop seq 30 retry event) | 13 / 12 | −14.1K (−2.2%) | −0.52 | −1.58 | M-W: 0.94 |

Mann-Whitney one-sided p-values for "87896 > 50000" range 0.91–0.97 — the data points the *opposite* direction. Two-sided p (0.07–0.18) also fails to clear α=0.05, so we can't say "they're different" either. **Conclusion: 50000 and 87896 are statistically indistinguishable on this fixture.**

**Tail risk re-confirmed**: in the n=15 sweep the 87896 group hit one Posts-retry event (`seq=30`, 347K rows/s, `retry_count=1`); the 50000 group hit zero retries across 15 runs. Same #132 Docker-VM concurrent-COPY pattern that took down `175792` in the original sweep, just less frequently at 87896.

**Why the n=3 result misled us**: with the observed n=15 standard deviation (~100K), the SE of a 3-run median is ~75K. A "+9.7% gap" between two 3-run samples is well within sampling noise. The +10% bar in the issue's decision rule, despite looking arbitrary, was a reasonable proxy for "an effect big enough to detect at n=3." Below that bar, n=3 is just under-powered.

**Verdict reaffirmed**: keep `min(50000, ceiling)`. The cap was already correct; this measurement makes the basis explicit instead of marginal.

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
