# Performance Benchmarks

Comprehensive benchmark results comparing Go and Rust implementations.

## Test Environment

- **Hardware**: Apple M3 Pro, 36GB RAM, 14 CPU cores
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
go build -o mssql-pg-migrate .

# MSSQL → PostgreSQL (drop_recreate)
./mssql-pg-migrate -config benchmark-config.yaml run

# MSSQL → PostgreSQL (upsert)
./mssql-pg-migrate -config benchmark-upsert.yaml run
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