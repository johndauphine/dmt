# Gemini Implementation Plan

## 1. Analysis & Findings

Based on the review of `@CODEX-PLAN.md` and the existing feature branches (`feature/airflow-support`, `feature/upsert-mode`), here is the synthesis of the current state and the path forward.

### Findings
1.  **Upsert Logic Exists:** The `feature/upsert-mode` branch already implements the core logic for idempotent transfers (PostgreSQL `ON CONFLICT` and SQL Server `Stage -> MERGE`). We do not need to rewrite this from scratch as suggested in parts of the Codex plan.
2.  **Performance Bottleneck Identified:** The SQL Server upsert implementation in `feature/upsert-mode` creates a HEAP staging table. This will cause performance degradation on large chunks (`Hash Match` joins). **Optimization required:** Create a clustered index on the staging table before the MERGE step.
3.  **Same-Engine Validation:** The only thing blocking PG→PG or MSSQL→MSSQL migrations is the validation logic in `internal/config/config.go`.
4.  **Legacy MSSQL Support:** The current codebase relies on `STRING_AGG` (SQL Server 2017+ / Compat 140+). To support older enterprise environments, we must implement the `STUFF / FOR XML PATH` fallback for metadata extraction.

## 2. Implementation Strategy

The approach is to **Consolidate, Optimize, then Expand**.

### Phase 1: Consolidate Feature Branches
We have two high-value branches that should be merged to `main` to form the foundation for the upgrade.

1.  **Merge `feature/airflow-support`:**
    *   Provides `FileState` (YAML backend) essential for stateless/containerized runs.
    *   Provides JSON logging/output for observability.
    *   *Action:* Merge immediately; no conflicts expected with core logic.

2.  **Merge `feature/upsert-mode`:**
    *   Provides the `target_mode: upsert` configuration.
    *   Provides the `writeChunkUpsert` pipeline hooks.
    *   *Action:* Merge immediately; this is the engine for the "Same-Engine" capability.

### Phase 2: Core Enhancements (The "Meat")

#### 2.1 Relax Configuration Validation
*   **File:** `internal/config/config.go`
*   **Change:** Modify `validate()` to allow `Source.Type == Target.Type` **ONLY IF** `TargetMode` is `upsert` or `truncate`.
*   **Reasoning:** `drop_recreate` on the same database is dangerous/redundant. `upsert` is the primary use case for same-engine sync.

#### 2.2 Optimize MSSQL Upsert Performance
*   **File:** `internal/target/mssql_pool.go`
*   **Function:** `UpsertChunk`
*   **Change:** Inject a `CREATE CLUSTERED INDEX` statement between the Bulk Copy and the MERGE.
    ```sql
    -- 1. Create Staging (Done)
    -- 2. Bulk Insert (Done)
    -- 3. NEW: Create Index
    CREATE CLUSTERED INDEX IX_Staging_PK ON #staging (pk_cols...);
    -- 4. MERGE (Done)
    ```
*   **Impact:** Changes the MERGE plan from a Hash Match (memory intensive) to a Merge Join or Stream Aggregate (fast, sorted).

#### 2.3 Legacy Metadata Support
*   **File:** `internal/source/pool.go` (and schema query definitions)
*   **Change:**
    1.  On connection, query `SELECT compatibility_level FROM sys.databases`.
    2.  If level < 140 (SQL Server 2017), use `STUFF((SELECT ',' + col FROM ... FOR XML PATH('')), 1, 1, '')` instead of `STRING_AGG`.
*   **Impact:** Unblocks users on SQL Server 2012/2014/2016.

### Phase 3: Same-Engine Type Mapping
*   **File:** `internal/typemap/types.go`
*   **Change:** Ensure the mapper doesn't unnecessarily cast types when source == target.
    *   *Example:* PG `jsonb` -> PG `jsonb` (currently might try to map to MSSQL `nvarchar` equivalent if not careful).
    *   Add a `SameEngine bool` flag to the type mapper or pass the target engine type explicitly to ensure direct mapping.

## 3. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| **Data Loss (Same Engine)** | Disable `drop_recreate` for same-engine by default. Force user to explicit opt-in or restrict to `upsert`. |
| **MSSQL Locking** | The `MERGE` statement can cause deadlocks under high concurrency. **Mitigation:** Recommendation in docs to lower `workers` count (e.g., 2-4) when using Upsert mode on MSSQL. |
| **Infinite Loops** | If `upsert` logic fails to detect changes correctly, it might report "success" but data remains stale. **Mitigation:** The current `EXCEPT` / `IS DISTINCT FROM` logic is robust. Add unit tests for NULL handling. |

## 4. Execution Order

1.  **Review & Merge** `feature/airflow-support` to `main`.
2.  **Review & Merge** `feature/upsert-mode` to `main`.
3.  **Create Branch** `fix/mssql-performance` -> Implement Indexing on Staging.
4.  **Create Branch** `feat/legacy-compat` -> Implement `STUFF` fallback.
5.  **Create Branch** `feat/same-engine` -> Relax config validation + verify TypeMap.
