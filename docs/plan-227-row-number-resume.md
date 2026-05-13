# Plan: ROW_NUMBER pagination resume safety (issue #227)

**Status:** Draft. Awaiting review feedback (Codex). Not yet implemented.

## Problem

ROW_NUMBER pagination (composite/varchar PK tables) has an unsafe resume window.

The checkpoint coordinator in `internal/transfer/transfer.go:1244-1273` saves progress every `checkpoint_freq` acked chunks (default `10`). Between any acked-chunk and the next `SaveProgress()` call, up to `checkpoint_freq - 1` chunks of fully-committed target-side data sit in a window where:

- The target has rows through row N + freq
- The state DB still says `lastRowNum = N`

On crash and resume:
- The reader's `initialRowNum` comes from state DB → resumes from row N
- The reader re-fetches rows N+1 .. N+freq from source
- The writer re-inserts those rows → **duplicates on PK conflict** (or silent overwrites for tables without unique constraints)

The keyset path (single integer PK) handles this via `cleanupPartialData` at `transfer.go:365` — `DELETE WHERE pk > savedLastPK AND pk <= maxPK`. The ROW_NUMBER path has no equivalent because row numbers exist only in the source's `ORDER BY` computation, not as a column on the target.

The limitation is documented at `docs/RESTARTABILITY.md:249-265`. Tracked as a known footgun. PR fixing it is the next P0 on the production-readiness epic (#236).

## Constraints

- Must not require schema modifications to user tables (no extra `chunk_id` column).
- Should not require new permissions on the target if avoidable (CREATE TABLE on target is *typically* already granted because dmt creates the target table itself, but assuming nothing here is the safest stance).
- Must not regress non-resume (clean-run) performance for ROW_NUMBER-paged tables.
- Should reuse existing writer code paths where possible.

## Proposed strategy: idempotent INSERT on resume

When **all three** of these conditions hold:

1. Pagination mode == `row_number` (composite/varchar PK, or PK-less — but PK-less is rejected at `transfer.go:993-996`)
2. `resumeLastPK != nil` (we are resuming, not first-time-running)
3. Table has a PK (the constraint is the truth we conflict on)

…the writer switches its insert mode for that table from plain `INSERT` to driver-specific idempotent-INSERT:

| Driver | Resume-mode insert | Notes |
|---|---|---|
| **PG** | `INSERT INTO target (cols) VALUES (...) ON CONFLICT (pk_cols) DO NOTHING` | Falls back from `COPY FROM STDIN` to per-batch `INSERT` on resume only. |
| **MySQL** | `INSERT IGNORE INTO target (cols) VALUES (...)` | Trivial keyword change in the extended-INSERT writer. |
| **MSSQL** | `MERGE target USING (...) ON pk WHEN NOT MATCHED THEN INSERT` | Reuse the existing MERGE path that `target_mode: upsert` uses. |

Normal (non-resume) runs continue to use plain `INSERT` — no perf regression for clean migrations. The behavior change is contained to the resume window only.

## Why this works

- The target's PK constraint becomes the truth: re-inserting a row that's already there is a silent no-op rather than a duplicate.
- ROW_NUMBER-paged tables, by their nature, have composite/varchar PKs that ARE the unique identifier. ON CONFLICT / INSERT IGNORE / MERGE all have something definite to conflict on.
- Mechanism is symmetric with `target_mode: upsert`, which already uses these constructs. We're reusing the same code path for a different trigger.

## Estimated scope

~250–350 LOC + tests. Smaller than #226, comparable to today's smaller PRs.

| File | Change |
|---|---|
| `internal/transfer/transfer.go` | Plumb a `resumeIdempotent bool` flag into `writeJob` when `paginationMode == row_number && resumeLastPK != nil` |
| `internal/driver/postgres/writer.go` | New code path: `WriteBatchIdempotent(rows, pkCols)` using `ON CONFLICT (pk) DO NOTHING`; used in place of COPY when flag set |
| `internal/driver/mysql/writer.go` | Switch generated INSERT to `INSERT IGNORE` when flag is set |
| `internal/driver/mssql/writer.go` | Route through the existing upsert MERGE path |
| `internal/driver/writer.go` | Add `WriteBatchOptions.IdempotentOnDup bool` |
| `docs/RESTARTABILITY.md` | Rewrite the "ROW_NUMBER Pagination Limitations" section — resume is now safe; idempotent on duplicate keys |
| Tests | Kill-and-resume integration test against the SO2010-minimal fixture using a composite-PK table; assert no duplicates after resume |

## What this PR does NOT do

- **Staging tables per chunk (Strategy A in the issue body).** The thorough belt-and-suspenders fix. ~700+ LOC, 2x disk during migration, requires CREATE TABLE on target. Deferred: file as follow-up if the perf hit of resume-mode INSERTs becomes a real complaint.
- **TRUNCATE + restart from row 0 (Strategy C).** Trivial implementation but bad UX for partial big-table migrations (resume on a 50%-done 100M-row table wastes 50M rows of work). Rejected for ergonomics.
- **Changes to the keyset path.** Already has clean cleanup-on-resume via `cleanupPartialData`. Untouched.

## Specific questions for review

These are the design risks I'm uncertain about. Honest "the plan is wrong because X" answers are more valuable than "looks good."

> CODEX: High-level review: the idempotent-insert direction is viable, but the plan needs two important corrections before implementation. First, the current `drop_recreate` path creates PKs before transfer, so Q1's "PKs are created after TaskTransfer" premise appears stale. Second, resume preflight currently checks only table-level progress before truncating, while large ROW_NUMBER transfers save partition-level progress. That is a separate correctness blocker: a resume can truncate the target, keep stale partition checkpoints, then restart each partition from a non-zero row number and skip data. Fix that before or with the writer idempotency work.

### Q1: PK availability at resume time (likely fatal — needs answer first)

ROW_NUMBER tables in the drop-recreate flow have their PK created in **TaskCreatePKs**, which runs **AFTER TaskTransfer** (see `orchestrator/target_mode.go`). If a migration crashes mid-TaskTransfer and resumes mid-TaskTransfer, the PK constraint **does NOT yet exist on the target**. `ON CONFLICT` has nothing to conflict on; `INSERT IGNORE` has nothing to ignore.

This may kill the plan. Options:
- (a) Add a unique index on the PK columns at the START of TaskTransfer for ROW_NUMBER tables, then convert it to a real PK later. Cheap-ish; the index already gets built later anyway.
- (b) Run TaskCreatePKs *before* TaskTransfer for ROW_NUMBER tables specifically. Diverges the phase order per pagination strategy.
- (c) Different fix entirely (Strategy A or C from above).

Reviewer: please verify the constraint-ordering claim against the actual code path, and weigh in on (a)/(b)/(c) — or propose another option.

> CODEX: I do not think Q1 is fatal in the current code. `internal/orchestrator/orchestrator.go` calls `targetMode.PrepareTables()` before `transferAll()`, and `dropRecreateStrategy.PrepareTables()` immediately calls `CreateTableWithOptions()` then `CreatePrimaryKey()` for each table. For MySQL and MSSQL, `CreatePrimaryKey()` is a no-op because the generated table DDL includes the PK inline. For PostgreSQL, `CreatePrimaryKey()` runs `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY` before transfer starts. The `TaskCreatePKs` constant still exists, but this path does not appear to run PK creation as a post-transfer phase anymore. I would remove options (a)/(b) from the main plan unless there is another execution path not covered by `Run()`/`Resume()`.

> CODEX: The caveat is resume when the table was missing and `Resume()` recreates it. That path calls `targetPool.CreateTable(ctx, &t, ...)`, not `targetMode.PrepareTables()`. For PG, confirm whether `CreateTable()` includes PK inline via the current table mapper; if not, a missing-table resume would recreate a table without the separate `CreatePrimaryKey()` call. This is less common than mid-transfer crash, but the implementation should either call the same prepare strategy or explicitly ensure PK existence before any idempotent insert.

### Q2: PG COPY → per-batch INSERT on resume

Switching from `COPY FROM STDIN` to per-batch `INSERT ... ON CONFLICT` changes the throughput shape during resume:

- `COPY` is a streamed protocol with no per-statement param limit.
- `INSERT VALUES` goes through the driver's prepared-statement param limit (pgx has one, ~65535 in some versions).

For large chunks (e.g. 100K rows × 10 cols = 1M params), the INSERT path could exceed the limit. Need either:
- Sub-chunk the INSERT (~10K rows per statement) — easy, slight overhead
- Stay on COPY but COPY into a staging table, then `INSERT-SELECT ... ON CONFLICT` (back to Strategy A's shape)

Reviewer: does pgx's param limit actually bite at the chunk sizes dmt uses? Or is the sub-chunking trivial enough that this is a non-issue?

> CODEX: Avoid the param-limit problem entirely if possible. PG already has an upsert staging shape in `internal/driver/postgres/writer.go`: temp table + `COPY` + `INSERT ... SELECT ... ON CONFLICT`. Add an insert-only conflict mode to that path (`DO NOTHING`) instead of building giant `INSERT VALUES` statements. That preserves COPY for resume and keeps behavior closer to the current high-throughput writer. If the implementation still chooses `INSERT VALUES`, then yes, it must sub-chunk by `floor(65535 / len(cols))` or lower; the default 100K chunk size will exceed the bind limit for many real tables.

> CODEX: Do not reuse the existing PG `UpsertBatch()` unchanged for resume. It emits `DO UPDATE SET ...`, so a replayed already-committed row can overwrite the target with a changed source value. The plan's desired semantics are "already present means no-op"; that needs a new option or a separate helper.

### Q3: MySQL `INSERT IGNORE` masks too much

`INSERT IGNORE` silently skips not just PK conflicts but **also data-conversion errors** (string-too-long, invalid date, out-of-range numeric, etc.). On resume, a row that was already in the target *and* now has a type-mismatch issue from source would silently disappear. That's data corruption masquerading as a successful resume.

Alternatives:
- `INSERT ... ON DUPLICATE KEY UPDATE pk_col = pk_col`: skips on PK conflict, errors on data issues. Slightly heavier (UPDATE is a no-op but still goes through the executor).
- Run with stricter `sql_mode = STRICT_ALL_TABLES` per connection — promotes warnings to errors, neutralizes IGNORE's masking. But changes connection state globally.

Reviewer: is `ON DUPLICATE KEY UPDATE pk = pk` the right call, or am I overthinking — would real-world type-mismatch errors during *resume* be caught by other layers (the source schema hasn't changed, so why would a value that worked on first attempt fail on retry)?

> CODEX: Use `INSERT ... ON DUPLICATE KEY UPDATE pk_col = pk_col`, not `INSERT IGNORE`. The masking behavior of `IGNORE` is broader than this feature needs, and resume safety should not depend on "the same row converted successfully last time." The existing MySQL `UpsertBatch()` updates all non-PK columns, so it is also too strong for this case. Add an insert-only/idempotent mode that uses a no-op PK assignment. For composite PKs, assigning the first PK column to itself should be enough.

### Q4: MSSQL MERGE path's hidden dependencies

The existing MERGE path is used only by `target_mode: upsert` today. Routing resume-INSERTs through it might trip over assumptions baked into the upsert codepath:

- TVP (Table-Valued Parameter) setup might assume a separate staging schema
- MERGE statement template might include UPDATE branches that only make sense for upsert semantics
- Source-side row generation might assume the upsert path's specific column ordering

Reviewer: is the MERGE wiring actually reusable for a resume-only context, or would I be adding a new code path that just *looks* like upsert?

> CODEX: Reuse the staging/bulk-load pieces, but not the current merge template as-is. `buildMerge()` includes a `WHEN MATCHED ... THEN UPDATE` branch. For resume, matched rows should be left untouched. Add an insert-only MERGE builder (`WHEN NOT MATCHED THEN INSERT ...`) or a mode flag that suppresses the matched/update branch. Also pass `partitionID` through `safeStagingName()`; `UpsertBatch()` currently calls `safeStagingName(opts.Table, opts.WriterID, nil)`, so two partitions on the same table can reuse the same local temp name on different connections safely, but partition-aware naming would make diagnostics and future non-local staging safer.

### Q5: Strategy A counterargument

I rejected staging tables (~700 LOC, 2x disk, CREATE TABLE perm). The user (issue filer) initially recommended staging as the proper fix. Counter-arguments to "ship idempotent-INSERT now, defer staging":

- Idempotent-INSERT only works when the PK exists at resume time (see Q1). Staging always works.
- Staging cleanly composes with the existing keyset path's `cleanupPartialData` model (orphan staging table = orphan data, drop it).
- 2x disk during migration is not actually expensive on modern hardware; 700 LOC is one focused PR.

Reviewer: is the staging approach actually better as the first cut, given (a) the PK-availability issue makes idempotent-INSERT unreliable, (b) staging is simpler to reason about, (c) the disk cost is less of a concern than I framed it?

> CODEX: Since PK availability is mostly not the blocker, I would not jump all the way to durable per-chunk staging as the first cut. But the PG and MSSQL implementations should probably use their existing temporary staging mechanics internally for performance and SQL shape. In other words: "staging inside a batch writer" looks attractive; "persistent staging tables as the correctness model" still looks heavier than necessary for issue #227.

> CODEX: The real first-cut blocker is resume preflight, not target-side staging. Before relying on idempotent insert, fix `Resume()` so large ROW_NUMBER tables with partition checkpoints are not truncated based on a nil table-level checkpoint. Reasonable options: teach resume preflight to inspect partition tasks for any saved progress, or clear all partition progress whenever it truncates. Without this, idempotent inserts may never get a chance to protect the replay window.

### Q6: Strategy C counterargument

Rejected as bad UX for partial big-table migrations. Counter-argument: how common is "ROW_NUMBER table that's halfway through" in real workloads?

- ROW_NUMBER is used for composite/varchar PKs
- Such tables are typically *lookups* (small) or *junction tables* (medium), not the giant fact tables
- Giant fact tables almost always have integer PKs → use keyset path
- So restart-from-zero on a ROW_NUMBER table is bounded by composite-PK-table size, which in real schemas is usually <10% of total data

Reviewer: if real workloads have most data on keyset paths, is Strategy C's "wasted work" actually small enough that the simplicity wins?

> CODEX: I would not choose Strategy C as the primary fix. The code already has ROW_NUMBER partitioning for large composite/varchar-PK tables, which implies the project expects some of these tables to be big enough to care about. Restart-from-zero is acceptable as an emergency fallback if constraints are missing or progress looks inconsistent, but making it the normal resume behavior would discard the value of the existing partition/checkpoint machinery.

## Recommendation pending answers

If Q1 is fatal (PK doesn't exist at resume time): **switch to Strategy A** (staging tables) — it's the only option that doesn't depend on a target-side constraint.

If Q1 has a clean workaround (option (a): pre-create unique index on PK columns at TaskTransfer start for ROW_NUMBER tables): proceed with the idempotent-INSERT plan above.

If reviewers convincingly argue Strategy C: simpler, ship that, move on.

> CODEX: Updated recommendation based on current code: proceed with idempotent insert-on-conflict, but scope it as an insert-only writer mode rather than reusing upsert semantics wholesale. Required plan changes:
>
> 1. Fix resume preflight for partition-level ROW_NUMBER checkpoints before writer changes are considered complete.
> 2. Ensure target PK existence on the resume missing-table path, especially PG `CreateTable()` vs `CreatePrimaryKey()`.
> 3. Add `WriteBatchOptions.IdempotentOnDup` or a similar option, plus PK columns, and implement insert-only behavior per target:
>    - PG: temp staging + COPY + `INSERT ... SELECT ... ON CONFLICT (...) DO NOTHING`.
>    - MySQL: `INSERT ... ON DUPLICATE KEY UPDATE pk = pk`.
>    - MSSQL: staging + insert-only `MERGE` with no `WHEN MATCHED` update.
> 4. Add tests for both the duplicate replay window and the resume-preflight truncation/stale-partition-checkpoint case.

## Reviewer instructions

Please review and update this doc in place. Add findings as new sections or inline comments using `> CODEX:` block-quote prefixes. Specific questions Q1-Q6 above are where I most need critical feedback. If the plan is fundamentally wrong, say so.
