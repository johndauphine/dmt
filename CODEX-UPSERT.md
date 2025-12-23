# Notes: Upsert/merge performance (MSSQL)

Context
- Current approach: load to staging, then one large MERGE. Millions of rows per MERGE is slow and log-heavy.
- Goal: faster upserts with less blocking/log pressure.

Recommendations
- Avoid one mega-MERGE. Batch by PK ranges (e.g., 50k–250k rows). Use a temp table of batch boundaries `(batch_id, min_pk, max_pk)` via `row_number()` or NTILE on staging PK.
- Prefer UPDATE + INSERT over MERGE for large batches. MERGE can be slower and has edge-case risks; explicit UPDATE/INSERT is easier to tune and batch.
- Compare before updating. Add change detection in UPDATE (`WHERE t.col1 <> s.col1 OR …` or a hash column) to skip untouched rows and reduce logging.
- Keep staging lean. Bulk insert with TABLOCK into a heap or a table with only the clustered PK; no secondary indexes. Use BULK_LOGGED/SIMPLE during load if allowed.
- Commit per batch. Tune batch size to balance throughput vs. log growth; keep transactions bounded.
- Partition-aware if possible. If target is partitioned and staging matches the scheme, process per partition; for insert-only partitions you can `ALTER TABLE … SWITCH`.
- Indexes/constraints. Rebuild nonclustered indexes after upserts, not on staging. Triggers/FKs increase cost—disable only if you can safely re-validate.
- Logging/locking. Ensure a supporting PK/unique index on target for join/NOT EXISTS. Monitor log size, waits, and lock escalation; adjust batch size accordingly.

Sketch T-SQL (pattern)
```
-- staging loaded, clustered on PK
;with bounds as (
  select min_pk = min(pk), max_pk = max(pk)
  from (
    select pk, batch = (row_number() over(order by pk)-1)/50000
    from staging
  ) d group by batch
)
declare @min bigint, @max bigint
declare c cursor fast_forward for
  select min_pk, max_pk from bounds order by min_pk
open c
fetch next from c into @min, @max
while @@fetch_status = 0
begin
  -- UPDATE changed rows only
  update t
    set ... -- columns
  from target t
  join staging s on t.pk = s.pk
  where s.pk between @min and @max
    and (t.col1 <> s.col1 or t.col2 <> s.col2 ...); -- or hash compare

  -- INSERT missing rows
  insert into target (col1, col2, ...)
  select col1, col2, ...
  from staging s
  where s.pk between @min and @max
    and not exists (select 1 from target t where t.pk = s.pk);

  fetch next from c into @min, @max
end
close c; deallocate c;
```

Tuning knobs
- Batch size (rows per batch), recovery model during load, presence of NCI/FKs/triggers, and whether to hash for change detection. Bench and adjust per environment.
