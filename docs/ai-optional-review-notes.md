# Review notes — AI-optional architecture epic (#167) and related issues

## Scope of this document

A second-AI reviewer of the dmt project's open issues authored these notes after reading:

- `#167` — [Epic] AI-optional architecture: deterministic-first with AI as enhancement
- `#168` — Deterministic type mapping baseline (canonical IR + per-driver mappers)
- `#169` — Deterministic DDL generation (CREATE TABLE, PK, FK, index, check, default)
- `#170` — Wire deterministic type mapper as default; AI becomes registered fallback
- `#171` — Replace AI runtime adjuster with rule-based controller
- `#172` — Deterministic DB tuning interrogation per driver (replaces ai_analyzer)
- `#173` — Pattern-matched error diagnosis catalog with AI fallback
- `#174` — Setup wizard and init-secrets: AI as opt-in, not gating
- `#166` — Per-target chunk_size optimum (bytes-based), not a universal anchor (parallel track)

The framing is sound — these are the right cleavage planes for flipping dmt from "AI-first, fails closed without it" to "deterministic-first with AI as registered enhancement." The critical path identification (typemap + DDL are the only hard-required AI surfaces; everything else has a soft fallback today) is accurate. Notes below are concerns and additions, not objections to the overall direction.

## Concerns by issue

### #168 — Deterministic type mapping baseline

**Concern: scope feels under-budgeted relative to what's being replaced.**

The current `internal/driver/ai_typemapper.go` is 2205 LOC. The proposed package layout in #168 is seven files with a 13-Kind canonical enum (`Integer`, `Decimal`, `Float`, `String`, `Bytes`, `Bool`, `Date`, `Time`, `Timestamp`, `TimestampTZ`, `UUID`, `JSON`, `Unknown`). The IR shape is right (pgloader / Apache Arrow precedent), but the messy parts of cross-DB type mapping aren't enumerated:

- Decimal precision/scale handling: `decimal(38,12)` (PG max precision) vs MSSQL `decimal(38,38)` vs MySQL `decimal(65,30)`. Mapping requires precision-loss decisions, not just kind dispatch.
- Datetime offset and precision: MSSQL `datetime2(7)` ↔ PG `timestamp(6)` (precision loss); `datetimeoffset` ↔ PG `timestamptz` (semantic alignment); MySQL `datetime` (no TZ) ↔ either of the above (timezone interpretation hazard).
- Charset / collation pinning: MSSQL `nvarchar(max)` is UTF-16, PG `text` is UTF-8 by default but DB-collation-dependent, MySQL varies by `character_set_*` settings.
- Length encoding: MSSQL `varchar(8000)` vs `varchar(max)` vs PG `text` vs MySQL `text`/`mediumtext`/`longtext` size tiers.
- Boolean representation: PG `boolean`, MSSQL `bit`, MySQL `tinyint(1)` — not all bit-shaped, not all 1-byte.

**Suggested action**: either expand #168 to specify these decisions (likely as appendix tables of "source kind+modifier → canonical → target"), or break #168 into "phase 1: enum + happy path" and "phase 1b: precision/charset/datetime semantics" so reviewers and implementers know the work isn't done at a 13-kind enum.

**See also: working precedent in UVG (`/Users/john/repos/uvg/`).** UVG implements exactly the canonical-IR + per-dialect-mapper architecture #168 proposes, in production today, dual-licensed Apache-2.0 / MIT (so dmt can port freely). Its 22-variant `CanonicalType` resolves several of the messy bits above by encoding them in the type system directly (see "Working precedent: UVG" section below). #168's enum should be expanded to match UVG's shape rather than be redesigned from scratch.

### #169 — Deterministic DDL generation

**Concern 1: DEFAULT expression translation table is referenced but not enumerated.**

The issue mentions `CURRENT_TIMESTAMP` / `GETDATE()` / `NOW()` translation but stops there. Real-world DEFAULTs include sequence references (PG `nextval('foo_seq')`, MSSQL `IDENTITY` properties surfaced as DEFAULT in dump-style schemas), `NEWID()` / `gen_random_uuid()` / `UUID()`, computed expressions referencing other columns, and vendor functions (`SYSUTCDATETIME()`, `CURRENT_USER`, etc.). Some of these MUST go through the AI fallback; the issue should call out which.

**Concern 2: round-trip test is great but coverage is implicit.**

"Round-trips for all reference fixtures" is the testable bar. But the reference fixtures (SO2010/SO2013/WWI/pgbench per #168) need to exercise the DDL surface (CHECK constraints, partial indexes, FK actions, composite PKs). It's worth auditing the fixtures explicitly — does WWI have computed columns? Does SO2013 have CHECK constraints? Does pgbench have FKs? If any DDL surface isn't exercised by the fixtures, the round-trip test won't catch its regressions.

**Suggested action**: add an appendix to #169 mapping each DDL surface to which fixture exercises it. Anything unexercised needs either a synthetic test fixture or an explicit "covered by AI fallback only" note.

**See also: UVG's `src/ddl_typemap/` covers the column-level DDL emission half of #169** — `from_canonical(ct, target) → DdlType { sql_type, is_approximate, warning }`. Per-dialect emission lives in ~150–230 LOC files. The remaining DDL surfaces (constraints, PK/FK, indexes, defaults) are not in UVG and stay #169's scope.

### #170 — Wire deterministic type mapper as default

**Concern 1: `migration.unmapped_type_action` is a new config knob that needs definition.**

The issue mentions it as "skip | fail | conservative-text" but doesn't say where in `internal/config/config.go` it lives, what the default is, or how it interacts with the existing AI-fallback path. Worth pinning before implementation.

**Suggested default**: `fail` for safety (a silently-skipped column is worse than an error), with `conservative-text` as a documented opt-in for users who want best-effort migration.

**Concern 2: removing `GetAITypeMapper` with no compat shim is bold but consistent with project policy.**

CLAUDE.md says no backwards-compat hacks. So this is in line with stated policy. But it's worth re-confirming with the maintainer before the PR lands — this is a public-ish API change (anything in `internal/` is technically internal, but external integrators may have copied or imported it).

### #171 — Replace AI runtime adjuster with rule-based controller

**Concern 1: rule set is sketched, not specified.**

The issue lists four rules, each shaped as `IF metric trend → adjustment`. Two issues:

- The rule *"queue_depth grew for 3 consecutive ticks → +1 writer"* responds to one shape of bottleneck. *"queue_depth has been high but stable for 3 ticks"* is a different bottleneck signal (sustained backpressure rather than growing) and should arguably trigger the same response. The proposed rule misses it.
- *"error_rate > 0 → -1 writer"* treats any non-zero error rate as a writer-overload signal, but errors can come from other causes (unrelated source-side issues, target DDL mismatches, transient network blips). Conflating them with writer pressure leads to over-throttling.

**Concern 2: replay test has no quantitative bar.**

"Controller should produce sane adjustments on the same traces (not necessarily identical to AI's, but defensible)" is a soft bar. What's "defensible" measured by? Same final throughput within X%? Fewer adjustments per run? Identical adjustment direction in M of N cases? Without a number, the test has no fail criterion.

**Concern 3: the existence of the AI adjuster's effectiveness circuit breaker is itself evidence.**

The current AI adjuster has a circuit that pauses adjustments for 10 minutes after 3 consecutive negative effects (`internal/monitor/ai_adjuster.go`). The fact that this exists tells you the AI's adjustments are *often wrong* in production. The replacement controller's rule set inherits the same risk — bad rules can also worsen things. The replacement needs to keep an effectiveness check (is the rolling throughput trend positive after my adjustments?) as a safety net, not just rely on rules being correct.

**Suggested action**: pin a quantitative replay-test bar (e.g., "controller's run-end throughput within 5% of AI-adjuster's on the same trace, p<0.10 by paired t-test across ≥10 traces"). Add an effectiveness fuse to the controller mirroring the AI version's.

### #172 — Deterministic DB tuning interrogation

**Concern 1: "20 queries per driver" gets us to 80%, not 100%, and the issue doesn't say so.**

The long tail of useful tuning recommendations is wider than 20 settings:
- PG: vacuum / autovacuum tuning, table fillfactor, parallel query workers, JIT settings, replication-lag impact on replication slots
- MSSQL: filegroup layout, tempdb sizing, query store thresholds, resource governor
- MySQL: InnoDB redo log sizing, doublewrite buffer, adaptive hash index, query cache (deprecated but still encountered)

These won't be in v1 of the deterministic catalog. AI fallback should be expected to handle them indefinitely or get folded in incrementally.

**Concern 2: the parity bar is too tight.**

"Recommendations match (within margin) what the AI analyzer produces today" is a hard bar because (a) AI sometimes generates better recommendations than rules would, and (b) AI sometimes generates worse ones. Either failure mode trips the parity test. Better bar: "no worse on benchmark fixtures" (measurable as throughput delta when each set of recommendations is applied).

**Suggested action**: soften the parity bar to "no regression on benchmarks" + "human-spot-check shows recommendations are reasonable per driver expert". Note explicitly that AI fallback covers the long tail.

### #173 — Pattern-matched error diagnosis catalog

**Concern 1: error catalogs bit-rot across DB versions.**

PG 12 → 16 changed several error message formats. MSSQL 2019 → 2022 added new error codes. MySQL 5.7 → 8.0 reorganized many SQLSTATE strings. A catalog that's hand-coded today against current DB versions will silently start missing patterns as users upgrade.

**Concern 2: 20 patterns per driver is a 80% cover for known errors.**

The long tail of error messages in production is enormous. Real users hit obscure errors (replication-slot overflow, partition-pruning bugs, character-set encoding edge cases, role-permission cascades) that won't be in the v1 catalog. AI fallback handles those — that's the right design, but the issue should set expectations (this catalog is not going to catch everything, and shouldn't try to).

**Concern 3: production telemetry as a pattern source is noted as "follow-up" but should be earlier.**

The fastest way to expand the catalog is to mine which errors users are actually hitting. If AI fallback fires often on the same error pattern, that's a candidate for promotion to the deterministic catalog. The wiring for this telemetry should land in the same PR as the catalog itself, not as an afterthought, otherwise the catalog grows by guesswork.

**Suggested action**: scope a per-driver test that runs against the project's `make test-dbs-up` containers across the supported DB versions, asserting the catalog patterns match. Add a fallback-firing counter (with anonymized error fingerprints) to the same telemetry surface as the AI fallback observability concern below.

### #174 — Setup wizard and init-secrets: AI as opt-in

**Concern 1: this is more than a wizard change — it's a docs/marketing repositioning.**

Once "no AI by default" lands, the entire user-facing framing of dmt changes:
- `README.md` Quick Start currently expects AI configuration
- `CLAUDE.md` describes dmt with "AI-driven parameter tuning" in the project overview
- `config.yaml.example` has the AI section prominent
- The project's positioning (in any external README, on PRs, in benchmarks docs) consistently says "AI-driven X"

The issue mentions README and `config.yaml.example` but doesn't mention CLAUDE.md or any other surface. Worth a sweep.

**Concern 2: existing-user migration is implicit.**

The issue doesn't say what happens to a user who already has `~/.secrets/dmt-config.yaml` configured. The straight read is "their behavior is unchanged" (AI configured → AI is used as fallback decorator). That's the right answer but should be said explicitly so reviewers and existing users aren't surprised.

**Suggested action**: expand #174 to include a "Documentation surfaces to update" subsection (CLAUDE.md, README, `config.yaml.example`, any benchmarks doc that calls out AI as headline), and explicitly state the migration story for existing AI-configured users (no-op).

### #167 — Epic

**Concern: backwards-compat policy is implicit.**

#170 says "no compatibility shim" for `GetAITypeMapper`. CLAUDE.md says "Avoid backwards-compatibility hacks." So they agree. But for a change of this magnitude — flipping the default for every new install — the maintainer should re-confirm explicitly. A user who upgrades dmt and hits a behavioral surprise might want a one-release deprecation window even if the policy says no.

**Suggested action**: get explicit maintainer sign-off on "we ship the AI-optional default in version V; pre-V users with AI configured see no change; pre-V users without AI configured (a new state, since today they can't migrate at all) get the new deterministic path."

## Missing from the current issue set

### A. AI fallback observability

Once AI becomes the long-tail handler, there needs to be a way to know:
- How often the deterministic path returns `Unknown` / `ErrUnsupported` and falls through to AI.
- Which types / errors / settings / DDL surfaces are most commonly the trigger.
- Across all users / all migrations (aggregated, anonymized).

A single log line per fallback isn't enough at scale. A counter incremented per fallback firing, with a debug-level fingerprint of the input that triggered it, would let the deterministic catalogs grow by data rather than guesswork. This crosscuts #168, #169, #172, #173 — should be a single observability issue that all four depend on.

### B. AI cache lifecycle

`~/.dmt/type-cache.json` today caches AI type mappings. After #170, the deterministic mapper will return the same answer most of the time, making the cache vestigial for the common case. But cached AI responses for vendor-specific types are still useful. The cache should be partitioned by source ("deterministic" vs "AI fallback") so the deterministic part isn't carrying cached AI lossage indefinitely, and the AI part can be invalidated independently when models update. Worth a small follow-up issue.

### C. Reference fixtures audit

#168 names SO2010, SO2013, WWI, and pgbench as the coverage target. SO2010 and SO2013 exist in the bench setup (`docs/BENCHMARKS.md` extensively cites them). WWI (Wide World Importers) and pgbench — are these wired into the project's test harness today? If not, getting them in is prerequisite work for #168/#169. Either way, listing them in an issue isn't enough; the actual fixture-loading path needs to exist for the round-trip tests to run.

**Suggested action**: file a small issue "Standardize benchmark/test fixtures: SO2010, SO2013, WWI, pgbench loadable from `make test-dbs-up`" if the loaders don't exist today.

### D. Smartconfig parallel track explicit issues

#167 says "Smartconfig parallel track: #166 (per-target chunk_size interface) is the precursor; smartconfig PR1 → smartconfig PR2 follow." But "smartconfig PR1" and "smartconfig PR2" are not filed as issues — they're sketched in user-side memory only. For the AI to coordinate work across the parallel tracks, those need to be explicit issues with scope + dependencies, not informal references.

## Working precedent: UVG (`/Users/john/repos/uvg/`)

The maintainer's own `uvg` repository implements **exactly** the canonical-IR + per-dialect-mapper architecture #168 / #169 propose, in production today (published to crates.io). It's a Rust project, dual-licensed Apache-2.0 / MIT, so dmt can port the design and code freely.

This *substantially* changes the work estimate for #168 / #169 — from "design and write ~2K LOC of mapping logic from scratch" to "port a working, tested implementation; adapt for dmt's `ColumnInfo` shape and Go idioms."

### Architecture, summarized

UVG splits cross-dialect type mapping into two packages:

- `src/ddl_typemap/` — canonical-IR + DDL emission (the analog of #168 + #169 for column types). ~940 LOC across 5 files.
- `src/typemap/` — column-type → SQLAlchemy expression mapping. dmt does not need this; it's specific to UVG's ORM-codegen use case.

Inside `ddl_typemap/`, the public API is:

```
to_canonical(col, source_dialect) → CanonicalType
from_canonical(ct, target_dialect) → DdlType { sql_type, is_approximate, warning }
map_ddl_type(col, source, target) = from_canonical(to_canonical(col, source), target)
```

This is the round-trip API #169 proposes (`source DDL → canonical → target DDL`).

### What's better than #168's sketch

1. **22-variant `CanonicalType` enum** (`src/ddl_typemap/mod.rs:11-50`) vs #168's 13-Kind sketch. Adds the variants whose absence I flagged in the #168 concerns:

   - `Boolean` (separate from numeric kinds — handles MSSQL `bit` vs PG `boolean` vs MySQL `tinyint(1)` cleanly)
   - `SmallInt` / `Integer` / `BigInt` separate (preserves precision; PG `int2` → MSSQL `smallint`, not lossy `int`)
   - `Float` / `Double` separate (REAL vs DOUBLE PRECISION)
   - `Decimal { precision: Option<i32>, scale: Option<i32> }` — precision/scale in the variant, not a side-channel
   - `Varchar { length }` / `Char { length }` separate, with optional length
   - `Text` separate from `Varchar` (PG `text` vs PG `varchar` semantic difference preserved)
   - `Bytes { length }`
   - `Date` / `Time { with_tz: bool }` / `Timestamp { with_tz: bool }` — TZ as flag, not duplicate variants
   - `Interval` (PG-only as native; lossy on MSSQL/MySQL)
   - `Uuid`
   - **`Json` and `Jsonb` separate** (not collapsed — PG distinguishes them, so canonical does too)
   - `Enum { values: Vec<String> }` — captures the enum values, not just "this is an enum"
   - `Array { element: Box<CanonicalType> }` — recursive; PG `_text` (array of text) maps cleanly through `Array { Text }`
   - `Raw { type_name: String }` — escape hatch for non-portable types (PG `inet`, `cidr`, `macaddr`)

2. **`DdlType { sql_type, is_approximate, warning }` is a better result shape than `Unknown + AI fallback`.** Instead of "I can't map this, ask AI," UVG's deterministic mapper says "here's my best mapping, with this caveat." Examples (`src/ddl_typemap/mssql.rs:91-103`):

   ```rust
   CanonicalType::Interval     → approx("NVARCHAR(255)",  "No INTERVAL type in MSSQL")
   CanonicalType::Json | Jsonb → approx("NVARCHAR(MAX)", "No native JSON type in MSSQL; using NVARCHAR(MAX)")
   CanonicalType::Array { .. } → approx("NVARCHAR(MAX)", "No array type in MSSQL; using NVARCHAR(MAX)")
   CanonicalType::Enum { .. }  → approx("NVARCHAR(255)", "No ENUM type in MSSQL; consider CHECK constraint")
   ```

   This collapses an entire class of cases that #168's sketch would route to AI ("lossy but reasonable") into deterministic-with-caveats. **AI fallback shrinks to "type not in the catalog at all" rather than "type has a lossy translation."** Major reduction in the AI surface. Also removes the need for `migration.unmapped_type_action` (#170 concern 1) for the lossy-but-mapped case — those just emit with a warning.

3. **Per-dialect file sizes are small and complete.** `src/ddl_typemap/{pg,mssql,mysql,sqlite}.rs` are 142–227 LOC each, including embedded unit tests. dmt's #168 estimate of 7 files / unspecified LOC becomes concrete: 4 files × ~180 LOC = ~720 LOC, with another ~220 LOC for the shared `mod.rs`. Less than half of `ai_typemapper.go`'s 2205 LOC.

4. **Tests are directly portable.** Each dialect file has `#[cfg(test)] mod tests` with the cross-dialect cases the AI mapper has been guessing at:

   ```
   pg int4         → mysql INT
   pg jsonb        → mysql JSON (approximate)
   pg uuid         → mysql CHAR(36)
   pg uuid         → mssql UNIQUEIDENTIFIER
   pg timestamptz  → mysql DATETIME
   pg array(text)  → mysql JSON (approximate)
   mysql tinyint(1)→ pg BOOLEAN
   mssql uniqueidentifier → pg UUID
   mssql money     → pg NUMERIC(19, 4)
   mssql datetimeoffset   → pg TIMESTAMP WITH TIME ZONE
   ```

   These become dmt's #168 / #169 regression-test corpus on day one, no synthesis required.

### What's NOT in UVG

- **Constraint / FK / index / CHECK DDL emission** — UVG emits column-level DDL only. `CREATE TABLE` orchestration, PK declarations, FK with ON DELETE/UPDATE, indexes, CHECK constraints, DEFAULT expression translation — those stay #169's scope. UVG's `src/codegen/ddl.rs` does some of this for SQLAlchemy use cases but isn't directly portable; #169 needs its own implementation.
- **Charset / collation pinning** — UVG doesn't pin charsets at the canonical level (varchar is varchar). For dmt's PG↔MSSQL flow this is usually fine; for MySQL targets with non-utf8mb4 defaults it may need an extension.
- **Same-dialect "preserve original" passthrough** — UVG's `Raw { type_name }` handles this implicitly, but dmt may want a richer same-dialect path for pg→pg / mssql→mssql migrations that should be byte-perfect.

### Concrete recommendation for the receiving AI

When implementing #168, the right starting move is:

1. Read `/Users/john/repos/uvg/src/ddl_typemap/mod.rs` (the canonical IR) and one of `pg.rs` / `mssql.rs` / `mysql.rs` (a per-dialect mapper).
2. Translate `CanonicalType` enum → a Go sealed-interface family or tagged-struct equivalent, preserving all 22 variants and their fields.
3. Translate the per-dialect `to_canonical` / `from_canonical` functions mechanically — match arms become switch cases, `Option<T>` becomes `*T` or zero-value, `Vec<String>` becomes `[]string`, `Box<CanonicalType>` becomes `*CanonicalType`.
4. Port the embedded unit tests directly. They catch most regressions for free.
5. Adapt for dmt's existing `ColumnInfo` / `TypeInfo` struct (UVG uses its own `ColumnInfo` which has the standard `udt_name`, `data_type`, `character_maximum_length`, `numeric_precision`, `numeric_scale` fields — same shape as the JDBC/PostgreSQL standard catalog, so the field map is straightforward).
6. Wire `is_approximate` + `warning` through to logging at the DDL-emission call site so users see lossy translations as warnings rather than silent surprises.

This puts #168 at maybe a quarter to a third of the effort the original issue scoped.

### License note

UVG is dual-licensed Apache-2.0 / MIT (`/Users/john/repos/uvg/LICENSE-APACHE`, `/Users/john/repos/uvg/LICENSE-MIT`). Both permit copying with attribution; no copyleft. Standard practice is to keep an attribution comment in the ported Go file pointing back at the UVG source.

## Sequencing notes

The dependency chain in #167 is:

```
#168 (typemap baseline)
  ↓
#169 (DDL gen)
  ↓
#170 (wiring)
  ↓
[AI now optional for migrations]
  ↓
#171, #172, #173, #174 (independent, parallel)
```

Things to watch:

- **#168 and #169 can land in parallel** if you accept that #169's DDL generator can stub the type-side via the existing AI mapper until #168 lands. Most teams won't bother; serialization is fine.
- **#174 is correctly listed as last** (depends on #170 being done so the no-AI path actually works end-to-end).
- **#171 / #172 / #173 are listed as parallel post-#170** but they have wildly different scopes:
  - #173 (error diagnosis) is the easiest — pattern catalogs are well-understood, the code has clear seams, ~400 LOC delta.
  - #172 (DB tuning) is medium — per-driver query catalogs + interpretation rules, ~600 LOC + tests.
  - #171 (runtime adjuster) is the hardest — control-loop replacement with replay-test validation, deletes 1216 LOC and replaces with rule-based controller. This is the one most likely to regress production behavior.
  
  Suggested order if not parallel: #173 first (cheap win, builds confidence), #172 next (medium), #171 last (most risky, do once everything else is stable so regressions are isolatable).

- **#166 is the chunk_size architecture issue.** Not on the critical path of the AI-optional epic — it's a parallel cleanup. Can land any time.

## Notes on prior-art / project context

- The deterministic-first / AI-as-fallback pattern matches what most data-engineering tools converged on (AWS DMS, pgloader, Liquibase). dmt is unusual in being AI-first today; the epic moves it toward the standard architecture, not away from it.
- The 50000-row chunk_size anchor measured in #164 is PG-specific and should be quoted as such, not as a universal default. #166 captures this.
- The smartconfig prompt's CRITICAL CONSTRAINT framing (post-#163) is doing real work — Sonnet 4.6 / Haiku 4.5 / 4 of 4 tested local LLMs now respect the chunk_size budget when prompted imperatively. That work is preserved; the AI-optional epic just makes smartconfig itself optional.
