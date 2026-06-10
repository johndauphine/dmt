# Integration test scripts

## Architecture

Two layers, deliberately separate:

| Layer | What runs it | Scripts |
|---|---|---|
| **Per-PR anchors** | `.github/workflows/integration.yml` on every PR | `integration-test.sh` (mssql→pg), `integration-test-sqlite.sh` (sqlite→sqlite), plus the feature-specific scripts (`-daily-driver`, `-schema-contracts-*`, `-schema-evolution`) |
| **Nightly cross-engine matrix** (#291) | `.github/workflows/integration-nightly.yml`, nightly + manual dispatch | `integration-test-pair.sh --pair <src>-<tgt>` for all 12 directed cross-engine pairs |

The anchors block PRs and stay bespoke on purpose — they carry
feature-specific assertions (upsert deltas, schema contracts, checkpoint
behavior). The matrix is the generic "does engine A migrate to engine B
at all" sweep; it deliberately excludes same-engine pairs because those
don't exercise the cross-DB type mapper.

`integration-test-pair.sh` is convention-driven: per-engine
`load_source_<engine>` / `prep_target_<engine>` functions, per-pair
configs at `fixtures/ci-<src>-<tgt>.yaml`, and data-aligned per-engine
fixture dumps `fixtures/so2010-minimal[-<engine>].sql` (every source
produces the same rows, so `dmt validate` is the single parity check
for every pair).

## Adding an engine to the matrix (#480 recipe)

Adding engine `X` as the fifth engine touches exactly these spots — no
Go changes and no new scripts:

1. **Fixture dump** — `fixtures/so2010-minimal-X.sql`, byte-aligned on
   data with the existing four (same tables, same rows; engine-native
   DDL/INSERT syntax).
2. **`integration-test-pair.sh`** — add `X` to the two engine-validation
   `case` lists, and write `load_source_X()` + `prep_target_X()`
   (typically <10 lines each; see the existing four of each).
3. **Per-pair configs** — `fixtures/ci-X-<existing>.yaml` and
   `fixtures/ci-<existing>-X.yaml` for each pairing you want covered
   (copy a neighbor, swap the endpoint block). This is 2·n small files;
   if the engine count ever makes that unwieldy (~6 engines), that's the
   cue to generate them from per-engine endpoint snippets — don't
   pre-build that machinery before it pays.
4. **`integration-nightly.yml`** — one service-container block for `X`
   (image, password env, health check; mirror the mysql block) and the
   new `{src, tgt}` matrix rows. Every matrix job starts all services
   regardless of pair — conditionalizing services per entry isn't well
   supported by GHA and the startup waste amortizes across the parallel
   matrix.
5. **Local reproduction** — a `make X-bench-up`-style target (mirror
   `mysql-bench-up`) so `./scripts/integration-test-pair.sh --pair X-pg`
   works on a laptop.

Catalog engines from the generic driver (#191) follow the same recipe —
the matrix doesn't care whether the driver behind a pair is hand-written
or catalog-driven.
