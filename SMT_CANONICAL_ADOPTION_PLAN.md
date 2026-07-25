# Plan: Adopt SMT's Canonical Type Mapper in DMT (Option A — shared package in the SMT repo)

**Status**: proposed, not started
**Author**: drafted for handoff
**Target repos**: `github.com/johndauphine/dmt`, `github.com/johndauphine/smt` (siblings at `/home/johnd/repos/{dmt,smt}`)

---

## 1. Goal and non-goals

**Goal.** DMT's deterministic type mapping (`internal/typemap`) currently produces lower-fidelity DDL
than SMT's equivalent (`internal/canonical`). Both were independently ported from the same Rust
ancestor (UVG, `src/ddl_typemap`) and have since diverged; SMT received fixes DMT never did. Adopt
SMT's canonical mapper as the single shared implementation, consumed by both tools.

**Non-goals** (explicitly out of scope for this plan):

| Not doing | Why |
|---|---|
| Merging DMT and SMT into one application | Different lifecycles; DMT is a long-running data mover, SMT is a short-lived schema tool |
| Porting SMT's `internal/schemadiff` (2,278 LOC) | Generates `ALTER`s by diffing snapshots. DMT has no schema-evolution path |
| Porting SMT's `internal/expr` (2,252 LOC) | DEFAULT/CHECK expression IR. Lower value for DMT's bulk-load + finalize model. Revisit later |
| Replacing DMT's `internal/typemap/ddl` renderer | DMT's is the same size as SMT's, from the same ancestor, and is deliberately shaped around DMT's finalize-phase constraint split. Cherry-pick fixes instead |

**Hard constraint.** DMT must remain a **single statically-linked executable**. This is preserved
automatically: Go modules are compile-time source dependencies. DMT already depends on 100 external
modules and ships as one 39 MB binary whose only shared libraries are `libc` and the loader (from
cgo/SQLite). Adding one more module changes nothing about deployment.

---

## 2. Verified facts (re-verify before relying on these)

All line references were confirmed against `dmt@026b83cd` and `smt@78211be`.

### SMT side

- `smt/internal/canonical` is a **true leaf**: its only imports are `errors`, `fmt`, `strings`.
  Nothing internal. This is what makes the extraction cheap.
- Public API surface (`internal/canonical/`):
  - `type Kind int` — 30 kinds: `Unknown, Boolean, BitString, VarBitString, TinyInt, SmallInt,
    MediumInt, Integer, BigInt, Decimal, Real, Double, Varchar, Char, Text, Binary, VarBinary, Blob,
    Date, Time, Timestamp, Uuid, Json, Xml, RowVersion, Enum, Set, Array, Spatial, Raw`
  - `type CanonicalType struct` (+ method `Fspv() (int, bool)`)
  - `type TypeMeta struct { MaxLength, Precision, Scale int; DatetimePrecision *int; IsUnsigned bool;
    DisplayWidth int; EnumValues []string; SRID int; SpatialSubType string }`
  - `type RenderOpts struct`, `type MappingWarning struct`
  - `func ToCanonical(typeName string, m TypeMeta, dialect string) CanonicalType`
  - `func FromCanonical(ct CanonicalType, dialect string, opts RenderOpts) (string, error)`
  - `func FromCanonicalWithWarnings(ct, dialect, opts) (string, []MappingWarning, error)`
  - `func IsSpatialTypeName(typeName string) bool`
- **Dialect dispatch is a hard-coded switch** (`from_canonical.go:49-66`) supporting only
  `postgres`, `mssql`, `mysql`; unknown dialects return an error. `canonDialect()` normalizes aliases.
- In-repo consumers of `canonical` (6 non-test + 3 external test files) — all need import-path updates:
  `internal/orchestrator/mapping_warnings.go`, `internal/driver/types.go`,
  `internal/driver/verify_compare.go`, `internal/schemadiff/render_deterministic.go`,
  `internal/ddl/renderer.go`, `internal/driver/postgres/deterministic.go`, plus
  `internal/canonical/{oracle_all,oracle_pg,from_canonical_tz}_test.go`.
- `go.mod` declares `module smt` and `go 1.25.7`. Latest tag `v1.0.0`.
- Module rename blast radius: **95 files, 215 import lines** matching `"smt/`.

### DMT side

- `internal/typemap` public API in use by consumers:
  - `MapDDLType(col, sourceDialect, targetDialect) DdlType` — `deterministic_mapper.go:63`
  - `ToCanonical(col ColumnInfo, dialect) CanonicalType` — `typemap/ddl/column.go:39`
  - `FromCanonical(ct, dialect) DdlType` — `typemap/ddl/column.go:155`
  - `CanonicalType`, `Kind*` constants (**referenced directly by consumers** — see §5.3),
    `ColumnInfo`, `DdlType{SQLType, IsApproximate, Warning}`, `Dialect*` constants,
    `Register`, `Supported`, `SupportedDialects`, `IntPtr`, `Int64Ptr`
- `ColumnInfo` is **narrower** than SMT's `TypeMeta`:
  `{Name, UDTName, DataType string; CharacterMaximumLength, NumericPrecision, NumericScale *int;
  IsNullable bool}` — no unsigned, display width, enum values, datetime precision, or spatial subtype.
- **Dialect dispatch is a registry**, not a switch (`internal/typemap/registry.go:23-29`): a
  `map[string]DialectMapper` pre-seeded with `postgres, mssql, mysql, sqlite, clickhouse`, plus
  `Register()` so catalog-driven engines (#191) can add mappers at catalog-load time.
- Consumers of `internal/typemap`: `internal/driver/typemap_chain.go`,
  `internal/driver/deterministic_mapper.go`, `internal/typemap/ddl/{column,keycols,identifier}.go`.
- `go.mod` declares `module github.com/johndauphine/dmt` and **`go 1.25.0`**.
- `driver.Column` (`internal/driver/types.go:216`) lacks the metadata SMT captures, but **does** carry
  `FullDataType`, populated from MySQL's `COLUMN_TYPE` (`internal/driver/generic/reader.go:334`).
  The MySQL catalog comment (`internal/driver/generic/catalogs/mysql.yaml:110`) states COLUMN_TYPE
  "carries unsigned/enum/tinyint(1)/bit(N)" — so that metadata is recoverable by parsing, not by new queries.
- **Not captured in any catalog**: `datetime_precision`, computed/generated columns, generation
  expressions, `ON UPDATE`. Verified by grepping all five catalog YAMLs — zero hits.

---

## 3. Blockers that must be resolved before code moves

| # | Blocker | Resolution |
|---|---|---|
| B1 | `smt/internal/canonical` is under `internal/` — Go **forbids** import from outside the SMT tree. Not a convention; a compile error. | Move to a public path: `smt/schema/canonical` |
| B2 | SMT's module path is `smt`, not remotely importable | Rename to `github.com/johndauphine/smt` (95 files, 215 import lines) |
| B3 | SMT declares `go 1.25.7`; DMT declares `go 1.25.0`. A module cannot depend on one requiring a **higher** Go version. | Bump DMT's `go` directive to `1.25.7` (or lower SMT's if 1.25.7 features are unused — check first) |
| B4 | SMT's canonical supports 3 dialects; DMT needs 5 (`sqlite`, `clickhouse`) | Port DMT's sqlite + clickhouse mappers into the shared package, and replace SMT's switch with DMT's registry (§5.2) |
| B5 | SMT's `TypeMeta` needs metadata DMT doesn't populate | Phase 1 parses MySQL `FullDataType`; Phase 3 enriches readers (§5.4, §6) |

---

## 4. Target architecture

```
github.com/johndauphine/smt/schema/canonical     <-- shared, public, leaf (stdlib only)
        ^                              ^
        |                              |
   smt/internal/*                 dmt/internal/typemap   <-- anti-corruption layer
                                        ^
                                        |
                            dmt/internal/{driver,typemap/ddl}   <-- UNCHANGED
```

**Key design decision: keep `dmt/internal/typemap` as a thin adapter.** Do *not* rewrite DMT's
consumers to call the shared package directly. `internal/typemap` retains its current exported API
(`MapDDLType`, `ToCanonical`, `FromCanonical`, `ColumnInfo`, `DdlType`, `Dialect*`, registry
functions) and internally delegates to `schema/canonical`. This keeps the blast radius inside one
package and makes the change reversible by reverting one directory.

**Second decision: the shared package adopts DMT's registry, not SMT's switch.** DMT's catalog-driven
engine model (#191) requires runtime `Register()`; SMT's switch cannot express it. The registry is
strictly more general and costs SMT nothing.

---

## 5. Detailed changes

### 5.1 SMT: rename module and expose the package

```bash
cd /home/johnd/repos/smt
git checkout -b feat/expose-canonical-schema-package

# B2 — module rename
sed -i 's|^module smt$|module github.com/johndauphine/smt|' go.mod
grep -rl '"smt/' --include='*.go' . | xargs sed -i 's|"smt/|"github.com/johndauphine/smt/|g'

# B1 — lift canonical out of internal/
mkdir -p schema
git mv internal/canonical schema/canonical
grep -rl 'github.com/johndauphine/smt/internal/canonical' --include='*.go' . \
  | xargs sed -i 's|smt/internal/canonical|smt/schema/canonical|g'

gofmt -l . && go build ./... && go test ./...
```

Add a package-doc note to `schema/canonical/canonical.go` recording that the package is now a
published API consumed by DMT, and that changes must be additive or version-gated.

**Tag `v1.1.0`** once green. DMT pins that tag.

### 5.2 Shared package: registry + two new dialects

Replace the switch in `schema/canonical/from_canonical.go` with a registry mirroring
`dmt/internal/typemap/registry.go`:

```go
// schema/canonical/registry.go  (new)
package canonical

type DialectMapper struct {
    ToCanonical   func(typeName string, m TypeMeta) CanonicalType
    FromCanonical func(ct CanonicalType, opts RenderOpts) (string, error)
}

var (
    registryMu     sync.RWMutex
    dialectMappers = map[string]DialectMapper{
        "postgres":   {toCanonicalPG,    fromCanonicalPG},
        "mssql":      {toCanonicalMSSQL, fromCanonicalMSSQL},
        "mysql":      {toCanonicalMySQL, fromCanonicalMySQL},
        "sqlite":     {toCanonicalSQLite, fromCanonicalSQLite},         // ported from DMT
        "clickhouse": {toCanonicalClickHouse, fromCanonicalClickHouse}, // ported from DMT
    }
)

func Register(name string, m DialectMapper) { /* panic on dup/partial, as DMT does */ }
func Supported(name string) bool
func SupportedDialects() []string
```

`FromCanonicalWithWarnings` keeps its signature and looks the mapper up via
`canonDialect(dialect)` → registry, returning the existing
`fmt.Errorf("FromCanonical: unsupported target dialect %q", dialect)` on a miss.

**Porting sqlite + clickhouse is real work, not a copy.** DMT's mappers
(`internal/typemap/{sqlite,clickhouse}.go`) are written against DMT's **21-kind** enum. They must be
rewritten against the shared **30-kind** enum. Concretely, every `KindBytes` branch must fan out to
`Binary` / `VarBinary` / `Blob`, and integer branches gain `TinyInt` / `MediumInt` cases that DMT's
enum could not express. Do not mechanically sed these — walk each switch.

### 5.3 DMT: the adapter (the core of the work)

`internal/typemap/canonical.go` becomes an adapter. Sketch:

```go
package typemap

import schema "github.com/johndauphine/smt/schema/canonical"

// Re-export so consumers keep compiling unchanged.
type CanonicalType = schema.CanonicalType
type Kind = schema.Kind

const (
    KindBoolean = schema.Boolean
    KindVarchar = schema.Varchar
    // ... map every DMT Kind that has a 1:1 counterpart
)

// DdlType is retained: DMT consumers depend on the struct shape.
type DdlType struct {
    SQLType       string
    IsApproximate bool
    Warning       string
}

// toTypeMeta bridges DMT's narrower ColumnInfo to SMT's TypeMeta.
func toTypeMeta(col ColumnInfo) schema.TypeMeta {
    m := schema.TypeMeta{}
    if col.CharacterMaximumLength != nil { m.MaxLength = *col.CharacterMaximumLength }
    if col.NumericPrecision != nil       { m.Precision = *col.NumericPrecision }
    if col.NumericScale != nil           { m.Scale = *col.NumericScale }
    // Phase 1: recover MySQL metadata packed in DataType/FullDataType.
    applyMySQLTypeDetails(&m, col.DataType)
    return m
}

func ToCanonical(col ColumnInfo, dialect string) CanonicalType {
    return schema.ToCanonical(col.UDTName, toTypeMeta(col), dialect)
}

// FromCanonical adapts (string, []MappingWarning, error) -> DdlType.
// The error case must NOT panic: DMT's contract is that an unmappable
// type falls through to the AI fallback chain via ErrUnsupportedDDL.
func FromCanonical(ct CanonicalType, dialect string) DdlType {
    sqlType, warns, err := schema.FromCanonicalWithWarnings(ct, dialect, schema.RenderOpts{})
    if err != nil {
        return DdlType{SQLType: "", IsApproximate: true, Warning: err.Error()}
    }
    out := DdlType{SQLType: sqlType}
    if len(warns) > 0 {
        out.IsApproximate = true
        out.Warning = warns[0].Message // confirm field name against schema.MappingWarning
    }
    return out
}
```

**Delete** `internal/typemap/{postgres,mssql,mysql,sqlite,clickhouse}.go` and
`internal/typemap/registry.go` once the shared package owns them; keep `Dialect*` constants and
`Register`/`Supported`/`SupportedDialects` as pass-throughs to `schema.*` so
`deterministic_mapper.go:217-221` keeps compiling.

**Consumer break to fix (do not miss this).** `internal/typemap/ddl/column.go` reads `Kind` directly:

- line 40: `canonical.Kind == typemap.KindBoolean` — 1:1, safe via the alias above.
- line 66: `canonical.Kind == typemap.KindBytes` — **no 1:1 counterpart.** SMT splits this into
  `Binary`, `VarBinary`, `Blob`. Rewrite as a helper:

```go
func isByteKind(k Kind) bool {
    return k == schema.Binary || k == schema.VarBinary || k == schema.Blob
}
```

Audit for any other direct `Kind` comparisons before switching the default:
`grep -rn "typemap\.Kind" --include='*.go' internal/`

### 5.4 DMT: MySQL metadata recovery (Phase 1)

SMT's MySQL mapper relies on `TypeMeta.DisplayWidth` for the `tinyint(1)` → boolean convention,
`IsUnsigned`, and `EnumValues`. DMT does not populate these. Without them the ported mapper is
**worse** than today's for MySQL sources, because DMT's current mapper sniffs the `DataType` string.

Parse them instead:

```go
// internal/typemap/mysql_details.go (new)
//
// Recovers structured metadata from MySQL's COLUMN_TYPE string, which DMT
// already captures as driver.Column.FullDataType (generic/reader.go:334) and
// passes through as ColumnInfo.DataType — e.g. "tinyint(1)", "int unsigned",
// "enum('a','b')", "bit(8)".
func applyMySQLTypeDetails(m *schema.TypeMeta, columnType string) {
    lc := strings.ToLower(strings.TrimSpace(columnType))
    if strings.Contains(lc, "unsigned") { m.IsUnsigned = true }
    if strings.HasPrefix(lc, "tinyint(") {
        if w, err := strconv.Atoi(betweenParens(lc)); err == nil { m.DisplayWidth = w }
    }
    if strings.HasPrefix(lc, "enum(") || strings.HasPrefix(lc, "set(") {
        m.EnumValues = parseQuotedMembers(lc) // must handle escaped quotes
    }
}
```

Confirm `ColumnInfo.DataType` is actually fed from `FullDataType` for MySQL; if it is fed from the
narrower `DATA_TYPE`, add a `FullDataType` field to `ColumnInfo` and populate it at
`deterministic_mapper.go:295` (`typeInfoToTypemapColumn`).

---

## 6. Phasing

| Phase | Work | Exit criterion |
|---|---|---|
| **0** | Parity harness (§7) built against *current* DMT behavior | Golden corpus committed, passes on unmodified DMT |
| **1** | SMT rename + `schema/canonical` exposed + registry + sqlite/clickhouse ported; tag `v1.1.0` | `go test ./...` green in SMT; both new dialects covered |
| **2** | DMT adapter (§5.3) + MySQL metadata recovery (§5.4); DMT `go` directive bumped | Parity harness diffs reviewed and each intentional |
| **3** | Reader enrichment: `datetime_precision`, computed columns, `ON UPDATE` into catalogs + `driver.Column` | New metadata flows into `TypeMeta`; fidelity tests added |
| **4** | *Optional, separate decision*: evaluate `internal/expr` for DEFAULT/CHECK | — |

Phases 3 and 4 are independent follow-ups. **Phase 2 is shippable on its own** and delivers the
type-fidelity win that motivates this plan.

---

## 7. Verification strategy

**The parity harness is mandatory and must be built in Phase 0, before any behavior changes.**

Build a table-driven corpus that maps every (source dialect, source type, target dialect) triple DMT
supports — 5 × 5 dialect pairs — and records the emitted DDL type string. Seed it from the existing
type tests (`internal/typemap/*_test.go`, 1,195 LOC; `internal/typemap/ddl/*_test.go`, 1,643 LOC).

1. Phase 0: run against unmodified DMT, commit output as a golden file.
2. Phase 2: re-run against the adapter. **Every diff is either an intended fidelity improvement or a
   regression** — classify each one explicitly in the PR description. Expect intended diffs for
   MySQL `tinyint(1)`, `mediumint`, unsigned integers, and binary-type splits.
3. Regressions block the merge.

Also required:

- `make test` and `make lint` green in both repos.
- `go build ./...` in DMT, then confirm `ldd ./dmt` still lists only `libc`/loader — proves the
  single-binary property held.
- Existing conformance suites (`internal/driver/generic/conformance_*_test.go`) unchanged and passing.
- A real end-to-end migration for at least one cross-engine pair (mysql→postgres exercises the most
  changed paths), verifying created target DDL against expectations.

---

## 8. Local development setup

Use a Go workspace so DMT compiles against the local SMT tree without publishing tags mid-iteration:

```bash
cd /home/johnd/repos
go work init ./dmt ./smt
```

`go.work` is local-only and git-ignored; released builds resolve the real version from `go.mod`.
**Before opening the DMT PR**, remove or ignore the workspace and confirm the build resolves the
published `github.com/johndauphine/smt v1.1.0` — otherwise CI will fail on a module CI cannot fetch.

DMT `go.mod` gains:

```
require github.com/johndauphine/smt v1.1.0
go 1.25.7   // bumped from 1.25.0 per B3
```

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| Silent DDL regressions for engines SMT never supported (sqlite, clickhouse) | Parity harness covers all 25 dialect pairs, not just the 3 SMT knows |
| `TypeMeta` fields unpopulated → *worse* mapping than today | §5.4 MySQL parsing is part of Phase 2, not deferred; parity harness catches the rest |
| SMT module rename breaks its published v1.0.0 consumers | SMT is consumed only by its own binary today; verify no external importers before renaming |
| Import cycle: shared package must stay a leaf | Enforce in review — `schema/canonical` imports stdlib only, exactly as `internal/canonical` does today |
| Divergence recurs | The whole point of a shared module. Do **not** fork the file into DMT "temporarily" |

---

## 10. Open questions for the maintainer

1. **SMT module rename** touches 95 files and changes its import path. SMT is tagged `v1.0.0` with
   published release artifacts. Confirm this is acceptable, or decide to publish the shared package
   under a new module path instead (`github.com/johndauphine/smt/schema` as its own nested module).
2. **Go version**: is SMT's `go 1.25.7` load-bearing? If not, lowering it to `1.25.0` avoids bumping DMT.
3. **`RenderOpts`**: this plan passes a zero value. Review what SMT sets in
   `internal/ddl/renderer.go` and decide whether DMT should mirror any of it.
4. **Kind vocabulary in DMT's `ddl` package**: `KindBytes` fan-out (§5.3) may reveal other places
   where DMT's coarser vocabulary was load-bearing. Budget time for that audit.
