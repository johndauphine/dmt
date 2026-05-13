# Versioning policy

dmt follows [Semantic Versioning 2.0.0](https://semver.org/). This document is the source of truth for what does and doesn't trigger a major/minor/patch bump, what config fields you can rely on as stable, and how breaking changes are deprecated before removal.

## Triggers

### MAJOR (`v1.x.x` → `v2.0.0`) — breaking changes

A change requires a MAJOR bump if it could cause a previously-working migration to fail or silently produce different results. Specifically:

- A YAML config field is **removed** or **renamed** without a deprecation cycle (see below)
- A CLI subcommand or flag is **removed** or **renamed**
- A database driver's interface signature changes in a way that breaks downstream `dmt` packages used as libraries
- **Default behavior changes** in a way that could silently produce different results without an operator opt-in (example: the `target_mode: drop_recreate` default switching to `upsert` would be MAJOR even though both modes already exist)
- An **exit code** is renumbered or its meaning is materially changed
- An **environment variable** dmt reads is renamed or its semantics changed

### MINOR (`v1.0.x` → `v1.1.0`) — additive changes

A change is MINOR if it adds capability without breaking existing usage:

- New YAML config fields with sensible defaults that preserve pre-existing behavior
- New CLI subcommands or flags
- New database driver
- Performance improvements (even large ones) that don't change output
- **Deprecation warnings** — the old name still works but emits a warning. The next MAJOR is when removal happens.
- New optional dependency
- New optional feature flag

### PATCH (`v1.0.0` → `v1.0.1`) — bug fixes

A change is PATCH if it fixes incorrect behavior without changing the public contract:

- Behavior corrections where the pre-existing behavior was buggy
- Documentation
- Internal refactors with no user-visible effect
- Test-only changes
- CI / build-process changes
- Dependency updates that don't expose new surface (e.g., a `golang.org/x/net` bump for a CVE)

## Stability commitments

After v1.0.0:

- **Stable** — every YAML field listed in `config.yaml.example` without an `(experimental)` comment is stable. Stable fields participate in the deprecation cycle below; they don't disappear in a minor bump.
- **Experimental** — fields prefixed with `experimental_` in YAML or marked `(experimental)` in `config.yaml.example`. May change name, default, or semantics in any release. Use at your own risk.
- The CLI subcommands documented in `dmt --help` are stable. The `dmt help <subcommand>` output is the contract for each.
- Driver interfaces in `internal/driver/` are NOT stable — `internal/` is implementation, not public API. The CLI is dmt's public surface.

Before v1.0.0 (the current state — see [v1.0.0 readiness criteria](#v100-readiness-criteria) below), the SemVer policy applies on a best-effort basis. The [SemVer spec section 4](https://semver.org/#spec-item-4) explicitly allows breaking changes in `0.x.y` minor bumps; we try to avoid them but make no formal commitment.

## Deprecation cycle

A breaking change to a stable field after v1.0.0 must follow this cycle:

- **Release N (minor bump)** — introduce the new name. Continue to accept the old name with a `WARN`-level log message: `field "ai_adjust" is deprecated; rename to "runtime_tuning". Will be removed in v2.0.0.` Document the deprecation in `CHANGELOG.md` under "Deprecated".
- **Release N+1 ... (any number of minor bumps)** — old name still works, still warns. Operators have time to migrate.
- **Release N+M (next major bump)** — old name is removed. Document the removal in `CHANGELOG.md` under "Removed". The removal is the breaking change that gates the major bump.

The [`ai_adjust → runtime_tuning` rename (#211)](https://github.com/johndauphine/dmt/issues/211) is the model.

## What does NOT require a bump

- The internal `state.db` SQLite schema can change between PATCH releases; dmt auto-migrates on `New()` and doesn't promise schema stability.
- AI prompts and tuning heuristics are tuned freely — they're not part of the public contract. Output throughput numbers may improve or regress across versions.
- Error message wording (only error CODES are stable per the exit-code policy above).
- Log message wording in `text` mode. Structured JSON log field names ARE stable after v1.0.0 (covered by `internal/logging` and `internal/observability` package surface).

## v1.0.0 readiness criteria

`v1.0.0` ships when the [production-readiness epic #236](https://github.com/johndauphine/dmt/issues/236) closes. Concretely:

1. **Correctness** — Full-checksum validation default (#226 ✅), ROW_NUMBER resume safety (#227 ✅), partial migrations exit non-zero (#248 ✅), progress race-free (#249 ✅), no writer-failure goroutine leak (#250 ✅)
2. **AI optional** — Migration works without AI configured on all three reference drivers (#167 ✅ DoD met)
3. **Operability** — Preflight checks (#228 ✅), JSON logs + Prometheus /metrics + OTLP traces (#229 ✅)
4. **Verification** — Every PR runs cross-DB integration + race + lint + govulncheck (#230 ✅)
5. **Auth/transport correctness** — Kerberos works or is descoped (#251 ✅ descoped), MySQL TLS (#252 ✅), validation timeouts fail loud (#253 ✅)
6. **Security** — No secrets/PII in logs (#231), minimum privileges documented (#232)
7. **Release discipline** — This document (#233)
8. **Runbook** — SRE-ready failure-mode catalog (#234)
9. **State durability** — FileState atomic writes (#254 ✅), sync timestamps (#255 ✅)
10. **Compliance** — Audit trail (#235)

Checkboxes for items currently in progress (P2/P3) update as those issues close. When all ten are green, the next release is `v1.0.0` rather than another `v0.x`.

After `v1.0.0`, this policy is binding rather than best-effort.
