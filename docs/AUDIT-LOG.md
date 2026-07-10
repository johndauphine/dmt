# Audit log

dmt writes a per-run, append-only NDJSON record of every migration for compliance auditing (#235). The audit log is distinct from the SQLite state DB — that's mutable working state; this is an immutable historical record an auditor can hash, archive, and reason about months or years later.

## When the audit log fires

Every `dmt run` and `dmt resume` invocation writes one audit file:

```
<audit-dir>/<run_id>.ndjson
```

- `<audit-dir>` defaults to `$HOME/.dmt/audit` and is overridable via `--audit-dir=/path` or `migration.audit_dir` in YAML.
- `<run_id>` is the 8-char run identifier (same one shown by `dmt status` and `dmt history`).
- During the run, the file is mode 0600 (operator-only) and opened `O_APPEND` — every write lands at a unique end-of-file offset, even under concurrent writers.
- After the run ends successfully or with a hard failure, the file is `chmod 0444` (read-only) so even the operator can't accidentally truncate it. Filesystem-enforced immutability; pair with a snapshot tool for the regulator's preferred storage.
- **Exception — resumable runs**: when the operator interrupts a run with Ctrl-C/context deadline, or a transfer attempt ends `partial`, the file stays at 0600 so the eventual `dmt resume` can reopen it in `O_APPEND` and continue the same audit log. The lockdown happens on the final successful, accepted, abandoned, or hard-failed attempt.

Disable with `--no-audit` (CLI) or `migration.no_audit: true` (YAML). Use sparingly — the default audit has zero data-plane impact.

## Tamper-evident mode

For high-compliance scenarios (PCI, HIPAA, SOX, financial services), opt into hash-chained events:

```bash
dmt run --audit-tamper-evident
```

Each event then carries three additional fields:

- `seq` — monotonic counter, starting at 1
- `prev_hash` — `"GENESIS"` for the first event; otherwise the previous event's `hash`
- `hash` — `sha256(prev_hash || canonical_json(event_minus_hash))`, prefixed `sha256:`

Modifying any past event invalidates every downstream `hash`. The chain detects retroactive edits without requiring digital signatures or external timestamping (those are out of scope here — see [VERSIONING.md](VERSIONING.md) for the policy).

### Verifying the chain

```bash
# Walk events in order, recomputing each event's hash.
# Requires `jq` and `sha256sum`.

run=2026-05-13-abc12345
audit_file="$HOME/.dmt/audit/${run}.ndjson"
prev=GENESIS
ok=true
while IFS= read -r line; do
  on_disk=$(jq -r '.hash' <<<"$line")
  body=$(jq -cS 'del(.hash)' <<<"$line")
  expected="sha256:$(printf "%s%s" "$prev" "$body" | sha256sum | awk '{print $1}')"
  if [[ "$on_disk" != "$expected" ]]; then
    echo "MISMATCH at seq=$(jq -r .seq <<<"$line"): disk=$on_disk recomputed=$expected" >&2
    ok=false
  fi
  prev=$on_disk
done < "$audit_file"
$ok && echo "OK — chain intact across $(wc -l < "$audit_file") events"
```

If you run this against an unmodified audit file you should see `OK`. If anyone — including dmt itself, somehow — has modified an event after the fact, you'll see `MISMATCH` at the first tampered line.

## Event schema

Every line is a JSON object with at least these fields:

| Field | Type | Notes |
|---|---|---|
| `ts` | string | RFC 3339 nanosecond UTC timestamp |
| `type` | string | Event type — see catalog below |
| `run_id` | string | 8-char run identifier |
| `seq` | int | Tamper-evident mode only |
| `prev_hash` | string | Tamper-evident mode only |
| `hash` | string | Tamper-evident mode only |

Beyond those, the per-type payload varies. The catalog below documents every event type dmt emits today; future types are additive (MINOR bump per [VERSIONING.md](VERSIONING.md)).

### Event types

#### `run_start`

Emitted at the start of `dmt run`. First event in the file.

| Field | Type | Notes |
|---|---|---|
| `operator` | string | `user@hostname` — best-effort identity |
| `dmt_version` | string | Build version (matches `dmt --version`) |
| `source.driver` / `source.host` / `source.database` / `source.schema` | strings | Source identity tuple |
| `target.driver` / `target.host` / `target.database` / `target.schema` | strings | Target identity tuple |
| `config_hash` | string | SHA-256 of the sanitized config (matches what resume validates against) |

#### `resume_start`

Emitted at the start of `dmt resume`. First event for a resumed run (the file already contains the original `run_start` from the crashed Run).

| Field | Type | Notes |
|---|---|---|
| `operator` | string | Same shape as `run_start.operator` |
| `dmt_version` | string | Build version (may differ from the original Run if the operator upgraded between crash and resume) |
| `original_started_at` | string | When the original Run kicked off |

#### `validation_complete`

Emitted after `dmt run`'s validation phase finishes on the success path.

| Field | Type | Notes |
|---|---|---|
| `tables` | int | Tables validated |
| `rows_total` | int | Total rows the migration moved |

#### `checkpoint_periodic_save_degraded`

Emitted when an asynchronous periodic checkpoint save ends in failure but the
table's later synchronous final checkpoint succeeds. The final watermark is
durable, so the transfer succeeds; this event preserves the operational signal.

| Field | Type | Notes |
|---|---|---|
| `table` | string | Table whose periodic checkpoint save degraded |
| `consecutive_failures` | int | Trailing failed periodic save attempts before the final save |
| `last_error` | string | Scrubbed error from the latest failed periodic save |

#### `run_complete` / `resume_complete`

Final event for the run. Emitted via deferred handler, so even panics produce one of these.

| Field | Type | Notes |
|---|---|---|
| `status` | string | One of `success`, `partial`, `failed`, `cancelled`, `panic`. `partial` and `cancelled` remain resumable, so the audit file stays writable for `dmt resume` to append more events |
| `error` | string | Set when `status != success`; scrubbed. Empty string on the success path |
| `duration_ms` | int | Wall-clock duration of the Run/Resume |

## Scrubbing

Every field value flows through the same `internal/logging.Scrub` helpers established by #231:

- DSN passwords (URL form and libpq form, all three drivers)
- `password=` / `passwd=` / `api_key=` / `secret=` / `token=` key/value forms (`:` and `=` separators both preserved)
- `Authorization: Bearer` headers
- `sk-` and `sk-ant-` API keys
- Slack incoming-webhook URLs

In addition, audit-log fields whose KEY name matches a secret-looking pattern (`password`, `api_key`, `token`, etc.) are redacted regardless of value content. This catches the structured `{source: {password: "..."}}` case where the regex pattern wouldn't otherwise fire because the secret has no `password=` prefix.

**Row content is never logged.** Audit events describe operations, not data — no row count would justify exposing the rows themselves.

## Retention recommendations

Different compliance regimes have different requirements. Common floors:

| Regime | Typical retention | Notes |
|---|---|---|
| Operational debugging | 30 days | The default `dmt history` window |
| Standard production | 90 days | Long enough to investigate post-incident |
| SOC 2 / ISO 27001 | 365 days | Annual audit window |
| HIPAA | 6 years | Federal requirement for protected-health-information access logs |
| PCI-DSS | 1 year | Cardholder-data access audits |
| SOX | 7 years | Financial-reporting audit window |

For long retention, ship the audit files to immutable storage (object-store with versioning + retention lock, append-only S3 bucket, write-once optical media for the truly regulated). The dmt audit log itself is the authoritative on-disk record for the run's lifetime; archive it before the run record falls out of `~/.dmt/state.db`.

## Comparing audit log vs. structured logs

| | Audit log (#235) | Structured logs (#229) |
|---|---|---|
| Purpose | Compliance | Operational observability |
| Format | NDJSON file, one file per run | NDJSON stream to stderr or a log shipper |
| Lifecycle | Append-only during run, 0444 after | Streaming; filtered, sampled, expires |
| Retention | Operator's compliance regime | Operator's log retention policy |
| Tamper evidence | Optional hash chain | None (not the design goal) |
| Subset of events? | Subset of structured logs | Superset of audit |

Treat the audit log as the canonical record of what happened. Treat structured logs as the operational view of what's happening. They cover overlapping events; their durability contracts differ.
