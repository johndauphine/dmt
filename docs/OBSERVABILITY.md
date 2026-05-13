# Observability

dmt exposes three coordinated surfaces so an SRE team can ingest dmt's behavior into the same stack they use for everything else (#229). All three are **off by default** — enabling them is opt-in via CLI flags and has zero overhead in the default deployment.

| Surface | Flag | Default | Purpose |
|---|---|---|---|
| Structured JSON logs | `--log-format=json` | text | Aggregator-friendly log lines |
| Prometheus metrics | `--metrics-addr=:9090` | disabled | Scrape-based numerical observability |
| OpenTelemetry traces | `--otel-endpoint=URL` | disabled | Distributed-tracing UIs (Jaeger, Honeycomb, Tempo) |

All three share the same dimension names (`run_id`, `phase`, `table`, `source_db`, `target_db`) so you can pivot between log, metric, and trace views in your tooling without re-mapping.

## Structured JSON logs

```bash
dmt --log-format=json run --confirm-backup
```

Every log line is a JSON object on its own line (newline-delimited JSON, ndjson):

```json
{"ts":"2026-05-13T10:32:18-05:00","level":"info","msg":"Transfer complete: 90000 rows","run_id":"abc12345","phase":"validating","source_db":"mssql","target_db":"postgres"}
```

### Base attributes (every line)

| Key | Type | Description |
|---|---|---|
| `ts` | string | RFC3339 timestamp |
| `level` | string | `error` / `warn` / `info` / `debug` |
| `msg` | string | The log message (trimmed of leading/trailing whitespace) |
| `run_id` | string | 8-char run identifier (matches `dmt history`) |
| `phase` | string | Current orchestrator phase — see below |
| `source_db` | string | `mssql` / `postgres` / `mysql` |
| `target_db` | string | Same set |
| `resume` | bool | `true` only on `dmt resume` runs (absent for fresh `dmt run`) |

### Phases

Phases are stable snake_case identifiers. The complete set today:

`preflight` → `extracting_schema` → `creating_tables` → `transfer` → `finalizing` → `validating`

### Per-event fields

Some log lines also carry structured fields specific to the event. The most useful for log aggregators:

- Chunk-completion events include `table`, `rows`, `bytes`, `elapsed_ms`.
- Errors include `error_class` (one of `deadlock`, `timeout`, `network`, `constraint`, `permission`, `unknown`).
- Retries include `retry_count` and the underlying `error_class`.

## Prometheus metrics

```bash
dmt --metrics-addr=:9090 run --confirm-backup
# in another shell:
curl http://localhost:9090/metrics | grep dmt_
```

The Prometheus endpoint binds at the address given by `--metrics-addr`. It only starts when the flag is set; in the default deployment the listener doesn't exist and the metrics-registry code paths are no-ops.

### Available metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `dmt_rows_total` | counter | `run_id`, `table`, `phase` | Rows transferred to the target |
| `dmt_bytes_total` | counter | `run_id`, `table`, `phase` | Bytes transferred (estimated from row size) |
| `dmt_errors_total` | counter | `run_id`, `table`, `phase`, `error_class` | Errors raised during migration |
| `dmt_retries_total` | counter | `run_id`, `table` | Chunk-level retries |
| `dmt_chunk_duration_seconds` | histogram | `run_id`, `table` | Time to read+write a single chunk |
| `dmt_phase_duration_seconds` | histogram | `phase` | Wall-clock duration per orchestrator phase |
| `dmt_writer_queue_depth` | gauge | `run_id` | Current depth of the writer pool's input queue |
| `dmt_writers_active` | gauge | `run_id` | Active writer goroutines |
| `dmt_runtime_tuning_adjustments_total` | counter | `rule_name`, `direction` | Runtime parameter adjustments |
| `dmt_ai_fallback_total` | counter | `surface` | AI fallbacks fired (cross-ref #176) |

### Cardinality note

`run_id` is a per-run label. The gauges (`writer_queue_depth`, `writers_active`) are **cleared at RunComplete** so cardinality stays bounded across long-running dmt processes (TUI sessions, sidecar deployments). The counters (`rows_total` etc.) are NOT cleared — Prometheus counters must be monotonic — but their `run_id` label scopes them, so old series naturally fall off Prometheus's retention window.

## OpenTelemetry traces

```bash
dmt --otel-endpoint=http://otel-collector:4318 run
```

The OTLP HTTP exporter is enabled when `--otel-endpoint=URL` is set. The URL should be the collector's base URL — the exporter appends `/v1/traces` itself. `http://` is treated as insecure (dev setups); `https://` uses TLS.

### Span hierarchy

```
dmt.run (or dmt.resume)
├── phase.preflight
├── phase.extracting_schema
├── phase.creating_tables
├── phase.transfer
│   └── (table-level spans + per-chunk events emitted by the transfer goroutines)
├── phase.finalizing
└── phase.validating
```

### Span attributes

Every span carries the same dimensions as the metrics labels:

- `run_id` — 8-char run identifier
- `phase` — orchestrator phase name
- `source_db` / `target_db` — driver names
- `resume` — bool, only on resume runs

Per-chunk milestones are emitted as **span events** on the table span (not as separate spans). A 100M-row migration with 1000 chunks per table would otherwise flood tracing backends; events give you the same per-chunk timing without the cardinality explosion.

## Grafana dashboard

See [`docs/grafana-dmt-dashboard.json`](./grafana-dmt-dashboard.json) for a starting-point dashboard that covers:

- Throughput (rows/sec per table)
- Error rate by class
- Active writers + queue depth
- Phase timing breakdown
- Retry pressure over time

Import it in Grafana via *Dashboards → New → Import* and point the Prometheus datasource at your dmt scrape target.

## Production checklist

- **Scrape interval**: 15s is fine for most deployments. Don't drop below 5s — the per-run gauges update fast enough that aliasing isn't an issue.
- **Retention**: 30 days of `dmt_*` metrics typically fits in <1GB of TSDB storage for normal use (one migration per day at the default cardinality).
- **Alerting**: start with `rate(dmt_errors_total[5m]) > 0` and `dmt_writer_queue_depth > 100`. Tune by environment.
- **Log routing**: pipe stdout to your log aggregator; preflight failures, errors, and per-table summaries are all at info-level. Set `--verbosity=warn` to drop the routine progress chatter without losing failure context.
