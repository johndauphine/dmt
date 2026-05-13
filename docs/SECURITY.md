# Security & secrets posture

This document describes how `dmt` handles credentials, connection
strings, and potentially-regulated row content. It is intended for
operators planning to ship dmt logs to a shared log-aggregation
platform (Datadog, Splunk, Loki, CloudWatch) or to a third-party
support team.

## Threat model

`dmt` is **operator-trusted**: it runs on infrastructure the operator
controls, with credentials the operator supplied. It is not a
sandboxed runner. It does not attempt to defend against malicious
config files, malicious target databases, or an attacker who has
already obtained shell on the dmt host.

The threats `dmt` *does* defend against:

1. **Accidental credential leakage into shared log aggregators.** A
   password in a Datadog index is a password in everyone-on-the-team's
   hands. dmt does not log credentials to stdout/stderr.
2. **Accidental row-content leakage into log aggregators or third-party
   AI providers.** Migration sources frequently contain PII (emails,
   SSNs, phone numbers) or regulated data (PCI, HIPAA, GDPR). dmt does
   not include row content in logs, error messages, or AI prompts.
3. **Accidental webhook-URL leakage.** Slack incoming-webhook URLs are
   the credential — there is no separate password. dmt scrubs them
   from any error message produced by a failed Slack send.

## What dmt considers a secret

- **Database passwords** — `source.password`, `target.password` in
  config.
- **API keys** — `~/.secrets/dmt-config.yaml` `ai.providers.*.api_key`.
- **Slack webhook URLs** — `~/.secrets/dmt-config.yaml`
  `notifications.slack.webhook_url`. The path portion is the credential.
- **DMT master key** — `DMT_MASTER_KEY` env var used to encrypt
  saved migration profiles.
- **Bearer tokens** — any `Authorization: Bearer …` header value seen
  by dmt (e.g. quoted in an HTTP error from an AI provider).

## What dmt scrubs from logs

Every log line, every error message wrapped by dmt's driver code, and
every Slack-notifier failure passes through one of:

- `internal/logging.Scrub(s string) string` — applies a fixed set of
  regex patterns (DSN userinfo, libpq-style `password=…` parameters,
  `Authorization: Bearer …`, `sk-…` API keys, Slack webhook URLs) and
  replaces the credential with the literal string `[REDACTED]`.
- `internal/logging.ScrubError(err error) error` — wraps an error so
  its `Error()` string is scrubbed; `errors.Is` / `errors.As` continue
  to work against the underlying sentinel.

The scrubbing fires at four structural points:

1. **Driver `NewReader` / `NewWriter`** (`internal/driver/postgres`,
   `mssql`, `mysql`). Errors from `pgxpool.ParseConfig`, `sql.Open`,
   and `db.Ping` can echo the DSN; dmt scrubs them before they leave
   the driver package.
2. **Setup-wizard connection probe** (`internal/setup/conntest.go`).
   The `dmt setup` wizard's "test connection" feedback is scrubbed so
   a screenshot pasted into a support ticket doesn't leak the
   password.
3. **Orchestrator health-check** (`internal/orchestrator/healthcheck.go`).
   `dmt preflight` / `dmt health-check` rendering of `source_error` /
   `target_error` in the JSON output is scrubbed.
4. **Slack notifier** (`internal/notify/slack.go`). A failed Slack
   `Post` returns an error whose `.Error()` includes the full webhook
   URL; the path portion is the credential. dmt scrubs it.

## What dmt does NOT log

- **Row content.** No INSERT failures, scan errors, or constraint
  violations are logged with the offending row values. The driver
  libraries `dmt` builds on (pgx, go-mssqldb, go-sql-driver/mysql) do
  not include row values in their standard error messages; for
  long-tail driver errors that *do* include row content, dmt's error
  diagnosis path applies a structural prefix-and-hash (see
  `internal/driver/errordiag_dispatch.go::errorFingerprint`) so the
  catalog-growth signal stays alive without preserving the message
  text. This means an unmatched error logs:

  ```
  error diagnosis: no deterministic pattern for driver="postgres" prefix="duplicate key value violates unique constraint" hash=8e2c1a5b
  ```

  …not the constraint name, not the row values, not the DETAIL clause.

- **AI prompts with sample row data.** The AI type-mapper
  (`internal/driver/ai_typemapper.go`) was refactored so its
  `buildPrompt` function operates **only on DDL metadata** — the
  data type, max length, precision, scale, and the target dialect.
  Sample values that an earlier iteration carried in `TypeInfo.SampleValues`
  are intentionally not included, even when callers populate that
  field. Test: `TestAITypeMapper_BuildPromptExcludesSampleValues`.
  The smartconfig analyzer (`internal/driver/ai_smartconfig.go`)
  similarly operates only on aggregate row-size statistics, never on
  row content. AI-driven error diagnosis was removed entirely in #173
  to close the long-tail PII-egress vector.

## How an operator verifies the posture

1. Run a migration with `--log-format=json` and `--log-level=debug`,
   redirecting stdout to a file.
2. Grep the file for the canonical sentinel passwords from your
   config:

   ```
   grep "$(yq '.source.password' config.yaml)" debug.log
   grep "$(yq '.target.password' config.yaml)" debug.log
   ```

   Both should return no matches.
3. For a failed-connection scenario, use a deliberately-wrong host or
   port and rerun `dmt preflight --output-json`. Inspect
   `source_error` / `target_error` in the JSON — they should contain
   `[REDACTED]` where the password would otherwise appear.

The CI suite enforces this property programmatically:

- `internal/logging/scrub_test.go` covers the patterns themselves.
- `internal/driver/{postgres,mssql,mysql}/scrub_test.go` exercises
  each driver's `NewReader` / `NewWriter` with a sentinel password
  against an unroutable host and asserts the password does not
  appear in the surfaced error.
- `internal/setup/conntest_test.go` mirrors the same assertion for
  the setup-wizard's `TestConnection` helper.

## Known gaps

- **Driver-library log output** (`pgx.PoolConfig.ConnConfig.Tracer`,
  the MSSQL driver's internal logger, etc.) can emit messages outside
  dmt's scrubbing surface if explicitly enabled. dmt does not enable
  driver-side tracing by default; operators who turn it on must
  separately scrub those logs.
- **Operating-system memory dumps** are not in scope. If dmt
  segfaults and the host's `ulimit -c` permits a core dump, the dump
  will contain in-memory password buffers.
- **Profile encryption** (`internal/checkpoint/profiles.go`) relies
  on `DMT_MASTER_KEY` set in the operator's environment. The same
  thread-model caveat applies: if the operator's environment is
  compromised, encrypted profiles are recoverable.

## Reporting a sensitive-value leak

If you find a log line, error message, or error-diagnosis output
that includes a password, API key, webhook URL, or row content,
please open an issue tagged `security` against
[johndauphine/dmt](https://github.com/johndauphine/dmt/issues). The
scrubbing patterns live in `internal/logging/scrub.go` and accept
new rules under the same audit policy described in
[#231](https://github.com/johndauphine/dmt/issues/231).
