# WebUI

`dmt --webui` launches a browser-based operator console — the third front-end
alongside the CLI and TUI (epic #577). It replicates the TUI's command surface
as a modern single-page app: run/resume with live progress, preflight and
validation checks, the guided setup wizard, profiles, and session defaults.

The front end is embedded in the binary (`go:embed`), so there is nothing to
install or serve separately — `dmt` stays a single self-contained binary.

## Maturity

- **Local, single-operator use — ready.** On a loopback bind (the default) the
  WebUI is a peer to the TUI, verified end-to-end. Use it freely.
- **Remote / team-facing server — beta.** The remote path works and is gated by
  a token + TLS, but v1 is **single-operator with a single shared secret**:
  there are no user accounts, RBAC, per-user audit, or login rate-limiting yet.
  For a shared server, run it **behind a TLS-terminating reverse proxy and a
  firewall**, and treat the token as you would a root password.

Multi-user accounts, RBAC, and SSO are non-goals for v1. Remaining hardening for
the server case (notably login rate-limiting) is tracked in issue #599.

## Launch

```bash
dmt --webui                      # loopback, auto-generated token, prints a one-click URL
dmt --webui --webui-addr :8484   # bind all interfaces (requires auth + TLS/insecure — see below)
```

With no other command, `dmt --webui` starts the server and prints a URL. On a
loopback bind the URL carries the auto-generated token so the link signs you in
with a single click; the front end scrubs it from the address bar immediately.
(The token is the shared secret — it stays valid for the server's lifetime, not
single-use — so treat the printed link as sensitive.)

## Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--webui` | off | Launch the WebUI instead of the TUI. |
| `--webui-addr` | `127.0.0.1:8484` | Bind address. A non-loopback host enables remote access (with the requirements below). |
| `--webui-auth-token` | — | Shared-secret bearer token. Supports `${env:VAR}` / `${file:/path}` expansion so the secret stays out of the process list. **Required** for a non-loopback bind; auto-generated for loopback. |
| `--webui-tls-cert` / `--webui-tls-key` | — | Serve HTTPS directly (PEM cert + key). |
| `--webui-insecure` | off | Allow a non-loopback bind over plaintext HTTP — only behind a TLS-terminating reverse proxy. |

## Security model

- **Loopback by default.** `127.0.0.1` with an auto-generated token; nothing is
  reachable from the network.
- **Remote binds are gated.** Binding a non-loopback address is refused at
  startup unless a `--webui-auth-token` is set **and** either TLS
  (`--webui-tls-cert`/`--webui-tls-key`) or `--webui-insecure` is provided.
- **Auth.** The token is exchanged at login for an `HttpOnly`, `SameSite=Strict`
  session cookie (cookie-based so the `EventSource` progress stream works, since
  it cannot send headers). Token comparison is constant-time. API clients may
  instead send `Authorization: Bearer <token>`. The session slides while in use
  (an open tab pings the server so it never expires mid-migration) up to a
  7-day absolute cap.
- **Brute-force throttling.** Repeated failed auth attempts (login or bearer)
  from an IP are rate-limited (lockout after ~10 failures/minute). A
  non-loopback bind additionally refuses an operator token shorter than 16
  characters. Note: the limiter keys on the connecting IP and does not trust
  `X-Forwarded-For`, so **behind a reverse proxy all clients share one bucket**
  — an attacker could cause a brief (1-minute, self-healing) login lockout for
  everyone; existing sessions keep working. A per-client trusted-proxy mode is
  tracked in issue #604.
- **DNS-rebinding protection.** On a loopback bind the server enforces a
  `Host`-header allowlist, blocking a malicious page from pointing its own
  hostname at `127.0.0.1`.
- **No shell escape.** The TUI's `!<command>` escape is deliberately absent.
- **Secrets never reach the browser.** Profile blobs, config passwords, and the
  like are never serialized to API responses; error messages are scrubbed;
  profile export writes to a server-owned `~/.dmt/exports/` directory (never a
  client-supplied path).
- **Setup keeps passwords out of the config.** The setup wizard defaults to
  writing DB passwords to `0600` sidecar files referenced by `${file:…}`, so a
  password you type into the browser lands in a locked-down file, not in the
  config YAML. A wizard step lets you opt back into plaintext if you prefer.

## Server deployment

For a shared server, run dmt behind a TLS-terminating reverse proxy and keep
the token out of the process list with `${env:}` or `${file:}`:

```bash
export DMT_WEBUI_TOKEN=…                 # or store it in a 0600 file
dmt --webui \
    --webui-addr 127.0.0.1:8484 \
    --webui-auth-token "${env:DMT_WEBUI_TOKEN}" \
    --webui-insecure          # TLS is terminated by the proxy in front
```

### nginx

```nginx
server {
  listen 443 ssl;
  server_name dmt.example.com;
  ssl_certificate     /etc/ssl/dmt.crt;
  ssl_certificate_key /etc/ssl/dmt.key;
  location / {
    proxy_pass http://127.0.0.1:8484;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto https;   # marks the cookie Secure
    proxy_http_version 1.1;
    proxy_set_header Connection "";              # keep the SSE stream open
    proxy_buffering off;                         # don't buffer /api/events
  }
}
```

### Caddy

```caddy
dmt.example.com {
  reverse_proxy 127.0.0.1:8484 {
    header_up X-Forwarded-Proto https
    flush_interval -1        # stream SSE without buffering
  }
}
```

Or serve TLS directly (no proxy):

```bash
dmt --webui --webui-addr 0.0.0.0:8484 \
    --webui-auth-token "${file:/run/secrets/dmt-token}" \
    --webui-tls-cert /etc/ssl/dmt.crt --webui-tls-key /etc/ssl/dmt.key
```

Restrict access to the port with your firewall regardless — the token is the
only application-level barrier (v1 is single-operator; there are no user
accounts or RBAC).

On shutdown (Ctrl+C / SIGTERM) the server cancels any in-flight migration and
waits up to ~10 seconds for its checkpoint to flush before exiting. If the
flush is cut short it is the same exposure as killing the CLI mid-run: state
stays consistent (SQLite WAL) and the run is resumable — keyset tables clean up
partial data on resume; ROW_NUMBER tables may re-transfer their last chunk.

## Command surface

The WebUI operates the same production commands as the TUI. The authoritative
disposition lives in `internal/command` (`WebSurface`); `docs/TUI_COMMANDS.md`
carries the human-readable CLI ↔ TUI ↔ WebUI table. A parity test
(`internal/webui/parity_surface_test.go`) enforces that every `WebSupported`
command is wired to a live route, so the surface can't silently drift.

| View | Commands / endpoints |
|------|----------------------|
| Dashboard | `run`, `resume`, cancel, live progress (`/api/run`, `/api/resume`, `/api/run/cancel`, `/api/events`) |
| History | `status`, `history` (`/api/status`, `/api/history`) |
| Checks | `preflight`, config-check (dry run), `validate`, `diagnose`, `analyze`, `ai config-review` |
| Setup | guided `setup` wizard (`/api/setup/*`) |
| Profiles | `profile save`/`list`/`delete`/`export` (`/api/profiles*`) |
| Settings | session defaults, `cache clear`, `init-secrets` |
| ⌘K palette | every command by name |

Deliberately CLI-only in the WebUI (`WebNA`): `init` (the guided setup covers
it) and `ai evals` (a developer/eval harness).
