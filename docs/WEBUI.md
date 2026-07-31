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

## Desktop GUI (`--gui`)

`dmt --gui` turns the WebUI into a desktop app, with no native shell and no new
runtime dependency — dmt stays a single pure-Go, `CGO_ENABLED=0` binary, cross-
compiled for Windows/macOS/Linux from one machine
(`make build-all`). "Desktop" here means: dmt opens your browser at itself and
makes that window behave like an installed app, rather than embedding a
WebView.

```bash
dmt --gui                 # opens your default browser, signed in automatically
dmt --gui --app-window    # opens a chromeless window (Chrome/Edge/Brave)
```

What `--gui` adds on top of `--webui`:

- **Auto-open.** A browser opens at the server's URL once it starts listening
  — the one-click loopback link, opened for you instead of printed for you to
  click. Only valid on a loopback bind: `--gui` with a non-loopback
  `--webui-addr` is a startup error, since auto-opening a browser at a
  token-bearing URL would hand that token to whatever local process the
  launcher invokes.
- **`--app-window`.** Requests a chromeless, address-bar-less window on
  Chrome/Edge/Brave. On macOS with none of those installed, it opens a normal
  Safari window instead — Safari has no command-line app mode, but **File →
  Add to Dock** (macOS 14+/Safari 17+) is the equivalent: it installs dmt as a
  standalone, Dock-resident window. Chromium-family browsers also offer an
  "Install" affordance from the address bar (the page is a PWA — see below).
- **Single instance.** A second `dmt --gui` launch detects the first one (a
  lock file under `~/.dmt/gui.lock`) and opens a window at its URL instead of
  failing to bind the same port.
- **Idle exit.** dmt exits automatically a few seconds after the last browser
  window closes — but never while a migration is running; an active run
  always keeps the process alive until it finishes (or Ctrl+C). Plain
  `--webui` never does this — a server deployment can't be stopped by someone
  closing a browser tab.
- **Completion notifications.** If the window is backgrounded when a migration
  finishes, a desktop notification fires (after a one-time permission prompt
  triggered by starting a run).

### Limitations

- The installed PWA is pinned to whatever address it was installed from
  (`127.0.0.1:8484` by default). If that port is taken, `--gui` won't
  automatically re-target an already-installed app to a different port.
- Notifications and "Install app" both require a secure context: `localhost`/
  `127.0.0.1` qualify automatically (no TLS setup needed), but they will not
  work over a plaintext non-loopback bind (`--webui-insecure`) — expected,
  since `--gui` doesn't support non-loopback binds regardless.
- Safari's Add to Dock needs macOS 14+/Safari 17+; older Safari runs the app
  as an ordinary tab. Safari also has no `beforeinstallprompt` event, so any
  in-app "Install" button only appears on Chromium-family browsers — Safari
  users install via File → Add to Dock instead.

## Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--webui` | off | Launch the WebUI instead of the TUI. |
| `--gui` | off | Launch the WebUI as a desktop app: auto-open, single-instance handoff, idle-exit (see above). Implies `--webui`; loopback bind only. |
| `--app-window` | off | With `--gui`, open a chromeless app-style window instead of a browser tab. |
| `--webui-addr` | `127.0.0.1:8484` | Bind address. A non-loopback host enables remote access (with the requirements below). |
| `--webui-auth-token` | — | Shared-secret bearer token. Supports `${env:VAR}` / `${file:/path}` expansion so the secret stays out of the process list. **Required** for a non-loopback bind; auto-generated for loopback. |
| `--webui-tls-cert` / `--webui-tls-key` | — | Serve HTTPS directly (PEM cert + key). |
| `--webui-insecure` | off | Allow a non-loopback bind over plaintext HTTP — only behind a TLS-terminating reverse proxy. |
| `--webui-trusted-proxy` | — | CIDR (or IP) of a trusted reverse proxy whose `X-Forwarded-For` is honored for rate-limiting and audit logging. Repeatable. Off by default. |

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
  characters. By default the limiter keys on the connecting IP and does not
  trust `X-Forwarded-For` (it is client-spoofable), so **behind a reverse proxy
  all clients share one bucket** unless you opt in. Set
  `--webui-trusted-proxy <cidr>` (repeatable) to name your proxy: when the
  direct peer is in that set, the real client is taken from `X-Forwarded-For`
  (walking past further trusted hops), restoring per-client throttling and
  accurate audit logging without opening spoofing — an attacker connecting
  directly is still keyed on their real peer IP.
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
    --webui-insecure \             # TLS is terminated by the proxy in front
    --webui-trusted-proxy 127.0.0.1  # trust the local proxy's X-Forwarded-For
```

The `--webui-trusted-proxy` value is the address dmt sees the proxy connecting
*from* (here the loopback proxy). Without it, the login limiter can only see the
proxy's IP, so every user shares one rate-limit bucket; with it, throttling and
audit logs attribute to the real client. Make sure the proxy sets
`X-Forwarded-For` (nginx's `proxy_add_x_forwarded_for` below does).

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
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;  # real client IP
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
