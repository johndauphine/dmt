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
- **Remote / team-facing server — supported.** The hardening that gated this is
  in place: brute-force throttling on both the login and bearer paths with a
  minimum token length, sliding sessions with a 7-day absolute cap, a
  loopback-only metrics listener, a CSP with no inline script, opt-in
  trusted-proxy client attribution, and soak coverage for the long-lived
  server. Run it **behind a TLS-terminating reverse proxy and a firewall** (see
  Server deployment below), and treat the token as you would a root password.

**By design, v1 is single-operator with a single shared secret.** There are no
user accounts, RBAC, per-user audit, or SSO — those are non-goals for v1.
Everyone holding the token is the same operator, with full control of the
migration; scope access at the proxy and firewall, not inside dmt.

Known gaps: the console is verified in Chromium and WebKit but not yet in Gecko
(Firefox). Accessibility is audited to WCAG 2.1 AA and verified in Chrome (see
Accessibility below), but has not been driven with a real screen reader.

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

## Accessibility

The console targets WCAG 2.1 AA (#598).

- **Keyboard.** Everything is reachable and operable without a mouse. A skip
  link jumps past the sidebar to the view; the command palette (`⌘K`/`Ctrl-K`)
  and the config/profile picker are modal dialogs that trap Tab, close on
  Escape, and return focus to whatever opened them.
- **Screen readers.** The primary nav is a named landmark and marks the active
  view with `aria-current`. Migration progress is published two ways: the bar
  is a `progressbar` with a live `aria-valuenow`/`aria-valuetext`, and a polite
  live region speaks phase and percentage. That region is throttled — it speaks
  on a phase change, and otherwise at most once every 15 seconds — because SSE
  progress arrives several times a second and an unthrottled region makes the
  page unusable. Toasts are a separate polite region, so a finished migration
  is announced exactly once.
- **Color.** Every foreground/background pairing in both themes clears AA
  (4.5:1 for text, 3:1 for control boundaries and the focus ring). The ratios
  are asserted in `internal/webui/contrast_test.go`, so a palette edit that
  regresses one fails the build with the measured number. Status is never
  carried by color alone — badges pair the color with their text.
- **Motion.** `prefers-reduced-motion: reduce` drops the spinner, pulse, and
  slide-in animations.

Verified in Chrome 151 by driving the running console over the DevTools
Protocol: the computed accessibility tree (every interactive control across all
six views resolves to a non-empty accessible name), and real keyboard
interaction — Tab wraps inside both modals, Escape closes them, focus returns
to the opener, and `aria-activedescendant` tracks the palette selection. The
progress throttle and the announce-once behavior were exercised against
synthetic run states. `internal/webui/a11y_test.go` and
`internal/webui/contrast_test.go` pin the same properties in CI, where no
browser is available.

Not yet driven with an actual screen reader (VoiceOver/NVDA/JAWS) — the
accessibility tree is verified, but how a given reader voices it is not. Gecko
remains unverified for accessibility as it is for rendering.

### Verifying in a real browser

The Go tests pin structure (roles, labels, contrast ratios) but cannot tell you
whether focus is *actually* trapped or what the browser *actually* computes for
the accessibility tree. That needs a real browser driven over the Chrome
DevTools Protocol. There is no committed harness — CI has no browser, and a
one-off script is not worth a dependency — so this is the method, not a tool.

Start the console with a known token, and Chrome headless with a debug port:

```bash
dmt --webui --webui-addr 127.0.0.1:8484 --webui-auth-token "$(openssl rand -hex 24)"
```

```bash
chrome --headless --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/dmt-verify about:blank
```

`GET http://localhost:9222/json` lists targets; connect a WebSocket to the page
target's `webSocketDebuggerUrl` and send CDP commands. Then `Page.navigate` to
`http://localhost:8484/?token=…` — the one-click URL logs the SPA in, so the app
shell mounts without scripting the login form.

What is worth asking for, once attached:

- `Accessibility.getFullAXTree` — assert every node whose role is interactive
  (`button`, `link`, `textbox`, `combobox`, `checkbox`, `option`) has a
  non-empty name. Sweep all six views by evaluating `go('<view>')` between
  passes. This is the check that finds unlabeled controls.
- `Input.dispatchKeyEvent` — Tab repeatedly with a modal open and assert
  `document.activeElement` stays inside it; Escape and assert focus returns to
  the opener; ArrowDown in the palette and assert `aria-activedescendant` moves.
- `Runtime.evaluate` against `applyProgress()` / `applyRunState()` with
  synthetic payloads. Real progress needs two live databases, but the logic
  being checked — the live-region throttle, and announcing a finished run
  exactly once — is entirely client-side.
- `Page.captureScreenshot` in both themes after setting
  `document.documentElement.dataset.theme`, to eyeball a palette change.

Three traps cost real time here, so they are worth knowing up front:

- **`--dump-dom` returns the pre-boot DOM.** The SPA mounts after an async
  health check and login round-trip, so a plain dump shows only the loading
  shell. Drive the page over CDP and wait, rather than trusting a one-shot dump.
- **`--virtual-time-budget` never completes.** The console holds an SSE stream
  (`/api/events`) open for the life of the page, so virtual time never advances
  to idle and Chrome hangs instead of dumping. Always wrap browser invocations
  in a `timeout`.
- **`/json/new` requires `PUT`** on Chrome 111+; a `GET` returns 405. Reusing
  the existing `about:blank` page target and calling `Page.navigate` avoids the
  endpoint entirely.
- **The service worker will serve you a stale `app.js`.** Its cache name keys on
  the dmt version (see `sw.js`), so rebuilding the binary without bumping the
  version leaves a previously-registered worker happily serving the old asset —
  you will verify a fix that the page never loaded. Call
  `Network.setCacheDisabled` and `ServiceWorker.setForceUpdateOnPageLoad` before
  navigating, and assert the page actually has your change (evaluate
  `someFunction.toString().includes(…)`) before trusting a result.
- **Kill stale Chrome instances first.** A second `--remote-debugging-port=9222`
  launch cannot bind the port while the first is alive, so CDP silently attaches
  to the *old* browser — with the old profile and the old service worker.
  Confirm the kill took rather than assuming it did.

Under WSL, Chrome installed on the Windows side works: launch the `.exe` and
both `localhost:9222` (the debug port) and `localhost:8484` (the console)
resolve across the boundary. `--remote-debugging-pipe` does *not* — the file
descriptors do not cross into Windows — so the WebSocket transport is required
there.

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
