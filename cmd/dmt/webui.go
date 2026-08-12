package main

import (
	"github.com/johndauphine/dmt/v5/internal/webui"

	"github.com/urfave/cli/v2"
)

// startWebUI launches the browser front-end (#578). It mirrors startTUI: the
// no-args Action routes here when --webui or --gui is set. Global origin
// flags are threaded through so later WebUI issues can resolve
// configs/profiles the way the CLI and TUI do.
//
// --gui layers desktop behavior on top of plain --webui (internal/desktop):
// it opens a browser at the server once listening, coordinates a single
// running instance so a second launch hands off instead of failing to bind
// the same port, and exits shortly after the last browser window closes
// (never while a migration is in flight). --app-window additionally requests
// a chromeless app-style window and only has an effect together with --gui.
func startWebUI(c *cli.Context) error {
	opts := webui.Options{
		Addr:           c.String("webui-addr"),
		AuthToken:      c.String("webui-auth-token"),
		TLSCert:        c.String("webui-tls-cert"),
		TLSKey:         c.String("webui-tls-key"),
		Insecure:       c.Bool("webui-insecure"),
		TrustedProxies: c.StringSlice("webui-trusted-proxy"),
		ConfigPath:     c.String("config"),
		StateFile:      c.String("state-file"),
		Profile:        c.String("profile"),
	}
	if c.Bool("gui") {
		opts.OpenBrowser = true
		opts.AppWindow = c.Bool("app-window")
		opts.SingleInstance = true
		opts.ExitWhenIdle = true
	}
	return webui.Start(opts)
}
