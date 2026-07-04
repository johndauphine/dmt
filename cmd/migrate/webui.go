package main

import (
	"github.com/johndauphine/dmt/internal/webui"

	"github.com/urfave/cli/v2"
)

// startWebUI launches the browser front-end (#578). It mirrors startTUI: the
// no-args Action routes here when --webui is set. Global origin flags are
// threaded through so later WebUI issues can resolve configs/profiles the way
// the CLI and TUI do.
func startWebUI(c *cli.Context) error {
	return webui.Start(webui.Options{
		Addr:       c.String("webui-addr"),
		AuthToken:  c.String("webui-auth-token"),
		TLSCert:    c.String("webui-tls-cert"),
		TLSKey:     c.String("webui-tls-key"),
		Insecure:   c.Bool("webui-insecure"),
		ConfigPath: c.String("config"),
		StateFile:  c.String("state-file"),
		Profile:    c.String("profile"),
	})
}
