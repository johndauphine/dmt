package main

import (
	"github.com/johndauphine/dmt/v5/internal/tui"

	"github.com/urfave/cli/v2"
)

func startTUI(c *cli.Context) error {
	return tui.Start()
}
